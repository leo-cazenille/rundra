from __future__ import annotations

import json
import re
import shlex
from dataclasses import replace
from math import ceil
from pathlib import PurePath

from rundra.adapters._remote_shell import serialize_remote_command
from rundra.adapters.slurm import (
    render_slurm_bundle_manifest,
    render_slurm_compact_bundle_manifest,
)
from rundra.domain.models import Command, NativeValue, ResourceRequest
from rundra.domain.states import ExecutionState
from rundra.ports import (
    CommandResult,
    CompactSchedulerArrayRequest,
    CompactSchedulerSubmission,
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmission,
    SchedulerSubmissionFailure,
    SchedulerSubmissionOutcome,
    Transport,
)

_MIB = 1024 * 1024
_SAFE_VALUE = re.compile(r"[A-Za-z0-9._+,:/@=-]+\Z")
_JOB_ID = re.compile(
    r"(?P<number>[0-9]+)(?P<array>\[\])?(?P<server>\.[A-Za-z0-9._-]+)?\Z"
)
_REFERENCE = re.compile(r"[0-9]+(?:\[[0-9]*\])?(?:\.[A-Za-z0-9._-]+)?\Z")
_NATIVE_OPTIONS = frozenset({"account", "place", "priority", "project", "queue"})
_SUBMIT_SCRIPT = """\
set -eu
script=$(mktemp "${TMPDIR:-/tmp}/rundra-pbs.XXXXXX")
trap 'rm -f "$script"' EXIT HUP INT TERM
printf '%s' "$1" > "$script"
chmod 500 "$script"
if [ "$3" = - ]; then
    "$2" "$script"
else
    "$2" -W "depend=afterok:$3" "$script"
fi
"""
_PERSIST_AND_SUBMIT_SCRIPT = """\
set -eu
path=$1
payload=$2
qsub=$3
dependency=$4
directory=${path%/*}
mkdir -p -- "$directory"
[ ! -e "$path" ] || exit 73
tmp="${path}.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM
printf '%s' "$payload" > "$tmp"
chmod 500 "$tmp"
mv -- "$tmp" "$path"
trap - EXIT HUP INT TERM
if [ "$dependency" = - ]; then
    "$qsub" "$path"
else
    "$qsub" -W "depend=afterok:$dependency" "$path"
fi
"""
_PERSIST_COMPACT_AND_SUBMIT_SCRIPT = """\
set -eu
manifest=$1
manifest_payload=$2
script_payload=$3
qsub=$4
dependency=$5
directory=${manifest%/*}
mkdir -p -- "$directory"
[ ! -e "$manifest" ] || exit 73
manifest_tmp="${manifest}.$$"
script=$(mktemp "${TMPDIR:-/tmp}/rundra-pbs.XXXXXX")
trap 'rm -f "$manifest_tmp" "$script"' EXIT HUP INT TERM
printf '%s' "$manifest_payload" > "$manifest_tmp"
chmod 500 "$manifest_tmp"
mv -- "$manifest_tmp" "$manifest"
printf '%s' "$script_payload" > "$script"
chmod 500 "$script"
if [ "$dependency" = - ]; then
    "$qsub" "$script"
else
    "$qsub" -W "depend=afterok:$dependency" "$script"
fi
"""


class PBSSchedulerError(RuntimeError):
    """Base error for the OpenPBS adapter."""


class PBSSubmissionError(PBSSchedulerError, SchedulerSubmissionFailure):
    def __init__(
        self,
        message: str,
        *,
        outcome: SchedulerSubmissionOutcome = SchedulerSubmissionOutcome.UNCERTAIN,
        phase: str = "scheduler_submit",
        exit_code: int | None = None,
    ) -> None:
        SchedulerSubmissionFailure.__init__(
            self,
            message,
            backend="pbs",
            phase=phase,
            outcome=outcome,
            exit_code=exit_code,
        )


class PBSQueryError(PBSSchedulerError):
    pass


class PBSCancellationError(PBSSchedulerError):
    pass


class PBSScriptError(PBSSchedulerError):
    pass


class OpenPBSScheduler:
    """Submit portable scheduler requests through OpenPBS client commands."""

    def __init__(
        self,
        transport: Transport,
        *,
        qsub: str = "qsub",
        qstat: str = "qstat",
        qdel: str = "qdel",
        log_directory: PurePath | None = None,
    ) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("OpenPBSScheduler transport must implement Transport")
        for name, executable in (("qsub", qsub), ("qstat", qstat), ("qdel", qdel)):
            if (
                type(executable) is not str
                or not executable.strip()
                or "\x00" in executable
            ):
                raise ValueError(f"OpenPBSScheduler {name} must be nonblank and safe")
        if log_directory is not None and (
            not isinstance(log_directory, PurePath)
            or not log_directory.is_absolute()
            or "\x00" in str(log_directory)
        ):
            raise ValueError("OpenPBSScheduler log_directory must be absolute and safe")
        self._transport = transport
        self._qsub = qsub
        self._qstat = qstat
        self._qdel = qdel
        self._log_directory = log_directory

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        return self._submit(group, dependency=None)

    def submit_afterok(
        self, group: SchedulerGroup, dependency: SchedulerReference
    ) -> SchedulerSubmission:
        return self._submit(group, dependency=_dependency(dependency))

    def _submit(
        self, group: SchedulerGroup, *, dependency: str | None
    ) -> SchedulerSubmission:
        if type(group) is not SchedulerGroup or len(group.units) != 1:
            raise PBSSubmissionError(
                "OpenPBS single submission requires one Task",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        script = render_qsub_script(group, log_directory=self._log_directory)
        command = Command(
            (
                "/bin/sh",
                "-c",
                _SUBMIT_SCRIPT,
                "rundra-pbs-submit",
                script,
                self._qsub,
                dependency or "-",
            )
        )
        result = self._run(command, PBSSubmissionError, "qsub submission")
        reference = SchedulerReference(_submitted_id(result))
        return SchedulerSubmission(
            reference, {group.units[0].task_id: reference.native_id}
        )

    def submit_array(self, request: SchedulerArrayRequest) -> SchedulerSubmission:
        return self._submit_array(request, dependency=None)

    def submit_array_afterok(
        self, request: SchedulerArrayRequest, dependency: SchedulerReference
    ) -> SchedulerSubmission:
        return self._submit_array(request, dependency=_dependency(dependency))

    def _submit_array(
        self, request: SchedulerArrayRequest, *, dependency: str | None
    ) -> SchedulerSubmission:
        if type(request) is not SchedulerArrayRequest:
            raise TypeError(
                "OpenPBSScheduler.submit_array requires SchedulerArrayRequest"
            )
        worker_limits = tuple(
            limit
            for limit in (request.max_concurrent_jobs, request.max_workers)
            if limit is not None
        )
        if request.task_slots_per_worker > 1 or (
            worker_limits and len(request.mapping) > min(worker_limits)
        ):
            return self._submit_bundled_array(
                request, dependency=dependency, worker_limits=worker_limits
            )
        script = render_qsub_array_script(request, log_directory=self._log_directory)
        command = Command(
            (
                "/bin/sh",
                "-c",
                _PERSIST_AND_SUBMIT_SCRIPT,
                "rundra-pbs-array-submit",
                str(request.manifest_path),
                script,
                self._qsub,
                dependency or "-",
            )
        )
        result = self._run(command, PBSSubmissionError, "qsub array submission")
        submitted = _submitted_id(result)
        match = _JOB_ID.fullmatch(submitted)
        assert match is not None
        root = match.group("number")
        server = match.group("server") or ""
        reference = SchedulerReference(submitted)
        return SchedulerSubmission(
            reference,
            {
                item.task_id: f"{root}[{item.array_index}]{server}"
                for item in request.mapping
            },
        )

    def _submit_bundled_array(
        self,
        request: SchedulerArrayRequest,
        *,
        dependency: str | None,
        worker_limits: tuple[int, ...],
    ) -> SchedulerSubmission:
        if self._log_directory is None:
            raise PBSSubmissionError(
                "OpenPBS bundle submission requires a configured log directory",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        if not worker_limits:
            raise PBSSubmissionError(
                "OpenPBS bundle submission requires an explicit worker limit",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        task_slots = min(request.task_slots_per_worker, len(request.mapping))
        worker_count = min(
            *worker_limits,
            (len(request.mapping) + task_slots - 1) // task_slots,
        )
        max_assignment = (len(request.mapping) + worker_count * task_slots - 1) // (
            worker_count * task_slots
        )
        resources = request.group.units[0].resources
        if any(unit.resources != resources for unit in request.group.units[1:]):
            raise PBSScriptError("OpenPBS bundled Tasks must use uniform resources")
        if task_slots > 1 and (
            resources.nodes != 1 or resources.tasks != 1 or resources.gpus_per_task != 0
        ):
            raise PBSScriptError(
                "Concurrent OpenPBS workers require one-node, one-task, CPU-only "
                "logical resources"
            )
        derived_worker_resources = replace(
            resources,
            nodes=1,
            tasks=task_slots,
            gpus_per_task=0,
            memory_bytes=(
                resources.memory_bytes * task_slots
                if resources.memory_bytes is not None
                else None
            ),
            walltime=(
                resources.walltime * max_assignment
                if resources.walltime is not None
                else None
            ),
        )
        if (
            request.worker_resources is not None
            and request.worker_resources != derived_worker_resources
        ):
            raise PBSScriptError(
                "Planned worker resources do not match logical Task resources"
            )
        worker_resources = request.worker_resources or derived_worker_resources
        manifest_path = request.manifest_path.with_name(
            f"{request.manifest_path.stem}.workers{request.manifest_path.suffix}"
        )
        status_root = request.manifest_path.parent / "bundle-status"
        manifest_payload = render_slurm_bundle_manifest(
            request,
            worker_count=worker_count,
            task_slots_per_worker=task_slots,
            status_root=status_root,
        )
        quoted_manifest = shlex.quote(str(manifest_path))
        quoted_status = shlex.quote(str(status_root))
        lane_launches = tuple(
            line
            for lane in range(task_slots)
            for line in (
                "(",
                f"  export SLURM_PROCID={lane}",
                f'  /bin/sh {quoted_manifest} "$PBS_ARRAY_INDEX"',
                ") &",
                'pids="$pids $!"',
            )
        )
        merge_lines = tuple(
            line
            for lane in range(task_slots)
            for line in (
                (
                    f'lane="$status_root/${{SLURM_ARRAY_JOB_ID}}_'
                    f'${{SLURM_ARRAY_TASK_ID}}.lane-{lane}.tsv"'
                ),
                '[ -f "$lane" ] || exit 74',
                'cat -- "$lane" >> "$journal_tmp"',
            )
        )
        body = (
            "index=${PBS_ARRAY_INDEX:?missing PBS_ARRAY_INDEX}",
            "export SLURM_ARRAY_JOB_ID=$PBS_JOBID",
            "export SLURM_ARRAY_TASK_ID=$index",
            "export SLURM_RESTART_COUNT=0",
            f"status_root={quoted_status}",
            'mkdir -p -- "$status_root"',
            'journal="$status_root/${PBS_JOBID}.tsv"',
            'journal_tmp="${journal}.$$"',
            '[ ! -e "$journal" ] || exit 73',
            ': > "$journal_tmp"',
            "trap 'rm -f \"$journal_tmp\"' EXIT HUP INT TERM",
            'pids=""',
            *lane_launches,
            'status=0; for pid in $pids; do wait "$pid" || status=$?; done',
            '[ "$status" -eq 0 ] || exit "$status"',
            *merge_lines,
            'chmod 400 "$journal_tmp"',
            'mv -- "$journal_tmp" "$journal"',
            "trap - EXIT HUP INT TERM",
        )
        script = _script(
            _directives(
                worker_resources,
                job_name="rundra-worker",
                array_stop=worker_count - 1,
                array_limit=worker_count,
            ),
            body,
            self._log_directory,
        )
        command = Command(
            (
                "/bin/sh",
                "-c",
                _PERSIST_COMPACT_AND_SUBMIT_SCRIPT,
                "rundra-pbs-worker-submit",
                str(manifest_path),
                manifest_payload,
                script,
                self._qsub,
                dependency or "-",
            )
        )
        result = self._run(command, PBSSubmissionError, "qsub worker submission")
        submitted = _submitted_id(result)
        match = _JOB_ID.fullmatch(submitted)
        assert match is not None
        root = match.group("number")
        server = match.group("server") or ""
        reference = SchedulerReference(submitted)
        return SchedulerSubmission(
            reference,
            {
                item.task_id: f"{root}[{ordinal % worker_count}]{server}"
                for ordinal, item in enumerate(request.mapping)
            },
        )

    def submit_compact_array(
        self, request: CompactSchedulerArrayRequest
    ) -> CompactSchedulerSubmission:
        """Submit one constant-size OpenPBS worker array."""
        return self._submit_compact_array(request, dependency=None)

    def submit_compact_array_afterok(
        self,
        request: CompactSchedulerArrayRequest,
        dependency: SchedulerReference,
    ) -> CompactSchedulerSubmission:
        """Submit compact workers after a framework-owned dependency."""
        return self._submit_compact_array(request, dependency=_dependency(dependency))

    def _submit_compact_array(
        self,
        request: CompactSchedulerArrayRequest,
        *,
        dependency: str | None,
    ) -> CompactSchedulerSubmission:
        if type(request) is not CompactSchedulerArrayRequest:
            raise TypeError("OpenPBS compact submission requires a compact request")
        if self._log_directory is None:
            raise PBSSubmissionError(
                "OpenPBS compact submission requires a configured log directory",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        if request.requeue_limit != 0:
            raise PBSSubmissionError(
                "OpenPBS compact workers require requeue_limit 0",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        manifest_path = request.manifest_path.with_name(
            f"{request.manifest_path.stem}.compact-workers{request.manifest_path.suffix}"
        )
        status_root = request.manifest_path.parent / "bundle-status"
        manifest = render_slurm_compact_bundle_manifest(
            request, status_root=status_root
        )
        quoted_manifest = shlex.quote(str(manifest_path))
        quoted_status = shlex.quote(str(status_root))
        lane_launches = tuple(
            line
            for lane in range(request.task_slots_per_worker)
            for line in (
                "(",
                f"  export SLURM_PROCID={lane}",
                f'  /bin/sh {quoted_manifest} "$PBS_ARRAY_INDEX"',
                ") &",
                'pids="$pids $!"',
            )
        )
        merge_lines = tuple(
            line
            for lane in range(request.task_slots_per_worker)
            for line in (
                (
                    f'lane="$status_root/${{SLURM_ARRAY_JOB_ID}}_'
                    f'${{SLURM_ARRAY_TASK_ID}}.lane-{lane}.attempt-0.tsv"'
                ),
                '[ -f "$lane" ] || exit 74',
                'cat -- "$lane" >> "$journal_tmp"',
            )
        )
        body = (
            "index=${PBS_ARRAY_INDEX:?missing PBS_ARRAY_INDEX}",
            "export SLURM_ARRAY_JOB_ID=$PBS_JOBID",
            "export SLURM_ARRAY_TASK_ID=$index",
            "export SLURM_RESTART_COUNT=0",
            f"status_root={quoted_status}",
            'mkdir -p -- "$status_root"',
            'journal="$status_root/${PBS_JOBID}.tsv"',
            'journal_tmp="${journal}.$$"',
            '[ ! -e "$journal" ] || exit 73',
            ': > "$journal_tmp"',
            "trap 'rm -f \"$journal_tmp\"' EXIT HUP INT TERM",
            'pids=""',
            *lane_launches,
            'status=0; for pid in $pids; do wait "$pid" || status=$?; done',
            '[ "$status" -eq 0 ] || exit "$status"',
            *merge_lines,
            'chmod 400 "$journal_tmp"',
            'mv -- "$journal_tmp" "$journal"',
            "trap - EXIT HUP INT TERM",
        )
        script = _script(
            _directives(
                request.worker_resources,
                job_name="rundra-worker",
                array_stop=request.worker_count - 1,
                array_limit=request.worker_count,
            ),
            body,
            self._log_directory,
        )
        command = Command(
            (
                "/bin/sh",
                "-c",
                _PERSIST_COMPACT_AND_SUBMIT_SCRIPT,
                "rundra-pbs-compact-submit",
                str(manifest_path),
                manifest,
                script,
                self._qsub,
                dependency or "-",
            )
        )
        result = self._run(
            command, PBSSubmissionError, "qsub compact worker submission"
        )
        submitted = _submitted_id(result)
        match = _JOB_ID.fullmatch(submitted)
        assert match is not None
        root = match.group("number")
        server = match.group("server") or ""
        return CompactSchedulerSubmission(
            SchedulerReference(submitted),
            request.task_space,
            tuple(f"{root}[{index}]{server}" for index in range(request.worker_count)),
        )

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        normalized = _references(references)
        if not normalized:
            return ()
        command = Command(
            (
                self._qstat,
                "-x",
                "-f",
                "-F",
                "json",
                *(item.native_id for item in normalized),
            )
        )
        result = self._run(command, PBSQueryError, "qstat query")
        try:
            document = json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError) as error:
            raise PBSQueryError("qstat returned invalid JSON") from error
        jobs = document.get("Jobs") if isinstance(document, dict) else None
        if not isinstance(jobs, dict):
            raise PBSQueryError("qstat JSON is missing Jobs")
        return tuple(
            self._observation(reference, jobs.get(reference.native_id))
            for reference in normalized
        )

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        normalized = _references(references)
        if not normalized:
            return ()
        command = Command((self._qdel, *(item.native_id for item in normalized)))
        result = self._run(
            command, PBSCancellationError, "qdel cancellation", allow_failure=True
        )
        if result.exit_code != 0:
            observations = self.query(normalized)
            if any(item.state not in _TERMINAL for item in observations):
                raise PBSCancellationError(
                    f"qdel failed with exit code {result.exit_code}; scheduler diagnostic redacted"
                )
            return observations
        return tuple(
            self._with_logs(
                SchedulerObservation(
                    reference, ExecutionState.CANCELLED, "DELETION_REQUESTED"
                )
            )
            for reference in normalized
        )

    def _observation(
        self, reference: SchedulerReference, value: object
    ) -> SchedulerObservation:
        if not isinstance(value, dict):
            return self._with_logs(
                SchedulerObservation(
                    reference,
                    ExecutionState.UNKNOWN,
                    "HISTORY_PENDING",
                    metadata={"accounting_pending": True},
                )
            )
        native_state = str(value.get("job_state", "UNKNOWN"))
        exit_value = value.get("Exit_status")
        exit_code = (
            int(exit_value)
            if isinstance(exit_value, (int, str))
            and str(exit_value).lstrip("-").isdigit()
            else None
        )
        comment = str(value.get("comment", ""))
        state = _execution_state(native_state, exit_code, comment)
        metadata: dict[str, NativeValue] = {}
        exec_host = value.get("exec_host")
        if isinstance(exec_host, str) and exec_host:
            metadata["node_list"] = exec_host
        observation_exit = exit_code if state in _TERMINAL else None
        return self._with_logs(
            SchedulerObservation(
                reference,
                state,
                native_state,
                exit_code=observation_exit,
                metadata=metadata,
            )
        )

    def _with_logs(self, observation: SchedulerObservation) -> SchedulerObservation:
        if self._log_directory is None:
            return observation
        return replace(
            observation,
            metadata={
                **observation.metadata,
                "stdout_path": str(
                    self._log_directory / f"{observation.reference.native_id}.stdout"
                ),
                "stderr_path": str(
                    self._log_directory / f"{observation.reference.native_id}.stderr"
                ),
            },
        )

    def _run(
        self,
        command: Command,
        error_type: type[PBSSchedulerError],
        action: str,
        *,
        allow_failure: bool = False,
    ) -> CommandResult:
        try:
            result = self._transport.run(command)
        except Exception as error:
            raise error_type(f"Could not start {action}") from error
        if result.exit_code != 0 and not allow_failure:
            message = (
                f"{action} failed with exit code {result.exit_code}; "
                "scheduler diagnostic redacted"
            )
            if error_type is PBSSubmissionError:
                raise PBSSubmissionError(
                    message,
                    outcome=SchedulerSubmissionOutcome.REJECTED,
                    exit_code=result.exit_code,
                )
            raise error_type(message)
        return result


def render_qsub_script(
    group: SchedulerGroup, *, log_directory: PurePath | None = None
) -> str:
    if type(group) is not SchedulerGroup or len(group.units) != 1:
        raise PBSScriptError("PBS script rendering requires one SchedulerUnit")
    unit = group.units[0]
    return _script(
        _directives(unit.resources, job_name="rundra-task"),
        (serialize_remote_command(unit.command),),
        log_directory,
    )


def render_qsub_array_script(
    request: SchedulerArrayRequest, *, log_directory: PurePath | None = None
) -> str:
    if type(request) is not SchedulerArrayRequest:
        raise TypeError("render_qsub_array_script requires SchedulerArrayRequest")
    resources = request.group.units[0].resources
    if any(unit.resources != resources for unit in request.group.units[1:]):
        raise PBSScriptError("PBS array Tasks must use uniform resources")
    stop = len(request.mapping) - 1
    limit = min(
        request.max_concurrent_jobs or len(request.mapping), len(request.mapping)
    )
    status_root = request.manifest_path.parent / "bundle-status"
    branches: list[str] = []
    for unit, item in zip(request.group.units, request.mapping, strict=True):
        branches.extend(
            (
                f"  {item.array_index})",
                f"    # task_id={item.task_id} seed={item.seed}",
                f"    ( {serialize_remote_command(unit.command)} )",
                "    ;;",
            )
        )
    body = (
        "index=${PBS_ARRAY_INDEX:?missing PBS_ARRAY_INDEX}",
        "export SLURM_ARRAY_JOB_ID=$PBS_JOBID",
        "export SLURM_ARRAY_TASK_ID=$index",
        "export SLURM_RESTART_COUNT=0",
        'case "$index" in',
        *branches,
        "  *) exit 64 ;;",
        "esac",
        f"status_root={shlex.quote(str(status_root))}",
        'aggregate="$status_root/${PBS_JOBID}.tsv"',
        'aggregate_tmp="${aggregate}.$$"',
        ': > "$aggregate_tmp"',
        "found=0",
        (
            'for path in "$status_root/${PBS_JOBID}_${index}".lane-*.tsv '
            '"$status_root/${PBS_JOBID}_${index}".lane-*.tsv.*; do'
        ),
        '  if [ -f "$path" ]; then cat -- "$path" >> "$aggregate_tmp"; found=1; fi',
        "done",
        (
            'if [ "$found" -eq 1 ]; then chmod 400 "$aggregate_tmp"; '
            'mv -- "$aggregate_tmp" "$aggregate"; else rm -f "$aggregate_tmp"; fi'
        ),
    )
    return _script(
        _directives(
            resources, job_name="rundra-array", array_stop=stop, array_limit=limit
        ),
        body,
        log_directory,
    )


def validate_pbs_resources(resources: ResourceRequest) -> None:
    if type(resources) is not ResourceRequest:
        raise TypeError("PBS resources must be a ResourceRequest")
    unsupported = sorted(set(resources.native) - {"pbs"})
    if unsupported:
        raise PBSScriptError(
            "Unsupported resources.native backend namespaces for PBS: "
            + ", ".join(unsupported)
        )
    _native_directives(resources)


def _script(
    directives: tuple[str, ...], body: tuple[str, ...], log_directory: PurePath | None
) -> str:
    prelude = ["#!/bin/sh", *directives, "", "set -eu"]
    if log_directory is not None:
        rendered = str(log_directory)
        if not log_directory.is_absolute() or "\x00" in rendered:
            raise PBSScriptError("PBS log directory must be absolute and safe")
        quoted = shlex.quote(rendered)
        prelude.extend(
            (
                f"log_directory={quoted}",
                'mkdir -p -- "$log_directory"',
                'exec > "$log_directory/${PBS_JOBID}.stdout"',
                'exec 2> "$log_directory/${PBS_JOBID}.stderr"',
            )
        )
    return "\n".join((*prelude, *body, ""))


def _directives(
    resources: ResourceRequest,
    *,
    job_name: str,
    array_stop: int | None = None,
    array_limit: int | None = None,
) -> tuple[str, ...]:
    validate_pbs_resources(resources)
    tasks_per_node = ceil(resources.tasks / resources.nodes)
    ncpus = tasks_per_node * resources.cpus_per_task
    select = f"select={resources.nodes}:ncpus={ncpus}:mpiprocs={tasks_per_node}"
    if resources.memory_bytes is not None:
        select += f":mem={ceil(resources.memory_bytes / _MIB)}mb"
    if resources.gpus_per_task:
        select += f":ngpus={tasks_per_node * resources.gpus_per_task}"
    lines = [f"#PBS -N {job_name}", f"#PBS -l {select}"]
    if resources.walltime is not None:
        total = ceil(resources.walltime.total_seconds())
        hours, remainder = divmod(total, 3600)
        minutes, seconds = divmod(remainder, 60)
        lines.append(f"#PBS -l walltime={hours:02d}:{minutes:02d}:{seconds:02d}")
    if array_stop is not None:
        suffix = f"%{array_limit}" if array_limit is not None else ""
        lines.append(f"#PBS -J 0-{array_stop}{suffix}")
    lines.extend(_native_directives(resources))
    return tuple(lines)


def _native_directives(resources: ResourceRequest) -> tuple[str, ...]:
    options = resources.native.get("pbs", {})
    unsupported = sorted(set(options) - _NATIVE_OPTIONS)
    if unsupported:
        raise PBSScriptError(
            "Unsupported resources.native.pbs options: " + ", ".join(unsupported)
        )
    lines: list[str] = []
    for name in ("queue", "account", "project", "priority", "place"):
        if name not in options:
            continue
        value = options[name]
        if (
            type(value) not in (str, int)
            or type(value) is bool
            or _SAFE_VALUE.fullmatch(str(value)) is None
        ):
            raise PBSScriptError(
                f"resources.native.pbs.{name} contains an unsafe value"
            )
        flag = {"queue": "-q", "account": "-A", "project": "-P", "priority": "-p"}.get(
            name
        )
        lines.append(
            f"#PBS {flag} {value}" if flag is not None else f"#PBS -l place={value}"
        )
    return tuple(lines)


def _submitted_id(result: CommandResult) -> str:
    value = result.stdout.strip()
    if _JOB_ID.fullmatch(value) is None:
        raise PBSSubmissionError("qsub returned an invalid job identifier")
    return value


def _references(
    value: tuple[SchedulerReference, ...],
) -> tuple[SchedulerReference, ...]:
    if not isinstance(value, tuple) or any(
        type(item) is not SchedulerReference for item in value
    ):
        raise TypeError("PBS references must be SchedulerReferences")
    if len(set(value)) != len(value):
        raise ValueError("PBS references must be unique")
    if any(_REFERENCE.fullmatch(item.native_id) is None for item in value):
        raise ValueError("PBS reference has an unsafe identifier")
    return value


def _dependency(value: SchedulerReference) -> str:
    if (
        type(value) is not SchedulerReference
        or _REFERENCE.fullmatch(value.native_id) is None
    ):
        raise ValueError("PBS dependency must be a safe SchedulerReference")
    return value.native_id


_TERMINAL = frozenset(
    {ExecutionState.SUCCEEDED, ExecutionState.FAILED, ExecutionState.CANCELLED}
)


def _execution_state(
    native: str, exit_code: int | None, comment: str
) -> ExecutionState:
    if native in {"Q", "H", "W", "S", "T"}:
        return ExecutionState.SUBMITTED
    if native in {"R", "E", "B"}:
        return ExecutionState.RUNNING
    if native in {"F", "C"}:
        if exit_code == 271 or "delet" in comment.lower():
            return ExecutionState.CANCELLED
        return (
            ExecutionState.SUCCEEDED
            if exit_code in {None, 0}
            else ExecutionState.FAILED
        )
    return ExecutionState.UNKNOWN
