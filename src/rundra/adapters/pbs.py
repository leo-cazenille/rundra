from __future__ import annotations

import json
import re
import shlex
from dataclasses import replace
from math import ceil
from pathlib import PurePath

from rundra.adapters._remote_shell import serialize_remote_command
from rundra.domain.models import Command, NativeValue, ResourceRequest
from rundra.domain.states import ExecutionState
from rundra.ports import (
    CommandResult,
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
    branches: list[str] = []
    for unit, item in zip(request.group.units, request.mapping, strict=True):
        branches.extend(
            (
                f"  {item.array_index})",
                f"    # task_id={item.task_id} seed={item.seed}",
                f"    {serialize_remote_command(unit.command)}",
                "    ;;",
            )
        )
    body = (
        "index=${PBS_ARRAY_INDEX:?missing PBS_ARRAY_INDEX}",
        'case "$index" in',
        *branches,
        "  *) exit 64 ;;",
        "esac",
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
