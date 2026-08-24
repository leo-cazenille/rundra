from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import PurePath
from shlex import quote

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

_CLUSTER_RANGE = re.compile(r"\s*(\d+)\.(\d+)\s+-\s+(\d+)\.(\d+)\s*\Z")
_SINGLE_ID = re.compile(r"\s*(\d+)\.(\d+)\s*\Z")
_NATIVE_ID = re.compile(r"(\d+)(?:\.(\d+))?\Z")
_SAFE_ACCOUNTING_GROUP = re.compile(r"[A-Za-z0-9_.-]+\Z")
_SAFE_LIMITS = re.compile(r"[A-Za-z0-9_.:-]+(?:,[A-Za-z0-9_.:-]+)*\Z")
_SIZE = re.compile(r"([1-9][0-9]*)(B|KiB|MiB|GiB|TiB)\Z")
_TERMINAL = {
    ExecutionState.SUCCEEDED,
    ExecutionState.FAILED,
    ExecutionState.CANCELLED,
}

_PERSIST_AND_SUBMIT = """\
set -eu
wrapper=$1
wrapper_text=$2
submit_file=$3
submit_text=$4
submit_command=$5
mkdir -p "$(dirname "$wrapper")" "$(dirname "$submit_file")"
wrapper_tmp="${wrapper}.tmp.$$"
submit_tmp="${submit_file}.tmp.$$"
trap 'rm -f "$wrapper_tmp" "$submit_tmp"' EXIT HUP INT TERM
printf '%s' "$wrapper_text" > "$wrapper_tmp"
chmod 755 "$wrapper_tmp"
mv "$wrapper_tmp" "$wrapper"
printf '%s' "$submit_text" > "$submit_tmp"
chmod 600 "$submit_tmp"
mv "$submit_tmp" "$submit_file"
exec "$submit_command" -terse "$submit_file"
"""


class HTCondorSchedulerError(RuntimeError):
    pass


class HTCondorSubmissionError(HTCondorSchedulerError, SchedulerSubmissionFailure):
    def __init__(
        self,
        message: str,
        *,
        outcome: SchedulerSubmissionOutcome,
        phase: str = "condor_submit",
        exit_code: int | None = None,
    ) -> None:
        SchedulerSubmissionFailure.__init__(
            self,
            message,
            backend="htcondor",
            phase=phase,
            outcome=outcome,
            exit_code=exit_code,
        )


class HTCondorQueryError(HTCondorSchedulerError):
    pass


class HTCondorCancellationError(HTCondorSchedulerError):
    pass


class HTCondorScriptError(HTCondorSchedulerError):
    pass


class HTCondorScheduler:
    """HTCondor adapter for vanilla jobs on an explicitly shared workspace."""

    def __init__(
        self,
        transport: Transport,
        *,
        condor_submit: str = "condor_submit",
        condor_q: str = "condor_q",
        condor_history: str = "condor_history",
        condor_rm: str = "condor_rm",
        log_directory: PurePath | None = None,
    ) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("HTCondorScheduler transport must implement Transport")
        for name, executable in (
            ("condor_submit", condor_submit),
            ("condor_q", condor_q),
            ("condor_history", condor_history),
            ("condor_rm", condor_rm),
        ):
            if (
                type(executable) is not str
                or not executable.strip()
                or "\x00" in executable
            ):
                raise ValueError(f"HTCondorScheduler {name} must be nonblank and safe")
        if log_directory is not None and (
            not isinstance(log_directory, PurePath)
            or not log_directory.is_absolute()
            or "\x00" in str(log_directory)
        ):
            raise ValueError(
                "HTCondorScheduler log_directory must be absolute and safe"
            )
        self._transport = transport
        self._submit_command = condor_submit
        self._q_command = condor_q
        self._history_command = condor_history
        self._rm_command = condor_rm
        self._log_directory = log_directory

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        if type(group) is not SchedulerGroup or len(group.units) != 1:
            raise HTCondorSubmissionError(
                "HTCondor single submission requires one Task",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        unit = group.units[0]
        path = self._submission_path(unit.task_id.value)
        result = self._persist_and_submit(
            path.with_suffix(".sh"),
            _wrapper((unit.command,), _walltime_seconds(unit.resources)),
            path.with_suffix(".submit"),
            render_condor_submit(group, log_directory=self._log_directory),
        )
        cluster, first, last = _submitted_range(result.stdout)
        if first != 0 or last != 0:
            raise HTCondorSubmissionError(
                "condor_submit returned an unexpected process range",
                outcome=SchedulerSubmissionOutcome.UNCERTAIN,
                phase="receipt_parsing",
            )
        native_id = f"{cluster}.0"
        return SchedulerSubmission(
            SchedulerReference(str(cluster)), {unit.task_id: native_id}
        )

    def submit_array(self, request: SchedulerArrayRequest) -> SchedulerSubmission:
        if type(request) is not SchedulerArrayRequest:
            raise TypeError(
                "HTCondorScheduler.submit_array requires SchedulerArrayRequest"
            )
        resources = request.group.units[0].resources
        if any(unit.resources != resources for unit in request.group.units):
            raise HTCondorSubmissionError(
                "HTCondor arrays require identical resources for every Task",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        wrapper_path = self._submission_path(
            f"{request.manifest_path.parent.parent.name}-array.sh"
        )
        submit_path = request.manifest_path.with_suffix(".submit")
        result = self._persist_and_submit(
            wrapper_path,
            _wrapper(
                tuple(unit.command for unit in request.group.units),
                _walltime_seconds(resources),
            ),
            submit_path,
            render_condor_array_submit(
                request,
                log_directory=self._log_directory,
                executable=wrapper_path,
            ),
        )
        cluster, first, last = _submitted_range(result.stdout)
        if first != 0 or last != len(request.mapping) - 1:
            raise HTCondorSubmissionError(
                "condor_submit returned an unexpected process range",
                outcome=SchedulerSubmissionOutcome.UNCERTAIN,
                phase="receipt_parsing",
            )
        return SchedulerSubmission(
            SchedulerReference(str(cluster)),
            {item.task_id: f"{cluster}.{item.array_index}" for item in request.mapping},
        )

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        normalized = _references(references)
        if not normalized:
            return ()
        active = self._query_json(self._q_command, normalized, allow_missing=True)
        missing = tuple(item for item in normalized if not _ads_for(item, active))
        history = (
            self._query_json(self._history_command, missing, allow_missing=True)
            if missing
            else ()
        )
        return tuple(
            self._observation(reference, _ads_for(reference, (*active, *history)))
            for reference in normalized
        )

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        normalized = _references(references)
        if not normalized:
            return ()
        result = self._run(
            Command((self._rm_command, *(item.native_id for item in normalized))),
            HTCondorCancellationError,
            "condor_rm cancellation",
            allow_failure=True,
        )
        if result.exit_code != 0:
            observations = self.query(normalized)
            if any(item.state not in _TERMINAL for item in observations):
                raise HTCondorCancellationError(
                    f"condor_rm failed with exit code {result.exit_code}; scheduler diagnostic redacted"
                )
            return observations
        return tuple(
            self._with_logs(
                SchedulerObservation(
                    item, ExecutionState.CANCELLED, "REMOVAL_REQUESTED"
                )
            )
            for item in normalized
        )

    def _submission_path(self, name: str) -> PurePath:
        if self._log_directory is None:
            raise HTCondorSubmissionError(
                "HTCondor submission requires an absolute framework log directory",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        return self._log_directory / f"rundra-{name}"

    def _persist_and_submit(
        self, wrapper_path: PurePath, wrapper: str, submit_path: PurePath, submit: str
    ) -> CommandResult:
        return self._run(
            Command(
                (
                    "/bin/sh",
                    "-c",
                    _PERSIST_AND_SUBMIT,
                    "rundra-condor-submit",
                    str(wrapper_path),
                    wrapper,
                    str(submit_path),
                    submit,
                    self._submit_command,
                )
            ),
            HTCondorSubmissionError,
            "condor_submit submission",
        )

    def _query_json(
        self,
        executable: str,
        references: tuple[SchedulerReference, ...],
        *,
        allow_missing: bool,
    ) -> tuple[Mapping[str, object], ...]:
        result = self._run(
            Command(
                (
                    executable,
                    *(item.native_id for item in references),
                    "-json",
                    "-attributes",
                    "ClusterId,ProcId,JobStatus,ExitCode,ExitBySignal,ExitSignal,QDate,JobStartDate,CompletionDate,RemoteHost,LastRemoteHost,HoldReason,RemoveReason",
                )
            ),
            HTCondorQueryError,
            f"{executable} query",
            allow_failure=allow_missing,
        )
        if result.exit_code != 0 and allow_missing:
            return ()
        try:
            document = json.loads(result.stdout or "[]")
        except json.JSONDecodeError as error:
            raise HTCondorQueryError(f"{executable} returned invalid JSON") from error
        if not isinstance(document, list) or any(
            not isinstance(item, dict) for item in document
        ):
            raise HTCondorQueryError(f"{executable} JSON must contain a list of jobs")
        return tuple(document)

    def _observation(
        self, reference: SchedulerReference, ads: tuple[Mapping[str, object], ...]
    ) -> SchedulerObservation:
        if not ads:
            return self._with_logs(
                SchedulerObservation(
                    reference,
                    ExecutionState.UNKNOWN,
                    "ACCOUNTING_PENDING",
                    metadata={"accounting_pending": True},
                )
            )
        observations = tuple(_ad_observation(reference, ad) for ad in ads)
        if len(observations) == 1:
            return self._with_logs(observations[0])
        states = tuple(item.state for item in observations)
        if all(item is ExecutionState.SUCCEEDED for item in states):
            state = ExecutionState.SUCCEEDED
            exit_code: int | None = 0
        elif any(item is ExecutionState.FAILED for item in states):
            state, exit_code = ExecutionState.FAILED, 1
        elif any(item is ExecutionState.RUNNING for item in states):
            state, exit_code = ExecutionState.RUNNING, None
        elif any(
            item in {ExecutionState.SUBMITTED, ExecutionState.QUEUED} for item in states
        ):
            state, exit_code = ExecutionState.QUEUED, None
        elif all(item is ExecutionState.CANCELLED for item in states):
            state, exit_code = ExecutionState.CANCELLED, None
        else:
            state, exit_code = ExecutionState.UNKNOWN, None
        return self._with_logs(
            SchedulerObservation(reference, state, "AGGREGATED", exit_code=exit_code)
        )

    def _with_logs(self, observation: SchedulerObservation) -> SchedulerObservation:
        if self._log_directory is None:
            return observation
        suffix = observation.reference.native_id
        return replace(
            observation,
            metadata={
                **observation.metadata,
                "stdout_path": str(self._log_directory / f"{suffix}.stdout"),
                "stderr_path": str(self._log_directory / f"{suffix}.stderr"),
                "scheduler_log_path": str(self._log_directory / f"{suffix}.condor.log"),
            },
        )

    def _run(
        self,
        command: Command,
        error_type: type[HTCondorSchedulerError],
        action: str,
        *,
        allow_failure: bool = False,
    ) -> CommandResult:
        try:
            result = self._transport.run(command)
        except Exception as error:
            if error_type is HTCondorSubmissionError:
                raise HTCondorSubmissionError(
                    f"Could not start {action}",
                    outcome=SchedulerSubmissionOutcome.UNCERTAIN,
                ) from error
            raise error_type(f"Could not start {action}") from error
        if result.exit_code != 0 and not allow_failure:
            message = f"{action} failed with exit code {result.exit_code}; scheduler diagnostic redacted"
            if error_type is HTCondorSubmissionError:
                raise HTCondorSubmissionError(
                    message,
                    outcome=SchedulerSubmissionOutcome.REJECTED,
                    exit_code=result.exit_code,
                )
            raise error_type(message)
        return result


def render_condor_submit(
    group: SchedulerGroup, *, log_directory: PurePath | None
) -> str:
    if type(group) is not SchedulerGroup or len(group.units) != 1:
        raise HTCondorScriptError("HTCondor submit rendering requires one Task")
    return _submit_description(
        group.units[0].resources,
        executable=log_directory / f"rundra-{group.units[0].task_id.value}.sh"
        if log_directory is not None
        else None,
        log_directory=log_directory,
        count=1,
        max_materialize=None,
    )


def render_condor_array_submit(
    request: SchedulerArrayRequest,
    *,
    log_directory: PurePath | None,
    executable: PurePath | None = None,
) -> str:
    if type(request) is not SchedulerArrayRequest:
        raise TypeError("render_condor_array_submit requires SchedulerArrayRequest")
    limits = tuple(
        item
        for item in (request.max_concurrent_jobs, request.max_workers)
        if item is not None
    )
    return _submit_description(
        request.group.units[0].resources,
        executable=executable or request.manifest_path,
        log_directory=log_directory,
        count=len(request.mapping),
        max_materialize=min(limits) if limits else None,
    )


def validate_htcondor_resources(resources: ResourceRequest) -> None:
    if type(resources) is not ResourceRequest:
        raise TypeError("HTCondor resource validation requires ResourceRequest")
    if resources.nodes != 1 or resources.tasks != 1:
        raise HTCondorScriptError("HTCondor Tasks require nodes=1 and tasks=1")
    options = resources.native.get("htcondor", {})
    allowed = {
        "accounting_group",
        "requirements",
        "rank",
        "job_priority",
        "request_disk",
        "concurrency_limits",
    }
    unknown = sorted(set(options) - allowed)
    if unknown:
        raise HTCondorScriptError(
            f"Unsupported HTCondor native option(s): {', '.join(unknown)}"
        )
    accounting = options.get("accounting_group")
    if accounting is not None and (
        type(accounting) is not str
        or _SAFE_ACCOUNTING_GROUP.fullmatch(accounting) is None
    ):
        raise HTCondorScriptError("HTCondor accounting_group is invalid")
    for name in ("requirements", "rank"):
        value = options.get(name)
        if value is not None and (
            type(value) is not str
            or not value.strip()
            or any(char in value for char in ("\r", "\n", "\x00"))
            or "$(" in value
        ):
            raise HTCondorScriptError(
                f"HTCondor {name} must be a safe single-line expression"
            )
    priority = options.get("job_priority")
    if priority is not None and type(priority) is not int:
        raise HTCondorScriptError("HTCondor job_priority must be an integer")
    disk = options.get("request_disk")
    if disk is not None and (type(disk) is not str or _SIZE.fullmatch(disk) is None):
        raise HTCondorScriptError(
            "HTCondor request_disk must be a positive binary size"
        )
    limits = options.get("concurrency_limits")
    if limits is not None and (
        type(limits) is not str or _SAFE_LIMITS.fullmatch(limits) is None
    ):
        raise HTCondorScriptError("HTCondor concurrency_limits is invalid")


def _submit_description(
    resources: ResourceRequest,
    *,
    executable: PurePath | None,
    log_directory: PurePath | None,
    count: int,
    max_materialize: int | None,
) -> str:
    validate_htcondor_resources(resources)
    if (
        log_directory is None
        or not log_directory.is_absolute()
        or executable is None
        or not executable.is_absolute()
    ):
        raise HTCondorScriptError("HTCondor requires absolute executable and log paths")
    if any(
        character in str(executable) + str(log_directory)
        for character in ('"', "\r", "\n")
    ):
        raise HTCondorScriptError(
            "HTCondor framework paths contain unsupported characters"
        )
    lines = [
        "universe = vanilla",
        f"executable = {executable}",
        "arguments = $(ProcId)",
        "should_transfer_files = NO",
        "transfer_executable = False",
        "notification = Never",
        f"output = {log_directory}/$(ClusterId).$(ProcId).stdout",
        f"error = {log_directory}/$(ClusterId).$(ProcId).stderr",
        f"log = {log_directory}/$(ClusterId).$(ProcId).condor.log",
        f"request_cpus = {resources.cpus_per_task}",
    ]
    if resources.memory_bytes is not None:
        memory_mib = (resources.memory_bytes + 1024**2 - 1) // 1024**2
        lines.append(f"request_memory = {memory_mib}")
    if resources.gpus_per_task:
        lines.append(f"request_gpus = {resources.gpus_per_task}")
    options = resources.native.get("htcondor", {})
    names = {
        "accounting_group": "accounting_group",
        "requirements": "requirements",
        "rank": "rank",
        "job_priority": "priority",
        "request_disk": "request_disk",
        "concurrency_limits": "concurrency_limits",
    }
    lines.extend(f"{names[name]} = {value}" for name, value in options.items())
    if max_materialize is not None and max_materialize < count:
        lines.append(f"max_materialize = {max_materialize}")
    lines.append(f"queue {count}")
    return "\n".join(lines) + "\n"


def _wrapper(commands: tuple[Command, ...], walltime_seconds: int | None) -> str:
    lines = ["#!/bin/sh", "set -eu", 'case "$1" in']
    for index, command in enumerate(commands):
        serialized = serialize_remote_command(command)
        if walltime_seconds is not None:
            serialized = f"timeout --signal=TERM {walltime_seconds} /bin/sh -c {quote(serialized)}"
        lines.extend(
            (f"  {index})", f"    exec /bin/sh -c {quote(serialized)}", "    ;;")
        )
    lines.extend(("  *) exit 64 ;;", "esac", ""))
    return "\n".join(lines)


def _walltime_seconds(resources: ResourceRequest) -> int | None:
    return (
        int(resources.walltime.total_seconds())
        if resources.walltime is not None
        else None
    )


def _submitted_range(value: str) -> tuple[int, int, int]:
    match = _CLUSTER_RANGE.fullmatch(value)
    if match is not None:
        first_cluster, first, last_cluster, last = (
            int(item) for item in match.groups()
        )
        if first_cluster != last_cluster:
            raise HTCondorSubmissionError(
                "condor_submit returned multiple clusters",
                outcome=SchedulerSubmissionOutcome.UNCERTAIN,
                phase="receipt_parsing",
            )
        return first_cluster, first, last
    single = _SINGLE_ID.fullmatch(value)
    if single is not None:
        cluster, proc = (int(item) for item in single.groups())
        return cluster, proc, proc
    raise HTCondorSubmissionError(
        "condor_submit returned an unrecognized identifier",
        outcome=SchedulerSubmissionOutcome.UNCERTAIN,
        phase="receipt_parsing",
    )


def _references(
    value: tuple[SchedulerReference, ...],
) -> tuple[SchedulerReference, ...]:
    if not isinstance(value, tuple) or any(
        type(item) is not SchedulerReference for item in value
    ):
        raise TypeError("HTCondor references must be a tuple of SchedulerReferences")
    for item in value:
        if _NATIVE_ID.fullmatch(item.native_id) is None:
            raise HTCondorQueryError(
                f"Invalid HTCondor scheduler identifier: {item.native_id}"
            )
    return value


def _ads_for(
    reference: SchedulerReference, ads: tuple[Mapping[str, object], ...]
) -> tuple[Mapping[str, object], ...]:
    match = _NATIVE_ID.fullmatch(reference.native_id)
    assert match is not None
    cluster = int(match.group(1))
    proc = int(match.group(2)) if match.group(2) is not None else None
    return tuple(
        ad
        for ad in ads
        if ad.get("ClusterId") == cluster and (proc is None or ad.get("ProcId") == proc)
    )


def _ad_observation(
    reference: SchedulerReference, ad: Mapping[str, object]
) -> SchedulerObservation:
    raw_status = ad.get("JobStatus")
    status = raw_status if type(raw_status) is int else None
    native = (
        {
            1: "IDLE",
            2: "RUNNING",
            3: "REMOVED",
            4: "COMPLETED",
            5: "HELD",
            6: "TRANSFERRING_OUTPUT",
            7: "SUSPENDED",
        }.get(status, "UNKNOWN")
        if status is not None
        else "UNKNOWN"
    )
    raw_exit_code = ad.get("ExitCode")
    exit_code: int | None = raw_exit_code if type(raw_exit_code) is int else None
    signalled = ad.get("ExitBySignal") is True
    if status == 1:
        state = ExecutionState.QUEUED
    elif status in {2, 6, 7}:
        state = ExecutionState.RUNNING
    elif status == 3:
        state = ExecutionState.CANCELLED
    elif status == 4:
        state = (
            ExecutionState.SUCCEEDED
            if exit_code == 0 and not signalled
            else ExecutionState.FAILED
        )
    elif status == 5:
        state = ExecutionState.SUBMITTED
    else:
        state = ExecutionState.UNKNOWN
    metadata: dict[str, NativeValue] = {}
    for source, target in (
        ("RemoteHost", "node_list"),
        ("LastRemoteHost", "last_node"),
        ("HoldReason", "hold_reason"),
        ("RemoveReason", "remove_reason"),
        ("ExitSignal", "exit_signal"),
    ):
        value = ad.get(source)
        if isinstance(value, (str, int, float, bool)):
            metadata[target] = value
    return SchedulerObservation(
        reference,
        state,
        native,
        exit_code=exit_code if state in _TERMINAL else None,
        metadata=metadata,
        started_at=_timestamp(ad.get("JobStartDate")),
        finished_at=_timestamp(ad.get("CompletionDate"))
        if state in _TERMINAL
        else None,
    )


def _timestamp(value: object) -> datetime | None:
    return (
        datetime.fromtimestamp(value, UTC) if type(value) is int and value > 0 else None
    )
