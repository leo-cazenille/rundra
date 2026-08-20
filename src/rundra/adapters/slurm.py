from __future__ import annotations

import base64
import gzip
import re
import shlex
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, tzinfo
from pathlib import PurePath

from rundra.adapters._remote_shell import serialize_remote_command
from rundra.domain.mappings import ArrayTaskMapping
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

_MIB = 1024**2
_VALUE_OPTIONS = ("account", "constraint", "partition", "qos")
_FLAG_OPTIONS = ("exclusive",)
_ALLOWED_OPTIONS = frozenset((*_VALUE_OPTIONS, *_FLAG_OPTIONS))
_SAFE_NATIVE_VALUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@,+:/=\[\]&|*~-]*\Z")
_PARSABLE_SUBMISSION = re.compile(r"(?P<job_id>[0-9]+)(?:;(?P<cluster>[^;\r\n]+))?\Z")
_SLURM_REFERENCE = re.compile(r"[0-9]+(?:_[0-9]+)?\Z")
_MAX_ARRAY_SIZE = re.compile(r"(?:^|\n)\s*MaxArraySize\s*=\s*([0-9]+)\s*(?:\n|$)")
_MAX_QUERY_REFERENCES = 500
_COMPRESSED_ARGUMENT_CHUNK = 60 * 1024
_SUBMIT_SCRIPT = """\
set -eu
script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")
trap 'rm -f "$script"' EXIT HUP INT TERM
printf '%s' "$1" > "$script"
"$2" --parsable "$script"
"""
_SUBMIT_WITH_LOG_DIR_SCRIPT = """\
set -eu
mkdir -p -- "$3"
script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")
trap 'rm -f "$script"' EXIT HUP INT TERM
printf '%s' "$1" > "$script"
"$2" --parsable "$script"
"""
_SUBMIT_DEPENDENT_SCRIPT = """\
set -eu
script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")
trap 'rm -f "$script"' EXIT HUP INT TERM
printf '%s' "$1" > "$script"
"$2" --parsable --dependency="$3" "$script"
"""
_SUBMIT_DEPENDENT_WITH_LOG_DIR_SCRIPT = """\
set -eu
mkdir -p -- "$3"
script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")
trap 'rm -f "$script"' EXIT HUP INT TERM
printf '%s' "$1" > "$script"
"$2" --parsable --dependency="$4" "$script"
"""
_SUBMIT_ARRAY_SCRIPT = """\
set -eu
manifest=$1
if [ -e "$manifest" ]; then
  printf '%s\n' 'Rundra array manifest already exists' >&2
  exit 73
fi
manifest_tmp=$(mktemp "${manifest}.XXXXXX")
script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")
trap 'rm -f "$manifest_tmp" "$script"' EXIT HUP INT TERM
printf '%s' "$2" | base64 -d | gzip -d > "$manifest_tmp"
chmod 500 "$manifest_tmp"
mv -- "$manifest_tmp" "$manifest"
manifest_tmp=
mkdir -p -- "$4"
printf '%s' "$3" > "$script"
"$5" --parsable "$script"
"""
_SUBMIT_DEPENDENT_ARRAY_SCRIPT = """\
set -eu
manifest=$1
if [ -e "$manifest" ]; then
  printf '%s\n' 'Rundra array manifest already exists' >&2
  exit 73
fi
manifest_tmp=$(mktemp "${manifest}.XXXXXX")
script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")
trap 'rm -f "$manifest_tmp" "$script"' EXIT HUP INT TERM
printf '%s' "$2" | base64 -d | gzip -d > "$manifest_tmp"
chmod 500 "$manifest_tmp"
mv -- "$manifest_tmp" "$manifest"
manifest_tmp=
mkdir -p -- "$4"
printf '%s' "$3" > "$script"
"$5" --parsable --dependency="$6" "$script"
"""
_SUBMIT_CHUNKED_ARRAY_SCRIPT = """\
set -eu
manifest=$1
encoded=$2
if [ -e "$manifest" ]; then
  printf '%s\n' 'Rundra array manifest already exists' >&2
  exit 73
fi
script_content=$3
log_directory=$4
sbatch=$5
dependency=$6
manifest_tmp=$(mktemp "${manifest}.XXXXXX")
script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")
trap 'rm -f "$manifest_tmp" "$encoded" "$script"' EXIT HUP INT TERM
base64 -d < "$encoded" | gzip -d > "$manifest_tmp"
chmod 500 "$manifest_tmp"
mv -- "$manifest_tmp" "$manifest"
manifest_tmp=
mkdir -p -- "$log_directory"
printf '%s' "$script_content" > "$script"
if [ "$dependency" != - ]; then
  "$sbatch" --parsable --dependency="$dependency" "$script"
else
  "$sbatch" --parsable "$script"
fi
"""
_CREATE_ENCODED_MANIFEST_SCRIPT = """\
set -eu
encoded=$1
manifest=$2
if [ -e "$encoded" ] || [ -e "$manifest" ]; then
  exit 73
fi
umask 077
: > "$encoded"
"""
_APPEND_ENCODED_MANIFEST_SCRIPT = """\
set -eu
printf '%s' "$1" >> "$2"
"""


class SlurmScriptError(ValueError):
    """Raised when a normalized group cannot be represented as an sbatch script."""


class SlurmSubmissionError(SchedulerSubmissionFailure):
    """Raised when sbatch submission fails or returns invalid structured output."""

    def __init__(
        self,
        message: str,
        *,
        outcome: SchedulerSubmissionOutcome = SchedulerSubmissionOutcome.UNCERTAIN,
        phase: str = "scheduler_submit",
        exit_code: int | None = None,
    ) -> None:
        super().__init__(
            message,
            backend="slurm",
            phase=phase,
            outcome=outcome,
            exit_code=exit_code,
        )


class SlurmQueryError(RuntimeError):
    """Raised when Slurm state output cannot be queried or represented safely."""


class SlurmCancellationError(RuntimeError):
    """Raised when scancel fails and the job remains nonterminal."""


@dataclass(frozen=True, slots=True)
class SlurmArrayRequest:
    """Validated pure inputs for rendering one bounded Slurm array."""

    group: SchedulerGroup
    mapping: tuple[ArrayTaskMapping, ...]
    manifest_path: PurePath
    max_array_size: int
    allow_duplicate_seeds: bool = False

    def __post_init__(self) -> None:
        if type(self.group) is not SchedulerGroup:
            raise TypeError("SlurmArrayRequest group must be a SchedulerGroup")
        if len(self.group.units) < 2:
            raise SlurmScriptError("Slurm arrays require at least two Tasks")
        if not isinstance(self.mapping, Sequence) or isinstance(
            self.mapping, (str, bytes)
        ):
            raise TypeError("SlurmArrayRequest mapping must be a sequence")
        mapping = tuple(self.mapping)
        if any(type(item) is not ArrayTaskMapping for item in mapping):
            raise TypeError("SlurmArrayRequest mapping must contain ArrayTaskMappings")
        expected_task_ids = tuple(unit.task_id for unit in self.group.units)
        if tuple(item.task_id for item in mapping) != expected_task_ids:
            raise SlurmScriptError(
                "Slurm array mapping must match SchedulerGroup Task order"
            )
        if tuple(item.array_index for item in mapping) != tuple(range(len(mapping))):
            raise SlurmScriptError(
                "Slurm array indices must be contiguous and zero-based"
            )
        if type(self.allow_duplicate_seeds) is not bool:
            raise TypeError("SlurmArrayRequest allow_duplicate_seeds must be bool")
        if not self.allow_duplicate_seeds and len(
            {item.seed for item in mapping}
        ) != len(mapping):
            raise SlurmScriptError("Slurm array mapping seeds must be unique")
        resources = self.group.units[0].resources
        if any(unit.resources != resources for unit in self.group.units[1:]):
            raise SlurmScriptError("Slurm array Tasks must use uniform resources")
        if not isinstance(self.manifest_path, PurePath):
            raise TypeError("SlurmArrayRequest manifest_path must be a path")
        rendered_path = str(self.manifest_path)
        if not self.manifest_path.is_absolute() or "\x00" in rendered_path:
            raise SlurmScriptError(
                "Slurm array manifest path must be absolute and safe"
            )
        if type(self.max_array_size) is not int:
            raise TypeError("SlurmArrayRequest max_array_size must be an integer")
        if self.max_array_size <= 0:
            raise ValueError("SlurmArrayRequest max_array_size must be positive")
        if len(mapping) > self.max_array_size:
            raise SlurmScriptError(
                "Slurm array Task count exceeds configured MaxArraySize"
            )
        object.__setattr__(self, "mapping", mapping)


def _compressed_text(value: str) -> str:
    """Encode deterministic UTF-8 text for bounded remote command transport."""
    return base64.b64encode(gzip.compress(value.encode("utf-8"), mtime=0)).decode(
        "ascii"
    )


class SlurmScheduler:
    """Submit normalized groups to Slurm through a configured Transport."""

    def __init__(
        self,
        transport: Transport,
        *,
        sbatch: str = "sbatch",
        squeue: str = "squeue",
        sacct: str = "sacct",
        scancel: str = "scancel",
        scontrol: str = "scontrol",
        srun: str = "srun",
        timezone: tzinfo | None = None,
        log_directory: PurePath | None = None,
    ) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("SlurmScheduler transport must implement Transport")
        for name, executable in (
            ("sbatch", sbatch),
            ("squeue", squeue),
            ("sacct", sacct),
            ("scancel", scancel),
            ("scontrol", scontrol),
            ("srun", srun),
        ):
            if (
                type(executable) is not str
                or not executable.strip()
                or "\x00" in executable
            ):
                raise ValueError(
                    f"SlurmScheduler {name} executable must be nonblank and safe"
                )
        if timezone is not None and not isinstance(timezone, tzinfo):
            raise TypeError("SlurmScheduler timezone must be a tzinfo or None")
        if log_directory is not None:
            if not isinstance(log_directory, PurePath):
                raise TypeError("SlurmScheduler log_directory must be a path or None")
            rendered_log_directory = str(log_directory)
            if (
                not log_directory.is_absolute()
                or "\x00" in rendered_log_directory
                or any(character.isspace() for character in rendered_log_directory)
            ):
                raise ValueError(
                    "SlurmScheduler log_directory must be absolute and safe"
                )
        self._transport = transport
        self._sbatch = sbatch
        self._squeue = squeue
        self._sacct = sacct
        self._scancel = scancel
        self._scontrol = scontrol
        self._srun = srun
        self._timezone = timezone
        self._log_directory = log_directory

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        """Submit one generated script and retain opaque Slurm identity."""
        return self._submit(group, dependency=None)

    def submit_afterok(
        self,
        group: SchedulerGroup,
        dependency: SchedulerReference,
    ) -> SchedulerSubmission:
        """Submit one Task after a framework-owned successful-job dependency."""
        return self._submit(group, dependency=_dependency(dependency))

    def _submit(
        self,
        group: SchedulerGroup,
        *,
        dependency: str | None,
    ) -> SchedulerSubmission:
        if type(group) is SchedulerGroup and len(group.units) != 1:
            raise SlurmSubmissionError(
                "M5.3 array submission is not available until per-Task "
                "reconciliation is implemented",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        stdout_path, stderr_path = self._log_paths("%j")
        script = render_sbatch_script(
            group, stdout_path=stdout_path, stderr_path=stderr_path
        )
        command_arguments = [
            "/bin/sh",
            "-c",
            _SUBMIT_SCRIPT,
            "rundra-slurm-submit",
            script,
            self._sbatch,
        ]
        if self._log_directory is not None:
            command_arguments[2] = _SUBMIT_WITH_LOG_DIR_SCRIPT
            command_arguments.append(str(self._log_directory))
        if dependency is not None:
            command_arguments[2] = (
                _SUBMIT_DEPENDENT_WITH_LOG_DIR_SCRIPT
                if self._log_directory is not None
                else _SUBMIT_DEPENDENT_SCRIPT
            )
            command_arguments.append(dependency)
        command = Command(tuple(command_arguments))
        try:
            result = self._transport.run(command)
        except Exception as error:
            raise SlurmSubmissionError("Could not start sbatch submission") from error
        if result.exit_code != 0:
            raise SlurmSubmissionError(
                f"sbatch failed with exit code {result.exit_code}; "
                "scheduler diagnostic redacted",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                exit_code=result.exit_code,
            )
        output = result.stdout.strip()
        match = _PARSABLE_SUBMISSION.fullmatch(output)
        if match is None:
            raise SlurmSubmissionError("sbatch returned invalid parsable output")
        job_id = match.group("job_id")
        reference = SchedulerReference(job_id)
        return SchedulerSubmission(reference, {group.units[0].task_id: job_id})

    def submit_array(self, request: SchedulerArrayRequest) -> SchedulerSubmission:
        """Discover the controller bound and submit a portable mapped array."""
        if type(request) is not SchedulerArrayRequest:
            raise TypeError(
                "SlurmScheduler.submit_array requires a SchedulerArrayRequest"
            )
        return self._submit_array_chunks(request, dependency=None)

    def submit_array_afterok(
        self,
        request: SchedulerArrayRequest,
        dependency: SchedulerReference,
    ) -> SchedulerSubmission:
        """Submit a mapped array after a framework-owned successful dependency."""
        if type(request) is not SchedulerArrayRequest:
            raise TypeError(
                "SlurmScheduler.submit_array_afterok requires a SchedulerArrayRequest"
            )
        return self._submit_array_chunks(request, dependency=_dependency(dependency))

    def submit_compact_array(
        self, request: CompactSchedulerArrayRequest
    ) -> CompactSchedulerSubmission:
        """Submit one constant-size ordinal-driven worker array."""
        if type(request) is not CompactSchedulerArrayRequest:
            raise TypeError("Slurm compact submission requires a compact request")
        return self._submit_compact_array(request, dependency=None)

    def submit_compact_array_afterok(
        self,
        request: CompactSchedulerArrayRequest,
        dependency: SchedulerReference,
    ) -> CompactSchedulerSubmission:
        """Submit one compact worker array after a preparation dependency."""
        if type(request) is not CompactSchedulerArrayRequest:
            raise TypeError("Slurm compact submission requires a compact request")
        return self._submit_compact_array(request, dependency=_dependency(dependency))

    def _submit_compact_array(
        self,
        request: CompactSchedulerArrayRequest,
        *,
        dependency: str | None,
    ) -> CompactSchedulerSubmission:
        if self._log_directory is None:
            raise SlurmSubmissionError(
                "Slurm compact submission requires a configured log directory",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        if request.worker_count > self.array_limit():
            raise SlurmSubmissionError(
                "Compact worker count exceeds Slurm MaxArraySize",
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
        stdout_path, stderr_path = self._log_paths("%A_%a")
        if stdout_path is None or stderr_path is None:  # pragma: no cover
            raise AssertionError("configured Slurm logs must produce paths")
        directives = _sbatch_directives(
            job_name="rundra-worker",
            resources=request.worker_resources,
            stdout_path=_array_log_path(stdout_path, name="stdout"),
            stderr_path=_array_log_path(stderr_path, name="stderr"),
            array_stop=request.worker_count - 1,
        )
        if request.requeue_limit > 0:
            directives = (*directives, "#SBATCH --requeue")
        quoted_manifest = shlex.quote(str(manifest_path))
        quoted_srun = shlex.quote(self._srun)
        quoted_status_root = shlex.quote(str(status_root))
        merge_lines = tuple(
            line
            for lane in range(request.task_slots_per_worker)
            for line in (
                (
                    f'lane="$status_root/${{SLURM_ARRAY_JOB_ID}}_'
                    f"${{SLURM_ARRAY_TASK_ID}}.lane-{lane}."
                    'attempt-${attempt}.tsv"'
                ),
                '[ -f "$lane" ] || exit 74',
                'cat -- "$lane" >> "$journal_tmp"',
            )
        )
        script = "\n".join(
            (
                "#!/bin/sh",
                *directives,
                "",
                "set -eu",
                "attempt=${SLURM_RESTART_COUNT:-0}",
                'case "$attempt" in ""|*[!0-9]*) exit 64 ;; esac',
                f'[ "$attempt" -le {request.requeue_limit} ] || exit 75',
                f"status_root={quoted_status_root}",
                'mkdir -p -- "$status_root"',
                (
                    'journal="$status_root/${SLURM_ARRAY_JOB_ID}_'
                    '${SLURM_ARRAY_TASK_ID}.attempt-${attempt}.tsv"'
                ),
                'journal_tmp="${journal}.$$"',
                'if [ -e "$journal" ]; then exit 73; fi',
                ': > "$journal_tmp"',
                "trap 'rm -f \"$journal_tmp\"' EXIT HUP INT TERM",
                (
                    f"{quoted_srun} --nodes=1 "
                    f"--ntasks={request.task_slots_per_worker} "
                    f"--ntasks-per-node={request.task_slots_per_worker} "
                    f"--cpus-per-task={request.resources.cpus_per_task} "
                    f"--kill-on-bad-exit=1 /bin/sh {quoted_manifest} "
                    '"$SLURM_ARRAY_TASK_ID"'
                ),
                *merge_lines,
                'chmod 400 "$journal_tmp"',
                'mv -- "$journal_tmp" "$journal"',
                "trap - EXIT HUP INT TERM",
                "",
            )
        )
        job_id = self._submit_chunked_array_payload(
            manifest_path, manifest, script, dependency=dependency
        )
        return CompactSchedulerSubmission(
            SchedulerReference(job_id),
            request.task_space,
            tuple(f"{job_id}_{index}" for index in range(request.worker_count)),
        )

    def _submit_array_chunks(
        self,
        request: SchedulerArrayRequest,
        *,
        dependency: str | None,
    ) -> SchedulerSubmission:
        max_array_size = self.array_limit()
        worker_limits = tuple(
            limit
            for limit in (request.max_concurrent_jobs, request.max_workers)
            if limit is not None
        )
        if request.task_slots_per_worker > 1 or (
            worker_limits and len(request.mapping) > min(worker_limits)
        ):
            return self._submit_bundled_array(
                request,
                dependency=dependency,
                max_array_size=max_array_size,
            )
        submissions: list[SchedulerSubmission] = []
        try:
            for chunk_ordinal, start in enumerate(
                range(0, len(request.mapping), max_array_size)
            ):
                source_mapping = request.mapping[start : start + max_array_size]
                units = request.group.units[start : start + max_array_size]
                if len(units) == 1:
                    submissions.append(
                        self._submit(SchedulerGroup(units), dependency=dependency)
                    )
                    continue
                mapping = tuple(
                    ArrayTaskMapping(item.task_id, item.seed, array_index)
                    for array_index, item in enumerate(source_mapping)
                )
                manifest_path = request.manifest_path.with_name(
                    f"{request.manifest_path.stem}.part-{chunk_ordinal:06d}"
                    f"{request.manifest_path.suffix}"
                )
                bounded = SlurmArrayRequest(
                    SchedulerGroup(units),
                    mapping,
                    manifest_path,
                    max_array_size,
                    request.allow_duplicate_seeds,
                )
                submissions.append(
                    self._submit_bounded_array(bounded, dependency=dependency)
                )
        except Exception as error:
            if submissions:
                try:
                    self._transport.run(
                        Command(
                            (
                                self._scancel,
                                "--",
                                *(item.reference.native_id for item in submissions),
                            )
                        )
                    )
                except Exception:
                    pass
                raise SlurmSubmissionError(
                    "A partial Slurm submission required cleanup; scheduler "
                    "outcome is uncertain",
                    phase="partial_submission_cleanup",
                ) from error
            raise

        primary, *additional = submissions
        return SchedulerSubmission(
            primary.reference,
            {
                task_id: native_id
                for submission in submissions
                for task_id, native_id in submission.task_native_ids.items()
            },
            tuple(item.reference for item in additional),
        )

    def _submit_bundled_array(
        self,
        request: SchedulerArrayRequest,
        *,
        dependency: str | None,
        max_array_size: int,
    ) -> SchedulerSubmission:
        if self._log_directory is None:
            raise SlurmSubmissionError(
                "Slurm bundle submission requires a configured log directory",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        limits = tuple(
            limit
            for limit in (request.max_concurrent_jobs, request.max_workers)
            if limit is not None
        )
        if not limits:
            raise SlurmSubmissionError(
                "Slurm bundle submission requires an explicit worker limit",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        task_slots = min(request.task_slots_per_worker, len(request.mapping))
        worker_count = min(
            *limits,
            max_array_size,
            (len(request.mapping) + task_slots - 1) // task_slots,
        )
        max_assignment = (len(request.mapping) + worker_count * task_slots - 1) // (
            worker_count * task_slots
        )
        resources = request.group.units[0].resources
        if any(unit.resources != resources for unit in request.group.units[1:]):
            raise SlurmScriptError("Slurm bundled Tasks must use uniform resources")
        if task_slots > 1 and (
            resources.nodes != 1 or resources.tasks != 1 or resources.gpus_per_task != 0
        ):
            raise SlurmScriptError(
                "Concurrent Slurm workers require one-node, one-task, CPU-only "
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
            raise SlurmScriptError(
                "Planned worker resources do not match logical Task resources"
            )
        worker_resources = request.worker_resources or derived_worker_resources
        manifest_path = request.manifest_path.with_name(
            f"{request.manifest_path.stem}.workers{request.manifest_path.suffix}"
        )
        status_root = request.manifest_path.parent / "bundle-status"
        manifest = render_slurm_bundle_manifest(
            request,
            worker_count=worker_count,
            task_slots_per_worker=task_slots,
            status_root=status_root,
        )
        stdout_path, stderr_path = self._log_paths("%A_%a")
        if stdout_path is None or stderr_path is None:  # pragma: no cover
            raise AssertionError("configured Slurm logs must produce paths")
        directives = _sbatch_directives(
            job_name="rundra-worker",
            resources=worker_resources,
            stdout_path=_array_log_path(stdout_path, name="stdout"),
            stderr_path=_array_log_path(stderr_path, name="stderr"),
            array_stop=worker_count - 1,
        )
        quoted_manifest = shlex.quote(str(manifest_path))
        quoted_srun = shlex.quote(self._srun)
        quoted_status_root = shlex.quote(str(status_root))
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
        script = "\n".join(
            (
                "#!/bin/sh",
                *directives,
                "",
                "set -eu",
                'if [ "${SLURM_ARRAY_TASK_ID+x}" != x ]; then',
                "  printf '%s\\n' 'missing SLURM_ARRAY_TASK_ID' >&2",
                "  exit 64",
                "fi",
                f"status_root={quoted_status_root}",
                'mkdir -p -- "$status_root"',
                'journal="$status_root/${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.tsv"',
                'journal_tmp="${journal}.$$"',
                'if [ -e "$journal" ]; then exit 73; fi',
                ': > "$journal_tmp"',
                "trap 'rm -f \"$journal_tmp\"' EXIT HUP INT TERM",
                (
                    f"{quoted_srun} --nodes=1 --ntasks={task_slots} "
                    f"--ntasks-per-node={task_slots} "
                    f"--cpus-per-task={resources.cpus_per_task} "
                    f"--kill-on-bad-exit=1 /bin/sh {quoted_manifest} "
                    '"$SLURM_ARRAY_TASK_ID"'
                ),
                *merge_lines,
                'chmod 400 "$journal_tmp"',
                'mv -- "$journal_tmp" "$journal"',
                "trap - EXIT HUP INT TERM",
                "",
            )
        )
        job_id = self._submit_chunked_array_payload(
            manifest_path,
            manifest,
            script,
            dependency=dependency,
        )
        return SchedulerSubmission(
            SchedulerReference(job_id),
            {
                item.task_id: f"{job_id}_{ordinal % worker_count}"
                for ordinal, item in enumerate(request.mapping)
            },
        )

    def _submit_chunked_array_payload(
        self,
        manifest_path: PurePath,
        manifest: str,
        script: str,
        *,
        dependency: str | None,
    ) -> str:
        assert self._log_directory is not None
        encoded = _compressed_text(manifest)
        chunks = tuple(
            encoded[offset : offset + _COMPRESSED_ARGUMENT_CHUNK]
            for offset in range(0, len(encoded), _COMPRESSED_ARGUMENT_CHUNK)
        )
        encoded_path = manifest_path.with_name(f".{manifest_path.name}.encoded")
        commands = [
            Command(
                (
                    "/bin/sh",
                    "-c",
                    _CREATE_ENCODED_MANIFEST_SCRIPT,
                    "rundra-bundle-create",
                    str(encoded_path),
                    str(manifest_path),
                )
            ),
            *(
                Command(
                    (
                        "/bin/sh",
                        "-c",
                        _APPEND_ENCODED_MANIFEST_SCRIPT,
                        "rundra-bundle-append",
                        chunk,
                        str(encoded_path),
                    )
                )
                for chunk in chunks
            ),
        ]
        try:
            for command in commands:
                result = self._transport.run(command)
                if result.exit_code != 0:
                    raise SlurmSubmissionError(
                        "Bundle manifest chunk persistence failed with "
                        f"exit code {result.exit_code}; diagnostic redacted",
                        outcome=SchedulerSubmissionOutcome.REJECTED,
                        phase="manifest_persistence",
                        exit_code=result.exit_code,
                    )
        except SlurmSubmissionError:
            raise
        except Exception as error:
            raise SlurmSubmissionError(
                "Could not persist the bundle manifest chunks",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="manifest_persistence",
            ) from error
        command = Command(
            (
                "/bin/sh",
                "-c",
                _SUBMIT_CHUNKED_ARRAY_SCRIPT,
                "rundra-slurm-bundle-submit",
                str(manifest_path),
                str(encoded_path),
                script,
                str(self._log_directory),
                self._sbatch,
                dependency or "-",
            )
        )
        try:
            result = self._transport.run(command)
        except Exception as error:
            raise SlurmSubmissionError(
                "Could not persist the bundle manifest and start sbatch submission"
            ) from error
        if result.exit_code != 0:
            raise SlurmSubmissionError(
                "Bundle manifest persistence or sbatch submission failed with "
                f"exit code {result.exit_code}; scheduler diagnostic redacted",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="scheduler_submit",
                exit_code=result.exit_code,
            )
        output = result.stdout.strip()
        match = _PARSABLE_SUBMISSION.fullmatch(output)
        if match is None:
            raise SlurmSubmissionError("sbatch returned invalid parsable output")
        return match.group("job_id")

    def submit_bounded_array(self, request: SlurmArrayRequest) -> SchedulerSubmission:
        """Persist one immutable manifest and submit its bounded Slurm array."""
        return self._submit_bounded_array(request, dependency=None)

    def _submit_bounded_array(
        self,
        request: SlurmArrayRequest,
        *,
        dependency: str | None,
    ) -> SchedulerSubmission:
        if type(request) is not SlurmArrayRequest:
            raise TypeError(
                "SlurmScheduler.submit_bounded_array requires a SlurmArrayRequest"
            )
        if self._log_directory is None:
            raise SlurmSubmissionError(
                "Slurm array submission requires a configured log directory",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="request_validation",
            )
        manifest = render_slurm_array_manifest(request)
        stdout_path, stderr_path = self._log_paths("%A_%a")
        if stdout_path is None or stderr_path is None:  # pragma: no cover - invariant
            raise AssertionError("configured Slurm logs must produce paths")
        script = render_sbatch_array_script(
            request,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        command_arguments = [
            "/bin/sh",
            "-c",
            _SUBMIT_ARRAY_SCRIPT,
            "rundra-slurm-array-submit",
            str(request.manifest_path),
            _compressed_text(manifest),
            script,
            str(self._log_directory),
            self._sbatch,
        ]
        if dependency is not None:
            command_arguments[2] = _SUBMIT_DEPENDENT_ARRAY_SCRIPT
            command_arguments.append(dependency)
        command = Command(tuple(command_arguments))
        try:
            result = self._transport.run(command)
        except Exception as error:
            raise SlurmSubmissionError(
                "Could not persist the array manifest and start sbatch submission"
            ) from error
        if result.exit_code != 0:
            raise SlurmSubmissionError(
                "Array manifest persistence or sbatch submission failed with "
                f"exit code {result.exit_code}; scheduler diagnostic redacted",
                outcome=SchedulerSubmissionOutcome.REJECTED,
                phase="scheduler_submit",
                exit_code=result.exit_code,
            )
        output = result.stdout.strip()
        match = _PARSABLE_SUBMISSION.fullmatch(output)
        if match is None:
            raise SlurmSubmissionError("sbatch returned invalid parsable output")
        job_id = match.group("job_id")
        return SchedulerSubmission(
            SchedulerReference(job_id),
            {item.task_id: f"{job_id}_{item.array_index}" for item in request.mapping},
        )

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        """Query active state, then accounting for references absent from squeue."""
        normalized = _references(references)
        if not normalized:
            return ()
        observations: dict[SchedulerReference, SchedulerObservation] = {}
        for start in range(0, len(normalized), _MAX_QUERY_REFERENCES):
            observations.update(
                self._query_batch(normalized[start : start + _MAX_QUERY_REFERENCES])
            )
        return tuple(observations[reference] for reference in normalized)

    def _query_batch(
        self, normalized: tuple[SchedulerReference, ...]
    ) -> dict[SchedulerReference, SchedulerObservation]:
        joined = ",".join(reference.native_id for reference in normalized)
        queued = self._run_query(
            Command(
                (
                    self._squeue,
                    "--noheader",
                    "--array",
                    "--jobs",
                    joined,
                    "--format",
                    "%i|%T|%S|%N",
                )
            ),
            source="squeue",
        )
        observations = _parse_squeue(queued.stdout, normalized, self._timezone)
        missing = tuple(
            reference for reference in normalized if reference not in observations
        )
        if missing:
            try:
                accounting = self._run_query(
                    Command(
                        (
                            self._sacct,
                            "--noheader",
                            "--parsable2",
                            "--jobs",
                            ",".join(reference.native_id for reference in missing),
                            "--format",
                            # JobID preserves the root_index array alias. JobIDRaw
                            # may be an unrelated allocation ID on older Slurm.
                            "JobID,State%32,ExitCode,Start,End,NodeList",
                        )
                    ),
                    source="sacct",
                )
            except SlurmQueryError as accounting_error:
                try:
                    observations.update(self._query_scontrol(missing))
                except SlurmQueryError as fallback_error:
                    raise SlurmQueryError(
                        f"{accounting_error}; scontrol fallback failed: {fallback_error}"
                    ) from fallback_error
            else:
                observations.update(
                    _parse_sacct(accounting.stdout, missing, self._timezone)
                )
        return {
            reference: self._with_log_metadata(
                observations.get(
                    reference,
                    SchedulerObservation(
                        reference,
                        ExecutionState.UNKNOWN,
                        "ACCOUNTING_PENDING",
                        metadata={"accounting_pending": True},
                    ),
                )
            )
            for reference in normalized
        }

    def array_limit(self) -> int:
        """Read the controller's positive MaxArraySize submission bound."""
        result = self._run_query(
            Command((self._scontrol, "show", "config")), source="scontrol"
        )
        match = _MAX_ARRAY_SIZE.search(result.stdout)
        if match is None:
            raise SlurmQueryError("scontrol did not return MaxArraySize")
        limit = int(match.group(1))
        if limit <= 0:
            raise SlurmQueryError("scontrol returned a non-positive MaxArraySize")
        return limit

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        """Request cancellation and reconcile the scheduler's resulting state."""
        normalized = _references(references)
        if not normalized:
            return ()
        command = Command(
            (self._scancel, "--", *(reference.native_id for reference in normalized))
        )
        try:
            result = self._transport.run(command)
        except Exception as error:
            raise SlurmCancellationError("Could not start scancel") from error
        observations = self.query(normalized)
        terminal = {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
        if result.exit_code != 0 and any(
            observation.state not in terminal for observation in observations
        ):
            raise SlurmCancellationError(
                f"scancel failed with exit code {result.exit_code}; "
                "scheduler diagnostic redacted"
            )
        return observations

    def _run_query(self, command: Command, *, source: str) -> CommandResult:
        try:
            result = self._transport.run(command)
        except Exception as error:
            raise SlurmQueryError(f"Could not start {source} query") from error
        if result.exit_code != 0:
            raise SlurmQueryError(
                f"{source} failed with exit code {result.exit_code}; "
                "scheduler diagnostic redacted"
            )
        return result

    def _query_scontrol(
        self, references: tuple[SchedulerReference, ...]
    ) -> dict[SchedulerReference, SchedulerObservation]:
        observations: dict[SchedulerReference, SchedulerObservation] = {}
        for reference in references:
            result = self._run_query(
                Command(
                    (
                        self._scontrol,
                        "show",
                        "job",
                        "-o",
                        reference.native_id,
                    )
                ),
                source="scontrol",
            )
            observations[reference] = _parse_scontrol(
                result.stdout, reference, self._timezone
            )
        return observations

    def _log_paths(self, job_id: str) -> tuple[PurePath | None, PurePath | None]:
        if self._log_directory is None:
            return None, None
        return (
            self._log_directory / f"{job_id}.stdout",
            self._log_directory / f"{job_id}.stderr",
        )

    def _with_log_metadata(
        self, observation: SchedulerObservation
    ) -> SchedulerObservation:
        stdout_path, stderr_path = self._log_paths(observation.reference.native_id)
        if stdout_path is None or stderr_path is None:
            return observation
        return replace(
            observation,
            metadata={
                **observation.metadata,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
            },
        )


def render_slurm_array_manifest(request: SlurmArrayRequest) -> str:
    """Render a deterministic shell dispatcher with safely serialized commands."""
    if type(request) is not SlurmArrayRequest:
        raise TypeError("render_slurm_array_manifest requires a SlurmArrayRequest")
    branches: list[str] = []
    for unit, mapping in zip(request.group.units, request.mapping, strict=True):
        branches.extend(
            (
                f"  {mapping.array_index})",
                f"    # task_id={mapping.task_id} seed={mapping.seed}",
                f"    {serialize_remote_command(unit.command)}",
                "    ;;",
            )
        )
    return "\n".join(
        (
            "#!/bin/sh",
            "set -eu",
            'if [ "$#" -ne 1 ]; then',
            "  printf '%s\\n' 'invalid Rundra array index' >&2",
            "  exit 64",
            "fi",
            'case "$1" in',
            *branches,
            "  *)",
            "    printf '%s\\n' 'invalid Rundra array index' >&2",
            "    exit 64",
            "    ;;",
            "esac",
            "",
        )
    )


def render_slurm_bundle_manifest(
    request: SchedulerArrayRequest,
    *,
    worker_count: int,
    task_slots_per_worker: int = 1,
    status_root: PurePath,
) -> str:
    """Render bounded workers that continue and journal logical Task outcomes."""
    if type(request) is not SchedulerArrayRequest:
        raise TypeError("render_slurm_bundle_manifest requires a SchedulerArrayRequest")
    if type(worker_count) is not int or not 1 <= worker_count <= len(request.mapping):
        raise ValueError("worker_count must fit the SchedulerArrayRequest")
    if type(task_slots_per_worker) is not int or not 1 <= task_slots_per_worker <= len(
        request.mapping
    ):
        raise ValueError("task_slots_per_worker must fit the SchedulerArrayRequest")
    status = _directive_path(status_root, name="bundle status")
    branches: list[str] = []
    for worker_index in range(worker_count):
        branches.extend((f"  {worker_index})", '    case "$SLURM_PROCID" in'))
        for lane_index in range(task_slots_per_worker):
            branches.extend(
                (
                    f"      {lane_index})",
                    f"        status_root={shlex.quote(status)}",
                    '        mkdir -p -- "$status_root"',
                    (
                        '        journal="$status_root/${SLURM_ARRAY_JOB_ID}_'
                        '${SLURM_ARRAY_TASK_ID}.lane-${SLURM_PROCID}.tsv"'
                    ),
                    '        journal_tmp="${journal}.$$"',
                    '        if [ -e "$journal" ]; then exit 73; fi',
                    "        printf 'RUNDRA_TASK_EVENTS\\t1\\n' > \"$journal\"",
                    '        : > "$journal_tmp"',
                )
            )
            start = worker_index + lane_index * worker_count
            stride = worker_count * task_slots_per_worker
            lane_task_ids: list[str] = []
            for ordinal in range(start, len(request.mapping), stride):
                unit = request.group.units[ordinal]
                mapping = request.mapping[ordinal]
                lane_task_ids.append(str(mapping.task_id))
                timeout = _task_timeout_seconds(unit.resources.walltime)
                command = serialize_remote_command(unit.command)
                rendered = (
                    command.replace("exec env --", "env --", 1)
                    if timeout is None
                    else command.replace(
                        "exec env --",
                        (f"timeout --signal=TERM --kill-after=30s {timeout}s env --"),
                        1,
                    )
                )
                branches.extend(
                    (
                        f"        # task_id={mapping.task_id} seed={mapping.seed}",
                        (
                            f"        printf 'START\\t%s\\t%s\\t%s\\n' "
                            f'{mapping.task_id} "$(date +%s)" "$(hostname)" '
                            '>> "$journal"'
                        ),
                        "        set +e",
                        f"        {rendered}",
                        "        task_status=$?",
                        "        set -e",
                        (
                            f"        printf '%s\\t%s\\n' {mapping.task_id} "
                            '"$task_status" >> "$journal_tmp"'
                        ),
                        (
                            f"        printf 'FINISH\\t%s\\t%s\\t%s\\t%s\\n' "
                            f'{mapping.task_id} "$task_status" "$(date +%s)" '
                            '"$(hostname)" >> "$journal"'
                        ),
                    )
                )
            if request.output_root is not None and request.shard_root is not None:
                branches.extend(
                    _render_lane_shard(
                        request.output_root,
                        request.shard_root,
                        tuple(lane_task_ids),
                    )
                )
            branches.extend(
                (
                    '        rm -f -- "$journal_tmp"',
                    '        chmod 400 "$journal"',
                    "        ;;",
                )
            )
        branches.extend(("      *) exit 64 ;;", "    esac", "    ;;"))
    return "\n".join(
        (
            "#!/bin/sh",
            "set -eu",
            'if [ "$#" -ne 1 ]; then exit 64; fi',
            'if [ "${SLURM_ARRAY_JOB_ID+x}" != x ]; then exit 64; fi',
            'if [ "${SLURM_PROCID+x}" != x ]; then exit 64; fi',
            'case "$1" in',
            *branches,
            "  *) exit 64 ;;",
            "esac",
            "",
        )
    )


def _render_lane_shard(
    output_root: PurePath,
    shard_root: PurePath,
    task_ids: tuple[str, ...],
) -> tuple[str, ...]:
    quoted_output = shlex.quote(str(output_root))
    quoted_shards = shlex.quote(str(shard_root))
    quoted_tasks = " ".join(shlex.quote(task_id) for task_id in task_ids)
    task_cases = "|".join(task_ids)
    return (
        f"        output_root={quoted_output}",
        f"        shard_root={quoted_shards}",
        '        mkdir -p -- "$shard_root"',
        (
            '        shard="$shard_root/${SLURM_ARRAY_JOB_ID}_'
            '${SLURM_ARRAY_TASK_ID}.lane-${SLURM_PROCID}.tar"'
        ),
        '        shard_tmp="${shard}.$$"',
        '        checksum="${shard}.sha256"',
        '        checksum_tmp="${checksum}.$$"',
        '        index_dir=$(mktemp -d "$shard_root/.index.XXXXXX")',
        '        index="$index_dir/index.tsv"',
        (
            "        printf 'RUNDRA_SHARD\\t2\\t%s\\t%s\\n' "
            '"$SLURM_ARRAY_TASK_ID" "$SLURM_PROCID" > "$index"'
        ),
        "        tab=$(printf '\\t')",
        '        while IFS="$tab" read -r task_id task_status; do',
        f'          case "$task_id" in {task_cases}) ;; *) exit 76 ;; esac',
        (
            "          printf 'TASK\\t%s\\t%s\\n' \"$task_id\" "
            '"$task_status" >> "$index"'
        ),
        '          task_dir="$output_root/$task_id"',
        '          [ -d "$task_dir" ] || exit 76',
        '          [ -z "$(find "$task_dir" -type l -print -quit)" ] || exit 76',
        (
            "          find \"$task_dir\" -type f -printf '%P\\t%s\\n' "
            '| LC_ALL=C sort | while IFS="$tab" read -r member size; do'
        ),
        '            [ -n "$member" ] || exit 76',
        '            digest=$(sha256sum -- "$task_dir/$member")',
        "            digest=${digest%% *}",
        (
            "            printf 'MEMBER\\t%s/%s\\t%s\\t%s\\n' "
            '"$task_id" "$member" "$size" "$digest" >> "$index"'
        ),
        "          done",
        '        done < "$journal_tmp"',
        (
            "        tar --sort=name --mtime=@0 --owner=0 --group=0 "
            '--numeric-owner -cf "$shard_tmp" -C "$output_root" '
            f'{quoted_tasks} -C "$index_dir" index.tsv'
        ),
        '        shard_digest=$(sha256sum -- "$shard_tmp")',
        "        shard_digest=${shard_digest%% *}",
        (
            "        printf '%s  %s\\n' \"$shard_digest\" "
            '"${shard##*/}" > "$checksum_tmp"'
        ),
        '        chmod 400 "$shard_tmp" "$checksum_tmp"',
        '        mv -- "$shard_tmp" "$shard"',
        '        mv -- "$checksum_tmp" "$checksum"',
        "        rm -rf -- "
        + " ".join(f'"$output_root/{task_id}"' for task_id in task_ids),
        '        rm -rf -- "$index_dir"',
    )


def render_slurm_compact_bundle_manifest(
    request: CompactSchedulerArrayRequest,
    *,
    status_root: PurePath,
) -> str:
    """Render worker logic whose size is independent of logical Task count."""

    if type(request) is not CompactSchedulerArrayRequest:
        raise TypeError("compact manifest requires a CompactSchedulerArrayRequest")
    status = _directive_path(status_root, name="bundle status")
    task_space = request.task_space
    timeout = _task_timeout_seconds(request.resources.walltime)
    command_cases = tuple(
        line
        for index, command in enumerate(request.commands)
        for line in (
            f"          {index})",
            f"            {_render_compact_command(command, timeout=timeout)}",
            "            ;;",
        )
    )
    run_root = request.manifest_path.parent.parent
    output_root = run_root / "output"
    runtime_root = run_root / "runtime"
    lane_cases: list[str] = []
    for lane in range(request.task_slots_per_worker):
        lane_cases.extend(
            (
                f"    {lane})",
                f"      ordinal=$((worker + {lane * request.worker_count}))",
                f"      stride={request.worker_count * request.task_slots_per_worker}",
                f"      status_root={shlex.quote(status)}",
                f"      output_root={shlex.quote(str(output_root))}",
                f"      runtime_root={shlex.quote(str(runtime_root))}",
                '      mkdir -p -- "$status_root"',
                "      attempt=${SLURM_RESTART_COUNT:-0}",
                '      case "$attempt" in ""|*[!0-9]*) exit 64 ;; esac',
                f'      [ "$attempt" -le {request.requeue_limit} ] || exit 75',
                (
                    '      journal="$status_root/${SLURM_ARRAY_JOB_ID}_'
                    "${SLURM_ARRAY_TASK_ID}.lane-${SLURM_PROCID}."
                    'attempt-${attempt}.tsv"'
                ),
                '      journal_tmp="${journal}.$$"',
                '      if [ -e "$journal" ]; then exit 73; fi',
                "      printf 'RUNDRA_TASK_EVENTS\\t2\\n' > \"$journal\"",
                '      : > "$journal_tmp"',
                "      tab=$(printf '\\t')",
                f'      while [ "$ordinal" -lt {task_space.task_count} ]; do',
                f"        parameter_set=$((ordinal / {task_space.seeds.count}))",
                f"        seed_index=$((ordinal % {task_space.seeds.count}))",
                (
                    f"        seed=$(({task_space.seeds.start} + "
                    f"seed_index * {task_space.seeds.step}))"
                ),
                '        task_id=$(printf "task_%06d" "$ordinal")',
                "        finished=0",
                "        prior_starts=0",
                (
                    '        for prior in "$status_root/${SLURM_ARRAY_JOB_ID}_'
                    "${SLURM_ARRAY_TASK_ID}.lane-${SLURM_PROCID}."
                    'attempt-"*.tsv; do'
                ),
                '          [ -f "$prior" ] || continue',
                '          [ "$prior" = "$journal" ] && continue',
                (
                    '          if grep -F "FINISH${tab}${task_id}${tab}" '
                    '"$prior" >/dev/null; then finished=1; break; fi'
                ),
                (
                    '          if grep -F "START${tab}${task_id}${tab}" '
                    '"$prior" >/dev/null; then '
                    "prior_starts=$((prior_starts + 1)); fi"
                ),
                "        done",
                (
                    '        if [ "$finished" -eq 1 ]; then '
                    "ordinal=$((ordinal + stride)); continue; fi"
                ),
                '        rm -rf -- "$output_root/$task_id" "$runtime_root/$task_id"',
                '        mkdir -p -- "$output_root/$task_id" "$runtime_root/$task_id"',
                (
                    f'        if [ "$prior_starts" -gt '
                    f"{request.infrastructure_retry_limit} ]; then"
                ),
                (
                    "          printf 'FINISH\\t%s\\t%s\\t125\\t%s\\t%s\\n' "
                    '"$task_id" "$attempt" "$(date +%s)" "$(hostname)" '
                    '>> "$journal"'
                ),
                '          printf \'%s\\t125\\n\' "$task_id" >> "$journal_tmp"',
                "          ordinal=$((ordinal + stride))",
                "          continue",
                "        fi",
                (
                    "        printf 'START\\t%s\\t%s\\t%s\\t%s\\n' \"$task_id\" "
                    '"$attempt" "$(date +%s)" "$(hostname)" >> "$journal"'
                ),
                "        set +e",
                '        case "$parameter_set" in',
                *command_cases,
                "          *) exit 64 ;;",
                "        esac",
                "        task_status=$?",
                "        set -e",
                (
                    '        printf \'%s\\t%s\\n\' "$task_id" "$task_status" '
                    '>> "$journal_tmp"'
                ),
                (
                    "        printf 'FINISH\\t%s\\t%s\\t%s\\t%s\\t%s\\n' "
                    '"$task_id" "$attempt" "$task_status" "$(date +%s)" '
                    '"$(hostname)" >> "$journal"'
                ),
                "        ordinal=$((ordinal + stride))",
                "      done",
                *(
                    _render_compact_lane_shard(request.output_root, request.shard_root)
                    if request.output_root is not None
                    and request.shard_root is not None
                    else ()
                ),
                '      rm -f -- "$journal_tmp"',
                '      chmod 400 "$journal"',
                "      ;;",
            )
        )
    return "\n".join(
        (
            "#!/bin/sh",
            "set -eu",
            'if [ "$#" -ne 1 ]; then exit 64; fi',
            "worker=$1",
            'case "$worker" in *[!0-9]*|"") exit 64 ;; esac',
            f'if [ "$worker" -ge {request.worker_count} ]; then exit 64; fi',
            'case "${SLURM_PROCID:-}" in',
            *lane_cases,
            "  *) exit 64 ;;",
            "esac",
            "",
        )
    )


def _render_compact_command(command: Command, *, timeout: int | None) -> str:
    words = ["env", "--"]
    words.extend(
        _compact_shell_word(f"{name}={value}")
        for name, value in sorted(command.environment.items())
    )
    words.extend(_compact_shell_word(argument) for argument in command.argv)
    rendered = " ".join(words)
    if command.working_directory is not None:
        rendered = (
            f"cd -- {_compact_shell_word(str(command.working_directory))} && {rendered}"
        )
    if timeout is not None:
        rendered = f"timeout --signal=TERM --kill-after=30s {timeout}s {rendered}"
    return rendered


def _compact_shell_word(value: str) -> str:
    if "\x00" in value:
        raise SlurmScriptError("Compact command values must not contain NUL")
    parts = re.split(r"(__RUNDRA_(?:TASK_ID|SEED)__)", value)
    rendered: list[str] = []
    for part in parts:
        if part == "__RUNDRA_TASK_ID__":
            rendered.append('"$task_id"')
        elif part == "__RUNDRA_SEED__":
            rendered.append('"$seed"')
        elif part:
            rendered.append(shlex.quote(part))
    return "".join(rendered) or "''"


def _render_compact_lane_shard(
    output_root: PurePath, shard_root: PurePath
) -> tuple[str, ...]:
    return (
        f"      output_root={shlex.quote(str(output_root))}",
        f"      shard_root={shlex.quote(str(shard_root))}",
        '      mkdir -p -- "$shard_root"',
        (
            '      shard="$shard_root/${SLURM_ARRAY_JOB_ID}_'
            "${SLURM_ARRAY_TASK_ID}.lane-${SLURM_PROCID}."
            'attempt-${attempt}.tar"'
        ),
        '      shard_tmp="${shard}.$$"',
        '      checksum="${shard}.sha256"',
        '      checksum_tmp="${checksum}.$$"',
        '      index_dir=$(mktemp -d "$shard_root/.index.XXXXXX")',
        '      index="$index_dir/index.tsv"',
        '      task_list="$index_dir/tasks.txt"',
        '      : > "$task_list"',
        (
            "      printf 'RUNDRA_SHARD\\t2\\t%s\\t%s\\n' "
            '"$SLURM_ARRAY_TASK_ID" "$SLURM_PROCID" > "$index"'
        ),
        "      tab=$(printf '\\t')",
        '      while IFS="$tab" read -r task_id task_status; do',
        '        case "$task_id" in task_*) ;; *) exit 76 ;; esac',
        "        suffix=${task_id#task_}",
        '        case "$suffix" in ""|*[!0-9]*) exit 76 ;; esac',
        '        printf "%s\\n" "$task_id" >> "$task_list"',
        ('        printf \'TASK\\t%s\\t%s\\n\' "$task_id" "$task_status" >> "$index"'),
        '        task_dir="$output_root/$task_id"',
        '        [ -d "$task_dir" ] || exit 76',
        '        [ -z "$(find "$task_dir" -type l -print -quit)" ] || exit 76',
        (
            "        find \"$task_dir\" -type f -printf '%P\\t%s\\n' "
            '| LC_ALL=C sort | while IFS="$tab" read -r member size; do'
        ),
        '          [ -n "$member" ] || exit 76',
        '          digest=$(sha256sum -- "$task_dir/$member")',
        (
            "          printf 'MEMBER\\t%s/%s\\t%s\\t%s\\n' "
            '"$task_id" "$member" "$size" "${digest%% *}" >> "$index"'
        ),
        "        done",
        '      done < "$journal_tmp"',
        (
            "      tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner "
            '-cf "$shard_tmp" -C "$output_root" -T "$task_list" '
            '-C "$index_dir" index.tsv'
        ),
        '      shard_digest=$(sha256sum -- "$shard_tmp")',
        (
            "      printf '%s  %s\\n' \"${shard_digest%% *}\" "
            '"${shard##*/}" > "$checksum_tmp"'
        ),
        '      chmod 400 "$shard_tmp" "$checksum_tmp"',
        '      mv -- "$shard_tmp" "$shard"',
        '      mv -- "$checksum_tmp" "$checksum"',
        '      while IFS= read -r task_id; do rm -rf -- "$output_root/$task_id"; done < "$task_list"',
        '      rm -rf -- "$index_dir"',
    )


def _task_timeout_seconds(value: timedelta | None) -> int | None:
    if value is None:
        return None
    microseconds = (
        value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
    )
    return (microseconds + 999_999) // 1_000_000


def render_sbatch_array_script(
    request: SlurmArrayRequest,
    *,
    stdout_path: PurePath,
    stderr_path: PurePath,
) -> str:
    """Render a bounded Slurm array script that dispatches through its manifest."""
    if type(request) is not SlurmArrayRequest:
        raise TypeError("render_sbatch_array_script requires a SlurmArrayRequest")
    stdout = _array_log_path(stdout_path, name="stdout")
    stderr = _array_log_path(stderr_path, name="stderr")
    resources = request.group.units[0].resources
    directives = _sbatch_directives(
        job_name="rundra-array",
        resources=resources,
        stdout_path=stdout,
        stderr_path=stderr,
        array_stop=len(request.mapping) - 1,
    )
    manifest_path = shlex.quote(str(request.manifest_path))
    command = (
        'if [ "${SLURM_ARRAY_TASK_ID+x}" != x ]; then\n'
        "  printf '%s\\n' 'missing SLURM_ARRAY_TASK_ID' >&2\n"
        "  exit 64\n"
        "fi\n"
        f'exec /bin/sh {manifest_path} "$SLURM_ARRAY_TASK_ID"'
    )
    return "\n".join(("#!/bin/sh", *directives, "", "set -eu", command, ""))


def render_sbatch_script(
    group: SchedulerGroup,
    *,
    stdout_path: PurePath | None = None,
    stderr_path: PurePath | None = None,
) -> str:
    """Render one deterministic, inspectable single-Task sbatch script."""
    if type(group) is not SchedulerGroup:
        raise TypeError("render_sbatch_script requires a SchedulerGroup")
    if len(group.units) != 1:
        raise SlurmScriptError("M3 Slurm submission requires exactly one Task")
    unit = group.units[0]
    resources = unit.resources
    if (stdout_path is None) != (stderr_path is None):
        raise SlurmScriptError(
            "Slurm stdout and stderr paths must be provided together"
        )
    directives = _sbatch_directives(
        job_name=f"rundra-{unit.task_id}",
        resources=resources,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    command = serialize_remote_command(unit.command)
    return "\n".join(("#!/bin/sh", *directives, "", "set -eu", command, ""))


def _sbatch_directives(
    *,
    job_name: str,
    resources: ResourceRequest,
    stdout_path: PurePath | None,
    stderr_path: PurePath | None,
    array_stop: int | None = None,
) -> tuple[str, ...]:
    validate_slurm_resources(resources)
    directives = [f"#SBATCH --job-name={job_name}"]
    if array_stop is not None:
        directives.append(f"#SBATCH --array=0-{array_stop}")
    directives.extend(
        (
            f"#SBATCH --nodes={resources.nodes}",
            f"#SBATCH --ntasks={resources.tasks}",
            f"#SBATCH --cpus-per-task={resources.cpus_per_task}",
        )
    )
    if stdout_path is not None and stderr_path is not None:
        directives.extend(
            (
                f"#SBATCH --output={_directive_path(stdout_path, name='stdout')}",
                f"#SBATCH --error={_directive_path(stderr_path, name='stderr')}",
            )
        )
    if resources.gpus_per_task:
        directives.append(f"#SBATCH --gpus-per-task={resources.gpus_per_task}")
    if resources.memory_bytes is not None:
        memory_mib = (resources.memory_bytes + _MIB - 1) // _MIB
        directives.append(f"#SBATCH --mem={memory_mib}M")
    if resources.walltime is not None:
        directives.append(f"#SBATCH --time={_slurm_duration(resources.walltime)}")
    directives.extend(_native_directives(resources))
    return tuple(directives)


def _array_log_path(value: PurePath, *, name: str) -> PurePath:
    rendered = _directive_path(value, name=name)
    if "%A" not in rendered or "%a" not in rendered:
        raise SlurmScriptError(
            f"Slurm array {name} path must contain %A and %a placeholders"
        )
    return value


def _native_directives(resources: ResourceRequest) -> tuple[str, ...]:
    options = resources.native.get("slurm", {})
    unsupported = sorted(set(options) - _ALLOWED_OPTIONS)
    if unsupported:
        names = ", ".join(unsupported)
        raise SlurmScriptError(f"Unsupported resources.native.slurm options: {names}")
    directives: list[str] = []
    for name in _VALUE_OPTIONS:
        if name in options:
            directives.append(f"#SBATCH --{name}={_native_value(name, options[name])}")
    for name in _FLAG_OPTIONS:
        if name in options:
            value = options[name]
            if type(value) is not bool:
                raise SlurmScriptError(
                    f"resources.native.slurm.{name} must be a boolean"
                )
            if value:
                directives.append(f"#SBATCH --{name}")
    return tuple(directives)


def validate_slurm_resources(resources: ResourceRequest) -> None:
    """Validate that a portable request is representable by this Slurm adapter."""
    if type(resources) is not ResourceRequest:
        raise TypeError("Slurm resources must be a ResourceRequest")
    unsupported_backends = sorted(set(resources.native) - {"slurm"})
    if unsupported_backends:
        names = ", ".join(unsupported_backends)
        raise SlurmScriptError(
            f"Unsupported resources.native backend namespaces for Slurm: {names}"
        )
    _native_directives(resources)


def _directive_path(value: PurePath, *, name: str) -> str:
    if not isinstance(value, PurePath):
        raise TypeError(f"Slurm {name} path must be a path")
    rendered = str(value)
    if (
        not value.is_absolute()
        or "\x00" in rendered
        or any(character.isspace() for character in rendered)
    ):
        raise SlurmScriptError(f"Slurm {name} path must be absolute and safe")
    return rendered


def _native_value(name: str, value: NativeValue) -> str:
    if type(value) not in (str, int) or type(value) is bool:
        raise SlurmScriptError(
            f"resources.native.slurm.{name} must be a string or integer"
        )
    rendered = str(value)
    if _SAFE_NATIVE_VALUE.fullmatch(rendered) is None:
        raise SlurmScriptError(
            f"resources.native.slurm.{name} contains an unsafe directive value"
        )
    return rendered


def _slurm_duration(value: timedelta) -> str:
    total_microseconds = (
        value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
    )
    total_seconds = (total_microseconds + 999_999) // 1_000_000
    days, remainder = divmod(total_seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, seconds = divmod(remainder, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock


def _references(
    references: tuple[SchedulerReference, ...],
) -> tuple[SchedulerReference, ...]:
    if not isinstance(references, tuple) or any(
        type(reference) is not SchedulerReference for reference in references
    ):
        raise TypeError("Slurm references must be a tuple of SchedulerReference values")
    if len(set(references)) != len(references):
        raise ValueError("Slurm references must be unique")
    if any(
        _SLURM_REFERENCE.fullmatch(reference.native_id) is None
        for reference in references
    ):
        raise ValueError(
            "Slurm references must be numeric job or job_array-index identities"
        )
    return references


def _dependency(reference: SchedulerReference) -> str:
    if type(reference) is not SchedulerReference:
        raise TypeError("Slurm dependency must be a SchedulerReference")
    if re.fullmatch(r"[0-9]+", reference.native_id) is None:
        raise SlurmSubmissionError(
            "Slurm afterok dependency must be one root numeric job ID",
            outcome=SchedulerSubmissionOutcome.REJECTED,
            phase="request_validation",
        )
    return f"afterok:{reference.native_id}"


def _parse_squeue(
    output: str,
    requested: tuple[SchedulerReference, ...],
    timezone: tzinfo | None,
) -> dict[SchedulerReference, SchedulerObservation]:
    expected = {reference.native_id: reference for reference in requested}
    observations: dict[SchedulerReference, SchedulerObservation] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split("|")
        if len(fields) != 4:
            raise SlurmQueryError("squeue returned a malformed row")
        job_id, native_state, raw_start, nodes = fields
        reference = expected.get(job_id)
        if reference is None:
            continue
        if reference in observations:
            raise SlurmQueryError(f"squeue returned duplicate job {job_id}")
        state = _portable_state(native_state, None)
        metadata: dict[str, NativeValue] = {"source": "squeue"}
        if raw_start not in {"", "N/A", "Unknown", "None"}:
            metadata["native_start"] = raw_start
        if nodes not in {"", "(null)", "N/A"}:
            metadata["allocated_nodes"] = nodes
        observations[reference] = SchedulerObservation(
            reference,
            state,
            native_state,
            metadata=metadata,
            started_at=(
                _parse_timestamp(raw_start, timezone)
                if state is ExecutionState.RUNNING
                else None
            ),
        )
    return observations


def _parse_sacct(
    output: str,
    requested: tuple[SchedulerReference, ...],
    timezone: tzinfo | None,
) -> dict[SchedulerReference, SchedulerObservation]:
    expected = {reference.native_id: reference for reference in requested}
    observations: dict[SchedulerReference, SchedulerObservation] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split("|")
        if fields and fields[-1] == "":
            fields.pop()
        if len(fields) != 6:
            raise SlurmQueryError("sacct returned a malformed row")
        job_id, native_state, raw_exit, raw_start, raw_end, nodes = fields
        reference = expected.get(job_id)
        if reference is None:
            continue
        if reference in observations:
            raise SlurmQueryError(f"sacct returned duplicate job {job_id}")
        exit_code = _parse_exit_code(raw_exit)
        state = _portable_state(native_state, exit_code)
        terminal = state in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
        metadata: dict[str, NativeValue] = {"source": "sacct"}
        if raw_start not in {"", "N/A", "Unknown", "None"}:
            metadata["native_start"] = raw_start
        if raw_end not in {"", "N/A", "Unknown", "None"}:
            metadata["native_end"] = raw_end
        if nodes not in {"", "(null)", "Unknown"}:
            metadata["allocated_nodes"] = nodes
        observations[reference] = SchedulerObservation(
            reference,
            state,
            native_state,
            exit_code=exit_code if terminal else None,
            metadata=metadata,
            started_at=_parse_timestamp(raw_start, timezone),
            finished_at=(_parse_timestamp(raw_end, timezone) if terminal else None),
        )
    return observations


def _parse_scontrol(
    output: str,
    reference: SchedulerReference,
    timezone: tzinfo | None,
) -> SchedulerObservation:
    rows = tuple(line for line in output.splitlines() if line.strip())
    observations: list[SchedulerObservation] = []
    root_reference = "_" not in reference.native_id
    for row in rows:
        job_id = _scontrol_field(row, "JobId")
        array_job_id = _scontrol_field(row, "ArrayJobId")
        array_task_id = _scontrol_field(row, "ArrayTaskId")
        array_alias = (
            f"{array_job_id}_{array_task_id}"
            if array_job_id is not None and array_task_id is not None
            else None
        )
        if (
            job_id != reference.native_id
            and array_alias != reference.native_id
            and not (root_reference and array_job_id == reference.native_id)
        ):
            raise SlurmQueryError("scontrol returned a mismatched job")
        observations.append(_parse_scontrol_row(row, reference, timezone))
    if not observations:
        raise SlurmQueryError("scontrol returned a mismatched job")
    if len(observations) == 1:
        return observations[0]
    return _aggregate_scontrol_array(observations, reference)


def _parse_scontrol_row(
    output: str,
    reference: SchedulerReference,
    timezone: tzinfo | None,
) -> SchedulerObservation:
    fields = {
        name: _scontrol_field(output, name)
        for name in (
            "JobState",
            "ExitCode",
            "StartTime",
            "EndTime",
            "NodeList",
        )
    }
    native_state = fields["JobState"]
    if native_state is None:
        raise SlurmQueryError("scontrol did not return JobState")
    raw_exit = fields["ExitCode"] or "Unknown"
    exit_code = _parse_exit_code(raw_exit)
    state = _portable_state(native_state, exit_code)
    terminal = state in {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }
    raw_start = fields["StartTime"] or "Unknown"
    raw_end = fields["EndTime"] or "Unknown"
    nodes = fields["NodeList"] or "Unknown"
    metadata: dict[str, NativeValue] = {"source": "scontrol"}
    if raw_start not in {"N/A", "Unknown", "None"}:
        metadata["native_start"] = raw_start
    if raw_end not in {"N/A", "Unknown", "None"}:
        metadata["native_end"] = raw_end
    if nodes not in {"(null)", "N/A", "Unknown"}:
        metadata["allocated_nodes"] = nodes
    return SchedulerObservation(
        reference,
        state,
        native_state,
        exit_code=exit_code if terminal else None,
        metadata=metadata,
        started_at=_parse_timestamp(raw_start, timezone),
        finished_at=_parse_timestamp(raw_end, timezone) if terminal else None,
    )


def _aggregate_scontrol_array(
    observations: list[SchedulerObservation],
    reference: SchedulerReference,
) -> SchedulerObservation:
    states = {observation.state for observation in observations}
    if ExecutionState.RUNNING in states:
        state = ExecutionState.RUNNING
    elif ExecutionState.QUEUED in states or ExecutionState.SUBMITTED in states:
        state = ExecutionState.QUEUED
    elif ExecutionState.UNKNOWN in states:
        state = ExecutionState.UNKNOWN
    elif ExecutionState.FAILED in states:
        state = ExecutionState.FAILED
    elif ExecutionState.CANCELLED in states:
        state = ExecutionState.CANCELLED
    else:
        state = ExecutionState.SUCCEEDED
    terminal = state in {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }
    native_states = sorted({item.native_state for item in observations})
    exit_codes = tuple(
        item.exit_code for item in observations if item.exit_code is not None
    )
    nonzero_exit = next((code for code in exit_codes if code != 0), None)
    exit_code = None
    if terminal:
        exit_code = nonzero_exit if nonzero_exit is not None else 0
        if state is ExecutionState.FAILED and exit_code == 0:
            exit_code = 1
    metadata: dict[str, NativeValue] = {
        "source": "scontrol",
        "array_elements": len(observations),
    }
    for name, reducer in (("native_start", min), ("native_end", max)):
        values = tuple(
            str(item.metadata[name]) for item in observations if name in item.metadata
        )
        if values:
            metadata[name] = reducer(values)
    nodes = sorted(
        {
            str(item.metadata["allocated_nodes"])
            for item in observations
            if "allocated_nodes" in item.metadata
        }
    )
    if nodes:
        metadata["allocated_nodes"] = ",".join(nodes)
    starts = tuple(
        item.started_at for item in observations if item.started_at is not None
    )
    finishes = tuple(
        item.finished_at for item in observations if item.finished_at is not None
    )
    return SchedulerObservation(
        reference,
        state,
        ",".join(native_states),
        exit_code=exit_code,
        metadata=metadata,
        started_at=min(starts) if starts else None,
        finished_at=max(finishes) if terminal and finishes else None,
    )


def _scontrol_field(output: str, name: str) -> str | None:
    match = re.search(rf"(?:^|\s){re.escape(name)}=(\S+)", output)
    return None if match is None else match.group(1)


def _portable_state(native_state: str, exit_code: int | None) -> ExecutionState:
    normalized = native_state.strip().upper().removesuffix("+").split(" ", 1)[0]
    if not normalized:
        raise SlurmQueryError("Slurm returned a blank native state")
    if normalized in {"PENDING", "CONFIGURING", "RESIZING", "REQUEUED"}:
        return ExecutionState.QUEUED
    if normalized in {"RUNNING", "COMPLETING", "SUSPENDED", "STAGE_OUT"}:
        return ExecutionState.RUNNING
    if normalized == "COMPLETED":
        return (
            ExecutionState.SUCCEEDED
            if exit_code in {None, 0}
            else ExecutionState.FAILED
        )
    if normalized == "CANCELLED":
        return ExecutionState.CANCELLED
    if normalized in {
        "BOOT_FAIL",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "REVOKED",
        "SPECIAL_EXIT",
        "TIMEOUT",
    }:
        return ExecutionState.FAILED
    return ExecutionState.UNKNOWN


def _parse_exit_code(value: str) -> int | None:
    if value in {"", "N/A", "Unknown"}:
        return None
    match = re.fullmatch(r"(-?[0-9]+):[0-9]+", value)
    if match is None:
        raise SlurmQueryError("sacct returned an invalid exit code")
    return int(match.group(1))


def _parse_timestamp(value: str, timezone: tzinfo | None) -> datetime | None:
    if value in {"", "N/A", "Unknown", "None"}:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise SlurmQueryError("Slurm returned an invalid timestamp") from error
    if parsed.tzinfo is not None:
        return parsed
    return None if timezone is None else parsed.replace(tzinfo=timezone)
