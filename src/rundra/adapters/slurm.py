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
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmission,
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


class SlurmScriptError(ValueError):
    """Raised when a normalized group cannot be represented as an sbatch script."""


class SlurmSubmissionError(RuntimeError):
    """Raised when sbatch submission fails or returns invalid structured output."""


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
                "reconciliation is implemented"
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
                "scheduler diagnostic redacted"
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

    def _submit_array_chunks(
        self,
        request: SchedulerArrayRequest,
        *,
        dependency: str | None,
    ) -> SchedulerSubmission:
        max_array_size = self.array_limit()
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
        except Exception:
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
                "Slurm array submission requires a configured log directory"
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
                f"exit code {result.exit_code}; scheduler diagnostic redacted"
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
            "Slurm afterok dependency must be one root numeric job ID"
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
    fields = {
        name: _scontrol_field(output, name)
        for name in (
            "JobId",
            "JobState",
            "ExitCode",
            "StartTime",
            "EndTime",
            "NodeList",
        )
    }
    if fields["JobId"] != reference.native_id:
        raise SlurmQueryError("scontrol returned a mismatched job")
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
