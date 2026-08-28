from __future__ import annotations

import os
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path, PurePath, PurePosixPath
from uuid import uuid4

from rundra.adapters._local_paths import (
    UnsafeLocalPathError,
    reject_destination_tree_symlinks,
    resolve_write_destination,
)
from rundra.domain.models import Artifact, ArtifactKind, Command, TaskId
from rundra.domain.states import ExecutionState
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    FetchRequest,
    FetchResult,
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmission,
    StagedWorkspace,
    StageRequest,
    Transport,
)
from rundra.sync import SyncExclusionError, is_sync_excluded, validated_sync_excludes

_WRITE_BITS = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


class LocalStagerError(RuntimeError):
    """Raised when local staging or retrieval cannot complete safely."""


class WorkspaceCollisionError(LocalStagerError):
    """Raised when a Run workspace has already been allocated."""


class LocalTransportError(RuntimeError):
    """Raised when a local argument-vector command cannot be started."""


class LocalSchedulerError(RuntimeError):
    """Raised when synchronous local scheduling cannot be represented safely."""


class LocalTransport:
    """Execute argument-vector commands directly on the local host."""

    def check(self) -> CapabilityCheck:
        """Report local process execution availability."""
        return CapabilityCheck("local")

    def run(self, command: Command) -> CommandResult:
        """Run one command without a shell and capture its textual output."""
        if type(command) is not Command:
            raise TypeError("LocalTransport.run requires a Command")
        environment = os.environ.copy()
        environment.update(command.environment)
        started_at = datetime.now(UTC)
        completed: subprocess.CompletedProcess[str] | None = None
        failure_detail: str | None = None
        try:
            completed = subprocess.run(
                command.argv,
                cwd=(
                    None
                    if command.working_directory is None
                    else str(command.working_directory)
                ),
                env=environment,
                capture_output=True,
                check=False,
                encoding="utf-8",
                errors="replace",
                shell=False,
            )
        except (OSError, ValueError) as error:
            failure_detail = type(error).__name__
            if isinstance(error, OSError) and error.errno is not None:
                failure_detail = f"{failure_detail} (errno {error.errno})"
        if failure_detail is not None:
            raise LocalTransportError(
                "Could not start local command "
                f"(argv=<redacted:{len(command.argv)}>, "
                f"environment=<redacted:{len(command.environment)}>, "
                "working_directory="
                f"{'<redacted>' if command.working_directory is not None else 'unset'}"
                f"): {failure_detail}"
            ) from None
        if completed is None:  # pragma: no cover - defensive subprocess boundary
            raise LocalTransportError("Local subprocess returned no result")
        finished_at = datetime.now(UTC)
        return CommandResult(
            command=command,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            started_at=started_at,
            finished_at=finished_at,
        )


class LocalScheduler:
    """Synchronously execute one unit through a Transport and retain its result."""

    def __init__(
        self,
        transport: Transport,
        *,
        reference_factory: Callable[[], str] | None = None,
    ) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("LocalScheduler transport must implement Transport")
        if reference_factory is not None and not callable(reference_factory):
            raise TypeError("LocalScheduler reference_factory must be callable")
        self._transport = transport
        self._reference_factory = reference_factory or _new_local_reference
        self._observations: dict[SchedulerReference, SchedulerObservation] = {}

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        """Execute exactly one M1 unit synchronously and return its reference."""
        if type(group) is not SchedulerGroup:
            raise TypeError("LocalScheduler submit requires a SchedulerGroup")
        if len(group.units) != 1:
            raise LocalSchedulerError(
                "M1 LocalScheduler requires exactly one execution unit"
            )
        unit = group.units[0]
        reference = self._allocate_references(1)[0]
        result = self._transport.run(unit.command)
        self._observations[reference] = _local_observation(reference, result)
        return SchedulerSubmission(reference, {unit.task_id: reference.native_id})

    def submit_array(self, request: SchedulerArrayRequest) -> SchedulerSubmission:
        """Execute mapped Tasks synchronously with bounded local concurrency."""
        if type(request) is not SchedulerArrayRequest:
            raise TypeError("LocalScheduler submit_array requires an array request")
        units = request.group.units
        references = self._allocate_references(len(units))
        worker_count = request.max_workers or request.max_concurrent_jobs or len(units)
        capacity = worker_count * request.task_slots_per_worker
        if request.max_concurrent_jobs is not None:
            capacity = min(capacity, request.max_concurrent_jobs)
        capacity = min(capacity, len(units))
        try:
            with ThreadPoolExecutor(
                max_workers=capacity,
                thread_name_prefix="rundra-local",
            ) as executor:
                results = tuple(
                    executor.submit(self._transport.run, unit.command) for unit in units
                )
                for reference, future in zip(references, results, strict=True):
                    self._observations[reference] = _local_observation(
                        reference, future.result()
                    )
        except Exception as error:
            raise LocalSchedulerError(
                f"Local array execution failed: {type(error).__name__}"
            ) from error
        return SchedulerSubmission(
            references[0],
            {
                unit.task_id: reference.native_id
                for unit, reference in zip(units, references, strict=True)
            },
            references[1:],
        )

    def _allocate_references(self, count: int) -> tuple[SchedulerReference, ...]:
        references: list[SchedulerReference] = []
        for _ in range(count):
            value = self._reference_factory()
            if type(value) is not str or not value:
                raise LocalSchedulerError(
                    "Local scheduler reference factory must return a nonempty string"
                )
            reference = SchedulerReference(value)
            if reference in self._observations or reference in references:
                raise LocalSchedulerError(
                    f"Local scheduler reference already exists: {reference.native_id}"
                )
            references.append(reference)
        return tuple(references)

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        """Return retained terminal observations in request order."""
        normalized = _local_references(references)
        try:
            return tuple(self._observations[reference] for reference in normalized)
        except KeyError as error:
            missing = error.args[0]
            raise LocalSchedulerError(
                f"Unknown local scheduler reference: {missing.native_id}"
            ) from error

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        """Return terminal results; synchronous local work cannot be cancelled."""
        return self.query(references)


class LocalStager:
    """Copy one isolated local source snapshot and retrieve its raw outputs."""

    def stage(self, request: StageRequest) -> StagedWorkspace:
        """Create, populate, and seal one local Run workspace."""
        if type(request) is not StageRequest:
            raise TypeError("LocalStager.stage requires a StageRequest")
        if request.target.staging.kind != "local":
            raise LocalStagerError(
                f"Target {request.target.name!r} staging backend is not local"
            )
        if request.remote_source_root is not None:
            raise LocalStagerError("Local staging cannot use a remote source root")
        patterns = _validated_patterns(request.experiment.sync_excludes)
        source = _source_directory(request.source_root)
        workspace_root = Path(str(request.target.workspace)).expanduser().resolve()
        if workspace_root == source:
            raise LocalStagerError("Local workspace root must differ from source root")
        workspace_relative = _relative_or_none(workspace_root, source)
        runs_root = workspace_root / "runs"
        run_root = runs_root / str(request.run_id)
        try:
            runs_root.mkdir(parents=True, exist_ok=True)
            run_root.mkdir()
        except FileExistsError as error:
            raise WorkspaceCollisionError(
                f"Run workspace {request.run_id} already exists at {run_root}"
            ) from error
        except OSError as error:
            raise LocalStagerError(
                f"Could not allocate Run workspace {run_root}: {error}"
            ) from error

        source_snapshot = run_root / "source"
        inputs = run_root / "input"
        config = inputs / "config.yaml"
        runtime = run_root / "runtime"
        outputs = run_root / "output"
        logs = run_root / "logs"
        metadata = run_root / "metadata"
        try:
            shutil.copytree(
                source,
                source_snapshot,
                ignore=_copy_ignore(source, patterns, workspace_relative),
                symlinks=False,
            )
            for directory in (inputs, runtime, outputs, logs, metadata):
                directory.mkdir()
            for task_id in request.task_ids:
                for directory in (runtime / str(task_id), outputs / str(task_id)):
                    directory.mkdir()
            config_content = request.config.content.encode("utf-8")
            with config.open("wb") as stream:
                stream.write(config_content)
                stream.flush()
                os.fsync(stream.fileno())
            task_config_paths: dict[TaskId, Path] = {}
            for task_id, task_config in request.task_configs.items():
                task_path = inputs / f"{task_id}.yaml"
                task_path.write_text(task_config.content, encoding="utf-8")
                task_config_paths[task_id] = task_path
            manifest_path = metadata / "tasks.json"
            if request.task_manifest is not None:
                manifest_path.write_text(request.task_manifest + "\n", encoding="utf-8")
            _seal(source_snapshot)
            _seal(inputs)
        except Exception as error:
            _remove_partial_workspace(run_root)
            if isinstance(error, LocalStagerError):
                raise
            raise LocalStagerError(
                f"Could not stage Run {request.run_id}: {error}"
            ) from error

        return StagedWorkspace(
            root=run_root,
            source=source_snapshot,
            inputs=inputs,
            config=config,
            runtime=runtime,
            outputs=outputs,
            logs=logs,
            metadata=metadata,
            artifacts=(
                Artifact(ArtifactKind.SOURCE_SNAPSHOT, source_snapshot),
                Artifact(
                    ArtifactKind.EFFECTIVE_CONFIG,
                    config,
                    size_bytes=len(config_content),
                ),
                *(
                    (
                        Artifact(
                            ArtifactKind.PROVENANCE_METADATA,
                            manifest_path,
                            size_bytes=len(request.task_manifest.encode("utf-8")) + 1,
                        ),
                    )
                    if request.task_manifest is not None
                    else ()
                ),
            ),
            task_configs=task_config_paths,
        )

    def fetch(self, request: FetchRequest) -> FetchResult:
        """Idempotently copy requested raw outputs to a local destination."""
        if type(request) is not FetchRequest:
            raise TypeError("LocalStager.fetch requires a FetchRequest")
        root = _existing_directory(request.workspace.root, name="Run workspace")
        outputs = _existing_directory(
            request.workspace.outputs, name="output directory"
        )
        try:
            outputs.relative_to(root)
        except ValueError as error:
            raise LocalStagerError(
                "Output directory escapes the Run workspace"
            ) from error
        try:
            destination = resolve_write_destination(request.destination)
            reject_destination_tree_symlinks(destination)
        except UnsafeLocalPathError as error:
            raise LocalStagerError(str(error)) from error
        if _is_relative_to(destination, root):
            raise LocalStagerError(
                "Fetch destination must remain outside the Run workspace"
            )
        try:
            destination.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise LocalStagerError(
                f"Could not create fetch destination {destination}: {error}"
            ) from error
        if not destination.is_dir():
            raise LocalStagerError(
                f"Fetch destination is not a directory: {destination}"
            )

        matches = _matching_output_files(outputs, request.patterns)
        artifacts: list[Artifact] = []
        for source in matches:
            relative = source.relative_to(outputs)
            target = destination / relative
            size_bytes = _copy_file_atomically(source, target)
            artifacts.append(
                Artifact(
                    ArtifactKind.RAW_RESULT,
                    target,
                    size_bytes=size_bytes,
                )
            )
        return FetchResult(tuple(artifacts))


def _local_observation(
    reference: SchedulerReference, result: CommandResult
) -> SchedulerObservation:
    state = ExecutionState.SUCCEEDED if result.exit_code == 0 else ExecutionState.FAILED
    return SchedulerObservation(
        reference=reference,
        state=state,
        native_state="EXITED",
        exit_code=result.exit_code,
        metadata={"transport": "local"},
        result=result,
    )


def _source_directory(value: PurePath) -> Path:
    source = Path(str(value)).expanduser()
    try:
        resolved = source.resolve(strict=True)
    except OSError as error:
        raise LocalStagerError(f"Source root does not exist: {source}") from error
    if not resolved.is_dir():
        raise LocalStagerError(f"Source root is not a directory: {resolved}")
    return resolved


def _existing_directory(value: PurePath, *, name: str) -> Path:
    path = Path(str(value)).expanduser()
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise LocalStagerError(f"{name} does not exist: {path}") from error
    if not resolved.is_dir():
        raise LocalStagerError(f"{name} is not a directory: {resolved}")
    return resolved


def _validated_patterns(patterns: tuple[str, ...]) -> tuple[str, ...]:
    try:
        return validated_sync_excludes(patterns)
    except SyncExclusionError as error:
        raise LocalStagerError(str(error)) from error


def _copy_ignore(
    source: Path,
    patterns: tuple[str, ...],
    workspace_relative: Path | None,
) -> Callable[[str, list[str]], set[str]]:
    def ignore(directory: str, names: list[str]) -> set[str]:
        current = Path(directory)
        excluded: set[str] = set()
        for name in names:
            relative = (current / name).relative_to(source)
            if workspace_relative is not None and (
                relative == workspace_relative or workspace_relative in relative.parents
            ):
                excluded.add(name)
                continue
            if is_sync_excluded(relative, patterns):
                excluded.add(name)
        return excluded

    return ignore


def _seal(root: Path) -> None:
    paths = tuple(root.rglob("*")) + (root,)
    for path in paths:
        if path.is_symlink():
            raise LocalStagerError(
                f"Staged immutable areas must not contain symbolic links: {path}"
            )
        mode = stat.S_IMODE(path.stat().st_mode)
        path.chmod(mode & ~_WRITE_BITS)


def _remove_partial_workspace(root: Path) -> None:
    if not root.exists():
        return
    try:
        for path in (root, *root.rglob("*")):
            if not path.is_symlink():
                path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)
        shutil.rmtree(root)
    except OSError:
        pass


def _matching_output_files(
    outputs: Path,
    patterns: tuple[str, ...],
) -> tuple[Path, ...]:
    matches: list[Path] = []
    for path in outputs.rglob("*"):
        relative = PurePosixPath(path.relative_to(outputs).as_posix())
        if not any(fnmatchcase(relative.as_posix(), pattern) for pattern in patterns):
            continue
        if path.is_symlink():
            raise LocalStagerError(
                f"Refusing to fetch symbolic link from mutable outputs: {path}"
            )
        if path.is_file():
            matches.append(path)
    return tuple(sorted(matches, key=lambda path: path.relative_to(outputs).as_posix()))


def _copy_file_atomically(source: Path, target: Path) -> int:
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            shutil.copy2(source, temporary, follow_symlinks=False)
            with temporary.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            return target.stat().st_size
        finally:
            temporary.unlink(missing_ok=True)
    except OSError as error:
        raise LocalStagerError(
            f"Could not atomically fetch {source} to {target}: {error}"
        ) from error


def _relative_or_none(path: Path, parent: Path) -> Path | None:
    try:
        return path.relative_to(parent)
    except ValueError:
        return None


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _new_local_reference() -> str:
    return f"local-{uuid4().hex}"


def _local_references(value: object) -> tuple[SchedulerReference, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("Local scheduler references must be a sequence")
    references = tuple(value)
    if any(type(reference) is not SchedulerReference for reference in references):
        raise TypeError(
            "Local scheduler references must contain SchedulerReference values"
        )
    return references
