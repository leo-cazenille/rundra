from __future__ import annotations

import os
import shutil
import stat
import tempfile
from collections.abc import Callable
from fnmatch import fnmatchcase
from pathlib import Path, PurePath, PurePosixPath

from rundra.domain.models import Artifact, ArtifactKind
from rundra.ports import FetchRequest, FetchResult, StagedWorkspace, StageRequest

_DEFAULT_EXCLUDES = (
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".rundra",
    "*.py[cod]",
)
_WRITE_BITS = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


class LocalStagerError(RuntimeError):
    """Raised when local staging or retrieval cannot complete safely."""


class WorkspaceCollisionError(LocalStagerError):
    """Raised when a Run workspace has already been allocated."""


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
            config_content = request.config.content.encode("utf-8")
            with config.open("wb") as stream:
                stream.write(config_content)
                stream.flush()
                os.fsync(stream.fileno())
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
            ),
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
        destination = Path(str(request.destination)).expanduser().resolve()
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
    normalized: list[str] = []
    for pattern in patterns:
        value = pattern.removeprefix("./").rstrip("/")
        path = PurePosixPath(value)
        if not value or path.is_absolute() or ".." in path.parts:
            raise LocalStagerError(
                f"Sync exclusion must be a nonempty safe relative exclusion: {pattern!r}"
            )
        normalized.append(value)
    return tuple((*_DEFAULT_EXCLUDES, *normalized))


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
            candidate = PurePosixPath(relative.as_posix())
            if workspace_relative is not None and (
                relative == workspace_relative or workspace_relative in relative.parents
            ):
                excluded.add(name)
                continue
            if any(
                fnmatchcase(candidate.as_posix(), pattern)
                or fnmatchcase(candidate.name, pattern)
                for pattern in patterns
            ):
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
