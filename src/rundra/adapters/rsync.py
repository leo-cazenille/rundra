from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from fnmatch import fnmatchcase
from pathlib import Path, PurePath, PurePosixPath

from rundra.adapters.remote import (
    RemoteWorkspaceAllocator,
    RemoteWorkspaceError,
    validate_remote_workspace,
)
from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    Command,
    NativeValue,
    TaskId,
)
from rundra.ports import (
    CapabilityCheck,
    FetchRequest,
    FetchResult,
    StagedWorkspace,
    StageRequest,
    Transport,
)

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


class RsyncStagerError(RuntimeError):
    """Base class for actionable rsync staging failures."""


class RsyncUnavailableError(RsyncStagerError):
    """Raised when the configured rsync executable cannot be discovered."""


class RsyncUploadError(RsyncStagerError):
    """Raised when source or input upload does not complete."""


class RsyncRetrievalError(RsyncStagerError):
    """Raised when remote output retrieval does not complete safely."""


class RsyncStager:
    """Upload a live source tree into one isolated remote workspace."""

    def __init__(
        self,
        transport: Transport,
        *,
        host: str | None = None,
        executable: str = "rsync",
    ) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("RsyncStager requires a Transport")
        if type(executable) is not str:
            raise TypeError("rsync executable must be a string")
        if not executable.strip():
            raise ValueError("rsync executable must not be blank")
        if "\x00" in executable:
            raise ValueError("rsync executable must not contain NUL")
        self._transport = transport
        self._host = None if host is None else _safe_host(host)
        self._executable = executable
        self._allocator = RemoteWorkspaceAllocator(transport)

    def check(self) -> CapabilityCheck:
        """Confirm that the configured local rsync executable is discoverable."""
        try:
            resolved = shutil.which(self._executable)
        except OSError as error:
            raise RsyncUnavailableError(
                f"Could not search for rsync executable {self._executable!r}"
            ) from error
        if resolved is None:
            raise RsyncUnavailableError(
                f"rsync executable {self._executable!r} was not found on PATH"
            )
        return CapabilityCheck("rsync")

    def stage(self, request: StageRequest) -> StagedWorkspace:
        """Upload exact source/config content and seal the remote snapshot."""
        if type(request) is not StageRequest:
            raise TypeError("RsyncStager.stage requires a StageRequest")
        _validate_remote_target(request)
        self.check()
        source = _source_directory(request.source_root)
        exclusions = _validated_exclusions(request.experiment.sync_excludes)
        host = _target_host(request.target.transport.options)
        if self._host is not None and self._host != host:
            raise RsyncStagerError("Configured rsync host does not match target host")
        workspace = self._allocator.create(request.run_id, request.target.workspace)

        source_argv: list[str] = [
            self._executable,
            "--archive",
            "--copy-links",
            "--delete",
            "--protect-args",
        ]
        for pattern in exclusions:
            source_argv.extend(("--exclude", pattern))
        source_argv.extend(
            (
                "--",
                f"{source}/",
                _remote_destination(host, workspace.source, directory=True),
            )
        )
        self._upload(tuple(source_argv), kind="source", run_id=str(request.run_id))

        config_content = request.config.content.encode("utf-8")
        try:
            with tempfile.NamedTemporaryFile(
                prefix="rundra-config-", suffix=".yaml"
            ) as stream:
                stream.write(config_content)
                stream.flush()
                os.fsync(stream.fileno())
                config_argv = (
                    self._executable,
                    "--archive",
                    "--protect-args",
                    "--",
                    stream.name,
                    _remote_destination(host, workspace.config, directory=False),
                )
                self._upload(
                    config_argv, kind="effective config", run_id=str(request.run_id)
                )
        except RsyncStagerError:
            raise
        except OSError as error:
            raise RsyncUploadError(
                f"Could not prepare effective config for Run {request.run_id}"
            ) from error

        try:
            seal = self._transport.run(
                _seal_command(workspace.source, workspace.inputs)
            )
        except Exception as error:
            raise RsyncUploadError(
                f"Could not seal remote snapshot for Run {request.run_id}"
            ) from error
        if seal.exit_code != 0:
            raise RsyncUploadError(
                f"Could not seal remote snapshot for Run {request.run_id}"
            )
        return StagedWorkspace(
            root=workspace.root,
            source=workspace.source,
            inputs=workspace.inputs,
            config=workspace.config,
            runtime=workspace.runtime,
            outputs=workspace.outputs,
            logs=workspace.logs,
            metadata=workspace.metadata,
            artifacts=(
                Artifact(ArtifactKind.SOURCE_SNAPSHOT, workspace.source),
                Artifact(
                    ArtifactKind.EFFECTIVE_CONFIG,
                    workspace.config,
                    size_bytes=len(config_content),
                ),
            ),
        )

    def fetch(self, request: FetchRequest) -> FetchResult:
        """Idempotently retrieve remote output, logs, and metadata."""
        if type(request) is not FetchRequest:
            raise TypeError("RsyncStager.fetch requires a FetchRequest")
        if self._host is None:
            raise RsyncRetrievalError("Independent rsync retrieval requires a host")
        try:
            validate_remote_workspace(request.workspace)
        except RemoteWorkspaceError as error:
            raise RsyncRetrievalError(str(error)) from error
        if any("\x00" in pattern for pattern in request.patterns):
            raise RsyncRetrievalError("Fetch patterns must not contain NUL")
        destination = _fetch_destination(request.destination)
        self.check()
        output_destination = destination / "output"
        logs_destination = destination / "logs"
        metadata_destination = destination / "metadata"
        for path in (output_destination, logs_destination, metadata_destination):
            try:
                path.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                raise RsyncRetrievalError(
                    "Could not create local retrieval destination"
                ) from error
            if not path.is_dir():
                raise RsyncRetrievalError(
                    "Local retrieval destination is not a directory"
                )

        output_argv: list[str] = [
            self._executable,
            "--archive",
            "--no-links",
            "--protect-args",
            "--delay-updates",
            "--prune-empty-dirs",
            "--include",
            "*/",
        ]
        for pattern in request.patterns:
            output_argv.extend(("--include", pattern))
        output_argv.extend(
            (
                "--exclude",
                "*",
                "--",
                _remote_destination(
                    self._host, request.workspace.outputs, directory=True
                ),
                f"{output_destination}/",
            )
        )
        self._retrieve(tuple(output_argv), tree="output")
        self._retrieve_tree(
            self._host,
            request.workspace.logs,
            logs_destination,
            tree="logs",
        )
        self._retrieve_tree(
            self._host,
            request.workspace.metadata,
            metadata_destination,
            tree="metadata",
        )
        return FetchResult(
            _retrieved_artifacts(
                output_destination,
                logs_destination,
                metadata_destination,
                request.patterns,
            )
        )

    def _upload(self, argv: tuple[str, ...], *, kind: str, run_id: str) -> None:
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                check=False,
                encoding="utf-8",
                errors="replace",
                shell=False,
            )
        except (OSError, ValueError):
            raise RsyncUploadError(
                f"Could not start rsync {kind} upload for Run {run_id}"
            ) from None
        if completed.returncode != 0:
            raise RsyncUploadError(
                f"rsync {kind} upload failed for Run {run_id} "
                f"with exit code {completed.returncode}"
            )

    def _retrieve_tree(
        self,
        host: str,
        source: PurePath,
        destination: Path,
        *,
        tree: str,
    ) -> None:
        self._retrieve(
            (
                self._executable,
                "--archive",
                "--no-links",
                "--protect-args",
                "--delay-updates",
                "--",
                _remote_destination(host, source, directory=True),
                f"{destination}/",
            ),
            tree=tree,
        )

    def _retrieve(self, argv: tuple[str, ...], *, tree: str) -> None:
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                check=False,
                encoding="utf-8",
                errors="replace",
                shell=False,
            )
        except (OSError, ValueError):
            raise RsyncRetrievalError(
                f"Could not start rsync {tree} retrieval"
            ) from None
        if completed.returncode != 0:
            raise RsyncRetrievalError(
                f"rsync {tree} retrieval failed with exit code {completed.returncode}"
            )


def _validate_remote_target(request: StageRequest) -> None:
    target = request.target
    if target.staging.kind != "rsync":
        raise RsyncStagerError(f"Target {target.name!r} staging backend is not rsync")
    if target.transport.kind != "ssh":
        raise RsyncStagerError(f"Target {target.name!r} transport backend is not SSH")


def _source_directory(value: object) -> Path:
    source = Path(str(value)).expanduser()
    try:
        resolved = source.resolve(strict=True)
    except OSError as error:
        raise RsyncStagerError(f"Source root does not exist: {source}") from error
    if not resolved.is_dir():
        raise RsyncStagerError(f"Source root is not a directory: {resolved}")
    return resolved


def _validated_exclusions(patterns: tuple[str, ...]) -> tuple[str, ...]:
    normalized: list[str] = []
    for pattern in patterns:
        value = pattern.removeprefix("./").rstrip("/")
        path = PurePosixPath(value)
        if not value or path.is_absolute() or ".." in path.parts or "\x00" in value:
            raise RsyncStagerError(
                "Sync exclusion must be a nonempty safe relative exclusion"
            )
        normalized.append(value)
    return (*_DEFAULT_EXCLUDES, *normalized)


def _target_host(options: Mapping[str, NativeValue]) -> str:
    value = options.get("host")
    if type(value) is not str:
        raise RsyncStagerError("SSH target requires a host alias for rsync")
    return _safe_host(value)


def _safe_host(value: str) -> str:
    if not value:
        raise RsyncStagerError("SSH target requires a host alias for rsync")
    if any(character.isspace() for character in value) or any(
        character in value for character in ("\x00", ":")
    ):
        raise RsyncStagerError("SSH target host cannot be represented safely for rsync")
    return value


def _remote_destination(host: str, path: PurePath, *, directory: bool) -> str:
    value = str(path)
    suffix = "/" if directory else ""
    return f"{host}:{value}{suffix}"


def _seal_command(source: PurePath, inputs: PurePath) -> Command:
    return Command(("chmod", "-R", "a-w", "--", str(source), str(inputs)))


def _fetch_destination(value: PurePath) -> Path:
    destination = Path(str(value)).expanduser().resolve()
    if destination == Path(destination.anchor):
        raise RsyncRetrievalError(
            "Local retrieval destination must not be filesystem root"
        )
    if destination.exists() and not destination.is_dir():
        raise RsyncRetrievalError(
            "Local retrieval destination exists and is not a directory"
        )
    return destination


def _retrieved_artifacts(
    outputs: Path,
    logs: Path,
    metadata: Path,
    patterns: tuple[str, ...],
) -> tuple[Artifact, ...]:
    artifacts: list[Artifact] = []
    for path in _regular_files(outputs):
        relative = path.relative_to(outputs).as_posix()
        if any(fnmatchcase(relative, pattern) for pattern in patterns):
            artifacts.append(
                Artifact(
                    ArtifactKind.RAW_RESULT,
                    path,
                    size_bytes=path.stat().st_size,
                )
            )
    for path in _regular_files(logs):
        task_id = _log_task_id(path)
        if task_id is None:
            continue
        kind = ArtifactKind.STDOUT if path.suffix == ".stdout" else ArtifactKind.STDERR
        artifacts.append(
            Artifact(kind, path, task_id=task_id, size_bytes=path.stat().st_size)
        )
    for path in _regular_files(metadata):
        artifacts.append(
            Artifact(
                ArtifactKind.SCHEDULER_METADATA,
                path,
                size_bytes=path.stat().st_size,
            )
        )
    return tuple(sorted(artifacts, key=lambda artifact: str(artifact.path)))


def _regular_files(root: Path) -> tuple[Path, ...]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if path.is_symlink():
            raise RsyncRetrievalError("Retrieved trees must not contain symbolic links")
        if path.is_file():
            files.append(path)
    return tuple(files)


def _log_task_id(path: Path) -> TaskId | None:
    if path.suffix not in {".stdout", ".stderr"}:
        return None
    try:
        return TaskId(path.stem)
    except (TypeError, ValueError):
        return None
