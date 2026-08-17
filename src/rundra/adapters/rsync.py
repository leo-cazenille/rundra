from __future__ import annotations

import hashlib
import os
import secrets
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from fnmatch import fnmatchcase
from pathlib import Path, PurePath, PurePosixPath

from rundra.adapters._local_paths import (
    UnsafeLocalPathError,
    reject_destination_tree_symlinks,
    resolve_write_destination,
)
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
from rundra.security import is_safe_ssh_destination
from rundra.sync import with_default_sync_excludes


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
        workspace = self._allocator.create(
            request.run_id,
            request.target.workspace,
            task_ids=request.task_ids,
        )

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

    def publish_verified_file(
        self,
        source: Path,
        destination: PurePath,
        sha256: str,
    ) -> str:
        """Publish one verified immutable file into a remote content cache."""
        if self._host is None:
            raise RsyncUploadError("Verified file publication requires a host")
        if not isinstance(source, Path) or source.is_symlink() or not source.is_file():
            raise RsyncUploadError("Verified file source must be a regular file")
        if (
            not isinstance(destination, PurePath)
            or not destination.is_absolute()
            or destination == PurePath("/")
            or "\x00" in str(destination)
        ):
            raise RsyncUploadError(
                "Verified file destination must be absolute and safe"
            )
        if (
            type(sha256) is not str
            or len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256)
        ):
            raise RsyncUploadError("Verified file SHA-256 is invalid")
        if _file_sha256(source) != sha256:
            raise RsyncUploadError("Verified file source digest does not match")
        self.check()
        parent = destination.parent
        temporary = parent / f".{destination.name}.tmp-{secrets.token_hex(8)}"
        lock = parent / f".{destination.name}.lock"
        _require_remote_success(
            self._transport,
            Command(("mkdir", "-p", "--", str(parent))),
            "create remote cache directory",
        )
        existing = _remote_file_digest(self._transport, destination)
        if existing is not None:
            if existing != sha256:
                raise RsyncUploadError(
                    "Existing target cache entry has the wrong digest"
                )
            return "reuse_target_image_cache"
        self._upload(
            (
                self._executable,
                "--archive",
                "--protect-args",
                "--",
                str(source),
                _remote_destination(self._host, temporary, directory=False),
            ),
            kind="verified file",
            run_id="preparation",
        )
        acquired = False
        try:
            _require_remote_success(
                self._transport,
                Command(
                    (
                        "/bin/sh",
                        "-c",
                        'attempt=0; while ! mkdir -- "$1" 2>/dev/null; do '
                        'attempt=$((attempt + 1)); [ "$attempt" -lt 900 ] || '
                        "exit 75; sleep 1; done",
                        "rundra-cache-lock",
                        str(lock),
                    )
                ),
                "acquire remote cache lock",
            )
            acquired = True
            existing = _remote_file_digest(self._transport, destination)
            if existing is not None:
                if existing != sha256:
                    raise RsyncUploadError(
                        "Existing target cache entry has the wrong digest"
                    )
                _require_remote_success(
                    self._transport,
                    Command(("rm", "-f", "--", str(temporary))),
                    "remove redundant remote transfer",
                )
                return "reuse_target_image_cache"
            transferred = _remote_file_digest(self._transport, temporary)
            if transferred != sha256:
                raise RsyncUploadError("Transferred target file digest does not match")
            _require_remote_success(
                self._transport,
                Command(("chmod", "a-w", "--", str(temporary))),
                "seal remote cache file",
            )
            _require_remote_success(
                self._transport,
                Command(("mv", "--", str(temporary), str(destination))),
                "publish remote cache file",
            )
            return "transfer_image_to_target"
        finally:
            if acquired:
                try:
                    self._transport.run(Command(("rmdir", "--", str(lock))))
                except Exception:
                    pass

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
        try:
            reject_destination_tree_symlinks(destination)
        except UnsafeLocalPathError as error:
            raise RsyncRetrievalError(str(error)) from error
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
    return with_default_sync_excludes(normalized)


def _target_host(options: Mapping[str, NativeValue]) -> str:
    value = options.get("host")
    if type(value) is not str:
        raise RsyncStagerError("SSH target requires a host alias for rsync")
    return _safe_host(value)


def _safe_host(value: str) -> str:
    if not is_safe_ssh_destination(value):
        raise RsyncStagerError("SSH target host cannot be represented safely for rsync")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_file_digest(transport: Transport, path: PurePath) -> str | None:
    try:
        link = transport.run(Command(("test", "-L", str(path))))
        if link.exit_code == 0:
            raise RsyncUploadError("Target cache path is a symbolic link")
        regular = transport.run(Command(("test", "-f", str(path))))
        if regular.exit_code != 0:
            return None
        result = transport.run(Command(("sha256sum", "--", str(path))))
    except Exception as error:
        raise RsyncUploadError("Could not inspect target cache file") from error
    if result.exit_code != 0:
        raise RsyncUploadError("Could not hash target cache file")
    digest = result.stdout.strip().split(maxsplit=1)[0]
    if len(digest) != 64:
        raise RsyncUploadError("Target cache file returned an invalid digest")
    return digest


def _require_remote_success(
    transport: Transport,
    command: Command,
    operation: str,
) -> None:
    try:
        result = transport.run(command)
    except Exception as error:
        raise RsyncUploadError(f"Could not {operation}") from error
    if result.exit_code != 0:
        raise RsyncUploadError(f"Could not {operation}")


def _remote_destination(host: str, path: PurePath, *, directory: bool) -> str:
    value = str(path)
    suffix = "/" if directory else ""
    return f"{host}:{value}{suffix}"


def _seal_command(source: PurePath, inputs: PurePath) -> Command:
    return Command(("chmod", "-R", "a-w", "--", str(source), str(inputs)))


def _fetch_destination(value: PurePath) -> Path:
    try:
        destination = resolve_write_destination(value)
    except UnsafeLocalPathError as error:
        raise RsyncRetrievalError(str(error)) from error
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
