from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
from dataclasses import replace
from pathlib import Path, PurePath

from rundra.adapters.local import LocalStager, LocalStagerError
from rundra.domain.models import Artifact, ArtifactKind, BackendConfig
from rundra.ports import (
    CapabilityCheck,
    FetchRequest,
    FetchResult,
    StagedWorkspace,
    StageRequest,
)


class SharedStagerError(RuntimeError):
    """Raised when shared-filesystem staging would escape its declared root."""


class SharedStager:
    """Stage and retrieve directly through an explicitly shared POSIX root."""

    def __init__(self, root: PurePath) -> None:
        if (
            not isinstance(root, PurePath)
            or not root.is_absolute()
            or root == PurePath("/")
            or "\x00" in str(root)
        ):
            raise ValueError("SharedStager root must be an absolute non-root path")
        self._root = Path(str(root)).expanduser().resolve()

    def check(self) -> CapabilityCheck:
        if not self._root.is_dir():
            raise SharedStagerError(f"Shared root does not exist: {self._root}")
        return CapabilityCheck("shared-posix")

    def stage(self, request: StageRequest) -> StagedWorkspace:
        if type(request) is not StageRequest:
            raise TypeError("SharedStager.stage requires a StageRequest")
        configured = request.target.staging.options.get("root")
        if type(configured) is not str or Path(configured).resolve() != self._root:
            raise SharedStagerError("Target shared root does not match the adapter")
        source = request.remote_source_root or request.source_root
        self._require_beneath(source, name="source root", must_exist=True)
        self._require_beneath(
            request.target.workspace, name="workspace root", must_exist=False
        )
        local_target = replace(
            request.target,
            staging=BackendConfig("local"),
        )
        local_request = replace(
            request,
            target=local_target,
            source_root=source,
            remote_source_root=None,
        )
        try:
            return LocalStager().stage(local_request)
        except LocalStagerError as error:
            raise SharedStagerError(str(error)) from error

    def fetch(self, request: FetchRequest) -> FetchResult:
        if type(request) is not FetchRequest:
            raise TypeError("SharedStager.fetch requires a FetchRequest")
        workspace = self._require_beneath(
            request.workspace.root, name="Run workspace", must_exist=True
        )
        self._require_beneath(
            request.workspace.outputs, name="output directory", must_exist=True
        )
        destination = self._require_beneath(
            request.destination, name="fetch destination", must_exist=False
        )
        if destination == workspace or destination.is_relative_to(workspace):
            raise SharedStagerError(
                "Fetch destination must remain outside the Run workspace"
            )
        mode = "reference" if request.mode == "auto" else request.mode
        if mode == "reference":
            return self._write_reference(request, destination)
        try:
            if mode == "archive":
                fetched = LocalStager().fetch(
                    replace(request, destination=destination / "output")
                )
                task_manifest = Path(request.workspace.metadata) / "tasks.json"
                metadata_target = destination / "metadata" / "tasks.json"
                metadata_target.parent.mkdir(parents=True, exist_ok=True)
                temporary = metadata_target.with_name(
                    f".{metadata_target.name}.tmp-{os.getpid()}"
                )
                try:
                    shutil.copyfile(task_manifest, temporary)
                    os.replace(temporary, metadata_target)
                finally:
                    temporary.unlink(missing_ok=True)
                return FetchResult(
                    (
                        *fetched.artifacts,
                        Artifact(
                            ArtifactKind.SCHEDULER_METADATA,
                            metadata_target,
                            size_bytes=metadata_target.stat().st_size,
                        ),
                    )
                )
            return LocalStager().fetch(request)
        except LocalStagerError as error:
            raise SharedStagerError(str(error)) from error

    def _write_reference(
        self, request: FetchRequest, destination: Path
    ) -> FetchResult:
        destination.mkdir(parents=True, exist_ok=True)
        manifest = destination / "rundra-reference.json"
        document = {
            "format_version": 1,
            "kind": "rundra-shared-reference",
            "immutable": True,
            "run_root": str(request.workspace.root),
            "output_root": str(request.workspace.outputs),
            "metadata_root": str(request.workspace.metadata),
            "log_root": str(request.workspace.logs),
            "patterns": list(request.patterns),
        }
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".rundra-reference.tmp-", dir=destination
        )
        temporary = Path(temporary_name)
        try:
            payload = json.dumps(
                document, sort_keys=True, separators=(",", ":")
            ).encode("utf-8") + b"\n"
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            os.replace(temporary, manifest)
        finally:
            temporary.unlink(missing_ok=True)
        return FetchResult(
            (
                Artifact(
                    ArtifactKind.REFERENCE_MANIFEST,
                    manifest,
                    size_bytes=manifest.stat().st_size,
                ),
            )
        )

    def publish_verified_file(
        self,
        source: Path,
        destination: PurePath,
        sha256: str,
    ) -> str:
        """Atomically publish an immutable file within the shared root."""
        if not source.is_file() or source.is_symlink():
            raise SharedStagerError("Verified file source must be a regular file")
        if len(sha256) != 64 or any(value not in "0123456789abcdef" for value in sha256):
            raise SharedStagerError("Verified file SHA-256 is invalid")
        if _sha256(source) != sha256:
            raise SharedStagerError("Verified file source digest does not match")
        target = self._require_beneath(
            destination, name="verified file destination", must_exist=False
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.is_symlink() or not target.is_file() or _sha256(target) != sha256:
                raise SharedStagerError(
                    "Existing shared cache entry has the wrong identity"
                )
            return "reuse_target_image_cache"
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.tmp-", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as output, source.open("rb") as input_:
                shutil.copyfileobj(input_, output)
                output.flush()
                os.fsync(output.fileno())
            if _sha256(temporary) != sha256:
                raise SharedStagerError("Published shared file failed verification")
            os.chmod(temporary, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return "transfer_local_image_cache"

    def _require_beneath(
        self,
        value: PurePath,
        *,
        name: str,
        must_exist: bool,
    ) -> Path:
        path = Path(str(value)).expanduser()
        try:
            resolved = path.resolve(strict=must_exist)
        except OSError as error:
            raise SharedStagerError(f"{name} does not exist: {path}") from error
        if resolved == self._root or not resolved.is_relative_to(self._root):
            raise SharedStagerError(
                f"{name} must remain beneath shared root {self._root}: {resolved}"
            )
        return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
