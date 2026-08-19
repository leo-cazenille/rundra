from __future__ import annotations

import json
import os
import stat
import tempfile
from pathlib import Path

from rundra.domain.models import Artifact, ArtifactKind
from rundra.ports import FetchRequest, FetchResult


def write_reference_manifest(
    request: FetchRequest,
    destination: Path,
) -> FetchResult:
    """Atomically publish a constant-size reference to a visible Run workspace."""
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
        payload = (
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
            + b"\n"
        )
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
