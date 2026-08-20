from __future__ import annotations

import hashlib
import json
import tarfile
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath
from typing import Any

from rundra.domain.models import TaskId
from rundra.orchestration.shards import (
    IndexedShardMember,
    ShardError,
    ShardIndex,
    read_shard_index,
)
from rundra.schema_versions import REFERENCE_MANIFEST_SCHEMA


@dataclass(frozen=True, slots=True)
class ResultShard:
    path: Path
    index: ShardIndex

    def members(self) -> tuple[PurePosixPath, ...]:
        return tuple(member.path for member in self.index.members)

    def read_bytes(
        self, member: str | PurePosixPath, *, max_bytes: int = 64 * 1024 * 1024
    ) -> bytes:
        """Read one indexed regular member after size and SHA-256 verification."""

        relative = PurePosixPath(member)
        indexed = next(
            (item for item in self.index.members if item.path == relative), None
        )
        if indexed is None:
            raise ShardError(f"Member is not indexed by output shard: {relative}")
        if indexed.size_bytes > max_bytes:
            raise ShardError(f"Output shard member exceeds read limit: {relative}")
        return _read_verified_member(self.path, indexed)


class ResultSetError(ValueError):
    """Raised when a materialized or referenced result set is unsafe."""


@dataclass(frozen=True, slots=True)
class ResultFile:
    """One regular output file exposed by a fetched result set."""

    path: Path
    relative_path: PurePosixPath
    size_bytes: int
    task_id: TaskId | None


@dataclass(frozen=True, slots=True)
class ResultSet:
    """A uniform, read-only view over copied or shared-filesystem results."""

    output_root: Path
    metadata_root: Path | None = None
    log_root: Path | None = None
    patterns: tuple[str, ...] = ()
    referenced: bool = False

    def iter_files(self, task_id: TaskId | str | None = None) -> tuple[ResultFile, ...]:
        """Return matching regular output files in deterministic path order."""

        selected = _coerce_task_id(task_id)
        files: list[ResultFile] = []
        for candidate in sorted(self.output_root.rglob("*")):
            if candidate.is_symlink() or not candidate.is_file():
                continue
            resolved = candidate.resolve(strict=True)
            if not resolved.is_relative_to(self.output_root):
                raise ResultSetError(f"Result file escapes output root: {candidate}")
            relative = PurePosixPath(candidate.relative_to(self.output_root).as_posix())
            owner = _task_owner(relative)
            if selected is not None and owner != selected:
                continue
            task_relative = PurePosixPath(*relative.parts[1:]) if owner else relative
            if self.patterns and not any(
                fnmatchcase(task_relative.as_posix(), pattern)
                for pattern in self.patterns
            ):
                continue
            files.append(
                ResultFile(resolved, relative, candidate.stat().st_size, owner)
            )
        return tuple(files)


def open_result_set(path: Path) -> ResultSet:
    """Open copied results or a Rundra shared-filesystem reference manifest."""

    selected = path.expanduser().absolute()
    manifest = selected / "rundra-reference.json" if selected.is_dir() else selected
    if manifest.name == "rundra-reference.json" and manifest.exists():
        return _open_reference_manifest(manifest)
    output_root = selected / "output"
    if not output_root.is_dir() or output_root.is_symlink():
        raise ResultSetError(
            f"Materialized result output directory is missing: {output_root}"
        )
    return ResultSet(
        output_root=_safe_root(output_root, "output_root"),
        metadata_root=_optional_materialized_root(selected / "metadata"),
        log_root=_optional_materialized_root(selected / "logs"),
    )


def open_result_shard(path: Path) -> ResultShard:
    """Open a Rundra result shard after verifying its whole-archive checksum."""

    checksum = Path(f"{path}.sha256")
    if not checksum.is_file() or checksum.is_symlink():
        raise ShardError(f"Output shard checksum is missing: {checksum}")
    fields = checksum.read_text(encoding="ascii").split()
    if (
        not fields
        or len(fields[0]) != 64
        or any(c not in "0123456789abcdef" for c in fields[0])
    ):
        raise ShardError("Output shard checksum is invalid")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    if digest.hexdigest() != fields[0]:
        raise ShardError("Output shard checksum mismatch")
    return ResultShard(path, read_shard_index(path))


def _open_reference_manifest(manifest: Path) -> ResultSet:
    if not manifest.is_file() or manifest.is_symlink():
        raise ResultSetError(f"Reference manifest is not a regular file: {manifest}")
    try:
        document = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ResultSetError(f"Reference manifest cannot be read: {error}") from error
    if not isinstance(document, dict):
        raise ResultSetError("Reference manifest must contain a JSON object")
    expected = {
        "format_version",
        "kind",
        "immutable",
        "run_root",
        "output_root",
        "metadata_root",
        "log_root",
        "patterns",
    }
    if set(document) != expected:
        raise ResultSetError("Reference manifest fields do not match format version 1")
    if document["format_version"] not in REFERENCE_MANIFEST_SCHEMA.supported:
        raise ResultSetError("Reference manifest format_version is unsupported")
    if document["kind"] != "rundra-shared-reference":
        raise ResultSetError("Reference manifest kind is invalid")
    if document["immutable"] is not True:
        raise ResultSetError("Reference manifest must declare immutable=true")

    run_root = _manifest_root(document, "run_root")
    output_root = _manifest_root(document, "output_root", within=run_root)
    metadata_root = _manifest_root(document, "metadata_root", within=run_root)
    log_root = _manifest_root(document, "log_root", within=run_root)
    patterns = _manifest_patterns(document.get("patterns"))
    return ResultSet(
        output_root=output_root,
        metadata_root=metadata_root,
        log_root=log_root,
        patterns=patterns,
        referenced=True,
    )


def _manifest_root(
    document: dict[str, Any], key: str, *, within: Path | None = None
) -> Path:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ResultSetError(f"Reference manifest {key} must be a path string")
    path = Path(value)
    if not path.is_absolute():
        raise ResultSetError(f"Reference manifest {key} must be absolute")
    resolved = _safe_root(path, key)
    if within is not None and not resolved.is_relative_to(within):
        raise ResultSetError(f"Reference manifest {key} escapes run_root")
    return resolved


def _safe_root(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise ResultSetError(f"Result {label} is not a non-symlink directory: {path}")
    try:
        return path.resolve(strict=True)
    except OSError as error:
        raise ResultSetError(f"Result {label} cannot be resolved: {error}") from error


def _optional_materialized_root(path: Path) -> Path | None:
    return _safe_root(path, path.name) if path.exists() else None


def _manifest_patterns(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ResultSetError("Reference manifest patterns must be a string array")
    patterns = tuple(value)
    for pattern in patterns:
        relative = PurePosixPath(pattern)
        if (
            not pattern
            or relative.is_absolute()
            or ".." in relative.parts
            or "\\" in pattern
        ):
            raise ResultSetError(f"Reference manifest pattern is unsafe: {pattern}")
    return patterns


def _coerce_task_id(value: TaskId | str | None) -> TaskId | None:
    if value is None or isinstance(value, TaskId):
        return value
    try:
        return TaskId(value)
    except (TypeError, ValueError) as error:
        raise ResultSetError(f"Invalid Task ID: {value}") from error


def _task_owner(relative: PurePosixPath) -> TaskId | None:
    if not relative.parts or not relative.parts[0].startswith("task_"):
        return None
    try:
        return TaskId(relative.parts[0])
    except (TypeError, ValueError):
        return None


def _read_verified_member(path: Path, indexed: IndexedShardMember) -> bytes:
    try:
        with tarfile.open(path, mode="r:") as archive:
            member = archive.getmember(indexed.path.as_posix())
            if (
                not member.isfile()
                or member.issym()
                or member.islnk()
                or member.size != indexed.size_bytes
            ):
                raise ShardError(f"Unsafe or inconsistent shard member: {indexed.path}")
            stream = archive.extractfile(member)
            if stream is None:
                raise ShardError(f"Shard member cannot be read: {indexed.path}")
            content = stream.read()
    except (OSError, KeyError, tarfile.TarError) as error:
        raise ShardError(f"Could not read output shard member: {error}") from error
    if hashlib.sha256(content).hexdigest() != indexed.sha256:
        raise ShardError(f"Shard member checksum mismatch: {indexed.path}")
    return content
