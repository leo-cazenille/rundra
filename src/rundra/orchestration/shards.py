from __future__ import annotations

import hashlib
import json
import os
import socket
import tarfile
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import cast


class ShardError(RuntimeError):
    """An immutable output shard is unsafe, corrupt, or inconsistent."""


@dataclass(frozen=True, slots=True)
class IndexedShardMember:
    path: PurePosixPath
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ShardIndex:
    lease_ordinal: int
    task_start: int
    task_stop: int
    task_exit_codes: Mapping[str, int]
    members: tuple[IndexedShardMember, ...]


def ensure_computation_host(hostname: str | None = None) -> None:
    """Reject framework-owned computation on the Shoal SSH controller."""

    selected = (hostname or socket.gethostname()).split(".", 1)[0].casefold()
    if selected == "fishvision":
        raise ShardError(
            "Shard verification/extraction must run on bigfish or a scheduled "
            "Shoal compute node, never fishvision"
        )


def read_shard_index(path: Path, *, hostname: str | None = None) -> ShardIndex:
    """Read and validate only the bounded index member of one output shard."""

    ensure_computation_host(hostname)
    if not path.is_file() or path.is_symlink():
        raise ShardError(f"Output shard is not a regular file: {path}")
    try:
        with tarfile.open(path, mode="r:") as archive:
            member = archive.getmember("index.json")
            if not member.isfile() or member.size > 16 * 1024 * 1024:
                raise ShardError("Output shard index is not a bounded regular file")
            stream = archive.extractfile(member)
            if stream is None:
                raise ShardError("Output shard index cannot be read")
            value: object = json.loads(stream.read().decode("utf-8"))
    except (
        OSError,
        tarfile.TarError,
        KeyError,
        UnicodeError,
        json.JSONDecodeError,
    ) as error:
        raise ShardError(f"Could not read output shard index: {error}") from error
    return _parse_index(value)


def extract_shard(
    path: Path,
    destination: Path,
    *,
    task_ids: Sequence[str] | None = None,
    hostname: str | None = None,
) -> tuple[Path, ...]:
    """Verify and extract selected ordinary files without trusting tar paths."""

    index = read_shard_index(path, hostname=hostname)
    selected = None if task_ids is None else frozenset(task_ids)
    if selected is not None:
        unknown = selected - set(index.task_exit_codes)
        if unknown:
            raise ShardError(f"Selected Task is not in shard: {sorted(unknown)[0]}")
    requested = tuple(
        member
        for member in index.members
        if selected is None or member.path.parts[0] in selected
    )
    destination.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ShardError("Shard extraction destination must not be a symlink")
    extracted: list[Path] = []
    try:
        with tarfile.open(path, mode="r:") as archive:
            archive_members = {member.name: member for member in archive.getmembers()}
            for indexed in requested:
                member = archive_members.get(indexed.path.as_posix())
                if (
                    member is None
                    or not member.isfile()
                    or member.issym()
                    or member.islnk()
                ):
                    raise ShardError(f"Unsafe or missing shard member: {indexed.path}")
                if member.size != indexed.size_bytes:
                    raise ShardError(f"Shard member size mismatch: {indexed.path}")
                stream = archive.extractfile(member)
                if stream is None:
                    raise ShardError(f"Shard member cannot be read: {indexed.path}")
                target = destination.joinpath(*indexed.path.parts)
                _reject_parent_symlinks(destination, target.parent)
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary: Path | None = None
                try:
                    with tempfile.NamedTemporaryFile(
                        dir=target.parent,
                        prefix=f".{target.name}.",
                        suffix=".tmp",
                        delete=False,
                    ) as output:
                        temporary = Path(output.name)
                        digest = hashlib.sha256()
                        size = 0
                        while block := stream.read(1024 * 1024):
                            output.write(block)
                            digest.update(block)
                            size += len(block)
                        output.flush()
                        os.fsync(output.fileno())
                    if (
                        size != indexed.size_bytes
                        or digest.hexdigest() != indexed.sha256
                    ):
                        raise ShardError(
                            f"Shard member digest mismatch: {indexed.path}"
                        )
                    os.replace(temporary, target)
                    target.chmod(0o444)
                    extracted.append(target)
                finally:
                    if temporary is not None:
                        temporary.unlink(missing_ok=True)
    except (OSError, tarfile.TarError) as error:
        raise ShardError(f"Could not extract output shard: {error}") from error
    return tuple(extracted)


def _parse_index(value: object) -> ShardIndex:
    if not isinstance(value, dict) or set(value) != {
        "format_version",
        "lease",
        "tasks",
        "members",
    }:
        raise ShardError("Output shard index has invalid fields")
    if value["format_version"] != 1:
        raise ShardError("Output shard index version is unsupported")
    lease = value["lease"]
    tasks = value["tasks"]
    members = value["members"]
    if not isinstance(lease, dict) or set(lease) != {
        "ordinal",
        "task_start",
        "task_stop",
    }:
        raise ShardError("Output shard lease index is invalid")
    if not isinstance(tasks, list) or not isinstance(members, list):
        raise ShardError("Output shard Tasks and members must be arrays")
    parsed_tasks: dict[str, int] = {}
    for task in cast(list[object], tasks):
        if not isinstance(task, dict) or set(task) != {
            "task_id",
            "ordinal",
            "exit_code",
            "timed_out",
        }:
            raise ShardError("Output shard Task index is invalid")
        task_id = task["task_id"]
        exit_code = task["exit_code"]
        if type(task_id) is not str or type(exit_code) is not int:
            raise ShardError("Output shard Task identity is invalid")
        parsed_tasks[task_id] = exit_code
    parsed_members: list[IndexedShardMember] = []
    for item in cast(list[object], members):
        if not isinstance(item, dict) or set(item) != {
            "path",
            "size_bytes",
            "sha256",
        }:
            raise ShardError("Output shard member index is invalid")
        raw_path, size, digest = item["path"], item["size_bytes"], item["sha256"]
        if (
            type(raw_path) is not str
            or type(size) is not int
            or type(digest) is not str
        ):
            raise ShardError("Output shard member values are invalid")
        relative = PurePosixPath(raw_path)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ShardError("Output shard member path is unsafe")
        if (
            size < 0
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ShardError("Output shard member size or digest is invalid")
        if relative.parts[0] not in parsed_tasks:
            raise ShardError("Output shard member does not belong to an indexed Task")
        parsed_members.append(IndexedShardMember(relative, size, digest))
    try:
        return ShardIndex(
            lease_ordinal=_index_integer(lease["ordinal"]),
            task_start=_index_integer(lease["task_start"]),
            task_stop=_index_integer(lease["task_stop"]),
            task_exit_codes=parsed_tasks,
            members=tuple(parsed_members),
        )
    except (TypeError, ValueError) as error:
        raise ShardError(f"Output shard lease values are invalid: {error}") from error


def _index_integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("expected a non-negative integer")
    return value


def _reject_parent_symlinks(root: Path, parent: Path) -> None:
    relative = parent.relative_to(root)
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ShardError(f"Shard extraction parent is a symlink: {current}")
