from __future__ import annotations

import hashlib
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from rundra.orchestration.shards import (
    IndexedShardMember,
    ShardError,
    ShardIndex,
    read_shard_index,
)


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
