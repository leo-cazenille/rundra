import os
from pathlib import Path

import pytest

from rundra.artifacts import open_result_shard
from rundra.domain.scaling import SeedRange, TaskSpace
from rundra.orchestration.shards import ShardError
from rundra.orchestration.workers import TaskOutcome, WorkerLease, seal_output_shard


def _result_shard(tmp_path: Path) -> Path:
    source = tmp_path / "source" / "task_000000" / "results"
    source.mkdir(parents=True)
    (source / "result.json").write_text('{"value":1}\n', encoding="utf-8")
    space = TaskSpace(1, SeedRange(0, 0))
    sealed = seal_output_shard(
        tmp_path / "source",
        tmp_path / "shards",
        WorkerLease(0, 0, 1),
        (TaskOutcome(space.coordinate(0), 0),),
    )
    Path(f"{sealed.path}.sha256").write_text(
        f"{sealed.sha256}  {sealed.path.name}\n", encoding="ascii"
    )
    return sealed.path


def test_public_result_shard_reader_verifies_and_reads_indexed_member(
    tmp_path: Path,
) -> None:
    path = _result_shard(tmp_path)

    shard = open_result_shard(path)

    assert shard.read_bytes("task_000000/results/result.json") == b'{"value":1}\n'
    with pytest.raises(ShardError, match="not indexed"):
        shard.read_bytes("task_000000/results/missing.json")


def test_public_result_shard_reader_rejects_archive_corruption(tmp_path: Path) -> None:
    path = _result_shard(tmp_path)
    os.chmod(path, 0o644)
    path.write_bytes(path.read_bytes() + b"corrupt")

    with pytest.raises(ShardError, match="checksum mismatch"):
        open_result_shard(path)
