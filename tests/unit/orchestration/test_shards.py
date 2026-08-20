from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from rundra.domain.scaling import SeedRange, TaskSpace
from rundra.orchestration.shards import ShardError, extract_shard, read_shard_index
from rundra.orchestration.workers import TaskOutcome, WorkerLease, seal_output_shard


def _shard(tmp_path: Path) -> Path:
    source = tmp_path / "source"
    first = source / "task_000000"
    second = source / "task_000001"
    first.mkdir(parents=True)
    second.mkdir()
    (first / "result.txt").write_text("first\n", encoding="utf-8")
    (second / "result.txt").write_text("second\n", encoding="utf-8")
    space = TaskSpace(1, SeedRange(0, 1))
    shard = seal_output_shard(
        source,
        tmp_path / "shards",
        WorkerLease(0, 0, 2),
        (
            TaskOutcome(space.coordinate(0), 0),
            TaskOutcome(space.coordinate(1), 7),
        ),
    )
    return shard.path


def test_shard_manifest_and_selected_extraction_are_verified(tmp_path: Path) -> None:
    shard = _shard(tmp_path)

    index = read_shard_index(
        shard, hostname="worker-node", controller_hostname="controller"
    )
    extracted = extract_shard(
        shard,
        tmp_path / "retrieved",
        task_ids=("task_000001",),
        hostname="worker-node",
        controller_hostname="controller",
    )

    assert index.task_exit_codes == {"task_000000": 0, "task_000001": 7}
    assert [
        path.relative_to(tmp_path / "retrieved").as_posix() for path in extracted
    ] == ["task_000001/result.txt"]
    assert extracted[0].read_text(encoding="utf-8") == "second\n"
    assert not (tmp_path / "retrieved/task_000000").exists()


def test_shard_computation_is_rejected_on_configured_controller(tmp_path: Path) -> None:
    shard = _shard(tmp_path)

    with pytest.raises(ShardError, match="configured remote controller"):
        read_shard_index(
            shard, hostname="controller", controller_hostname="controller.example"
        )


def test_shard_rejects_unknown_task_selection(tmp_path: Path) -> None:
    shard = _shard(tmp_path)

    with pytest.raises(ShardError, match="not in shard"):
        extract_shard(
            shard,
            tmp_path / "retrieved",
            task_ids=("task_999999",),
            hostname="worker-node",
            controller_hostname="controller",
        )


def test_attempt_shard_index_may_contain_no_new_tasks(tmp_path: Path) -> None:
    shard = tmp_path / "attempt.tar"
    content = b"RUNDRA_SHARD\t2\t0\t0\n"
    with tarfile.open(shard, mode="w") as archive:
        member = tarfile.TarInfo("index.tsv")
        member.size = len(content)
        archive.addfile(member, io.BytesIO(content))

    index = read_shard_index(shard)

    assert index.task_start == index.task_stop == 0
    assert index.task_exit_codes == {}
