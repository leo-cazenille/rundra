from __future__ import annotations

import json
import tarfile
from pathlib import Path

from rundra.domain.scaling import SeedRange, TaskSpace
from rundra.orchestration.workers import (
    TaskOutcome,
    WorkerAssignment,
    execute_worker,
)


def test_strided_worker_assignments_cover_large_task_space_exactly() -> None:
    assignments = tuple(
        WorkerAssignment(100_000_000, worker, 64, 100) for worker in range(64)
    )

    assert sum(item.assigned_task_count for item in assignments) == 100_000_000
    assert assignments[0].leases().__next__().task_start == 0
    assert tuple(assignments[63].leases())[-1].task_stop == 100_000_000


def test_worker_continues_scientific_failures_and_seals_indexed_shards(
    tmp_path: Path,
) -> None:
    task_space = TaskSpace(1, SeedRange(10, 14))
    calls: list[int] = []

    def runner(coordinate: object, task_root: Path) -> TaskOutcome:
        from rundra.domain.scaling import TaskCoordinate

        assert isinstance(coordinate, TaskCoordinate)
        calls.append(coordinate.ordinal)
        (task_root / "result.txt").write_text(
            f"seed={coordinate.seed}\n", encoding="utf-8"
        )
        return TaskOutcome(coordinate, 7 if coordinate.ordinal == 1 else 0)

    result = execute_worker(
        task_space,
        WorkerAssignment(5, 0, 1, 3),
        workspace=tmp_path,
        runner=runner,
    )

    assert calls == [0, 1, 2, 3, 4]
    assert result.completed_tasks == 5
    assert result.scientific_failures == 1
    assert result.completed_leases == 2
    assert not result.needs_requeue
    assert all(shard.path.stat().st_mode & 0o222 == 0 for shard in result.shards)
    with tarfile.open(result.shards[0].path, mode="r") as archive:
        index = json.loads(archive.extractfile("index.json").read())
    assert [item["exit_code"] for item in index["tasks"]] == [0, 7, 0]
    assert len(index["members"]) == 3
    journal = (tmp_path / "worker-000000.jsonl").read_text(encoding="utf-8")
    assert len(journal.splitlines()) == 5


def test_worker_requests_requeue_before_starting_an_unsealable_lease(
    tmp_path: Path,
) -> None:
    task_space = TaskSpace(1, SeedRange(0, 3))
    remaining = iter((1000.0, 30.0))
    calls: list[int] = []

    def runner(coordinate: object, task_root: Path) -> TaskOutcome:
        from rundra.domain.scaling import TaskCoordinate

        assert isinstance(coordinate, TaskCoordinate)
        calls.append(coordinate.ordinal)
        (task_root / "result").write_bytes(b"ok")
        return TaskOutcome(coordinate, 0)

    result = execute_worker(
        task_space,
        WorkerAssignment(4, 0, 1, 2),
        workspace=tmp_path,
        runner=runner,
        remaining_time=lambda: next(remaining),
        allocation_guard_seconds=60,
    )

    assert calls == [0, 1]
    assert result.completed_leases == 1
    assert result.needs_requeue
