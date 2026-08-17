from __future__ import annotations

import pytest

from rundra.domain.scaling import (
    ExecutionPolicy,
    SeedRange,
    TaskSpace,
    WorkerPoolPolicy,
)


def test_task_space_derives_large_ordinals_without_materializing_tasks() -> None:
    space = TaskSpace(2, SeedRange(0, 49_999_999))

    assert space.task_count == 100_000_000
    assert str(space.coordinate(0).task_id) == "task_000000"
    boundary = space.coordinate(49_999_999)
    assert boundary.parameter_set_ordinal == 0
    assert boundary.seed == 49_999_999
    final = space.coordinate(99_999_999)
    assert str(final.task_id) == "task_99999999"
    assert final.parameter_set_ordinal == 1
    assert final.seed == 49_999_999


def test_task_space_pages_are_bounded_and_parameter_major() -> None:
    space = TaskSpace(3, SeedRange(10, 14, 2))

    page = space.page(offset=2, limit=4)

    assert [(item.parameter_set_ordinal, item.seed) for item in page] == [
        (0, 14),
        (1, 10),
        (1, 12),
        (1, 14),
    ]
    assert space.page(offset=space.task_count, limit=10) == ()


@pytest.mark.parametrize(
    ("start", "stop", "step"),
    [(3, 2, 1), (0, 3, 2)],
)
def test_seed_range_rejects_non_deterministic_bounds(
    start: int, stop: int, step: int
) -> None:
    with pytest.raises(ValueError):
        SeedRange(start, stop, step)


def test_execution_policy_enforces_cross_field_safety_bounds() -> None:
    worker = WorkerPoolPolicy(100, 8, 20, 2, 4)

    with pytest.raises(ValueError, match="confirmation_threshold"):
        ExecutionPolicy(10, 11, 8, 4, 5, 5, worker)
    with pytest.raises(ValueError, match="activation_threshold"):
        ExecutionPolicy(10, 5, 8, 4, 5, 5, worker)
