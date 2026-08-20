from __future__ import annotations

from pathlib import Path

import pytest

from rundra.domain.models import RunId, TaskId
from rundra.domain.scaling import SeedRange, TaskSpace
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.persistence import SqliteTaskStore, TaskState
from rundra.persistence.errors import RunStoreError

_RUN_ID = RunId("run_1234567890abcdef1234567890abcdef")


def test_sparse_task_store_initializes_and_pages_one_hundred_million_tasks(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path)
    space = TaskSpace(2, SeedRange(0, 49_999_999))

    store.create(_RUN_ID, space)
    page = store.page(_RUN_ID, offset=99_999_997, limit=3)
    counts = store.counts(_RUN_ID)

    assert store.path(_RUN_ID).stat().st_size < 1024 * 1024
    assert page.total == 100_000_000
    assert [item.coordinate.ordinal for item in page.tasks] == [
        99_999_997,
        99_999_998,
        99_999_999,
    ]
    assert counts.execution[ExecutionState.CREATED] == 100_000_000
    assert counts.retrieval[RetrievalState.NOT_REQUESTED] == 100_000_000


def test_task_store_updates_batches_and_preserves_individual_identity(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path)
    space = TaskSpace(2, SeedRange(7, 9))
    store.create(_RUN_ID, space)
    first = space.coordinate(0)
    final = space.coordinate(5)

    store.update_batch(
        _RUN_ID,
        (
            TaskState(
                first,
                ExecutionState.FAILED,
                scheduler_id="800_0",
                native_state="FAILED",
                exit_code=7,
            ),
            TaskState(
                final,
                ExecutionState.CANCELLED,
                scheduler_id="801_2",
                native_state="CANCELLED",
            ),
        ),
    )

    assert store.get(_RUN_ID, 0).exit_code == 7
    assert store.get(_RUN_ID, 5).scheduler_id == "801_2"
    counts = store.counts(_RUN_ID)
    assert counts.execution[ExecutionState.CREATED] == 4
    assert counts.execution[ExecutionState.FAILED] == 1
    assert counts.execution[ExecutionState.CANCELLED] == 1


def test_task_store_rejects_invalid_transitions_and_unbounded_pages(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path)
    space = TaskSpace(1, SeedRange(0, 1))
    store.create(_RUN_ID, space)
    coordinate = space.coordinate(0)
    for state in (
        ExecutionState.STAGING,
        ExecutionState.RUNNING,
        ExecutionState.SUCCEEDED,
    ):
        store.update_batch(_RUN_ID, (TaskState(coordinate, state),))

    with pytest.raises(RunStoreError, match="Invalid execution state transition"):
        store.update_batch(_RUN_ID, (TaskState(coordinate, ExecutionState.RUNNING),))
    with pytest.raises(ValueError, match="between 1 and 1000"):
        store.page(_RUN_ID, limit=1001)


def test_task_store_initializes_compact_submission_and_retrieval(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path)
    space = TaskSpace(1, SeedRange(0, 2))
    store.create(_RUN_ID, space)

    store.initialize_submission(
        _RUN_ID,
        {
            TaskId.from_ordinal(0): "42_0",
            TaskId.from_ordinal(1): "42_1",
            TaskId.from_ordinal(2): "42_0",
        },
    )
    store.set_retrieval(_RUN_ID, (TaskId.from_ordinal(1),), RetrievalState.PENDING)

    assert [state.scheduler_id for state in store.all_states(_RUN_ID)] == [
        "42_0",
        "42_1",
        "42_0",
    ]
    assert store.get(_RUN_ID, 1).retrieval_state is RetrievalState.PENDING


def test_task_store_expands_bounded_worker_assignment_transactionally(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path)
    space = TaskSpace(2, SeedRange(0, 4))
    store.create(_RUN_ID, space)

    store.initialize_compact_submission(
        _RUN_ID,
        ("91_0", "91_1", "91_2"),
        scheduler_job_ids=("91",),
    )

    assert store.submission_job_ids(_RUN_ID) == ("91",)
    assert [state.scheduler_id for state in store.all_states(_RUN_ID)] == [
        "91_0",
        "91_1",
        "91_2",
        "91_0",
        "91_1",
        "91_2",
        "91_0",
        "91_1",
        "91_2",
        "91_0",
    ]
