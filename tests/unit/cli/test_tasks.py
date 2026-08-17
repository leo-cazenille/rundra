from __future__ import annotations

from pathlib import Path

from rundra.cli.operations import status_operation, tasks_operation
from rundra.cli.render import render_human, result_document
from rundra.domain.states import ExecutionState
from rundra.persistence import JsonRunStore, SqliteTaskStore, TaskState
from tests.unit.persistence.test_v4_record import _v4_record


def test_tasks_operation_returns_a_bounded_v4_page(tmp_path: Path) -> None:
    record = _v4_record()
    JsonRunStore(tmp_path).create(record)
    task_store = SqliteTaskStore(tmp_path)
    assert record.task_space is not None
    task_store.create(record.run.id, record.task_space)
    coordinate = record.task_space.coordinate(99_999_999)
    task_store.update_batch(
        record.run.id,
        (TaskState(coordinate, ExecutionState.FAILED, exit_code=9),),
    )

    result = tasks_operation(
        str(record.run.id),
        JsonRunStore(tmp_path),
        task_store,
        offset=99_999_998,
        limit=2,
    )

    assert result.ok
    document = result_document(result)
    assert document["format_version"] == 4
    assert document["tasks"]["returned"] == 2
    assert document["tasks"]["items"][1]["task_id"] == "task_99999999"
    assert document["tasks"]["items"][1]["exit_code"] == 9
    assert "total=100000000" in render_human(result)

    status = status_operation(
        str(record.run.id),
        JsonRunStore(tmp_path),
        task_store=task_store,
    )
    assert status.value is not None
    assert status.value.task_counts == {"CREATED": 99_999_999, "FAILED": 1}


def test_tasks_operation_rejects_legacy_records(tmp_path: Path) -> None:
    from tests.unit.persistence.test_json_store import _record

    record = _record()
    JsonRunStore(tmp_path).create(record)

    result = tasks_operation(
        str(record.run.id),
        JsonRunStore(tmp_path),
        SqliteTaskStore(tmp_path),
    )

    assert result.error is not None
    assert result.error.code == "TASK_PAGINATION_UNAVAILABLE"
