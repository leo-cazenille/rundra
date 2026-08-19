from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rundra.domain.models import RunId, TaskId
from rundra.persistence import SubmissionReceiptStore
from rundra.persistence.errors import RunStoreError


def test_submission_receipt_transitions_atomically_to_completed(tmp_path: Path) -> None:
    store = SubmissionReceiptStore(tmp_path)
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    tasks = (TaskId.from_ordinal(0), TaskId.from_ordinal(1))
    started = datetime(2026, 8, 19, 10, 0, tzinfo=UTC)

    pending = store.begin(run_id, tasks, started)
    assert not pending.completed
    assert store.load(run_id) == pending

    completed = store.complete(
        pending,
        ("1234",),
        {tasks[0]: "1234_0", tasks[1]: "1234_1"},
        started + timedelta(seconds=1),
    )

    assert completed.completed
    assert store.load(run_id) == completed
    assert completed.scheduler_job_ids == ("1234",)


def test_submission_receipt_rejects_a_second_attempt(tmp_path: Path) -> None:
    store = SubmissionReceiptStore(tmp_path)
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    task = TaskId.from_ordinal(0)
    started = datetime(2026, 8, 19, 10, 0, tzinfo=UTC)
    store.begin(run_id, (task,), started)

    with pytest.raises(RunStoreError, match="already has"):
        store.begin(run_id, (task,), started + timedelta(seconds=1))
