import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rundra.domain.models import RunId, TaskId
from rundra.persistence import SubmissionReceiptOutcome, SubmissionReceiptStore
from rundra.persistence.errors import RunStoreError


def test_submission_receipt_transitions_atomically_to_completed(tmp_path: Path) -> None:
    store = SubmissionReceiptStore(tmp_path)
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    tasks = (TaskId.from_ordinal(0), TaskId.from_ordinal(1))
    started = datetime(2026, 8, 19, 10, 0, tzinfo=UTC)

    pending = store.begin(run_id, tasks, started)
    assert not pending.completed
    assert pending.format_version == 2
    assert pending.outcome is SubmissionReceiptOutcome.PENDING
    assert store.load(run_id) == pending

    completed = store.complete(
        pending,
        ("1234",),
        {tasks[0]: "1234_0", tasks[1]: "1234_1"},
        started + timedelta(seconds=1),
    )

    assert completed.completed
    assert completed.outcome is SubmissionReceiptOutcome.ACCEPTED
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


def test_submission_receipt_records_rejected_and_uncertain_outcomes(
    tmp_path: Path,
) -> None:
    started = datetime(2026, 8, 19, 10, 0, tzinfo=UTC)
    task = TaskId.from_ordinal(0)
    rejected_id = RunId("run_0123456789abcdef0123456789abcdea")
    uncertain_id = RunId("run_0123456789abcdef0123456789abcdeb")
    store = SubmissionReceiptStore(tmp_path)

    rejected = store.reject(
        store.begin(rejected_id, (task,), started),
        backend="slurm",
        phase="scheduler_submit",
        failure_classification="scheduler_rejected",
        exit_code=1,
        updated_at=started + timedelta(seconds=1),
    )
    uncertain = store.mark_uncertain(
        store.begin(uncertain_id, (task,), started),
        backend="pbs",
        phase="scheduler_submit",
        failure_classification="scheduler_outcome_uncertain",
        exit_code=None,
        updated_at=started + timedelta(seconds=2),
    )

    assert rejected.outcome is SubmissionReceiptOutcome.REJECTED
    assert rejected.terminal
    assert rejected.completed_at == started + timedelta(seconds=1)
    assert uncertain.outcome is SubmissionReceiptOutcome.UNCERTAIN
    assert not uncertain.terminal
    assert store.load(rejected_id) == rejected
    assert store.load(uncertain_id) == uncertain


def test_submission_receipt_reads_pending_version_one_without_rewriting(
    tmp_path: Path,
) -> None:
    store = SubmissionReceiptStore(tmp_path)
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    task = TaskId.from_ordinal(0)
    started = datetime(2026, 8, 19, 10, 0, tzinfo=UTC)
    path = store.path(run_id)
    path.parent.mkdir(parents=True)
    document = {
        "completed_at": None,
        "format_version": 1,
        "run_id": str(run_id),
        "scheduler_job_ids": [],
        "started_at": started.isoformat(),
        "task_ids": [str(task)],
        "task_scheduler_ids": {},
    }
    path.write_text(json.dumps(document), encoding="utf-8")

    loaded = store.load(run_id)

    assert loaded.format_version == 1
    assert loaded.outcome is SubmissionReceiptOutcome.PENDING
    assert json.loads(path.read_text(encoding="utf-8")) == document
