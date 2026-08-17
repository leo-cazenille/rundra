from __future__ import annotations

from dataclasses import replace
from pathlib import PurePosixPath

import pytest

from rundra.domain.records import RunRecord
from rundra.domain.scaling import CompactRun, SeedRange, TaskSpace
from rundra.persistence import record_from_dict, record_to_dict
from rundra.persistence.errors import RunRecordFormatError
from tests.unit.persistence.test_json_store import _record


def _v4_record() -> RunRecord:
    original = _record()
    return replace(
        original,
        format_version=4,
        run=CompactRun(
            id=original.run.id,
            experiment_name=original.run.experiment_name,
            target=original.run.target,
            tasks=(),
            created_at=original.run.created_at,
        ),
        task_exit_codes={},
        artifacts=(),
        task_space=TaskSpace(2, SeedRange(0, 49_999_999)),
        execution_strategy="worker-pool",
        retrieval_policy="manifest",
        task_state_store=PurePosixPath(f"{original.run.id}.tasks.sqlite3"),
    )


def test_version_four_record_round_trips_without_materialized_tasks() -> None:
    record = _v4_record()

    document = record_to_dict(record)
    restored = record_from_dict(document)

    assert document["format_version"] == 4
    assert "tasks" not in document["run"]
    assert document["task_space"] == {
        "parameter_set_count": 2,
        "seeds": {"start": 0, "stop": 49_999_999, "step": 1},
        "task_count": 100_000_000,
    }
    assert restored == record


def test_version_four_record_rejects_tampered_counts_and_unbounded_maps() -> None:
    document = record_to_dict(_v4_record())
    document["task_space"]["task_count"] = 99

    with pytest.raises(RunRecordFormatError, match="task_count"):
        record_from_dict(document)

    document = record_to_dict(_v4_record())
    document["task_exit_codes"] = {"task_000000": 0}
    with pytest.raises(RunRecordFormatError, match="unknown TaskId"):
        record_from_dict(document)
