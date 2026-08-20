from __future__ import annotations

from dataclasses import replace
from pathlib import PurePosixPath

import pytest

from rundra.persistence import record_from_dict, record_to_dict
from rundra.persistence.errors import RunRecordFormatError
from tests.unit.persistence.test_json_store import _record
from tests.unit.persistence.test_v4_record import _v4_record


def test_version_five_materialized_record_has_persisted_retrieval_intent() -> None:
    record = replace(
        _record(),
        format_version=5,
        retrieval_destination=PurePosixPath("/retrieved/experiment"),
    )

    document = record_to_dict(record)

    assert document["run_kind"] == "materialized"
    assert document["retrieval_destination"] == "/retrieved/experiment"
    assert document["preparation"] is None
    assert document["task_space"] is None
    assert document["run"]["tasks"][0]["parameter_set"] is None
    assert record_from_dict(document) == record


def test_version_five_compact_record_uses_the_same_canonical_shape() -> None:
    record = replace(
        _v4_record(),
        format_version=5,
        retrieval_destination=PurePosixPath("/retrieved/large"),
    )

    document = record_to_dict(record)

    assert document["run_kind"] == "compact"
    assert document["retrieval_destination"] == "/retrieved/large"
    assert "tasks" not in document["run"]
    assert record_from_dict(document) == record


def test_version_five_rejects_missing_relative_or_mismatched_destination() -> None:
    with pytest.raises(ValueError, match="absolute safe path"):
        replace(
            _record(),
            format_version=5,
            retrieval_destination=PurePosixPath("relative"),
        )

    document = record_to_dict(
        replace(
            _record(),
            format_version=5,
            retrieval_destination=PurePosixPath("/retrieved/experiment"),
        )
    )
    del document["retrieval_destination"]
    with pytest.raises(RunRecordFormatError, match="missing field"):
        record_from_dict(document)

    document = record_to_dict(
        replace(
            _record(),
            format_version=5,
            retrieval_destination=PurePosixPath("/retrieved/experiment"),
        )
    )
    document["run_kind"] = "compact"
    with pytest.raises(RunRecordFormatError, match="unknown field"):
        record_from_dict(document)
