from dataclasses import replace
from pathlib import PurePosixPath

import pytest

from rundra.persistence import record_from_dict, record_to_dict
from rundra.persistence.errors import RunRecordFormatError
from tests.unit.persistence.test_json_store import _record
from tests.unit.persistence.test_v4_record import _v4_record


def test_version_six_materialized_record_has_typed_fetch_mode() -> None:
    record = replace(
        _record(),
        format_version=6,
        retrieval_destination=PurePosixPath("/retrieved/experiment"),
        fetch_mode="reference",
    )

    document = record_to_dict(record)

    assert document["run_kind"] == "materialized"
    assert document["fetch_mode"] == "reference"
    assert record_from_dict(document) == record


def test_version_six_compact_record_uses_the_same_schema() -> None:
    record = replace(
        _v4_record(),
        format_version=6,
        retrieval_destination=PurePosixPath("/retrieved/large"),
        fetch_mode="archive",
    )

    document = record_to_dict(record)

    assert document["run_kind"] == "compact"
    assert document["fetch_mode"] == "archive"
    assert record_from_dict(document) == record


def test_version_six_requires_a_fetch_mode() -> None:
    with pytest.raises(ValueError, match="fetch_mode"):
        replace(
            _record(),
            format_version=6,
            retrieval_destination=PurePosixPath("/retrieved/experiment"),
        )

    document = record_to_dict(
        replace(
            _record(),
            format_version=6,
            retrieval_destination=PurePosixPath("/retrieved/experiment"),
            fetch_mode="auto",
        )
    )
    del document["fetch_mode"]
    with pytest.raises(RunRecordFormatError, match="missing field"):
        record_from_dict(document)
