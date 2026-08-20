from __future__ import annotations

import json
from pathlib import Path

from rundra.persistence import record_from_dict, record_to_dict


def test_checked_run_record_v1_example_round_trips() -> None:
    source = Path("docs/schemas/run-record-v1.json")
    document: object = json.loads(source.read_text(encoding="utf-8"))

    record = record_from_dict(document)

    assert record_to_dict(record) == document


def test_checked_run_record_v5_example_round_trips() -> None:
    source = Path("docs/schemas/run-record-v5.json")
    document: object = json.loads(source.read_text(encoding="utf-8"))

    record = record_from_dict(document)

    assert record.format_version == 5
    assert record.retrieval_destination == Path("/tmp/rundra/retrieved/minimal")
    assert record_to_dict(record) == document


def test_checked_run_record_v6_example_round_trips() -> None:
    source = Path("docs/schemas/run-record-v6.json")
    document: object = json.loads(source.read_text(encoding="utf-8"))

    record = record_from_dict(document)

    assert record.format_version == 6
    assert record.fetch_mode == "auto"
    assert record_to_dict(record) == document
