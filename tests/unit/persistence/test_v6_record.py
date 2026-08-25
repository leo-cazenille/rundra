from dataclasses import replace
from datetime import timedelta
from pathlib import PurePosixPath

import pytest

from rundra.domain.scheduling import SlurmPartitionPolicy, SlurmPartitionRoute
from rundra.domain.storage import SlurmScratchPolicy
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


def test_version_six_round_trips_optional_slurm_scratch_policy() -> None:
    base = _record()
    target = replace(
        base.run.target,
        execution_storage=SlurmScratchPolicy(),
    )
    record = replace(
        base,
        format_version=6,
        run=replace(base.run, target=target),
        retrieval_destination=PurePosixPath("/retrieved/scratch"),
        fetch_mode="copy",
    )

    document = record_to_dict(record)

    assert document["run"]["target"]["execution_storage"] == {
        "type": "slurm_scratch",
        "cpu_environment": "SLURM_TMPDIR",
        "gpu_environment": "SLURM_GPUTMPDIR",
        "stage_image": True,
        "copy_back": "task",
    }
    assert record_from_dict(document) == record


def test_version_seven_round_trips_partition_policy() -> None:
    base = _record()
    target = replace(
        base.run.target,
        partition_policy=SlurmPartitionPolicy(
            (SlurmPartitionRoute("cpu_short", "cpu-short", "cpu", timedelta(hours=1)),)
        ),
    )
    record = replace(
        base,
        format_version=7,
        run=replace(base.run, target=target),
        retrieval_destination=PurePosixPath("/retrieved/routed"),
        fetch_mode="copy",
    )

    document = record_to_dict(record)

    assert document["run"]["target"]["partition_policy"]["routes"][0] == {
        "name": "cpu_short",
        "partition": "cpu-short",
        "resource_class": "cpu",
        "max_walltime_microseconds": 3_600_000_000,
    }
    assert record_from_dict(document) == record
