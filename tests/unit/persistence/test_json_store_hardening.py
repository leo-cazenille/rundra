from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event
from typing import TextIO

import pytest

import rundra.persistence.json_store as json_store
from rundra.domain.records import RunRecord
from rundra.persistence import (
    JsonRunStore,
    RunAlreadyExistsError,
    RunRecordFormatError,
    RunStoreConflictError,
    RunStoreError,
    record_to_dict,
)
from tests.unit.persistence.test_json_store import _record


def test_concurrent_create_has_exactly_one_winner_and_no_partial_files(
    tmp_path: Path,
) -> None:
    record = _record()

    def create() -> str:
        try:
            JsonRunStore(tmp_path).create(record)
        except RunAlreadyExistsError:
            return "collision"
        return "created"

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = tuple(executor.map(lambda _index: create(), range(32)))

    assert outcomes.count("created") == 1
    assert outcomes.count("collision") == 31
    assert JsonRunStore(tmp_path).load(record.run.id) == record
    assert list(tmp_path.glob(".*.tmp")) == []


def test_interrupted_temporary_write_is_cleaned_without_publishing_a_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = _record()

    def interrupt(document: object, stream: TextIO, **_options: object) -> None:
        del document
        stream.write('{"format_version":')
        raise OSError("simulated interrupted write")

    monkeypatch.setattr(json_store.json, "dump", interrupt)

    with pytest.raises(RunStoreError, match="temporary record"):
        JsonRunStore(tmp_path).create(record)

    assert not (tmp_path / f"{record.run.id}.json").exists()
    assert list(tmp_path.glob(".*.tmp")) == []


def test_store_rejects_programmatic_and_tampered_credential_fields(
    tmp_path: Path,
) -> None:
    original = _record()
    secret = "must-not-appear"
    credential_command = replace(
        original.experiment.command,
        environment={"OPENAI_API_KEY": secret},
    )
    credential_record = replace(
        original,
        experiment=replace(original.experiment, command=credential_command),
    )
    store = JsonRunStore(tmp_path)

    with pytest.raises(RunStoreError) as create_error:
        store.create(credential_record)

    assert "forbidden credential field" in str(create_error.value)
    assert secret not in str(create_error.value)
    assert not (tmp_path / f"{original.run.id}.json").exists()

    store.create(original)
    path = tmp_path / f"{original.run.id}.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["experiment"]["command"]["environment"] = {"AUTH_TOKEN": secret}
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(RunRecordFormatError) as load_error:
        store.load(original.run.id)

    assert "forbidden credential field" in str(load_error.value)
    assert secret not in str(load_error.value)


def test_load_and_list_reject_unknown_versions_without_hiding_valid_records(
    tmp_path: Path,
) -> None:
    valid = _record()
    invalid = replace(
        valid,
        run=replace(
            valid.run,
            id=type(valid.run.id)("run_ffffffffffffffffffffffffffffffff"),
            tasks=tuple(
                replace(
                    task,
                    run_id=type(valid.run.id)("run_ffffffffffffffffffffffffffffffff"),
                )
                for task in valid.run.tasks
            ),
        ),
    )
    store = JsonRunStore(tmp_path)
    store.create(valid)
    document = record_to_dict(invalid)
    document["format_version"] = 999
    invalid_path = tmp_path / f"{invalid.run.id}.json"
    invalid_path.write_text(json.dumps(document), encoding="utf-8")

    assert store.load(valid.run.id) == valid
    with pytest.raises(RunRecordFormatError, match="unsupported format_version 999"):
        store.load(invalid.run.id)
    with pytest.raises(RunRecordFormatError, match="unsupported format_version 999"):
        store.list()


def test_concurrent_readers_observe_only_complete_atomic_updates(
    tmp_path: Path,
) -> None:
    original = _record()
    JsonRunStore(tmp_path).create(original)
    start = Event()
    finished = Event()

    def write_updates() -> None:
        store = JsonRunStore(tmp_path)
        start.wait()
        try:
            for ordinal in range(100):
                current = store.load(original.run.id)
                store.update(
                    replace(current, initiator=f"writer-{ordinal}"),
                    expected=current,
                )
        finally:
            finished.set()

    def read_updates() -> int:
        store = JsonRunStore(tmp_path)
        start.wait()
        reads = 0
        while not finished.is_set():
            loaded = store.load(original.run.id)
            assert loaded.run.id == original.run.id
            assert loaded.initiator == "researcher" or loaded.initiator.startswith(
                "writer-"
            )
            reads += 1
        return reads

    with ThreadPoolExecutor(max_workers=5) as executor:
        writer = executor.submit(write_updates)
        readers = tuple(executor.submit(read_updates) for _index in range(4))
        start.set()
        writer.result()
        read_counts = tuple(reader.result() for reader in readers)

    assert sum(read_counts) > 0
    assert JsonRunStore(tmp_path).load(original.run.id).initiator == "writer-99"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_concurrent_stale_updates_cannot_silently_overwrite_each_other(
    tmp_path: Path,
) -> None:
    original = _record()
    JsonRunStore(tmp_path).create(original)
    start = Event()
    candidates = (
        replace(original, initiator="first-writer"),
        replace(original, initiator="second-writer"),
    )

    def update(candidate: RunRecord) -> str:
        start.wait()
        try:
            JsonRunStore(tmp_path).update(candidate, expected=original)
        except RunStoreConflictError:
            return "conflict"
        return "updated"

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(executor.submit(update, candidate) for candidate in candidates)
        start.set()
        outcomes = tuple(future.result() for future in futures)

    assert outcomes.count("updated") == 1
    assert outcomes.count("conflict") == 1
    assert JsonRunStore(tmp_path).load(original.run.id) in candidates


def test_concurrent_identical_updates_are_idempotent(tmp_path: Path) -> None:
    original = _record()
    desired = replace(original, initiator="same-update")
    JsonRunStore(tmp_path).create(original)
    start = Event()

    def update() -> None:
        start.wait()
        JsonRunStore(tmp_path).update(desired, expected=original)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = tuple(executor.submit(update) for _index in range(16))
        start.set()
        for future in futures:
            future.result()

    assert JsonRunStore(tmp_path).load(original.run.id) == desired
