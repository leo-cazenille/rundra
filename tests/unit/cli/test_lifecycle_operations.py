from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

import rundra.cli.operations as operations
from rundra.cli.operations import (
    FetchValue,
    InspectValue,
    ListRunsValue,
    LogsValue,
    RunValue,
    StatusValue,
    fetch_operation,
    inspect_operation,
    list_runs_operation,
    logs_operation,
    run_operation,
    status_operation,
    submit_unavailable_operation,
)
from rundra.cli.render import result_document
from rundra.domain.models import ArtifactKind
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.persistence import JsonRunStore, record_from_dict
from rundra.ports import CapabilityCheck, ContainerRequest


def _stored_record(tmp_path: Path) -> tuple[JsonRunStore, str]:
    document: object = json.loads(
        Path("docs/schemas/run-record-v1.json").read_text(encoding="utf-8")
    )
    original = record_from_dict(document)
    workspace_root = tmp_path / "workspace"
    run_root = workspace_root / "runs" / str(original.run.id)
    logs = run_root / "logs"
    outputs = run_root / "output/results"
    logs.mkdir(parents=True)
    outputs.mkdir(parents=True)
    stdout = logs / "task_000000.stdout"
    stderr = logs / "task_000000.stderr"
    stdout.write_text("hello stdout\n", encoding="utf-8")
    stderr.write_text("hello stderr\n", encoding="utf-8")
    (outputs / "result.json").write_text('{"value": 17}\n', encoding="utf-8")
    target = replace(original.run.target, workspace=workspace_root)
    artifacts = tuple(
        replace(
            artifact,
            path=(
                stdout
                if artifact.kind is ArtifactKind.STDOUT
                else stderr
                if artifact.kind is ArtifactKind.STDERR
                else artifact.path
            ),
            size_bytes=(
                len(b"hello stdout\n")
                if artifact.kind is ArtifactKind.STDOUT
                else len(b"hello stderr\n")
                if artifact.kind is ArtifactKind.STDERR
                else artifact.size_bytes
            ),
        )
        for artifact in original.artifacts
    )
    record = replace(
        original, run=replace(original.run, target=target), artifacts=artifacts
    )
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    return store, str(record.run.id)


def test_persisted_status_list_and_inspect_share_typed_record_values(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)

    status = status_operation(run_id, store)
    listed = list_runs_operation(store)
    inspected = inspect_operation(run_id, store)

    assert status.ok and isinstance(status.value, StatusValue)
    assert status.value.state is ExecutionState.SUCCEEDED
    assert status.value.retrieval_state is RetrievalState.SUCCEEDED
    assert status.value.task_counts == {"SUCCEEDED": 1}
    assert listed.ok and isinstance(listed.value, ListRunsValue)
    assert listed.value.runs == (status.value,)
    assert inspected.ok and isinstance(inspected.value, InspectValue)
    assert inspected.value.record == store.load(inspected.value.record.run.id)
    assert result_document(status)["status"]["run_id"] == run_id
    assert result_document(listed)["runs"][0]["state"] == "SUCCEEDED"
    assert result_document(inspected)["record"]["format_version"] == 1


def test_logs_are_selected_by_stable_task_without_native_filename_knowledge(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)

    logs = logs_operation(run_id, store, task="0")

    assert logs.ok and isinstance(logs.value, LogsValue)
    assert logs.value.task_id.value == "task_000000"
    assert logs.value.stdout == "hello stdout\n"
    assert logs.value.stderr == "hello stderr\n"
    assert result_document(logs)["logs"]["stdout"] == "hello stdout\n"


def test_fetch_is_idempotent_and_preserves_successful_retrieval_state(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)
    destination = tmp_path / "retrieved"

    first = fetch_operation(run_id, store, destination)
    second = fetch_operation(run_id, store, destination)

    assert first.ok and isinstance(first.value, FetchValue)
    assert second.ok and isinstance(second.value, FetchValue)
    assert first.value.retrieval_state is RetrievalState.SUCCEEDED
    assert (destination / "results/result.json").read_text(encoding="utf-8") == (
        '{"value": 17}\n'
    )
    assert (
        store.load(first.value.run_id).run.retrieval_state is RetrievalState.SUCCEEDED
    )
    assert result_document(second)["fetch"]["artifacts"][0]["kind"] == "raw_result"


def test_lifecycle_operations_return_structured_not_found_and_task_errors(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)

    missing = status_operation(
        "run_ffffffffffffffffffffffffffffffff",
        store,
    )
    bad_id = inspect_operation("not-a-run", store)
    bad_task = logs_operation(run_id, store, task="9")

    assert missing.error is not None and missing.error.code == "RUN_NOT_FOUND"
    assert bad_id.error is not None and bad_id.error.code == "INVALID_RUN_ID"
    assert bad_task.error is not None and bad_task.error.code == "TASK_NOT_FOUND"


def test_run_value_exit_semantics_and_submit_capability_error(tmp_path: Path) -> None:
    store, run_id = _stored_record(tmp_path)
    record = store.load(next(iter(store.list())).run.id)
    run_value = RunValue(record)

    assert run_value.exit_code == 0
    assert run_value.run_id.value == run_id
    unavailable = submit_unavailable_operation()
    assert unavailable.error is not None
    assert unavailable.error.code == "ASYNC_UNAVAILABLE"


def test_run_operation_returns_a_durable_structured_capability_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingRuntime:
        def check(self) -> CapabilityCheck:
            raise RuntimeError("test runtime is unavailable")

        def build_command(self, request: ContainerRequest) -> object:
            raise AssertionError("build must not follow failed capability check")

    monkeypatch.setattr(operations, "NativeRuntime", MissingRuntime)
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""\
version: 1
targets:
  local:
    transport:
      type: local
    scheduler:
      type: local
    staging:
      type: local
    container:
      type: native
    workspace: {tmp_path / "workspace"}
""",
        encoding="utf-8",
    )
    store = JsonRunStore(tmp_path / "records")

    result = run_operation(
        Path("examples/minimal/experiment.yaml"),
        Path("examples/minimal/config.yaml"),
        targets,
        "local",
        Path.cwd(),
        tmp_path / "retrieved",
        store,
        seed=17,
    )

    assert result.error is not None
    assert result.error.code == "CAPABILITY_CHECK_FAILED"
    assert "run_id" in result.error.details
    records = store.list()
    assert len(records) == 1
    assert records[0].run.state is ExecutionState.FAILED
