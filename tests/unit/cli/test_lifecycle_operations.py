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
    resolve_run_inputs_operation,
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


def test_run_input_resolution_uses_project_profile_and_user_defaults(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    experiment = project / "experiment.yaml"
    experiment.touch()
    (project / "rundra.yaml").write_text(
        """\
version: 1
default_profile: local
profiles:
  local:
    config: config.yaml
    seed: 17
    target: local
    source_root: .
    destination: retrieved
""",
        encoding="utf-8",
    )
    user = tmp_path / "user.yaml"
    user.write_text(
        """\
version: 1
defaults:
  targets_file: targets.yaml
  data_dir: records
""",
        encoding="utf-8",
    )

    result = resolve_run_inputs_operation(experiment, user_config_source=user)

    assert result.ok and result.value is not None
    assert result.value.config == (project / "config.yaml").resolve()
    assert result.value.seed == 17
    assert result.value.target == "local"
    assert result.value.source_root == project.resolve()
    assert result.value.destination == (project / "retrieved").resolve()
    assert result.value.targets_file == (tmp_path / "targets.yaml").resolve()
    assert result.value.data_dir == (tmp_path / "records").resolve()


def test_run_input_resolution_reports_all_unresolved_required_values(
    tmp_path: Path,
) -> None:
    result = resolve_run_inputs_operation(
        tmp_path / "experiment.yaml",
        user_config_source=tmp_path / "absent-user.yaml",
    )

    assert result.error is not None
    assert result.error.code == "LAUNCH_VALUE_REQUIRED"
    assert result.error.details == {"fields": ("config", "target")}


def test_fully_explicit_run_inputs_do_not_depend_on_optional_defaults(
    tmp_path: Path,
) -> None:
    malformed_user = tmp_path / "user.yaml"
    malformed_user.write_text("not: a-user-config\n", encoding="utf-8")

    result = resolve_run_inputs_operation(
        tmp_path / "experiment.yaml",
        config=tmp_path / "config.yaml",
        seed=4,
        target="local",
        targets_file=tmp_path / "targets.yaml",
        source_root=tmp_path,
        destination=tmp_path / "results",
        data_dir=tmp_path / "records",
        user_config_source=malformed_user,
    )

    assert result.ok and result.value is not None
    assert set(result.value.resolution.sources.values()) == {"cli"}


def test_omitted_run_seed_is_generated_exactly_once_after_required_resolution(
    tmp_path: Path,
) -> None:
    calls = 0

    def generate() -> int:
        nonlocal calls
        calls += 1
        return 123456789

    result = resolve_run_inputs_operation(
        tmp_path / "experiment.yaml",
        config=tmp_path / "config.yaml",
        target="local",
        targets_file=tmp_path / "targets.yaml",
        source_root=tmp_path,
        destination=tmp_path / "results",
        data_dir=tmp_path / "records",
        user_config_source=tmp_path / "absent.yaml",
        seed_factory=generate,
    )

    assert result.ok and result.value is not None
    assert result.value.seed == 123456789
    assert result.value.resolution.sources["seed"] == "generated"
    assert calls == 1


def test_random_seed_override_replaces_a_configured_fixed_seed(tmp_path: Path) -> None:
    project = tmp_path / "rundra.yaml"
    project.write_text(
        "version: 1\ndefaults: {config: config.yaml, target: local, seed: 17}\n",
        encoding="utf-8",
    )

    result = resolve_run_inputs_operation(
        tmp_path / "experiment.yaml",
        project_file=project,
        random_seed=True,
        seed_factory=lambda: 29,
        user_config_source=tmp_path / "absent.yaml",
    )

    assert result.ok and result.value is not None
    assert result.value.seed == 29
    assert result.value.resolution.sources["seed"] == "generated"


def test_seed_generation_rejects_conflicts_and_invalid_provider_values(
    tmp_path: Path,
) -> None:
    common = {
        "experiment_source": tmp_path / "experiment.yaml",
        "config": tmp_path / "config.yaml",
        "target": "local",
        "user_config_source": tmp_path / "absent.yaml",
    }

    conflict = resolve_run_inputs_operation(
        **common, seed=1, random_seed=True, seed_factory=lambda: 2
    )
    invalid = resolve_run_inputs_operation(**common, seed_factory=lambda: 2**63)

    assert conflict.error is not None and conflict.error.code == "SEED_CONFLICT"
    assert invalid.error is not None
    assert invalid.error.code == "SEED_GENERATION_FAILED"
