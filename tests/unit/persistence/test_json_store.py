from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import pytest

from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    BackendConfig,
    Command,
    ConfigSnapshot,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    Run,
    RunId,
    Target,
    Task,
    TaskId,
)
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.persistence import (
    JsonRunStore,
    RunAlreadyExistsError,
    RunNotFoundError,
    RunRecordFormatError,
    RunStore,
    RunStoreError,
    record_from_dict,
    record_to_dict,
)


def _record() -> RunRecord:
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    task_id = TaskId.from_ordinal(0)
    resources = ResourceRequest(
        cpus_per_task=2,
        gpus_per_task=1,
        memory_bytes=2 * 1024**3,
        walltime=timedelta(minutes=5, microseconds=7),
        native={"local": {"priority": 3, "exclusive": False}},
    )
    config = ConfigSnapshot(
        PurePosixPath("configs/example.yaml"),
        "alpha: 0.5\n",
    )
    task = Task(
        id=task_id,
        run_id=run_id,
        experiment_name="example",
        config=config,
        seed=17,
        resources=resources,
    )
    target = Target(
        name="local",
        transport=BackendConfig("local"),
        scheduler=BackendConfig("local"),
        staging=BackendConfig("local"),
        container=BackendConfig("apptainer", {"executable": "apptainer"}),
        workspace=PurePosixPath("/tmp/rundra"),
    )
    experiment = ExperimentSpec(
        version=1,
        name="example",
        command=Command(
            ("python", "main.py", "--seed", "{seed}"),
            environment={"MODE": "test"},
            working_directory=PurePosixPath("source"),
        ),
        resources=resources,
        container=ContainerSpec(PurePosixPath("images/example.sif"), gpu=True),
        outputs=("results/**",),
        sync_excludes=(".git/", "__pycache__/"),
    )
    run = Run(
        id=run_id,
        experiment_name="example",
        target=target,
        tasks=(task,),
        created_at=datetime(2026, 8, 15, 8, 0, tzinfo=UTC),
    )
    return RunRecord(
        format_version=1,
        framework_version="0.1.0.dev0",
        run=run,
        experiment=experiment,
        source_root=PurePosixPath("/work/example"),
        experiment_source=PurePosixPath("experiment.yaml"),
        initiator="researcher",
        git_commit="0123456789abcdef",
        git_branch="feature/test",
        git_dirty=True,
        git_diff="diff --git a/main.py b/main.py\n",
        container_digest="sha256:abcdef",
        scheduler_job_ids=("local-123",),
        allocated_nodes=("localhost",),
        submitted_at=datetime(2026, 8, 15, 8, 1, tzinfo=UTC),
        started_at=datetime(2026, 8, 15, 8, 2, tzinfo=UTC),
        completed_at=datetime(2026, 8, 15, 8, 3, tzinfo=UTC),
        native_state="EXITED",
        task_exit_codes={task_id: 0},
        artifacts=(
            Artifact(
                ArtifactKind.RAW_RESULT,
                PurePosixPath("output/result.json"),
                task_id=task_id,
                size_bytes=42,
            ),
        ),
    )


def _with_states(
    record: RunRecord,
    execution: ExecutionState,
    retrieval: RetrievalState,
) -> RunRecord:
    tasks = tuple(replace(task, state=execution) for task in record.run.tasks)
    return replace(
        record,
        run=replace(
            record.run,
            tasks=tasks,
            state=execution,
            retrieval_state=retrieval,
        ),
    )


def test_run_record_round_trips_every_known_field() -> None:
    record = _record()

    document = record_to_dict(record)

    assert document["format_version"] == 1
    assert document["run"]["state"] == "CREATED"
    assert document["run"]["retrieval_state"] == "NOT_REQUESTED"
    assert document["experiment"]["resources"]["walltime_microseconds"] == 300000007
    assert document["task_exit_codes"] == {"task_000000": 0}
    assert record_from_dict(document) == record
    json.dumps(document, allow_nan=False)


def test_run_record_round_trips_absent_optional_provenance_without_fabrication() -> (
    None
):
    full = _record()
    record = replace(
        full,
        experiment_source=None,
        initiator=None,
        git_commit=None,
        git_branch=None,
        git_dirty=None,
        git_diff=None,
        container_digest=None,
        scheduler_job_ids=(),
        allocated_nodes=(),
        submitted_at=None,
        started_at=None,
        completed_at=None,
        native_state=None,
        task_exit_codes={},
        artifacts=(),
    )

    document = record_to_dict(record)

    assert document["git_commit"] is None
    assert document["git_dirty"] is None
    assert document["container_digest"] is None
    assert record_from_dict(document) == record


def test_run_record_rejects_unknown_versions_and_fields() -> None:
    document = record_to_dict(_record())
    document["format_version"] = 2

    with pytest.raises(RunRecordFormatError, match="unsupported format_version 2"):
        record_from_dict(document)

    document = record_to_dict(_record())
    document["unexpected"] = True
    with pytest.raises(RunRecordFormatError, match="unknown field.*unexpected"):
        record_from_dict(document)


def test_run_record_rejects_invalid_nested_values_with_a_structured_error() -> None:
    document = record_to_dict(_record())
    document["run"]["created_at"] = "2026-08-15T08:00:00"

    with pytest.raises(
        RunRecordFormatError,
        match=r"run\.created_at.*timezone-aware",
    ):
        record_from_dict(document)


def test_run_record_copies_mutable_collections_and_rejects_unknown_task_ids() -> None:
    original = _record()
    scheduler_ids = ["local-123"]
    exit_codes = dict(original.task_exit_codes)
    artifacts = list(original.artifacts)
    copied = replace(
        original,
        scheduler_job_ids=scheduler_ids,
        task_exit_codes=exit_codes,
        artifacts=artifacts,
    )

    scheduler_ids.append("local-456")
    exit_codes.clear()
    artifacts.clear()

    assert copied.scheduler_job_ids == ("local-123",)
    assert copied.task_exit_codes == original.task_exit_codes
    assert copied.artifacts == original.artifacts
    with pytest.raises(ValueError, match="unknown TaskId"):
        replace(
            original,
            task_exit_codes={TaskId.from_ordinal(9): 1},
        )


def test_json_store_implements_boundary_and_round_trips_records(tmp_path: Path) -> None:
    store = JsonRunStore(tmp_path / "runs")
    record = _record()

    assert isinstance(store, RunStore)
    store.create(record)

    assert store.load(record.run.id) == record
    assert store.list() == (record,)
    persisted = json.loads(
        (tmp_path / "runs" / f"{record.run.id}.json").read_text(encoding="utf-8")
    )
    assert persisted == record_to_dict(record)


def test_json_store_create_is_collision_safe(tmp_path: Path) -> None:
    store = JsonRunStore(tmp_path)
    original = _record()
    store.create(original)

    replacement = replace(original, initiator="someone-else")
    with pytest.raises(RunAlreadyExistsError, match=str(original.run.id)):
        store.create(replacement)

    assert store.load(original.run.id) == original


def test_json_store_load_reports_missing_runs(tmp_path: Path) -> None:
    store = JsonRunStore(tmp_path)
    run_id = RunId("run_ffffffffffffffffffffffffffffffff")

    with pytest.raises(RunNotFoundError, match=str(run_id)):
        store.load(run_id)


def test_json_store_rejects_duplicate_json_fields(tmp_path: Path) -> None:
    store = JsonRunStore(tmp_path)
    record = _record()
    store.create(record)
    path = tmp_path / f"{record.run.id}.json"
    path.write_text(
        '{"format_version": 1, "format_version": 1}\n',
        encoding="utf-8",
    )

    with pytest.raises(RunRecordFormatError, match="duplicate field"):
        store.load(record.run.id)


def test_json_store_updates_computation_and_retrieval_independently(
    tmp_path: Path,
) -> None:
    store = JsonRunStore(tmp_path)
    created = replace(
        _record(),
        submitted_at=None,
        started_at=None,
        completed_at=None,
        native_state=None,
        task_exit_codes={},
        artifacts=(),
    )
    store.create(created)

    staging = _with_states(
        created, ExecutionState.STAGING, RetrievalState.NOT_REQUESTED
    )
    store.update(staging)
    retrieving = _with_states(staging, ExecutionState.STAGING, RetrievalState.PENDING)
    store.update(retrieving)

    assert store.load(created.run.id).run.state is ExecutionState.STAGING
    assert store.load(created.run.id).run.retrieval_state is RetrievalState.PENDING


@pytest.mark.parametrize(
    ("execution", "retrieval"),
    [
        (ExecutionState.SUCCEEDED, RetrievalState.NOT_REQUESTED),
        (ExecutionState.CREATED, RetrievalState.SUCCEEDED),
    ],
)
def test_json_store_rejects_invalid_lifecycle_transitions(
    tmp_path: Path,
    execution: ExecutionState,
    retrieval: RetrievalState,
) -> None:
    store = JsonRunStore(tmp_path)
    created = replace(
        _record(),
        submitted_at=None,
        started_at=None,
        completed_at=None,
        native_state=None,
        task_exit_codes={},
        artifacts=(),
    )
    store.create(created)

    with pytest.raises(RunStoreError, match="Invalid .* state transition"):
        store.update(_with_states(created, execution, retrieval))

    assert store.load(created.run.id) == created


def test_json_store_rejects_changes_to_run_identity_and_inputs(tmp_path: Path) -> None:
    store = JsonRunStore(tmp_path)
    original = _record()
    store.create(original)

    with pytest.raises(RunStoreError, match="immutable run definition"):
        store.update(replace(original, source_root=PurePosixPath("/work/different")))

    assert store.load(original.run.id) == original


def test_json_store_failed_atomic_update_preserves_previous_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rundra.persistence.json_store as json_store

    store = JsonRunStore(tmp_path)
    original = _record()
    store.create(original)
    updated = replace(original, initiator="updated")

    def fail_replace(source: Path, destination: Path) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(json_store.os, "replace", fail_replace)
    with pytest.raises(RunStoreError, match="atomic update"):
        store.update(updated)

    assert store.load(original.run.id) == original
    assert list(tmp_path.glob(".*.tmp")) == []
