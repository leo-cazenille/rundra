from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier

import pytest

import rundra.cli.operations as operations
from rundra.cli.operations import (
    LAST_RUN_SELECTOR,
    FetchValue,
    InspectValue,
    ListRunsValue,
    LogsValue,
    PreparationLogsValue,
    PurgeValue,
    RunValue,
    StatusValue,
    WaitValue,
    fetch_operation,
    inspect_operation,
    list_runs_operation,
    logs_operation,
    purge_operation,
    resolve_plan_inputs_operation,
    resolve_run_inputs_operation,
    run_operation,
    status_operation,
    wait_operation,
)
from rundra.cli.render import result_document
from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    BackendConfig,
    ContainerSpec,
    RunId,
    TaskId,
)
from rundra.domain.preparation import PreparationRecord
from rundra.domain.purge import PurgeOutcome
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.progress import ProgressEvent
from rundra.persistence import JsonRunStore, PurgeReceiptStore, record_from_dict
from rundra.ports import (
    CapabilityCheck,
    ContainerRequest,
    FetchRequest,
    FetchResult,
)
from rundra.results import OperationResult


class RecordingFetchStager:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.requests: list[FetchRequest] = []

    def fetch(self, request: FetchRequest) -> FetchResult:
        self.requests.append(request)
        if self.fail:
            raise RuntimeError("temporary retrieval failure")
        artifacts = tuple(
            Artifact(
                ArtifactKind.RAW_RESULT,
                request.destination
                / "output"
                / pattern.split("/", 1)[0]
                / "result.json",
            )
            for pattern in request.patterns
        )
        return FetchResult(artifacts)


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


def _stored_array_record(tmp_path: Path) -> tuple[JsonRunStore, str]:
    single_store, _ = _stored_record(tmp_path / "source-record")
    original = single_store.list()[0]
    first = replace(
        original.run.tasks[0],
        state=ExecutionState.SUCCEEDED,
    )
    second = replace(first, id=TaskId.from_ordinal(1), seed=23)
    tasks = (first, second)
    target = replace(
        original.run.target,
        transport=BackendConfig("ssh", {"host": "cluster"}),
        scheduler=BackendConfig("slurm"),
        staging=BackendConfig("rsync"),
        workspace=Path("/remote/work"),
    )
    record = replace(
        original,
        run=replace(
            original.run,
            target=target,
            tasks=tasks,
            state=ExecutionState.SUCCEEDED,
            retrieval_state=RetrievalState.NOT_REQUESTED,
        ),
        scheduler_job_ids=("777",),
        task_array_mapping=tuple(
            ArrayTaskMapping(task.id, task.seed, index)
            for index, task in enumerate(tasks)
        ),
        task_scheduler_ids={first.id: "777_0", second.id: "777_1"},
        task_native_states={first.id: "COMPLETED", second.id: "COMPLETED"},
        task_retrieval_states={
            first.id: RetrievalState.NOT_REQUESTED,
            second.id: RetrievalState.NOT_REQUESTED,
        },
        task_exit_codes={first.id: 0, second.id: 0},
        artifacts=tuple(
            artifact for artifact in original.artifacts if artifact.task_id is None
        ),
    )
    store = JsonRunStore(tmp_path / "array-records")
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
    assert listed.value.runs == (replace(status.value, task_details=()),)
    assert listed.value.total == 1
    assert listed.value.next_offset is None
    assert inspected.ok and isinstance(inspected.value, InspectValue)
    assert inspected.value.record == store.load(inspected.value.record.run.id)
    assert result_document(status)["status"]["run_id"] == run_id
    assert result_document(listed)["runs"][0]["state"] == "SUCCEEDED"
    assert result_document(inspected)["record"]["format_version"] == 1


def test_list_task_expansion_is_explicit_and_bounds_are_validated(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)

    compact = list_runs_operation(store, offset=0, limit=1)
    expanded = list_runs_operation(store, include_tasks=True)
    bad_offset = list_runs_operation(store, offset=-1)
    bad_limit = list_runs_operation(store, limit=1001)

    assert compact.ok and compact.value is not None
    assert compact.value.runs[0].task_details == ()
    assert result_document(compact)["page"] == {
        "offset": 0,
        "limit": 1,
        "returned": 1,
        "total": 1,
        "next_offset": None,
        "task_details_included": False,
    }
    assert expanded.ok and expanded.value is not None
    assert expanded.value.runs[0].task_details[0].task_id.value == "task_000000"
    assert str(expanded.value.runs[0].run_id) == run_id
    assert bad_offset.error is not None
    assert bad_offset.error.code == "INVALID_RUN_OFFSET"
    assert bad_limit.error is not None
    assert bad_limit.error.code == "INVALID_RUN_LIMIT"


def test_wait_returns_terminal_status_without_fetching(tmp_path: Path) -> None:
    store, run_id = _stored_record(tmp_path)
    progress_events: list[ProgressEvent] = []

    waited = wait_operation(run_id, store, timeout=0, progress=progress_events.append)

    assert waited.ok and isinstance(waited.value, WaitValue)
    assert waited.value.terminal is True
    assert waited.value.timed_out is False
    assert result_document(waited)["wait"]["status"]["state"] == "SUCCEEDED"
    assert progress_events[-1].completed == progress_events[-1].total


def test_last_run_selector_resolves_the_newest_registered_run(tmp_path: Path) -> None:
    store, run_id = _stored_record(tmp_path)

    status = status_operation(LAST_RUN_SELECTOR, store)

    assert status.ok and isinstance(status.value, StatusValue)
    assert str(status.value.run_id) == run_id


def test_last_run_selector_reports_an_empty_store(tmp_path: Path) -> None:
    status = status_operation(LAST_RUN_SELECTOR, JsonRunStore(tmp_path))

    assert not status.ok
    assert status.error is not None
    assert status.error.code == "RUN_NOT_FOUND"


def test_wait_timeout_is_a_successful_renewable_result(tmp_path: Path) -> None:
    source, run_id = _stored_record(tmp_path / "source")
    original = source.load(RunId(run_id))
    task = replace(original.run.tasks[0], state=ExecutionState.RUNNING)
    active = replace(
        original,
        run=replace(
            original.run,
            tasks=(task,),
            state=ExecutionState.RUNNING,
            retrieval_state=RetrievalState.NOT_REQUESTED,
        ),
        completed_at=None,
        task_retrieval_states={task.id: RetrievalState.NOT_REQUESTED},
    )
    store = JsonRunStore(tmp_path / "active")
    store.create(active)

    waited = wait_operation(run_id, store, timeout=0)

    assert waited.ok and isinstance(waited.value, WaitValue)
    assert waited.value.terminal is False
    assert waited.value.timed_out is True


def test_purge_outputs_requires_confirmation_and_preserves_record(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)
    receipts = PurgeReceiptStore(tmp_path / "records")

    rejected = purge_operation(run_id, store, receipts)
    dry_run = purge_operation(run_id, store, receipts, dry_run=True)
    purged = purge_operation(run_id, store, receipts, confirm=run_id)
    repeated = purge_operation(run_id, store, receipts, confirm=run_id)

    assert rejected.error is not None
    assert rejected.error.code == "PURGE_CONFIRMATION_REQUIRED"
    assert dry_run.ok and isinstance(dry_run.value, PurgeValue)
    assert dry_run.value.result.outcome is PurgeOutcome.PLANNED
    assert purged.ok and isinstance(purged.value, PurgeValue)
    assert purged.value.result.outcome is PurgeOutcome.PURGED
    assert repeated.ok and isinstance(repeated.value, PurgeValue)
    assert repeated.value.result.outcome is PurgeOutcome.ALREADY_ABSENT
    record = store.load(RunId(run_id))
    run_root = Path(record.run.target.workspace) / "runs" / run_id
    assert not (run_root / "output").exists()
    assert (run_root / "logs").exists()
    inspected = inspect_operation(run_id, store, receipts=receipts)
    assert inspected.ok and isinstance(inspected.value, InspectValue)
    assert inspected.value.retention is not None
    assert len(inspected.value.retention.attempts) == 2
    assert result_document(inspected)["format_version"] == 5


def test_replicated_status_and_run_values_expose_concise_per_task_details(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_array_record(tmp_path)
    record = store.list()[0]

    status = status_operation(run_id, store)
    run_document = result_document(OperationResult.success("run", RunValue(record)))[
        "run"
    ]

    assert status.ok and isinstance(status.value, StatusValue)
    assert [detail.task_id for detail in status.value.task_details] == [
        TaskId.from_ordinal(0),
        TaskId.from_ordinal(1),
    ]
    assert [detail.seed for detail in status.value.task_details] == [17, 23]
    assert [detail.native_id for detail in status.value.task_details] == [
        "777_0",
        "777_1",
    ]
    document = result_document(status)["status"]
    assert document["tasks"] == {"total": 2, "succeeded": 2}
    assert document["task_details"][1] == {
        "task_id": "task_000001",
        "seed": 23,
        "state": "SUCCEEDED",
        "retrieval_state": "NOT_REQUESTED",
        "native_id": "777_1",
        "native_state": "COMPLETED",
        "exit_code": 0,
    }
    assert run_document["seed"] is None
    assert run_document["seeds"] == [17, 23]


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


def test_version_two_status_and_preparation_logs_are_exposed_separately(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)
    original = store.list()[0]
    stdout = tmp_path / "prepare.stdout"
    stderr = tmp_path / "prepare.stderr"
    stdout.write_text("build output\n", encoding="utf-8")
    stderr.write_text("build warning\n", encoding="utf-8")
    preparation = PreparationRecord(
        source_identity="working-tree",
        source_digest="ab" * 32,
        source_action="snapshot_working_tree",
        image_uri="library://example/application:v1",
        image_sha256="cd" * 32,
        image_path=tmp_path / "application.sif",
        image_action="reuse_image_cache",
        resolution_location="local",
        builder_location="local",
        builder_status="SUCCEEDED",
        builder_state="EXITED",
        logs=(stdout, stderr),
    )
    updated = replace(
        original,
        format_version=2,
        experiment=replace(
            original.experiment, container=ContainerSpec(preparation.image_path)
        ),
        container_digest=preparation.image_sha256,
        preparation=preparation,
    )
    store = JsonRunStore(tmp_path / "v2-records")
    store.create(updated)

    status = status_operation(run_id, store)
    logs = logs_operation(run_id, store, preparation=True)

    assert status.ok and isinstance(status.value, StatusValue)
    assert result_document(status)["status"]["preparation"] == {
        "scheduler_id": None,
        "state": "SUCCEEDED",
        "native_state": "EXITED",
        "location": "local",
    }
    assert logs.ok and isinstance(logs.value, PreparationLogsValue)
    assert result_document(logs)["preparation_logs"]["stdout"] == "build output\n"


def test_preparation_logs_are_rejected_for_version_one_run(tmp_path: Path) -> None:
    store, run_id = _stored_record(tmp_path)

    logs = logs_operation(run_id, store, preparation=True)

    assert not logs.ok
    assert logs.error is not None
    assert logs.error.code == "PREPARATION_LOGS_UNAVAILABLE"


def test_fetch_is_idempotent_and_preserves_successful_retrieval_state(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)
    destination = tmp_path / "retrieved"
    progress_events: list[ProgressEvent] = []

    first = fetch_operation(run_id, store, destination, progress=progress_events.append)
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
    assert progress_events[-1].completed == progress_events[-1].total


def test_concurrent_fetches_are_idempotent_and_preserve_one_artifact(
    tmp_path: Path,
) -> None:
    _, run_id = _stored_record(tmp_path)
    store_path = tmp_path / "records"
    destination = tmp_path / "retrieved"

    def fetch() -> bool:
        return fetch_operation(
            run_id,
            JsonRunStore(store_path),
            destination,
        ).ok

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = tuple(executor.map(lambda _index: fetch(), range(32)))

    assert all(outcomes)
    persisted = JsonRunStore(store_path).list()[0]
    result_artifacts = tuple(
        artifact
        for artifact in persisted.artifacts
        if artifact.kind is ArtifactKind.RAW_RESULT
        and artifact.path == destination / "results/result.json"
    )
    assert len(result_artifacts) == 1
    assert (destination / "results/result.json").read_text(encoding="utf-8") == (
        '{"value": 17}\n'
    )


def test_disjoint_concurrent_fetches_are_serialized_without_lost_state(
    tmp_path: Path,
) -> None:
    _, run_id = _stored_array_record(tmp_path)
    store_path = tmp_path / "array-records"
    barrier = Barrier(2)

    class CoordinatedStore(JsonRunStore):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self._first_load = True

        def load(self, selected_run_id: RunId) -> RunRecord:
            record = super().load(selected_run_id)
            if self._first_load:
                self._first_load = False
                barrier.wait()
            return record

    selections = ("0", "1")

    def fetch(selection: str) -> OperationResult[FetchValue]:
        return fetch_operation(
            run_id,
            CoordinatedStore(store_path),
            tmp_path / "retrieved",
            tasks=(selection,),
            stager=RecordingFetchStager(),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(fetch, selections))

    assert all(result.ok for result in results)
    persisted = JsonRunStore(store_path).list()[0]
    assert (
        tuple(persisted.task_retrieval_states.values()).count(RetrievalState.SUCCEEDED)
        == 2
    )
    completed = JsonRunStore(store_path).list()[0]
    assert completed.run.retrieval_state is RetrievalState.SUCCEEDED
    assert set(completed.task_retrieval_states.values()) == {RetrievalState.SUCCEEDED}


def test_partial_array_fetch_tracks_tasks_and_becomes_complete_incrementally(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_array_record(tmp_path)
    stager = RecordingFetchStager()

    first = fetch_operation(
        run_id,
        store,
        tmp_path / "retrieved",
        tasks=("0",),
        stager=stager,
    )
    second = fetch_operation(
        run_id,
        store,
        tmp_path / "retrieved",
        tasks=("task_000001",),
        stager=stager,
    )
    repeated = fetch_operation(
        run_id,
        store,
        tmp_path / "retrieved",
        tasks=("0",),
        stager=stager,
    )

    assert first.ok and isinstance(first.value, FetchValue)
    assert first.value.task_ids == (TaskId.from_ordinal(0),)
    assert first.value.retrieval_state is RetrievalState.PENDING
    assert second.ok and isinstance(second.value, FetchValue)
    assert second.value.retrieval_state is RetrievalState.SUCCEEDED
    assert repeated.ok and isinstance(repeated.value, FetchValue)
    assert repeated.value.retrieval_state is RetrievalState.SUCCEEDED
    assert [request.patterns for request in stager.requests] == [
        ("task_000000/results/**",),
        ("task_000001/results/**",),
        ("task_000000/results/**",),
    ]
    record = store.list()[0]
    assert record.task_retrieval_states == {
        TaskId.from_ordinal(0): RetrievalState.SUCCEEDED,
        TaskId.from_ordinal(1): RetrievalState.SUCCEEDED,
    }


def test_failed_partial_fetch_is_retryable_without_losing_other_task_state(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_array_record(tmp_path)
    failing = RecordingFetchStager(fail=True)

    failed = fetch_operation(
        run_id,
        store,
        tmp_path / "retrieved",
        tasks=("1",),
        stager=failing,
    )
    retried = fetch_operation(
        run_id,
        store,
        tmp_path / "retrieved",
        tasks=("1",),
        stager=RecordingFetchStager(),
    )

    assert failed.error is not None
    assert failed.error.code == "RESULT_RETRIEVAL_FAILED"
    assert retried.ok and isinstance(retried.value, FetchValue)
    assert retried.value.retrieval_state is RetrievalState.PENDING
    record = store.list()[0]
    assert record.task_retrieval_states == {
        TaskId.from_ordinal(0): RetrievalState.NOT_REQUESTED,
        TaskId.from_ordinal(1): RetrievalState.SUCCEEDED,
    }


def test_partial_fetch_rejects_duplicate_or_unknown_task_selection(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_array_record(tmp_path)

    duplicate = fetch_operation(
        run_id,
        store,
        tmp_path / "retrieved",
        tasks=("0", "task_000000"),
        stager=RecordingFetchStager(),
    )
    unknown = fetch_operation(
        run_id,
        store,
        tmp_path / "retrieved",
        tasks=("7",),
        stager=RecordingFetchStager(),
    )

    assert duplicate.error is not None and duplicate.error.code == "DUPLICATE_TASK"
    assert unknown.error is not None and unknown.error.code == "TASK_NOT_FOUND"


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


def test_run_value_exit_semantics_include_successful_async_submission(
    tmp_path: Path,
) -> None:
    store, run_id = _stored_record(tmp_path)
    record = store.load(next(iter(store.list())).run.id)
    run_value = RunValue(record)

    assert run_value.exit_code == 0
    assert run_value.run_id.value == run_id
    submitted = RunValue(
        replace(
            record,
            run=replace(
                record.run,
                state=ExecutionState.SUBMITTED,
                tasks=tuple(
                    replace(task, state=ExecutionState.SUBMITTED)
                    for task in record.run.tasks
                ),
            ),
        )
    )
    assert submitted.exit_code == 0


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


def test_run_input_resolution_accepts_explicit_inclusive_seed_range(
    tmp_path: Path,
) -> None:
    result = resolve_run_inputs_operation(
        tmp_path / "experiment.yaml",
        config=tmp_path / "config.yaml",
        seeds="7:9",
        target="cluster",
        targets_file=tmp_path / "targets.yaml",
        source_root=tmp_path,
        destination=tmp_path / "retrieved",
        data_dir=tmp_path / "records",
    )

    assert result.ok and result.value is not None
    assert result.value.seeds == (7, 8, 9)
    assert result.value.seed is None
    assert result.value.launch.values["seeds"] == "7:9"
    assert result.value.launch.sources["seeds"] == "cli"


def test_run_input_resolution_derives_destination_from_config_name(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    experiment = project / "experiment.yaml"
    experiment.touch()
    (project / "rundra.yaml").write_text(
        "version: 1\ndefaults: {config: conf/ballistic.yaml, target: local}\n",
        encoding="utf-8",
    )

    result = resolve_run_inputs_operation(
        experiment,
        user_config_source=tmp_path / "absent.yaml",
    )

    assert result.ok and result.value is not None
    assert result.value.destination == (project / "retrieved/ballistic").resolve()
    assert result.value.resolution.sources["destination"] == "built_in"


def test_explicit_destination_overrides_derived_destination(tmp_path: Path) -> None:
    result = resolve_run_inputs_operation(
        tmp_path / "experiment.yaml",
        config=tmp_path / "conf/ballistic.yaml",
        target="local",
        destination=tmp_path / "custom",
        user_config_source=tmp_path / "absent.yaml",
    )

    assert result.ok and result.value is not None
    assert result.value.destination == tmp_path / "custom"
    assert result.value.resolution.sources["destination"] == "cli"


def test_run_input_resolution_rejects_seed_range_conflicts(tmp_path: Path) -> None:
    result = resolve_run_inputs_operation(
        tmp_path / "experiment.yaml",
        seed=7,
        seeds="7:9",
    )

    assert result.error is not None
    assert result.error.code == "SEED_CONFLICT"


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


def test_plan_input_resolution_generates_a_preview_without_planner_entropy(
    tmp_path: Path,
) -> None:
    project = tmp_path / "rundra.yaml"
    project.write_text(
        "version: 1\ndefaults: {config: config.yaml, target: local}\n",
        encoding="utf-8",
    )
    calls = 0

    def generate() -> int:
        nonlocal calls
        calls += 1
        return 41

    result = resolve_plan_inputs_operation(
        tmp_path / "experiment.yaml",
        project_file=project,
        targets_file=tmp_path / "targets.yaml",
        user_config_source=tmp_path / "absent.yaml",
        seed_factory=generate,
    )

    assert result.ok and result.value is not None
    assert result.value.seed == 41
    assert result.value.seeds is None
    assert result.value.resolution.sources["seed"] == "generated"
    assert calls == 1


def test_explicit_seed_range_overrides_a_configured_single_seed_for_plan(
    tmp_path: Path,
) -> None:
    project = tmp_path / "rundra.yaml"
    project.write_text(
        "version: 1\ndefaults: {config: config.yaml, target: local, seed: 17}\n",
        encoding="utf-8",
    )

    result = resolve_plan_inputs_operation(
        tmp_path / "experiment.yaml",
        project_file=project,
        targets_file=tmp_path / "targets.yaml",
        seeds="0:2",
        user_config_source=tmp_path / "absent.yaml",
        seed_factory=lambda: (_ for _ in ()).throw(AssertionError("no entropy")),
    )

    assert result.ok and result.value is not None
    assert result.value.seed is None
    assert result.value.seeds == "0:2"
    assert "seed" not in result.value.resolution.sources
