from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from importlib.metadata import version
from pathlib import Path, PurePath
from types import MappingProxyType

from rundra.adapters import (
    ApptainerRuntime,
    LocalScheduler,
    LocalStager,
    LocalTransport,
)
from rundra.config.errors import ConfigError
from rundra.config.experiments import load_config_snapshot, load_experiment
from rundra.config.targets import load_targets
from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    ExperimentSpec,
    RunId,
    Target,
    TaskId,
)
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.models import ExecutionPlan, PlanningError
from rundra.orchestration.planner import create_plan, expand_seeds
from rundra.orchestration.service import (
    OrchestrationError,
    OrchestrationService,
    RunExecutionRequest,
)
from rundra.persistence import RunNotFoundError, RunStore, RunStoreError
from rundra.ports import FetchRequest, StagedWorkspace
from rundra.results import OperationError, OperationResult


@dataclass(frozen=True, slots=True)
class ValidationValue:
    source: Path
    experiment: ExperimentSpec


@dataclass(frozen=True, slots=True)
class TargetsValue:
    source: Path
    targets: Mapping[str, Target]


@dataclass(frozen=True, slots=True)
class RunValue:
    record: RunRecord

    @property
    def run_id(self) -> RunId:
        return self.record.run.id

    @property
    def exit_code(self) -> int:
        return 0 if self.record.run.state is ExecutionState.SUCCEEDED else 2


@dataclass(frozen=True, slots=True)
class StatusValue:
    run_id: RunId
    experiment: str
    target: str
    state: ExecutionState
    retrieval_state: RetrievalState
    task_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "task_counts", MappingProxyType(dict(self.task_counts))
        )


@dataclass(frozen=True, slots=True)
class ListRunsValue:
    runs: tuple[StatusValue, ...]


@dataclass(frozen=True, slots=True)
class InspectValue:
    record: RunRecord


@dataclass(frozen=True, slots=True)
class LogsValue:
    run_id: RunId
    task_id: TaskId
    stdout: str
    stderr: str
    stdout_path: PurePath
    stderr_path: PurePath


@dataclass(frozen=True, slots=True)
class FetchValue:
    run_id: RunId
    destination: PurePath
    retrieval_state: RetrievalState
    artifacts: tuple[Artifact, ...]


def validate_operation(source: Path) -> OperationResult[ValidationValue]:
    try:
        return OperationResult.success(
            "validate", ValidationValue(source, load_experiment(source))
        )
    except ConfigError as error:
        return OperationResult.failure("validate", _config_error(error))


def plan_operation(
    experiment_source: Path,
    config_source: Path,
    targets_source: Path,
    target_name: str,
    *,
    seed: object = None,
    seeds: object = None,
) -> OperationResult[ExecutionPlan]:
    try:
        experiment = load_experiment(experiment_source)
        config = load_config_snapshot(config_source)
        targets = load_targets(targets_source)
        if target_name not in targets:
            return OperationResult.failure(
                "plan",
                OperationError(
                    "TARGET_NOT_FOUND",
                    f"Target '{target_name}' is not defined",
                    {"source": str(targets_source), "target": target_name},
                ),
            )
        plan = create_plan(
            experiment,
            config,
            targets[target_name],
            seeds=expand_seeds(seed=seed, seeds=seeds),
        )
        return OperationResult.success("plan", plan)
    except ConfigError as error:
        return OperationResult.failure("plan", _config_error(error))
    except PlanningError as error:
        return OperationResult.failure(
            "plan", OperationError(error.code, error.message, error.details)
        )


def targets_operation(source: Path) -> OperationResult[TargetsValue]:
    try:
        return OperationResult.success(
            "targets", TargetsValue(source, load_targets(source))
        )
    except ConfigError as error:
        return OperationResult.failure("targets", _config_error(error))


def run_operation(
    experiment_source: Path,
    config_source: Path,
    targets_source: Path,
    target_name: str,
    source_root: Path,
    destination: Path,
    store: RunStore,
    *,
    seed: object,
) -> OperationResult[RunValue]:
    try:
        experiment = load_experiment(experiment_source)
        config = load_config_snapshot(config_source)
        targets = load_targets(targets_source)
        if target_name not in targets:
            return OperationResult.failure(
                "run",
                OperationError(
                    "TARGET_NOT_FOUND",
                    f"Target '{target_name}' is not defined",
                    {"source": str(targets_source), "target": target_name},
                ),
            )
        target = targets[target_name]
        unsupported = _unsupported_local_target(target)
        if unsupported is not None:
            return OperationResult.failure("run", unsupported)
        plan = create_plan(
            experiment,
            config,
            target,
            seeds=expand_seeds(seed=seed),
        )
        transport = LocalTransport()
        service = OrchestrationService(
            store=store,
            stager=LocalStager(),
            runtime=ApptainerRuntime(),
            scheduler=LocalScheduler(transport),
            transport=transport,
            framework_version=version("rundra"),
        )
        result = service.execute_one(
            RunExecutionRequest(
                plan=plan,
                experiment=experiment,
                source_root=source_root,
                fetch_destination=destination,
                experiment_source=experiment_source,
            )
        )
        return OperationResult.success("run", RunValue(result.record))
    except ConfigError as error:
        return OperationResult.failure("run", _config_error(error))
    except PlanningError as error:
        return OperationResult.failure(
            "run", OperationError(error.code, error.message, error.details)
        )
    except OrchestrationError as error:
        details = {} if error.run_id is None else {"run_id": str(error.run_id)}
        return OperationResult.failure(
            "run", OperationError(error.code, error.message, details)
        )
    except RunStoreError as error:
        return OperationResult.failure(
            "run", OperationError("RUN_STORE_ERROR", str(error))
        )


def submit_unavailable_operation() -> OperationResult[RunValue]:
    return OperationResult.failure(
        "submit",
        OperationError(
            "ASYNC_UNAVAILABLE",
            "Asynchronous submit is unavailable until durable backend semantics exist",
        ),
    )


def status_operation(
    run_id: str,
    store: RunStore,
) -> OperationResult[StatusValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("status", error)
    assert record is not None
    return OperationResult.success("status", _status_value(record))


def list_runs_operation(store: RunStore) -> OperationResult[ListRunsValue]:
    try:
        return OperationResult.success(
            "list",
            ListRunsValue(tuple(_status_value(record) for record in store.list())),
        )
    except RunStoreError as error:
        return OperationResult.failure(
            "list", OperationError("RUN_STORE_ERROR", str(error))
        )


def inspect_operation(
    run_id: str,
    store: RunStore,
) -> OperationResult[InspectValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("inspect", error)
    assert record is not None
    return OperationResult.success("inspect", InspectValue(record))


def logs_operation(
    run_id: str,
    store: RunStore,
    *,
    task: str | None = None,
) -> OperationResult[LogsValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("logs", error)
    assert record is not None
    task_id = _selected_task_id(record, task)
    if isinstance(task_id, OperationError):
        return OperationResult.failure("logs", task_id)
    stdout = _artifact_for(record, ArtifactKind.STDOUT, task_id)
    stderr = _artifact_for(record, ArtifactKind.STDERR, task_id)
    if stdout is None or stderr is None:
        return OperationResult.failure(
            "logs",
            OperationError(
                "LOGS_UNAVAILABLE",
                f"Logs are unavailable for Task {task_id}",
                {"run_id": str(record.run.id), "task_id": str(task_id)},
            ),
        )
    try:
        stdout_text = Path(str(stdout.path)).read_text(encoding="utf-8")
        stderr_text = Path(str(stderr.path)).read_text(encoding="utf-8")
    except OSError as error:
        return OperationResult.failure(
            "logs",
            OperationError(
                "LOG_READ_FAILED",
                f"Could not read logs for Task {task_id}: {error}",
                {"run_id": str(record.run.id), "task_id": str(task_id)},
            ),
        )
    return OperationResult.success(
        "logs",
        LogsValue(
            record.run.id,
            task_id,
            stdout_text,
            stderr_text,
            stdout.path,
            stderr.path,
        ),
    )


def fetch_operation(
    run_id: str,
    store: RunStore,
    destination: Path,
) -> OperationResult[FetchValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("fetch", error)
    assert record is not None
    if record.run.target.staging.kind != "local" or len(record.run.tasks) != 1:
        return OperationResult.failure(
            "fetch",
            OperationError(
                "FETCH_UNSUPPORTED",
                "M1 fetch supports only one-Task Runs with local staging",
                {"run_id": str(record.run.id)},
            ),
        )
    original_state = record.run.retrieval_state
    pending = original_state in {
        RetrievalState.NOT_REQUESTED,
        RetrievalState.FAILED,
    }
    if pending:
        record = replace(
            record,
            run=replace(record.run, retrieval_state=RetrievalState.PENDING),
        )
        try:
            store.update(record)
        except RunStoreError as error:
            return OperationResult.failure(
                "fetch", OperationError("RUN_STORE_ERROR", str(error))
            )
    workspace = _local_workspace(record)
    try:
        fetched = LocalStager().fetch(
            FetchRequest(workspace, record.experiment.outputs, destination)
        )
    except (OSError, RuntimeError, ValueError) as error:
        if pending:
            try:
                store.update(
                    replace(
                        record,
                        run=replace(record.run, retrieval_state=RetrievalState.FAILED),
                    )
                )
            except RunStoreError:
                pass
        return OperationResult.failure(
            "fetch",
            OperationError(
                "RESULT_RETRIEVAL_FAILED",
                f"Run {record.run.id} result retrieval failed: {error}",
                {"run_id": str(record.run.id)},
            ),
        )
    task_id = record.run.tasks[0].id
    artifacts = tuple(
        replace(artifact, task_id=task_id) for artifact in fetched.artifacts
    )
    merged = _merge_artifacts(record.artifacts, artifacts)
    succeeded = replace(
        record,
        run=replace(record.run, retrieval_state=RetrievalState.SUCCEEDED),
        artifacts=merged,
    )
    try:
        store.update(succeeded)
    except RunStoreError as error:
        return OperationResult.failure(
            "fetch", OperationError("RUN_STORE_ERROR", str(error))
        )
    return OperationResult.success(
        "fetch",
        FetchValue(record.run.id, destination, RetrievalState.SUCCEEDED, artifacts),
    )


def _load_record(
    value: str,
    store: RunStore,
) -> tuple[RunRecord | None, OperationError | None]:
    try:
        run_id = RunId(value)
    except (TypeError, ValueError) as error:
        return None, OperationError("INVALID_RUN_ID", str(error), {"run_id": value})
    try:
        return store.load(run_id), None
    except RunNotFoundError as error:
        return None, OperationError("RUN_NOT_FOUND", str(error), {"run_id": value})
    except RunStoreError as error:
        return None, OperationError("RUN_STORE_ERROR", str(error), {"run_id": value})


def _status_value(record: RunRecord) -> StatusValue:
    counts: dict[str, int] = {}
    for task in record.run.tasks:
        counts[task.state.value] = counts.get(task.state.value, 0) + 1
    return StatusValue(
        run_id=record.run.id,
        experiment=record.run.experiment_name,
        target=record.run.target.name,
        state=record.run.state,
        retrieval_state=record.run.retrieval_state,
        task_counts=counts,
    )


def _selected_task_id(record: RunRecord, value: str | None) -> TaskId | OperationError:
    try:
        selected = (
            record.run.tasks[0].id
            if value is None and len(record.run.tasks) == 1
            else TaskId.from_ordinal(int(value))
            if value is not None and value.isdigit()
            else TaskId(value)
            if value is not None
            else None
        )
    except (TypeError, ValueError) as error:
        return OperationError("INVALID_TASK_ID", str(error))
    if selected is None:
        return OperationError(
            "TASK_REQUIRED", "Select a Task when a Run contains multiple Tasks"
        )
    if selected not in {task.id for task in record.run.tasks}:
        return OperationError(
            "TASK_NOT_FOUND",
            f"Task {selected} does not belong to Run {record.run.id}",
            {"run_id": str(record.run.id), "task_id": str(selected)},
        )
    return selected


def _artifact_for(
    record: RunRecord,
    kind: ArtifactKind,
    task_id: TaskId,
) -> Artifact | None:
    return next(
        (
            artifact
            for artifact in record.artifacts
            if artifact.kind is kind and artifact.task_id == task_id
        ),
        None,
    )


def _local_workspace(record: RunRecord) -> StagedWorkspace:
    root = (
        Path(str(record.run.target.workspace)).expanduser().resolve()
        / "runs"
        / str(record.run.id)
    )
    return StagedWorkspace(
        root=root,
        source=root / "source",
        inputs=root / "input",
        config=root / "input/config.yaml",
        runtime=root / "runtime",
        outputs=root / "output",
        logs=root / "logs",
        metadata=root / "metadata",
    )


def _merge_artifacts(
    existing: tuple[Artifact, ...],
    fetched: tuple[Artifact, ...],
) -> tuple[Artifact, ...]:
    merged = {
        (artifact.kind, artifact.task_id, artifact.path): artifact
        for artifact in existing
    }
    merged.update(
        ((artifact.kind, artifact.task_id, artifact.path), artifact)
        for artifact in fetched
    )
    return tuple(
        merged[key]
        for key in sorted(
            merged,
            key=lambda item: (item[0].value, str(item[1]), str(item[2])),
        )
    )


def _unsupported_local_target(target: Target) -> OperationError | None:
    actual = (
        target.transport.kind,
        target.scheduler.kind,
        target.staging.kind,
        target.container.kind,
    )
    expected = ("local", "local", "local", "apptainer")
    if actual == expected:
        return None
    return OperationError(
        "TARGET_UNSUPPORTED",
        f"Synchronous run does not support target '{target.name}'",
        {"target": target.name},
    )


def _config_error(error: ConfigError) -> OperationError:
    return OperationError(
        error.code,
        error.message,
        {"source": str(error.source), "path": error.path},
    )
