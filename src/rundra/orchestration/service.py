from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path, PurePath, PurePosixPath

from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    Command,
    ExperimentSpec,
    Run,
    RunId,
    Task,
)
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.models import ExecutionPlan, ExecutionUnit
from rundra.orchestration.planner import create_plan
from rundra.persistence.base import RunStore
from rundra.ports import (
    BindMount,
    ContainerRequest,
    ContainerRuntime,
    FetchRequest,
    Scheduler,
    SchedulerObservation,
    StagedWorkspace,
    Stager,
    StageRequest,
    Transport,
)
from rundra.provenance import GitProvenance, ProvenanceProvider

_CONTAINER_SOURCE = PurePosixPath("/workspace/source")
_CONTAINER_INPUTS = PurePosixPath("/workspace/input")
_CONTAINER_CONFIG = _CONTAINER_INPUTS / "config.yaml"
_CONTAINER_OUTPUTS = PurePosixPath("/workspace/output")
_CONTAINER_RUNTIME = PurePosixPath("/workspace/runtime")


class OrchestrationError(RuntimeError):
    """An actionable failure in the durable execution lifecycle."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        run_id: RunId | None = None,
    ) -> None:
        if type(code) is not str or not code:
            raise ValueError("OrchestrationError code must be nonempty")
        if type(message) is not str or not message:
            raise ValueError("OrchestrationError message must be nonempty")
        if run_id is not None and type(run_id) is not RunId:
            raise TypeError("OrchestrationError run_id must be a RunId or None")
        self.code = code
        self.message = message
        self.run_id = run_id
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class RunExecutionRequest:
    """Inputs for the M1 single-Task synchronous execution lifecycle."""

    plan: ExecutionPlan
    experiment: ExperimentSpec
    source_root: PurePath
    fetch_destination: PurePath
    experiment_source: PurePath | None = None
    initiator: str | None = None

    def __post_init__(self) -> None:
        if type(self.plan) is not ExecutionPlan:
            raise TypeError("RunExecutionRequest plan must be an ExecutionPlan")
        if type(self.experiment) is not ExperimentSpec:
            raise TypeError("RunExecutionRequest experiment must be an ExperimentSpec")
        for name in ("source_root", "fetch_destination"):
            if not isinstance(getattr(self, name), PurePath):
                raise TypeError(f"RunExecutionRequest {name} must be a PurePath")
        if self.experiment_source is not None and not isinstance(
            self.experiment_source, PurePath
        ):
            raise TypeError(
                "RunExecutionRequest experiment_source must be a PurePath or None"
            )
        if self.initiator is not None and (
            type(self.initiator) is not str or not self.initiator.strip()
        ):
            raise ValueError(
                "RunExecutionRequest initiator must be a nonblank string or None"
            )


@dataclass(frozen=True, slots=True)
class RunExecutionResult:
    """Completed computation and retrieval record plus its retained workspace."""

    record: RunRecord
    workspace: StagedWorkspace

    def __post_init__(self) -> None:
        if type(self.record) is not RunRecord:
            raise TypeError("RunExecutionResult record must be a RunRecord")
        if type(self.workspace) is not StagedWorkspace:
            raise TypeError("RunExecutionResult workspace must be a StagedWorkspace")


class OrchestrationService:
    """Coordinate one synchronous Task through portable infrastructure ports."""

    def __init__(
        self,
        *,
        store: RunStore,
        stager: Stager,
        runtime: ContainerRuntime,
        scheduler: Scheduler,
        transport: Transport,
        run_id_factory: Callable[[], RunId] = RunId.new,
        clock: Callable[[], datetime] | None = None,
        framework_version: str,
        provenance: ProvenanceProvider | None = None,
    ) -> None:
        for name, value, protocol in (
            ("store", store, RunStore),
            ("stager", stager, Stager),
            ("runtime", runtime, ContainerRuntime),
            ("scheduler", scheduler, Scheduler),
            ("transport", transport, Transport),
        ):
            if not isinstance(value, protocol):
                raise TypeError(
                    f"OrchestrationService {name} must implement {protocol.__name__}"
                )
        if not callable(run_id_factory):
            raise TypeError("OrchestrationService run_id_factory must be callable")
        if clock is not None and not callable(clock):
            raise TypeError("OrchestrationService clock must be callable")
        if type(framework_version) is not str or not framework_version.strip():
            raise ValueError("OrchestrationService framework_version must be nonblank")
        if provenance is not None and not isinstance(provenance, ProvenanceProvider):
            raise TypeError(
                "OrchestrationService provenance must implement ProvenanceProvider"
            )
        self.store = store
        self._stager = stager
        self._runtime = runtime
        self._scheduler = scheduler
        self._transport = transport
        self._run_id_factory = run_id_factory
        self._clock = clock or (lambda: datetime.now(UTC))
        self._framework_version = framework_version
        self._provenance = provenance

    def execute_one(self, request: RunExecutionRequest) -> RunExecutionResult:
        """Execute and fetch one planned Task while durably recording each phase."""
        self._validate_request(request)
        unit = request.plan.units[0]
        run_id = self._run_id_factory()
        if type(run_id) is not RunId:
            raise TypeError("Run ID factory must return a RunId")
        provenance = GitProvenance()
        if self._provenance is not None:
            try:
                captured = self._provenance.capture(request.source_root)
                if type(captured) is GitProvenance:
                    provenance = captured
            except Exception:
                provenance = GitProvenance()
        record = self._created_record(request, unit, run_id, provenance)
        self.store.create(record)

        try:
            self._transport.check()
            self._runtime.check()
        except Exception as error:
            self._fail_before_completion(record, "CAPABILITY_CHECK_FAILED")
            raise OrchestrationError(
                code="CAPABILITY_CHECK_FAILED",
                message=f"Run {run_id} capability check failed: {error}",
                run_id=run_id,
            ) from error

        record = _with_execution_state(record, ExecutionState.STAGING)
        self.store.update(record)
        try:
            workspace = self._stager.stage(
                StageRequest(
                    run_id=run_id,
                    experiment=request.experiment,
                    config=unit.config,
                    target=request.plan.target,
                    source_root=request.source_root,
                )
            )
        except Exception as error:
            self._fail_before_completion(record, "STAGING_FAILED")
            raise OrchestrationError(
                code="STAGING_FAILED",
                message=f"Run {run_id} staging failed: {error}",
                run_id=run_id,
            ) from error
        record = replace(record, artifacts=workspace.artifacts)
        self.store.update(record)

        try:
            scheduled_unit = replace(
                unit,
                command=self._runtime.build_command(
                    _container_request(request.experiment, unit, workspace)
                ),
            )
        except Exception as error:
            self._fail_before_completion(record, "CONTAINER_COMMAND_FAILED")
            raise OrchestrationError(
                code="CONTAINER_COMMAND_FAILED",
                message=f"Run {run_id} container command construction failed: {error}",
                run_id=run_id,
            ) from error

        try:
            submission = self._scheduler.submit((scheduled_unit,))
            observation = _single_observation(
                self._scheduler.query((submission.reference,)),
                submission.reference,
            )
            if submission.task_native_ids != {
                unit.task_id: submission.reference.native_id
            }:
                raise ValueError(
                    "Scheduler submission did not map the planned Task exactly"
                )
            command_result = observation.result
            if command_result is None or observation.exit_code is None:
                raise ValueError(
                    "Synchronous local scheduler observation lacks a command result"
                )
            if observation.state not in {
                ExecutionState.SUCCEEDED,
                ExecutionState.FAILED,
            }:
                raise ValueError(
                    "Synchronous local scheduler did not return a terminal state"
                )
            expected_state = (
                ExecutionState.SUCCEEDED
                if observation.exit_code == 0
                else ExecutionState.FAILED
            )
            if observation.state is not expected_state:
                raise ValueError(
                    "Synchronous local scheduler state conflicts with its exit code"
                )
            if command_result.command != scheduled_unit.command:
                raise ValueError(
                    "Synchronous local scheduler result describes another command"
                )
        except Exception as error:
            self._fail_before_completion(record, "EXECUTION_FAILED")
            raise OrchestrationError(
                code="EXECUTION_FAILED",
                message=f"Run {run_id} execution failed before reconciliation: {error}",
                run_id=run_id,
            ) from error

        record = _with_execution_state(record, ExecutionState.SUBMITTED)
        record = replace(
            record,
            scheduler_job_ids=(submission.reference.native_id,),
            submitted_at=command_result.started_at,
        )
        self.store.update(record)

        try:
            log_artifacts = _write_task_logs(
                workspace,
                unit,
                stdout=command_result.stdout,
                stderr=command_result.stderr,
            )
        except Exception as error:
            record = _completed_record(record, observation, unit, artifacts=())
            self.store.update(record)
            self._record_retrieval_failure(record)
            raise OrchestrationError(
                code="LOG_PERSISTENCE_FAILED",
                message=f"Run {run_id} completed but logs could not be persisted: {error}",
                run_id=run_id,
            ) from error

        record = _completed_record(record, observation, unit, artifacts=log_artifacts)
        self.store.update(record)
        record = replace(
            record,
            run=replace(record.run, retrieval_state=RetrievalState.PENDING),
        )
        self.store.update(record)
        try:
            fetched = self._stager.fetch(
                FetchRequest(
                    workspace=workspace,
                    patterns=request.experiment.outputs,
                    destination=request.fetch_destination,
                )
            )
            fetched_artifacts = _fetched_task_artifacts(fetched.artifacts, unit)
        except Exception as error:
            failed = replace(
                record,
                run=replace(record.run, retrieval_state=RetrievalState.FAILED),
            )
            self.store.update(failed)
            raise OrchestrationError(
                code="RESULT_RETRIEVAL_FAILED",
                message=f"Run {run_id} result retrieval failed: {error}",
                run_id=run_id,
            ) from error

        record = replace(
            record,
            run=replace(record.run, retrieval_state=RetrievalState.SUCCEEDED),
            artifacts=(*record.artifacts, *fetched_artifacts),
        )
        self.store.update(record)
        return RunExecutionResult(record, workspace)

    @staticmethod
    def _validate_request(request: RunExecutionRequest) -> None:
        if type(request) is not RunExecutionRequest:
            raise TypeError(
                "OrchestrationService.execute_one requires a RunExecutionRequest"
            )
        if len(request.plan.units) != 1:
            raise OrchestrationError(
                code="UNSUPPORTED_TASK_COUNT",
                message="M1.4 execution requires exactly one Task",
            )
        unit = request.plan.units[0]
        if request.plan.experiment_name != request.experiment.name:
            raise OrchestrationError(
                code="PLAN_MISMATCH",
                message="Execution plan and experiment names do not match",
            )
        if unit.resources != request.experiment.resources:
            raise OrchestrationError(
                code="PLAN_MISMATCH",
                message="Execution plan resources do not match the experiment",
            )
        expected_plan = create_plan(
            request.experiment,
            unit.config,
            request.plan.target,
            seeds=(unit.seed,),
        )
        if request.plan != expected_plan:
            raise OrchestrationError(
                code="PLAN_MISMATCH",
                message="Execution plan does not match the experiment and Task input",
            )

    def _created_record(
        self,
        request: RunExecutionRequest,
        unit: ExecutionUnit,
        run_id: RunId,
        provenance: GitProvenance,
    ) -> RunRecord:
        task = Task(
            id=unit.task_id,
            run_id=run_id,
            experiment_name=request.experiment.name,
            config=unit.config,
            seed=unit.seed,
            resources=unit.resources,
        )
        run = Run(
            id=run_id,
            experiment_name=request.experiment.name,
            target=request.plan.target,
            tasks=(task,),
            created_at=self._clock(),
        )
        return RunRecord(
            format_version=1,
            framework_version=self._framework_version,
            run=run,
            experiment=request.experiment,
            source_root=request.source_root,
            experiment_source=request.experiment_source,
            initiator=request.initiator,
            git_commit=provenance.commit,
            git_branch=provenance.branch,
            git_dirty=provenance.dirty,
            git_diff=provenance.diff,
        )

    def _fail_before_completion(self, record: RunRecord, native_state: str) -> None:
        failed = _with_execution_state(record, ExecutionState.FAILED)
        self.store.update(
            replace(
                failed,
                completed_at=self._clock(),
                native_state=native_state,
            )
        )

    def _record_retrieval_failure(self, record: RunRecord) -> None:
        pending = replace(
            record,
            run=replace(record.run, retrieval_state=RetrievalState.PENDING),
        )
        self.store.update(pending)
        self.store.update(
            replace(
                pending,
                run=replace(pending.run, retrieval_state=RetrievalState.FAILED),
            )
        )


def _with_execution_state(record: RunRecord, state: ExecutionState) -> RunRecord:
    tasks = tuple(replace(task, state=state) for task in record.run.tasks)
    return replace(record, run=replace(record.run, tasks=tasks, state=state))


def _container_request(
    experiment: ExperimentSpec,
    unit: ExecutionUnit,
    workspace: StagedWorkspace,
) -> ContainerRequest:
    container = experiment.container
    command = Command(
        tuple(
            argument.replace("{config}", str(_CONTAINER_CONFIG)).replace(
                "{seed}", str(unit.seed)
            )
            for argument in experiment.command.argv
        ),
        environment=unit.command.environment,
        working_directory=_container_working_directory(unit.command.working_directory),
    )
    image = None
    gpu = False
    if container is not None:
        image = (
            container.image
            if container.image.is_absolute()
            else workspace.source / container.image
        )
        gpu = container.gpu
    return ContainerRequest(
        command=command,
        image=image,
        gpu=gpu,
        binds=(
            BindMount(workspace.source, _CONTAINER_SOURCE, read_only=True),
            BindMount(workspace.inputs, _CONTAINER_INPUTS, read_only=True),
            BindMount(workspace.outputs, _CONTAINER_OUTPUTS, read_only=False),
            BindMount(workspace.runtime, _CONTAINER_RUNTIME, read_only=False),
        ),
    )


def _container_working_directory(value: PurePath | None) -> PurePath:
    if value is None:
        return _CONTAINER_SOURCE
    if value.is_absolute():
        return value
    return _CONTAINER_SOURCE / value


def _single_observation(
    observations: tuple[SchedulerObservation, ...],
    expected_reference: object,
) -> SchedulerObservation:
    if len(observations) != 1:
        raise ValueError("Scheduler must return exactly one observation")
    observation = observations[0]
    if observation.reference != expected_reference:
        raise ValueError("Scheduler observation reference does not match submission")
    return observation


def _write_task_logs(
    workspace: StagedWorkspace,
    unit: ExecutionUnit,
    *,
    stdout: str,
    stderr: str,
) -> tuple[Artifact, Artifact]:
    logs = Path(str(workspace.logs))
    stdout_path = logs / f"{unit.task_id}.stdout"
    stderr_path = logs / f"{unit.task_id}.stderr"
    stdout_size = _write_new_file(stdout_path, stdout)
    stderr_size = _write_new_file(stderr_path, stderr)
    return (
        Artifact(
            ArtifactKind.STDOUT,
            stdout_path,
            task_id=unit.task_id,
            size_bytes=stdout_size,
        ),
        Artifact(
            ArtifactKind.STDERR,
            stderr_path,
            task_id=unit.task_id,
            size_bytes=stderr_size,
        ),
    )


def _write_new_file(path: Path, content: str) -> int:
    encoded = content.encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    return len(encoded)


def _fetched_task_artifacts(
    artifacts: tuple[Artifact, ...],
    unit: ExecutionUnit,
) -> tuple[Artifact, ...]:
    if any(artifact.kind is not ArtifactKind.RAW_RESULT for artifact in artifacts):
        raise ValueError("Stager fetch returned a non-result artifact")
    if any(
        artifact.task_id is not None and artifact.task_id != unit.task_id
        for artifact in artifacts
    ):
        raise ValueError("Stager fetch returned an artifact for another Task")
    return tuple(replace(artifact, task_id=unit.task_id) for artifact in artifacts)


def _completed_record(
    record: RunRecord,
    observation: SchedulerObservation,
    unit: ExecutionUnit,
    *,
    artifacts: tuple[Artifact, ...],
) -> RunRecord:
    result = observation.result
    if result is None or observation.exit_code is None:
        raise ValueError("Terminal observation requires a command result and exit code")
    completed = _with_execution_state(record, observation.state)
    return replace(
        completed,
        started_at=result.started_at,
        completed_at=result.finished_at,
        native_state=observation.native_state,
        task_exit_codes={unit.task_id: observation.exit_code},
        artifacts=(*record.artifacts, *artifacts),
    )
