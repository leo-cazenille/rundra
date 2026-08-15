from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path, PurePath, PurePosixPath
from time import monotonic, sleep

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
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerUnit,
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
_TERMINAL_STATES = frozenset(
    {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }
)


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


class SchedulerLifecycleService:
    """Reconcile durable Run records with a scheduler without owning a daemon."""

    def __init__(
        self,
        *,
        store: RunStore,
        scheduler: Scheduler,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = sleep,
        monotonic_clock: Callable[[], float] = monotonic,
    ) -> None:
        if not isinstance(store, RunStore):
            raise TypeError("SchedulerLifecycleService store must implement RunStore")
        if not isinstance(scheduler, Scheduler):
            raise TypeError(
                "SchedulerLifecycleService scheduler must implement Scheduler"
            )
        if clock is not None and not callable(clock):
            raise TypeError("SchedulerLifecycleService clock must be callable")
        if not callable(sleeper) or not callable(monotonic_clock):
            raise TypeError("SchedulerLifecycleService timing hooks must be callable")
        self._store = store
        self._scheduler = scheduler
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper
        self._monotonic = monotonic_clock

    def refresh(self, record: RunRecord) -> RunRecord:
        """Query and durably apply one scheduler observation when active."""
        _require_record(record)
        if record.run.state in _TERMINAL_STATES:
            return record
        reference = _record_reference(record)
        observation = _single_observation(
            self._scheduler.query((reference,)), reference
        )
        updated = _observed_record(record, observation, self._clock())
        self._store.update(updated)
        return updated

    def wait(
        self,
        record: RunRecord,
        *,
        timeout: float | None = None,
        poll_interval: float = 2.0,
    ) -> RunRecord:
        """Poll until terminal, leaving an active Run intact on client timeout."""
        _require_record(record)
        if timeout is not None and (type(timeout) not in (int, float) or timeout < 0):
            raise ValueError("Scheduler wait timeout must be non-negative or None")
        if type(poll_interval) not in (int, float) or poll_interval <= 0:
            raise ValueError("Scheduler poll interval must be positive")
        deadline = None if timeout is None else self._monotonic() + float(timeout)
        current = record
        while current.run.state not in _TERMINAL_STATES:
            try:
                current = self.refresh(current)
            except Exception as error:
                raise OrchestrationError(
                    code="SCHEDULER_QUERY_FAILED",
                    message=f"Run {record.run.id} scheduler query failed: {error}",
                    run_id=record.run.id,
                ) from error
            if current.run.state in _TERMINAL_STATES:
                return current
            now = self._monotonic()
            if deadline is not None and now >= deadline:
                raise OrchestrationError(
                    code="SCHEDULER_TIMEOUT",
                    message=f"Run {record.run.id} did not finish before the wait timeout",
                    run_id=record.run.id,
                )
            delay = float(poll_interval)
            if deadline is not None:
                delay = min(delay, max(0.0, deadline - now))
            self._sleeper(delay)
        return current

    def cancel(
        self,
        record: RunRecord,
        *,
        timeout: float | None = 30.0,
        poll_interval: float = 1.0,
    ) -> RunRecord:
        """Cancel an active Run, treating an already terminal Run idempotently."""
        _require_record(record)
        if record.run.state in _TERMINAL_STATES:
            return record
        reference = _record_reference(record)
        try:
            observation = _single_observation(
                self._scheduler.cancel((reference,)), reference
            )
        except Exception as error:
            raise OrchestrationError(
                code="SCHEDULER_CANCEL_FAILED",
                message=f"Run {record.run.id} cancellation failed: {error}",
                run_id=record.run.id,
            ) from error
        current = _observed_record(record, observation, self._clock())
        self._store.update(current)
        return self.wait(current, timeout=timeout, poll_interval=poll_interval)


@dataclass(frozen=True, slots=True)
class RunExecutionRequest:
    """Inputs for one local or remote single-Task execution lifecycle."""

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
    """Coordinate one synchronous or asynchronous Task through portable ports."""

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
        return self._execute_one(request, wait=True)

    def submit_one(self, request: RunExecutionRequest) -> RunExecutionResult:
        """Stage and submit one Task, returning once its reference is durable."""
        return self._execute_one(request, wait=False)

    def _execute_one(
        self, request: RunExecutionRequest, *, wait: bool
    ) -> RunExecutionResult:
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

        submission_started_at = self._clock()
        try:
            submission = self._scheduler.submit(
                SchedulerGroup(
                    (
                        SchedulerUnit(
                            scheduled_unit.task_id,
                            scheduled_unit.command,
                            scheduled_unit.resources,
                        ),
                    )
                )
            )
            if submission.task_native_ids != {
                unit.task_id: submission.reference.native_id
            }:
                raise ValueError(
                    "Scheduler submission did not map the planned Task exactly"
                )
        except Exception as error:
            self._fail_before_completion(record, "SCHEDULER_SUBMISSION_FAILED")
            raise OrchestrationError(
                code="SCHEDULER_SUBMISSION_FAILED",
                message=f"Run {run_id} scheduler submission failed: {error}",
                run_id=run_id,
            ) from error

        record = replace(
            _with_execution_state(record, ExecutionState.SUBMITTED),
            scheduler_job_ids=(submission.reference.native_id,),
            submitted_at=submission_started_at,
        )
        self.store.update(record)

        if not wait:
            return RunExecutionResult(record, workspace)

        try:
            lifecycle = SchedulerLifecycleService(
                store=self.store,
                scheduler=self._scheduler,
                clock=self._clock,
            )
            record = lifecycle.wait(record)
            observation = _single_observation(
                self._scheduler.query((submission.reference,)),
                submission.reference,
            )
            command_result = observation.result
            if observation.state not in _TERMINAL_STATES:
                raise ValueError("Scheduler wait returned a nonterminal state")
            if (
                command_result is not None
                and command_result.command != scheduled_unit.command
            ):
                raise ValueError(
                    "Synchronous local scheduler result describes another command"
                )
        except OrchestrationError:
            raise
        except Exception as error:
            raise OrchestrationError(
                code="SCHEDULER_QUERY_FAILED",
                message=f"Run {run_id} scheduler reconciliation failed: {error}",
                run_id=run_id,
            ) from error

        if command_result is not None:
            try:
                log_artifacts = _write_task_logs(
                    workspace,
                    unit,
                    stdout=command_result.stdout,
                    stderr=command_result.stderr,
                )
            except Exception as error:
                self._record_retrieval_failure(record)
                raise OrchestrationError(
                    code="LOG_PERSISTENCE_FAILED",
                    message=(
                        f"Run {run_id} completed but logs could not be persisted: {error}"
                    ),
                    run_id=run_id,
                ) from error
            record = replace(record, artifacts=(*record.artifacts, *log_artifacts))
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
                message="M3 execution requires exactly one Task",
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


def _require_record(record: RunRecord) -> None:
    if type(record) is not RunRecord:
        raise TypeError("Scheduler lifecycle requires a RunRecord")


def _record_reference(record: RunRecord) -> SchedulerReference:
    if len(record.scheduler_job_ids) != 1:
        raise OrchestrationError(
            code="SCHEDULER_REFERENCE_UNAVAILABLE",
            message=f"Run {record.run.id} does not have exactly one scheduler reference",
            run_id=record.run.id,
        )
    return SchedulerReference(record.scheduler_job_ids[0])


def _observed_record(
    record: RunRecord,
    observation: SchedulerObservation,
    observed_at: datetime,
) -> RunRecord:
    state = observation.state
    updated = _with_execution_state(record, state)
    task_id = record.run.tasks[0].id
    exit_codes = dict(record.task_exit_codes)
    if observation.exit_code is not None:
        exit_codes[task_id] = observation.exit_code
    terminal = state in _TERMINAL_STATES
    nodes = observation.metadata.get("allocated_nodes")
    return replace(
        updated,
        allocated_nodes=(str(nodes),) if nodes is not None else record.allocated_nodes,
        started_at=record.started_at or observation.started_at,
        completed_at=(
            observation.finished_at or observed_at if terminal else record.completed_at
        ),
        native_state=observation.native_state,
        task_exit_codes=exit_codes,
    )


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
    allowed = {
        ArtifactKind.RAW_RESULT,
        ArtifactKind.STDOUT,
        ArtifactKind.STDERR,
        ArtifactKind.SCHEDULER_METADATA,
    }
    if any(artifact.kind not in allowed for artifact in artifacts):
        raise ValueError("Stager fetch returned an unsupported artifact")
    if any(
        artifact.task_id is not None and artifact.task_id != unit.task_id
        for artifact in artifacts
    ):
        raise ValueError("Stager fetch returned an artifact for another Task")
    return tuple(
        replace(artifact, task_id=unit.task_id)
        if artifact.kind is ArtifactKind.RAW_RESULT and artifact.task_id is None
        else artifact
        for artifact in artifacts
    )
