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
    TaskId,
)
from rundra.domain.records import RunRecord
from rundra.domain.states import (
    ExecutionState,
    RetrievalState,
    aggregate_execution_state,
)
from rundra.orchestration.models import SLURM_ARRAY, ExecutionPlan, ExecutionUnit
from rundra.orchestration.planner import create_plan
from rundra.persistence.base import RunStore
from rundra.ports import (
    ArrayScheduler,
    BindMount,
    ContainerRequest,
    ContainerRuntime,
    FetchRequest,
    Scheduler,
    SchedulerArrayRequest,
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
        """Query and durably apply every Task scheduler observation."""
        _require_record(record)
        task_references = _record_task_references(record)
        references = tuple(reference for _, reference in task_references)
        observations = self._scheduler.query(references)
        _validate_observations(observations, references)
        updated = _observed_records(
            record,
            tuple(
                (task_id, observation)
                for (task_id, _), observation in zip(
                    task_references, observations, strict=True
                )
            ),
            self._clock(),
        )
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
        if len(record.run.tasks) == 1:
            return self._cancel_single(
                record, timeout=timeout, poll_interval=poll_interval
            )
        try:
            current = self.refresh(record)
            if current.run.state in _TERMINAL_STATES:
                return current
            task_references = _record_task_references(current)
            active = tuple(
                (task_id, reference)
                for task_id, reference in task_references
                if next(task for task in current.run.tasks if task.id == task_id).state
                not in _TERMINAL_STATES
            )
            references = tuple(reference for _, reference in active)
            observations = self._scheduler.cancel(references)
            _validate_observations(observations, references)
        except Exception as error:
            raise OrchestrationError(
                code="SCHEDULER_CANCEL_FAILED",
                message=f"Run {record.run.id} cancellation failed: {error}",
                run_id=record.run.id,
            ) from error
        current = _observed_records(
            current,
            tuple(
                (task_id, observation)
                for (task_id, _), observation in zip(active, observations, strict=True)
            ),
            self._clock(),
        )
        self._store.update(current)
        return self.wait(current, timeout=timeout, poll_interval=poll_interval)

    def _cancel_single(
        self,
        record: RunRecord,
        *,
        timeout: float | None,
        poll_interval: float,
    ) -> RunRecord:
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
    """Inputs for one local or remote planned-Task execution lifecycle."""

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
    """Coordinate one synchronous or asynchronous Run through portable ports."""

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
        units = request.plan.units
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
        record = self._created_record(request, run_id, provenance)
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
                    config=units[0].config,
                    target=request.plan.target,
                    source_root=request.source_root,
                    task_ids=tuple(unit.task_id for unit in units),
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
            scheduled_units = tuple(
                replace(
                    unit,
                    command=self._runtime.build_command(
                        _container_request(
                            request.experiment,
                            unit,
                            workspace,
                            isolate_task=len(units) > 1,
                        )
                    ),
                )
                for unit in units
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
            scheduler_group = SchedulerGroup(
                tuple(
                    SchedulerUnit(unit.task_id, unit.command, unit.resources)
                    for unit in scheduled_units
                )
            )
            if request.plan.strategy == SLURM_ARRAY:
                if not isinstance(self._scheduler, ArrayScheduler):
                    raise TypeError(
                        "Configured scheduler does not support mapped arrays"
                    )
                submission = self._scheduler.submit_array(
                    SchedulerArrayRequest(
                        scheduler_group,
                        request.plan.array_mapping,
                        workspace.metadata / "slurm-array-tasks.sh",
                    )
                )
            else:
                submission = self._scheduler.submit(scheduler_group)
            if set(submission.task_native_ids) != {unit.task_id for unit in units}:
                raise ValueError(
                    "Scheduler submission did not map every planned Task exactly"
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
            task_scheduler_ids=submission.task_native_ids,
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
            command_result = None
            if len(units) == 1:
                task_reference = SchedulerReference(
                    submission.task_native_ids[units[0].task_id]
                )
                observation = _single_observation(
                    self._scheduler.query((task_reference,)), task_reference
                )
                command_result = observation.result
                if observation.state not in _TERMINAL_STATES:
                    raise ValueError("Scheduler wait returned a nonterminal state")
                if (
                    command_result is not None
                    and command_result.command != scheduled_units[0].command
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
                    units[0],
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
                    patterns=_fetch_patterns(request.experiment.outputs, units),
                    destination=request.fetch_destination,
                )
            )
            fetched_artifacts = _fetched_task_artifacts(fetched.artifacts, units)
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
        units = request.plan.units
        if len(units) > 1 and request.plan.strategy != SLURM_ARRAY:
            raise OrchestrationError(
                code="UNSUPPORTED_TASK_COUNT",
                message="Multi-Task execution currently requires a Slurm array plan",
            )
        if request.plan.experiment_name != request.experiment.name:
            raise OrchestrationError(
                code="PLAN_MISMATCH",
                message="Execution plan and experiment names do not match",
            )
        if any(unit.resources != request.experiment.resources for unit in units):
            raise OrchestrationError(
                code="PLAN_MISMATCH",
                message="Execution plan resources do not match the experiment",
            )
        expected_plan = create_plan(
            request.experiment,
            units[0].config,
            request.plan.target,
            seeds=tuple(unit.seed for unit in units),
        )
        if request.plan != expected_plan:
            raise OrchestrationError(
                code="PLAN_MISMATCH",
                message="Execution plan does not match the experiment and Task input",
            )

    def _created_record(
        self,
        request: RunExecutionRequest,
        run_id: RunId,
        provenance: GitProvenance,
    ) -> RunRecord:
        tasks = tuple(
            Task(
                id=unit.task_id,
                run_id=run_id,
                experiment_name=request.experiment.name,
                config=unit.config,
                seed=unit.seed,
                resources=unit.resources,
            )
            for unit in request.plan.units
        )
        run = Run(
            id=run_id,
            experiment_name=request.experiment.name,
            target=request.plan.target,
            tasks=tasks,
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
            task_array_mapping=request.plan.array_mapping,
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


def _record_task_references(
    record: RunRecord,
) -> tuple[tuple[TaskId, SchedulerReference], ...]:
    if record.task_scheduler_ids:
        return tuple(
            (task.id, SchedulerReference(record.task_scheduler_ids[task.id]))
            for task in record.run.tasks
        )
    if len(record.run.tasks) == 1:
        return ((record.run.tasks[0].id, _record_reference(record)),)
    raise OrchestrationError(
        code="SCHEDULER_REFERENCE_UNAVAILABLE",
        message=f"Run {record.run.id} has no durable per-Task scheduler identities",
        run_id=record.run.id,
    )


def _validate_observations(
    observations: tuple[SchedulerObservation, ...],
    references: tuple[SchedulerReference, ...],
) -> None:
    if not isinstance(observations, tuple) or len(observations) != len(references):
        raise ValueError("Scheduler must return one observation per Task reference")
    if tuple(observation.reference for observation in observations) != references:
        raise ValueError("Scheduler observations must preserve Task reference order")


def _observed_record(
    record: RunRecord,
    observation: SchedulerObservation,
    observed_at: datetime,
) -> RunRecord:
    return _observed_records(
        record, ((record.run.tasks[0].id, observation),), observed_at
    )


def _observed_records(
    record: RunRecord,
    task_observations: tuple[tuple[TaskId, SchedulerObservation], ...],
    observed_at: datetime,
) -> RunRecord:
    observations = dict(task_observations)
    expected_task_ids = {task.id for task in record.run.tasks}
    if not observations or not set(observations).issubset(expected_task_ids):
        raise ValueError("Scheduler observations contain no known Run Tasks")
    tasks = tuple(
        replace(task, state=observations[task.id].state)
        if task.id in observations
        else task
        for task in record.run.tasks
    )
    state = aggregate_execution_state(tuple(task.state for task in tasks))
    updated = replace(record, run=replace(record.run, tasks=tasks, state=state))
    exit_codes = dict(record.task_exit_codes)
    native_states = dict(record.task_native_states)
    for task_id, observation in task_observations:
        if observation.exit_code is not None:
            exit_codes[task_id] = observation.exit_code
        native_states[task_id] = observation.native_state
    terminal = state in _TERMINAL_STATES
    nodes = tuple(
        str(node)
        for _, observation in task_observations
        if (node := observation.metadata.get("allocated_nodes")) is not None
    )
    scheduler_metadata = dict(record.scheduler_metadata)
    if len(task_observations) == 1:
        observation = task_observations[0][1]
        if observation.native_state != "ACCOUNTING_PENDING":
            scheduler_metadata.pop("accounting_pending", None)
        scheduler_metadata.update(observation.metadata)
    else:
        pending_count = sum(
            observation.native_state == "ACCOUNTING_PENDING"
            for _, observation in task_observations
        )
        scheduler_metadata["task_observation_count"] = len(task_observations)
        if pending_count:
            scheduler_metadata["accounting_pending_tasks"] = pending_count
        else:
            scheduler_metadata.pop("accounting_pending_tasks", None)
    artifacts = list(record.artifacts)
    for task_id, observation in task_observations:
        for metadata_name, kind in (
            ("stdout_path", ArtifactKind.STDOUT),
            ("stderr_path", ArtifactKind.STDERR),
        ):
            path = observation.metadata.get(metadata_name)
            if type(path) is str and not any(
                artifact.kind is kind and artifact.task_id == task_id
                for artifact in artifacts
            ):
                artifacts.append(Artifact(kind, PurePosixPath(path), task_id=task_id))
    started_values = tuple(
        observation.started_at
        for _, observation in task_observations
        if observation.started_at is not None
    )
    finished_values = tuple(
        observation.finished_at
        for _, observation in task_observations
        if observation.finished_at is not None
    )
    distinct_native_states = tuple(dict.fromkeys(native_states.values()))
    return replace(
        updated,
        allocated_nodes=tuple(dict.fromkeys((*record.allocated_nodes, *nodes))),
        started_at=record.started_at
        or (min(started_values) if started_values else None),
        completed_at=(
            record.completed_at
            or (max(finished_values) if finished_values else None)
            or observed_at
            if terminal
            else record.completed_at
        ),
        native_state=(
            distinct_native_states[0] if len(distinct_native_states) == 1 else "MIXED"
        ),
        scheduler_metadata=scheduler_metadata,
        task_native_states=native_states,
        task_exit_codes=exit_codes,
        artifacts=tuple(artifacts),
    )


def _container_request(
    experiment: ExperimentSpec,
    unit: ExecutionUnit,
    workspace: StagedWorkspace,
    *,
    isolate_task: bool = False,
) -> ContainerRequest:
    container = experiment.container
    task_workspace = workspace.for_task(unit.task_id)
    outputs = task_workspace.outputs if isolate_task else workspace.outputs
    runtime = task_workspace.runtime if isolate_task else workspace.runtime
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
            BindMount(outputs, _CONTAINER_OUTPUTS, read_only=False),
            BindMount(runtime, _CONTAINER_RUNTIME, read_only=False),
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


def _fetch_patterns(
    patterns: tuple[str, ...], units: tuple[ExecutionUnit, ...]
) -> tuple[str, ...]:
    if len(units) == 1:
        return patterns
    return tuple(f"{unit.task_id}/{pattern}" for unit in units for pattern in patterns)


def _fetched_task_artifacts(
    artifacts: tuple[Artifact, ...],
    units: tuple[ExecutionUnit, ...],
) -> tuple[Artifact, ...]:
    allowed = {
        ArtifactKind.RAW_RESULT,
        ArtifactKind.STDOUT,
        ArtifactKind.STDERR,
        ArtifactKind.SCHEDULER_METADATA,
    }
    if any(artifact.kind not in allowed for artifact in artifacts):
        raise ValueError("Stager fetch returned an unsupported artifact")
    task_ids = {unit.task_id for unit in units}
    if any(
        artifact.task_id is not None and artifact.task_id not in task_ids
        for artifact in artifacts
    ):
        raise ValueError("Stager fetch returned an artifact for another Task")
    result: list[Artifact] = []
    for artifact in artifacts:
        if artifact.kind is not ArtifactKind.RAW_RESULT or artifact.task_id is not None:
            result.append(artifact)
            continue
        task_id: TaskId | None
        if len(units) == 1:
            task_id = units[0].task_id
        else:
            task_id = next(
                (
                    candidate
                    for candidate in task_ids
                    if str(candidate) in artifact.path.parts
                ),
                None,
            )
            if task_id is None:
                raise ValueError(
                    "Multi-Task raw artifact path does not identify its Task"
                )
        result.append(replace(artifact, task_id=task_id))
    return tuple(result)
