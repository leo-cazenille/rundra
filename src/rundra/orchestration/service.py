from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path, PurePath, PurePosixPath
from time import monotonic, sleep
from typing import cast

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
from rundra.domain.preparation import PreparationRecord
from rundra.domain.records import RunRecord
from rundra.domain.states import (
    ExecutionState,
    RetrievalState,
    aggregate_execution_state,
)
from rundra.domain.sweeps import ExpandedConfig
from rundra.orchestration.models import SLURM_ARRAY, ExecutionPlan, ExecutionUnit
from rundra.orchestration.planner import create_plan, create_sweep_plan
from rundra.orchestration.preparation import (
    RemotePreparationSpec,
    build_remote_preparation_command,
)
from rundra.orchestration.progress import ProgressEvent, ProgressObserver, ProgressPhase
from rundra.persistence.base import RunStore
from rundra.persistence.errors import RunStoreError
from rundra.ports import (
    ArrayScheduler,
    BindMount,
    ContainerRequest,
    ContainerRuntime,
    DependencyScheduler,
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
        progress: ProgressObserver | None = None,
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
        if progress is not None and not callable(progress):
            raise TypeError("SchedulerLifecycleService progress must be callable")
        self._store = store
        self._scheduler = scheduler
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper
        self._monotonic = monotonic_clock
        self._progress = progress

    def refresh(self, record: RunRecord) -> RunRecord:
        """Query and durably apply every Task scheduler observation."""
        _require_record(record)
        current = self._refresh_preparation(record)
        if current.run.state in _TERMINAL_STATES:
            return current
        task_references = _record_task_references(current)
        references = tuple(reference for _, reference in task_references)
        observations = self._scheduler.query(references)
        _validate_observations(observations, references)
        updated = _observed_records(
            current,
            tuple(
                (task_id, observation)
                for (task_id, _), observation in zip(
                    task_references, observations, strict=True
                )
            ),
            self._clock(),
        )
        self._store.update(updated, expected=current)
        return updated

    def _refresh_preparation(self, record: RunRecord) -> RunRecord:
        preparation = record.preparation
        if (
            preparation is None
            or preparation.builder_scheduler_id is None
            or preparation.builder_status in {"SUCCEEDED", "FAILED", "CANCELLED"}
        ):
            return record
        reference = SchedulerReference(preparation.builder_scheduler_id)
        observation = _single_observation(
            self._scheduler.query((reference,)),
            reference,
        )
        updated = replace(
            record,
            preparation=replace(
                preparation,
                builder_status=observation.state.value,
                builder_state=observation.native_state,
            ),
        )
        if observation.state in {ExecutionState.FAILED, ExecutionState.CANCELLED}:
            terminal_state = (
                ExecutionState.CANCELLED
                if observation.state is ExecutionState.CANCELLED
                else ExecutionState.FAILED
            )
            updated = replace(
                _with_execution_state(updated, terminal_state),
                completed_at=self._clock(),
                native_state=f"PREPARATION_{terminal_state.value}",
            )
        self._store.update(updated, expected=record)
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
        self._report_wait(current)
        while current.run.state not in _TERMINAL_STATES:
            previous = _progress_state(current)
            try:
                current = self.refresh(current)
            except RunStoreError:
                raise
            except Exception as error:
                raise OrchestrationError(
                    code="SCHEDULER_QUERY_FAILED",
                    message=f"Run {record.run.id} scheduler query failed: {error}",
                    run_id=record.run.id,
                ) from error
            if _progress_state(current) != previous:
                self._report_wait(current)
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

    def _report_wait(self, record: RunRecord) -> None:
        if self._progress is None:
            return
        preparation = record.preparation
        states = tuple(task.state for task in record.run.tasks)
        terminal_count = sum(state in _TERMINAL_STATES for state in states)
        running_count = states.count(ExecutionState.RUNNING)
        queued_count = states.count(ExecutionState.QUEUED) + states.count(
            ExecutionState.SUBMITTED
        )
        failed_count = states.count(ExecutionState.FAILED)
        details = [
            f"run={record.run.state.value}",
            f"native={record.native_state or '-'}",
            f"tasks={terminal_count}/{len(states)}",
            f"running={running_count}",
            f"queued={queued_count}",
            f"failed={failed_count}",
            f"nodes={len(record.allocated_nodes)}",
        ]
        if preparation is not None and preparation.builder_scheduler_id is not None:
            details.extend(
                (
                    f"preparation={preparation.builder_status or '-'}",
                    f"preparation_native={preparation.builder_state or '-'}",
                )
            )
        terminal = record.run.state in _TERMINAL_STATES
        self._progress(
            ProgressEvent(
                ProgressPhase.WAIT,
                (5 if terminal else 4) + terminal_count,
                6 + len(states),
                " ".join(details),
                record.run.id,
            )
        )

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
        record = self._cancel_preparation(record)
        if len(record.run.tasks) == 1:
            return self._cancel_single(
                record, timeout=timeout, poll_interval=poll_interval
            )
        try:
            current = self.refresh(record)
            if current.run.state in _TERMINAL_STATES:
                return current
            references = tuple(
                SchedulerReference(native_id) for native_id in current.scheduler_job_ids
            )
            if not references:
                raise ValueError("Run has no scheduler root job IDs")
            self._scheduler.cancel(references)
        except RunStoreError:
            raise
        except Exception as error:
            raise OrchestrationError(
                code="SCHEDULER_CANCEL_FAILED",
                message=f"Run {record.run.id} cancellation failed: {error}",
                run_id=record.run.id,
            ) from error
        return self.wait(current, timeout=timeout, poll_interval=poll_interval)

    def _cancel_preparation(self, record: RunRecord) -> RunRecord:
        preparation = record.preparation
        if (
            preparation is None
            or preparation.builder_scheduler_id is None
            or preparation.builder_status in {"SUCCEEDED", "FAILED", "CANCELLED"}
        ):
            return record
        reference = SchedulerReference(preparation.builder_scheduler_id)
        try:
            observation = _single_observation(
                self._scheduler.cancel((reference,)),
                reference,
            )
        except Exception as error:
            raise OrchestrationError(
                code="SCHEDULER_CANCEL_FAILED",
                message=f"Run {record.run.id} preparation cancellation failed: {error}",
                run_id=record.run.id,
            ) from error
        updated = replace(
            record,
            preparation=replace(
                preparation,
                builder_status=observation.state.value,
                builder_state=observation.native_state,
            ),
        )
        self._store.update(updated, expected=record)
        return updated

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
        self._store.update(current, expected=record)
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
    preparation: PreparationRecord | None = None
    remote_preparation: RemotePreparationSpec | None = None
    remote_source_root: PurePath | None = None

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
        if self.plan.version == 1 and self.preparation is not None:
            raise ValueError("Version-1 execution cannot contain preparation")
        if self.plan.version == 2 and type(self.preparation) is not PreparationRecord:
            raise ValueError("Version-2 execution requires a preparation record")
        if (
            self.remote_preparation is not None
            and type(self.remote_preparation) is not RemotePreparationSpec
        ):
            raise TypeError(
                "RunExecutionRequest remote preparation must be a spec or None"
            )
        if self.remote_preparation is not None and self.preparation is None:
            raise ValueError("Remote preparation requires preparation provenance")
        if self.remote_source_root is not None and (
            not isinstance(self.remote_source_root, PurePath)
            or not self.remote_source_root.is_absolute()
        ):
            raise ValueError("Remote source root must be an absolute path or None")


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
        progress: ProgressObserver | None = None,
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
        if progress is not None and not callable(progress):
            raise TypeError("OrchestrationService progress must be callable")
        self.store = store
        self._stager = stager
        self._runtime = runtime
        self._scheduler = scheduler
        self._transport = transport
        self._run_id_factory = run_id_factory
        self._clock = clock or (lambda: datetime.now(UTC))
        self._framework_version = framework_version
        self._provenance = provenance
        self._progress = progress

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
        if self._provenance is not None and request.remote_source_root is None:
            try:
                captured = self._provenance.capture(request.source_root)
                if type(captured) is GitProvenance:
                    provenance = captured
            except Exception:
                provenance = GitProvenance()
        record = self._created_record(request, run_id, provenance)
        self.store.create(record)
        self._report(
            ProgressPhase.STAGE,
            2,
            f"run={run_id} target={request.plan.target.name} tasks={len(units)} checking capabilities",
            run_id,
            len(units),
        )

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

        self._report(
            ProgressPhase.STAGE,
            2,
            "capabilities verified; staging immutable inputs",
            run_id,
            len(units),
        )

        updated = _with_execution_state(record, ExecutionState.STAGING)
        self.store.update(updated, expected=record)
        record = updated
        try:
            workspace = self._stager.stage(
                StageRequest(
                    run_id=run_id,
                    experiment=request.experiment,
                    config=units[0].config,
                    target=request.plan.target,
                    source_root=request.source_root,
                    task_ids=tuple(unit.task_id for unit in units),
                    task_configs=(
                        {unit.task_id: unit.config for unit in units}
                        if request.plan.version == 3
                        else {}
                    ),
                    task_manifest=(
                        _task_manifest(units) if request.plan.version == 3 else None
                    ),
                    remote_source_root=request.remote_source_root,
                )
            )
        except Exception as error:
            self._fail_before_completion(record, "STAGING_FAILED")
            raise OrchestrationError(
                code="STAGING_FAILED",
                message=f"Run {run_id} staging failed: {error}",
                run_id=run_id,
            ) from error
        updated = replace(record, artifacts=workspace.artifacts)
        self.store.update(updated, expected=record)
        record = updated
        self._report(
            ProgressPhase.STAGE,
            3,
            f"workspace={workspace.root}",
            run_id,
            len(units),
        )

        preparation_reference = None
        if request.remote_preparation is not None:
            build = request.remote_preparation.plan.recipe.build
            if build is None:
                self._fail_before_completion(record, "PREPARATION_RESOURCES_REQUIRED")
                raise OrchestrationError(
                    code="PREPARATION_RESOURCES_REQUIRED",
                    message="Remote preparation requires an explicit build resource request",
                    run_id=run_id,
                )
            try:
                preparation_submission = self._scheduler.submit(
                    SchedulerGroup(
                        (
                            SchedulerUnit(
                                TaskId.from_ordinal(0),
                                build_remote_preparation_command(
                                    request.remote_preparation,
                                    workspace,
                                ),
                                build.resources,
                            ),
                        )
                    )
                )
                preparation_reference = preparation_submission.reference
            except Exception as error:
                self._fail_before_completion(record, "PREPARATION_SUBMISSION_FAILED")
                raise OrchestrationError(
                    code="PREPARATION_SUBMISSION_FAILED",
                    message=f"Run {run_id} preparation submission failed: {error}",
                    run_id=run_id,
                ) from error
            assert record.preparation is not None
            updated = replace(
                record,
                preparation=replace(
                    record.preparation,
                    builder_scheduler_id=preparation_reference.native_id,
                    builder_status=ExecutionState.SUBMITTED.value,
                    logs=(
                        request.plan.target.workspace
                        / ".rundra-scheduler-logs"
                        / f"{preparation_reference.native_id}.stdout",
                        request.plan.target.workspace
                        / ".rundra-scheduler-logs"
                        / f"{preparation_reference.native_id}.stderr",
                    ),
                ),
            )
            self.store.update(updated, expected=record)
            record = updated
            self._report(
                ProgressPhase.SUBMIT,
                3,
                f"preparation_job={preparation_reference.native_id} dependency=afterok",
                run_id,
                len(units),
            )

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
            if preparation_reference is not None and not isinstance(
                self._scheduler, DependencyScheduler
            ):
                raise TypeError(
                    "Configured scheduler does not support preparation dependencies"
                )
            if request.plan.strategy == SLURM_ARRAY:
                if not isinstance(self._scheduler, ArrayScheduler):
                    raise TypeError(
                        "Configured scheduler does not support mapped arrays"
                    )
                array_request = SchedulerArrayRequest(
                    scheduler_group,
                    request.plan.array_mapping,
                    workspace.metadata / "slurm-array-tasks.sh",
                    allow_duplicate_seeds=request.plan.version == 3,
                )
                submission = (
                    cast(DependencyScheduler, self._scheduler).submit_array_afterok(
                        array_request,
                        preparation_reference,
                    )
                    if preparation_reference is not None
                    else self._scheduler.submit_array(array_request)
                )
            else:
                submission = (
                    cast(DependencyScheduler, self._scheduler).submit_afterok(
                        scheduler_group,
                        preparation_reference,
                    )
                    if preparation_reference is not None
                    else self._scheduler.submit(scheduler_group)
                )
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

        updated = replace(
            _with_execution_state(record, ExecutionState.SUBMITTED),
            scheduler_job_ids=tuple(
                reference.native_id for reference in submission.references
            ),
            task_scheduler_ids=submission.task_native_ids,
            submitted_at=submission_started_at,
        )
        self.store.update(updated, expected=record)
        record = updated
        self._report(
            ProgressPhase.SUBMIT,
            4,
            "scheduler_jobs="
            f"{','.join(reference.native_id for reference in submission.references)} "
            f"tasks={len(units)}",
            run_id,
            len(units),
        )

        if not wait:
            return RunExecutionResult(record, workspace)

        try:
            lifecycle = SchedulerLifecycleService(
                store=self.store,
                scheduler=self._scheduler,
                clock=self._clock,
                progress=self._progress,
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
            updated = replace(record, artifacts=(*record.artifacts, *log_artifacts))
            self.store.update(updated, expected=record)
            record = updated
        updated = replace(
            record,
            run=replace(record.run, retrieval_state=RetrievalState.PENDING),
            task_retrieval_states={
                task.id: RetrievalState.PENDING for task in record.run.tasks
            },
        )
        self.store.update(updated, expected=record)
        record = updated
        self._report(
            ProgressPhase.RETRIEVE,
            5 + len(units),
            f"destination={request.fetch_destination}",
            run_id,
            len(units),
        )
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
                task_retrieval_states={
                    task.id: RetrievalState.FAILED for task in record.run.tasks
                },
            )
            self.store.update(failed, expected=record)
            raise OrchestrationError(
                code="RESULT_RETRIEVAL_FAILED",
                message=f"Run {run_id} result retrieval failed: {error}",
                run_id=run_id,
            ) from error

        updated = replace(
            record,
            run=replace(record.run, retrieval_state=RetrievalState.SUCCEEDED),
            task_retrieval_states={
                task.id: RetrievalState.SUCCEEDED for task in record.run.tasks
            },
            artifacts=(*record.artifacts, *fetched_artifacts),
        )
        self.store.update(updated, expected=record)
        self._report(
            ProgressPhase.RETRIEVE,
            6 + len(units),
            f"artifacts={len(fetched_artifacts)} state={updated.run.retrieval_state.value}",
            run_id,
            len(units),
        )
        return RunExecutionResult(updated, workspace)

    def _report(
        self,
        phase: ProgressPhase,
        completed: int,
        message: str,
        run_id: RunId,
        task_total: int,
    ) -> None:
        if self._progress is not None:
            self._progress(
                ProgressEvent(phase, completed, 6 + task_total, message, run_id)
            )

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
        if request.plan.version == 3:
            parameter_configs: list[ExpandedConfig] = []
            seen_parameters: set[str] = set()
            for unit in units:
                assert unit.parameter_set is not None
                if unit.parameter_set.id not in seen_parameters:
                    parameter_configs.append(
                        ExpandedConfig(unit.config, unit.parameter_set)
                    )
                    seen_parameters.add(unit.parameter_set.id)
            first_parameter = parameter_configs[0].parameter_set
            assert first_parameter is not None
            seed_values = tuple(
                unit.seed for unit in units if unit.parameter_set == first_parameter
            )
            expected_plan = create_sweep_plan(
                request.experiment,
                parameter_configs,
                request.plan.target,
                seeds=seed_values,
                preparation=request.plan.preparation,
            )
        else:
            expected_plan = create_plan(
                request.experiment,
                units[0].config,
                request.plan.target,
                seeds=tuple(unit.seed for unit in units),
                preparation=request.plan.preparation,
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
                parameter_set=unit.parameter_set,
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
            format_version=request.plan.version,
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
            container_digest=(
                request.preparation.image_sha256
                if request.preparation is not None
                else None
            ),
            preparation=request.preparation,
            task_array_mapping=request.plan.array_mapping,
            task_retrieval_states={
                unit.task_id: RetrievalState.NOT_REQUESTED
                for unit in request.plan.units
            },
        )

    def _fail_before_completion(self, record: RunRecord, native_state: str) -> None:
        failed = _with_execution_state(record, ExecutionState.FAILED)
        self.store.update(
            replace(
                failed,
                completed_at=self._clock(),
                native_state=native_state,
            ),
            expected=record,
        )

    def _record_retrieval_failure(self, record: RunRecord) -> None:
        pending = replace(
            record,
            run=replace(record.run, retrieval_state=RetrievalState.PENDING),
            task_retrieval_states={
                task.id: RetrievalState.PENDING for task in record.run.tasks
            },
        )
        self.store.update(pending, expected=record)
        self.store.update(
            replace(
                pending,
                run=replace(pending.run, retrieval_state=RetrievalState.FAILED),
                task_retrieval_states={
                    task.id: RetrievalState.FAILED for task in pending.run.tasks
                },
            ),
            expected=pending,
        )


def _with_execution_state(record: RunRecord, state: ExecutionState) -> RunRecord:
    tasks = tuple(replace(task, state=state) for task in record.run.tasks)
    return replace(record, run=replace(record.run, tasks=tasks, state=state))


def _progress_state(
    record: RunRecord,
) -> tuple[str, str | None, str | None, str | None]:
    preparation = record.preparation
    return (
        record.run.state.value,
        record.native_state,
        preparation.builder_status if preparation is not None else None,
        preparation.builder_state if preparation is not None else None,
    )


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
    container_config = _CONTAINER_INPUTS / task_workspace.config.name
    command = Command(
        tuple(
            argument.replace("{config}", str(container_config)).replace(
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


def _task_manifest(units: tuple[ExecutionUnit, ...]) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "tasks": [
                {
                    "task_id": str(unit.task_id),
                    "seed": unit.seed,
                    "parameter_set": {
                        "id": unit.parameter_set.id,
                        "choices": dict(unit.parameter_set.choices),
                    },
                    "config_sha256": hashlib.sha256(
                        unit.config.content.encode("utf-8")
                    ).hexdigest(),
                    "output": f"output/{unit.task_id}",
                }
                for unit in units
                if unit.parameter_set is not None
            ],
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


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
