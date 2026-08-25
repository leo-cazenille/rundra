from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from math import ceil
from pathlib import Path, PurePath, PurePosixPath
from time import monotonic, sleep
from typing import cast

from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    Command,
    ExperimentSpec,
    NativeValue,
    ResourceRequest,
    Run,
    RunId,
    Task,
    TaskId,
)
from rundra.domain.preparation import (
    PreparationBuild,
    PreparationImageDefinition,
    PreparationRecord,
)
from rundra.domain.records import RunRecord
from rundra.domain.scaling import CompactRun, TaskSpace
from rundra.domain.states import (
    ExecutionState,
    RetrievalState,
    aggregate_execution_state,
)
from rundra.domain.sweeps import ExpandedConfig
from rundra.orchestration.models import (
    SCHEDULER_ARRAY,
    SLURM_ARRAY,
    ExecutionPlan,
    ExecutionUnit,
)
from rundra.orchestration.planner import create_plan, create_sweep_plan
from rundra.orchestration.preparation import (
    RemotePreparationSpec,
    build_remote_preparation_command,
    read_remote_preparation_result,
)
from rundra.orchestration.progress import ProgressEvent, ProgressObserver, ProgressPhase
from rundra.orchestration.shards import read_verified_shard_index
from rundra.persistence.base import CompactRunStore, RunStore
from rundra.persistence.errors import RunStoreError
from rundra.persistence.submission_store import (
    SubmissionReceiptOutcome,
    SubmissionReceiptStore,
)
from rundra.persistence.task_store import SqliteTaskStore, TaskState
from rundra.ports import (
    AllocationScratch,
    ArrayScheduler,
    BindMount,
    CompactArrayScheduler,
    CompactDependencyScheduler,
    CompactSchedulerArrayRequest,
    CompactSchedulerSubmission,
    ContainerRequest,
    ContainerRuntime,
    ContainerRuntimeIdentityProvider,
    DependencyScheduler,
    FetchRequest,
    Scheduler,
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmissionFailure,
    SchedulerSubmissionOutcome,
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
_COMPACT_TASK_ID = "__RUNDRA_TASK_ID__"
_COMPACT_SEED = "__RUNDRA_SEED__"
_TERMINAL_STATES = frozenset(
    {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }
)
_MIN_ETA_SAMPLE_COUNT = 20
_MIN_ETA_SAMPLE_FRACTION = 0.10
_MIN_ETA_WINDOW_SECONDS = 60


def _preparation_status(state: ExecutionState) -> str:
    return (
        ExecutionState.SUBMITTED.value
        if state is ExecutionState.QUEUED
        else state.value
    )


def _record_workspace(record: RunRecord) -> StagedWorkspace:
    root = record.run.target.workspace / "runs" / str(record.run.id)
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


def _remote_preparation_resources(
    image: object,
    build: PreparationBuild | None,
) -> ResourceRequest | None:
    if type(image) is not PreparationImageDefinition:
        return None if build is None else build.resources
    if build is None:
        return image.resources
    assert image.resources.memory_bytes is not None
    assert build.resources.memory_bytes is not None
    assert image.resources.walltime is not None
    assert build.resources.walltime is not None
    return ResourceRequest(
        cpus_per_task=max(image.resources.cpus_per_task, build.resources.cpus_per_task),
        memory_bytes=max(image.resources.memory_bytes, build.resources.memory_bytes),
        walltime=image.resources.walltime + build.resources.walltime,
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
        transport: Transport | None = None,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = sleep,
        monotonic_clock: Callable[[], float] = monotonic,
        progress: ProgressObserver | None = None,
        task_store: SqliteTaskStore | None = None,
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
        if transport is not None and not isinstance(transport, Transport):
            raise TypeError(
                "SchedulerLifecycleService transport must implement Transport"
            )
        self._store = store
        self._scheduler = scheduler
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper
        self._monotonic = monotonic_clock
        self._progress = progress
        self._transport = transport
        self._task_store = task_store

    def refresh(self, record: RunRecord) -> RunRecord:
        """Query and durably apply every Task scheduler observation."""
        _require_record(record)
        current = self._refresh_preparation(record)
        repaired = self._repair_aggregate_state(current)
        if repaired != current:
            self._store.update(repaired, expected=current)
            current = repaired
        if current.run.state in _TERMINAL_STATES:
            return current
        # A preparation scheduler identity is durable before scientific jobs are
        # submitted.  During that interval there are deliberately no per-Task
        # scheduler identities to query.
        if not current.scheduler_job_ids and not current.task_scheduler_ids:
            return current
        if current.is_compact:
            if self._task_store is None:
                raise OrchestrationError(
                    code="TASK_STATE_UNAVAILABLE",
                    message=f"Run {current.run.id} requires its compact Task state",
                    run_id=current.run.id,
                )
            updated = _compact_observed_record(
                current,
                self._scheduler,
                self._transport,
                self._task_store,
                self._clock(),
            )
            self._store.update(updated, expected=current)
            return updated
        task_references = _record_task_references(current)
        references = tuple(reference for _, reference in task_references)
        unique_references = tuple(dict.fromkeys(references))
        observations = self._scheduler.query(unique_references)
        _validate_observations(observations, unique_references)
        by_reference = {
            observation.reference: observation for observation in observations
        }
        task_observations = tuple(
            (task_id, by_reference[reference]) for task_id, reference in task_references
        )
        updated = _observed_records(
            current,
            task_observations,
            self._clock(),
        )
        updated = _apply_bundle_journals(
            updated,
            task_observations,
            self._transport,
        )
        self._store.update(updated, expected=current)
        return updated

    def _repair_aggregate_state(self, record: RunRecord) -> RunRecord:
        if record.is_compact:
            if self._task_store is None:
                return record
            counts = self._task_store.counts(record.run.id)
            state = aggregate_execution_state(
                tuple(item for item, count in counts.execution.items() if count > 0)
            )
            native_state = _aggregate_native_counts(
                self._task_store.native_state_counts(record.run.id)
            )
        else:
            if not record.run.tasks:
                return record
            state = aggregate_execution_state(
                tuple(task.state for task in record.run.tasks)
            )
            native_state = (
                _aggregate_native_counts(
                    {
                        value: sum(
                            item == value for item in record.task_native_states.values()
                        )
                        for value in set(record.task_native_states.values())
                    }
                )
                if set(record.task_native_states)
                == {task.id for task in record.run.tasks}
                else None
            )
        return replace(
            record,
            run=(
                replace(record.run, state=state)
                if record.run.state is not state
                else record.run
            ),
            native_state=native_state or record.native_state,
        )

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
        if (
            observation.state is ExecutionState.SUCCEEDED
            and record.format_version == 6
            and preparation.image_sha256 is None
        ):
            if self._transport is None:
                raise OrchestrationError(
                    code="PREPARATION_PROVENANCE_UNAVAILABLE",
                    message=(
                        f"Run {record.run.id} requires transport access to finalize "
                        "preparation provenance"
                    ),
                    run_id=record.run.id,
                )
            workspace = _record_workspace(record)
            result = read_remote_preparation_result(self._transport, workspace)
            if result is None:
                return record
            if result.image_sha256 is None or result.image_path is None:
                raise OrchestrationError(
                    code="PREPARATION_PROVENANCE_INVALID",
                    message=(
                        f"Run {record.run.id} completed preparation without a "
                        "verified image identity"
                    ),
                    run_id=record.run.id,
                )
            if result.image_path != preparation.image_path:
                raise OrchestrationError(
                    code="PREPARATION_PROVENANCE_INVALID",
                    message=(
                        f"Run {record.run.id} preparation image path does not match "
                        "its immutable definition"
                    ),
                    run_id=record.run.id,
                )
            updated = replace(
                record,
                container_digest=result.image_sha256,
                preparation=replace(
                    preparation,
                    image_sha256=result.image_sha256,
                    image_action=result.image_action,
                    build_cache_key=result.build_key,
                    build_action=result.build_action,
                    build_outputs=result.outputs,
                    builder_status=ExecutionState.SUCCEEDED.value,
                    builder_state=observation.native_state,
                ),
            )
            self._store.update(updated, expected=record)
            return updated
        updated = replace(
            record,
            preparation=replace(
                preparation,
                builder_status=_preparation_status(observation.state),
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
        if not record.scheduler_job_ids:
            updated = replace(
                _with_execution_state(record, ExecutionState.CANCELLED),
                completed_at=self._clock(),
                native_state="PREPARATION_ONLY_CANCELLED",
            )
            self._store.update(updated, expected=record)
            return updated
        if len(record.run.tasks) == 1:
            return self._cancel_single(
                record, timeout=timeout, poll_interval=poll_interval
            )
        try:
            references = tuple(
                SchedulerReference(native_id) for native_id in record.scheduler_job_ids
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
        return self.wait(record, timeout=timeout, poll_interval=poll_interval)

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
        if observation.state is ExecutionState.SUCCEEDED:
            return self._refresh_preparation(record)
        updated = replace(
            record,
            preparation=replace(
                preparation,
                builder_status=_preparation_status(observation.state),
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
    fetch_mode: str = "auto"
    experiment_source: PurePath | None = None
    initiator: str | None = None
    preparation: PreparationRecord | None = None
    remote_preparation: RemotePreparationSpec | None = None
    remote_source_root: PurePath | None = None
    max_concurrent_jobs: int | None = None
    max_workers: int | None = None
    task_slots_per_worker: int = 1
    shard_outputs: bool = False
    worker_resources: ResourceRequest | None = None
    requested_workers: int | None = None
    requested_task_slots_per_worker: int | None = None
    compact_plan: ExecutionPlan | None = None
    compact_configs: tuple[ExpandedConfig, ...] = ()

    def __post_init__(self) -> None:
        if type(self.plan) is not ExecutionPlan:
            raise TypeError("RunExecutionRequest plan must be an ExecutionPlan")
        if type(self.experiment) is not ExperimentSpec:
            raise TypeError("RunExecutionRequest experiment must be an ExperimentSpec")
        for name in ("source_root", "fetch_destination"):
            if not isinstance(getattr(self, name), PurePath):
                raise TypeError(f"RunExecutionRequest {name} must be a PurePath")
        if self.fetch_mode not in {"auto", "copy", "reference", "archive"}:
            raise ValueError("RunExecutionRequest fetch_mode is unsupported")
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
        if self.max_concurrent_jobs is not None and (
            type(self.max_concurrent_jobs) is not int or self.max_concurrent_jobs < 1
        ):
            raise ValueError("max_concurrent_jobs must be positive or None")
        if self.max_workers is not None and (
            type(self.max_workers) is not int or self.max_workers < 1
        ):
            raise ValueError("max_workers must be positive or None")
        if (
            type(self.task_slots_per_worker) is not int
            or self.task_slots_per_worker < 1
        ):
            raise ValueError("task_slots_per_worker must be positive")
        if type(self.shard_outputs) is not bool:
            raise TypeError("shard_outputs must be bool")
        if (
            self.worker_resources is not None
            and type(self.worker_resources) is not ResourceRequest
        ):
            raise TypeError("worker_resources must be a ResourceRequest or None")
        for name in ("requested_workers", "requested_task_slots_per_worker"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"{name} must be positive or None")
        if self.compact_plan is not None:
            if type(self.compact_plan) is not ExecutionPlan:
                raise TypeError("compact_plan must be an ExecutionPlan or None")
            direct = self.plan == self.compact_plan
            if (
                self.compact_plan.task_space is None
                or self.compact_plan.strategy != "worker-pool"
            ):
                raise ValueError("compact_plan must describe a worker-pool TaskSpace")
            if not direct and self.compact_plan.task_space.task_count != len(
                self.plan.units
            ):
                raise ValueError("compact_plan must describe every materialized Task")
            if direct and len(self.compact_configs) != (
                self.compact_plan.task_space.parameter_set_count
            ):
                raise ValueError(
                    "direct compact execution requires every parameter config"
                )
        if not isinstance(self.compact_configs, tuple) or any(
            type(config) is not ExpandedConfig for config in self.compact_configs
        ):
            raise TypeError("compact_configs must contain ExpandedConfigs")
        if self.compact_plan is None and self.compact_configs:
            raise ValueError("compact_configs require a compact_plan")


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
        submission_receipts: SubmissionReceiptStore | None = None,
        task_store: SqliteTaskStore | None = None,
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
        if (
            submission_receipts is not None
            and type(submission_receipts) is not SubmissionReceiptStore
        ):
            raise TypeError(
                "submission_receipts must be a SubmissionReceiptStore or None"
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
        self._progress = progress
        self._submission_receipts = submission_receipts
        self._task_store = task_store

    def execute_one(self, request: RunExecutionRequest) -> RunExecutionResult:
        """Execute and fetch one planned Task while durably recording each phase."""
        return self._execute_one(request, wait=True)

    def submit_one(self, request: RunExecutionRequest) -> RunExecutionResult:
        """Stage and submit one Task, returning once its reference is durable."""
        return self._execute_one(request, wait=False)

    def recover_submission(self, run_id: RunId) -> tuple[RunRecord, str]:
        """Adopt a completed receipt or report an already durable submission."""
        if type(run_id) is not RunId:
            raise TypeError("recover_submission requires a RunId")
        if self._submission_receipts is None:
            raise OrchestrationError(
                code="SUBMISSION_RECOVERY_UNAVAILABLE",
                message="Scheduler submission receipts are not configured",
                run_id=run_id,
            )
        with self.store.operation_lock(run_id):
            record = self.store.load(run_id)
            if record.scheduler_job_ids:
                return record, "found"
            try:
                receipt = self._submission_receipts.load(run_id)
            except RunStoreError as error:
                raise OrchestrationError(
                    code="SUBMISSION_RECEIPT_NOT_FOUND",
                    message=str(error),
                    run_id=run_id,
                ) from error
            if receipt.outcome is SubmissionReceiptOutcome.REJECTED:
                if record.run.state is ExecutionState.STAGING:
                    self._fail_before_completion(record, "SCHEDULER_SUBMISSION_FAILED")
                    record = self.store.load(run_id)
                if record.run.state is ExecutionState.FAILED:
                    return record, "rejected"
                raise OrchestrationError(
                    code="SUBMISSION_RECEIPT_MISMATCH",
                    message=(
                        f"Run {run_id} has a rejected receipt but is "
                        f"{record.run.state.value}"
                    ),
                    run_id=run_id,
                )
            if record.run.state is not ExecutionState.STAGING:
                raise OrchestrationError(
                    code="SUBMISSION_NOT_RECOVERABLE",
                    message=(
                        f"Run {run_id} is {record.run.state.value}, not an interrupted "
                        "scheduler submission"
                    ),
                    run_id=run_id,
                )
            if not receipt.completed:
                if receipt.format_version == 3 and self._task_store is not None:
                    try:
                        scheduler_job_ids = self._task_store.submission_job_ids(run_id)
                        self._task_store.all_states(run_id)
                        if scheduler_job_ids:
                            receipt = self._submission_receipts.complete_compact(
                                receipt,
                                scheduler_job_ids,
                                self._clock(),
                            )
                    except RunStoreError:
                        pass
            if not receipt.completed:
                description = (
                    "has an uncertain scheduler submission outcome"
                    if receipt.outcome is SubmissionReceiptOutcome.UNCERTAIN
                    else "has a pending legacy or interrupted submission receipt"
                )
                raise OrchestrationError(
                    code="SUBMISSION_OUTCOME_UNKNOWN",
                    message=(
                        f"Run {run_id} {description}. Rundra will not submit it "
                        "again; inspect the scheduler before taking manual action."
                    ),
                    run_id=run_id,
                )
            if receipt.format_version != 3:
                if not record.is_compact:
                    expected_task_ids = tuple(task.id for task in record.run.tasks)
                else:
                    assert record.task_space is not None
                    expected_task_ids = tuple(
                        record.task_space.coordinate(ordinal).task_id
                        for ordinal in range(record.task_space.task_count)
                    )
                if receipt.task_ids != expected_task_ids:
                    raise OrchestrationError(
                        code="SUBMISSION_RECEIPT_MISMATCH",
                        message=(
                            f"Run {run_id} receipt does not match its persisted Tasks"
                        ),
                        run_id=run_id,
                    )
            if receipt.format_version == 3:
                if self._task_store is None or not isinstance(
                    self.store, CompactRunStore
                ):
                    raise OrchestrationError(
                        code="SUBMISSION_RECOVERY_UNAVAILABLE",
                        message=f"Run {run_id} requires compact Task persistence",
                        run_id=run_id,
                    )
                assert receipt.task_space is not None
                if (
                    receipt.task_state_store
                    != PurePath(self._task_store.path(run_id).name)
                    or self._task_store.task_space(run_id) != receipt.task_space
                    or self._task_store.submission_job_ids(run_id)
                    != receipt.scheduler_job_ids
                ):
                    raise OrchestrationError(
                        code="SUBMISSION_RECEIPT_MISMATCH",
                        message=f"Run {run_id} compact receipt does not match its sidecar",
                        run_id=run_id,
                    )
                self._task_store.all_states(run_id)
                if not record.is_compact:
                    assert receipt.execution_strategy is not None
                    assert receipt.retrieval_policy is not None
                    compact = _compact_record_from_metadata(
                        record,
                        task_space=receipt.task_space,
                        execution_strategy=receipt.execution_strategy,
                        retrieval_policy=receipt.retrieval_policy,
                        task_store=self._task_store,
                    )
                    self.store.compact(compact, expected=record)
                    record = compact
                elif (
                    record.task_space != receipt.task_space
                    or record.execution_strategy != receipt.execution_strategy
                    or record.retrieval_policy != receipt.retrieval_policy
                    or record.task_state_store != receipt.task_state_store
                ):
                    raise OrchestrationError(
                        code="SUBMISSION_RECEIPT_MISMATCH",
                        message=f"Run {run_id} compact receipt does not match its record",
                        run_id=run_id,
                    )
            updated = replace(
                _with_execution_state(record, ExecutionState.SUBMITTED),
                scheduler_job_ids=receipt.scheduler_job_ids,
                task_scheduler_ids=(
                    {}
                    if receipt.format_version == 3 or record.is_compact
                    else receipt.task_scheduler_ids or {}
                ),
                submitted_at=receipt.started_at,
                native_state="SUBMISSION_RESUMED",
            )
            self.store.update(updated, expected=record)
            return updated, "resumed"

    def resolve_submission(
        self,
        run_id: RunId,
        *,
        confirmation: RunId,
    ) -> RunRecord:
        """Close an uncertain submission after explicit operator verification."""
        if type(run_id) is not RunId or type(confirmation) is not RunId:
            raise TypeError("resolve_submission requires RunId values")
        if confirmation != run_id:
            raise OrchestrationError(
                code="SUBMISSION_CONFIRMATION_MISMATCH",
                message=f"Confirmation must exactly match Run {run_id}",
                run_id=run_id,
            )
        if self._submission_receipts is None:
            raise OrchestrationError(
                code="SUBMISSION_RECOVERY_UNAVAILABLE",
                message="Scheduler submission receipts are not configured",
                run_id=run_id,
            )
        with self.store.operation_lock(run_id):
            record = self.store.load(run_id)
            if record.scheduler_job_ids or record.task_scheduler_ids:
                raise OrchestrationError(
                    code="SUBMISSION_IDENTITIES_PRESENT",
                    message=(
                        f"Run {run_id} has durable scheduler identities and cannot "
                        "be resolved as not submitted"
                    ),
                    run_id=run_id,
                )
            try:
                receipt = self._submission_receipts.load(run_id)
                if receipt.outcome is SubmissionReceiptOutcome.OPERATOR_RESOLVED:
                    if (
                        record.run.state is ExecutionState.FAILED
                        and record.native_state == "SUBMISSION_CONFIRMED_NOT_SUBMITTED"
                    ):
                        return record
                    raise RunStoreError(
                        f"Run {run_id} operator resolution does not match its state"
                    )
                if record.run.state is not ExecutionState.STAGING:
                    raise RunStoreError(
                        f"Run {run_id} is {record.run.state.value}, not an uncertain "
                        "scheduler submission"
                    )
                self._submission_receipts.resolve_not_submitted(
                    receipt,
                    updated_at=self._clock(),
                )
            except RunStoreError as error:
                raise OrchestrationError(
                    code="SUBMISSION_NOT_RESOLVABLE",
                    message=str(error),
                    run_id=run_id,
                ) from error
            self._fail_before_completion(record, "SUBMISSION_CONFIRMED_NOT_SUBMITTED")
            return self.store.load(run_id)

    def _execute_one(
        self, request: RunExecutionRequest, *, wait: bool
    ) -> RunExecutionResult:
        self._validate_request(request)
        units = request.plan.units
        task_total = (
            request.compact_plan.task_space.task_count
            if request.compact_plan is not None
            and request.compact_plan.task_space is not None
            else len(units)
        )
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
        direct_compact = _is_direct_compact_request(request)
        if direct_compact:
            assert self._task_store is not None
            assert request.compact_plan is not None
            assert request.compact_plan.task_space is not None
            self._task_store.create(run_id, request.compact_plan.task_space)
        record = self._created_record(request, run_id, provenance)
        self.store.create(record)
        self._report(
            ProgressPhase.STAGE,
            2,
            f"run={run_id} target={request.plan.target.name} tasks={task_total} checking capabilities",
            run_id,
            task_total,
        )

        try:
            self._transport.check()
            self._runtime.check()
            runtime_identity = (
                self._runtime.identity()
                if isinstance(self._runtime, ContainerRuntimeIdentityProvider)
                else None
            )
        except Exception as error:
            self._fail_before_completion(record, "CAPABILITY_CHECK_FAILED")
            raise OrchestrationError(
                code="CAPABILITY_CHECK_FAILED",
                message=f"Run {run_id} capability check failed: {error}",
                run_id=run_id,
            ) from error

        if runtime_identity is not None:
            runtime_metadata = {
                "container_runtime": runtime_identity.name,
            }
            if runtime_identity.version is not None:
                runtime_metadata["container_runtime_version"] = runtime_identity.version
            updated = replace(
                record,
                scheduler_metadata={
                    **record.scheduler_metadata,
                    **runtime_metadata,
                },
            )
            self.store.update(updated, expected=record)
            record = updated

        self._report(
            ProgressPhase.STAGE,
            2,
            "capabilities verified; staging immutable inputs",
            run_id,
            task_total,
        )

        updated = _with_execution_state(record, ExecutionState.STAGING)
        self.store.update(updated, expected=record)
        record = updated
        compact_units = (
            _compact_parameter_units(request)
            if request.compact_plan is not None
            else ()
        )
        try:
            workspace = self._stager.stage(
                StageRequest(
                    run_id=run_id,
                    experiment=request.experiment,
                    config=units[0].config,
                    target=request.plan.target,
                    source_root=request.source_root,
                    task_ids=(
                        tuple(unit.task_id for unit in compact_units)
                        if compact_units
                        else tuple(unit.task_id for unit in units)
                    ),
                    task_configs=(
                        {unit.task_id: unit.config for unit in compact_units}
                        if compact_units
                        else (
                            {unit.task_id: unit.config for unit in units}
                            if request.plan.version == 3
                            else {}
                        )
                    ),
                    task_manifest=(
                        _compact_task_manifest(request.compact_plan, compact_units)
                        if request.compact_plan is not None
                        else (
                            _task_manifest(units) if request.plan.version == 3 else None
                        )
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
            task_total,
        )

        preparation_reference = None
        if request.remote_preparation is not None:
            build = request.remote_preparation.plan.recipe.build
            image_recipe = request.remote_preparation.plan.recipe.image
            resources = _remote_preparation_resources(image_recipe, build)
            if resources is None:
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
                                    scratch_policy=request.plan.target.execution_storage,
                                ),
                                resources,
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
            # Definition images are published atomically at the deterministic
            # recipe-key path already recorded in the effective experiment.
            # The dependent scientific submission below can therefore be made
            # immediately; afterok prevents access to an incomplete image.

        try:
            allocation_scratch = _allocation_scratch(request, workspace, len(units))
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
                for unit in (() if compact_units else units)
            )
            compact_commands = tuple(
                self._runtime.build_command(
                    _compact_container_request(request.experiment, unit, workspace)
                )
                for unit in compact_units
            )
        except Exception as error:
            self._fail_before_completion(record, "CONTAINER_COMMAND_FAILED")
            raise OrchestrationError(
                code="CONTAINER_COMMAND_FAILED",
                message=f"Run {run_id} container command construction failed: {error}",
                run_id=run_id,
            ) from error

        worker_limits = tuple(
            limit
            for limit in (request.max_concurrent_jobs, request.max_workers)
            if limit is not None
        )
        planned_bundled = request.plan.strategy in {
            SLURM_ARRAY,
            SCHEDULER_ARRAY,
        } and (
            request.task_slots_per_worker > 1
            or (worker_limits and len(units) > min(worker_limits))
        )
        if planned_bundled:
            updated = replace(
                record,
                scheduler_metadata={
                    **record.scheduler_metadata,
                    "bundle_status_root": str(workspace.metadata / "bundle-status"),
                    "max_concurrent_jobs": request.max_concurrent_jobs or 0,
                    "max_workers": request.max_workers or 0,
                    "task_slots_per_worker": request.task_slots_per_worker,
                    "requested_workers": request.requested_workers or 0,
                    "requested_task_slots_per_worker": (
                        request.requested_task_slots_per_worker or 0
                    ),
                    "result_shards": request.shard_outputs,
                    **(
                        {"result_shard_root": str(workspace.outputs / ".rundra-shards")}
                        if request.shard_outputs
                        else {}
                    ),
                },
            )
            self.store.update(updated, expected=record)
            record = updated

        submission_started_at = self._clock()
        submission_receipts = self._submission_receipts
        if request.compact_plan is not None:
            if self._task_store is None:
                raise OrchestrationError(
                    code="TASK_STATE_UNAVAILABLE",
                    message=f"Run {run_id} requires a compact Task state store",
                    run_id=run_id,
                )
            assert request.compact_plan.task_space is not None
            self._task_store.create(run_id, request.compact_plan.task_space)
        pending_receipt = None
        if submission_receipts is not None:
            if request.compact_plan is not None:
                assert request.compact_plan.task_space is not None
                assert self._task_store is not None
                pending_receipt = submission_receipts.begin_compact(
                    run_id,
                    request.compact_plan.task_space,
                    submission_started_at,
                    execution_strategy=request.compact_plan.strategy,
                    retrieval_policy=request.compact_plan.retrieval_policy or "all",
                    task_state_store=PurePath(self._task_store.path(run_id).name),
                    backend=record.run.target.scheduler.kind,
                )
            else:
                pending_receipt = submission_receipts.begin(
                    run_id,
                    tuple(unit.task_id for unit in units),
                    submission_started_at,
                    backend=record.run.target.scheduler.kind,
                )
        try:
            if (
                preparation_reference is not None
                and request.compact_plan is None
                and not isinstance(self._scheduler, DependencyScheduler)
            ):
                raise TypeError(
                    "Configured scheduler does not support preparation dependencies"
                )
            compact_submission: CompactSchedulerSubmission | None = None
            if request.compact_plan is not None:
                if not isinstance(self._scheduler, CompactArrayScheduler):
                    raise TypeError("Configured scheduler lacks compact arrays")
                if preparation_reference is not None and not isinstance(
                    self._scheduler, CompactDependencyScheduler
                ):
                    raise TypeError(
                        "Configured scheduler lacks compact preparation dependencies"
                    )
                assert request.compact_plan.task_space is not None
                assert request.compact_plan.worker_count is not None
                assert request.compact_plan.execution_policy is not None
                assert request.worker_resources is not None
                worker_policy = request.compact_plan.execution_policy.worker_pool
                compact_request = CompactSchedulerArrayRequest(
                    request.compact_plan.task_space,
                    compact_commands,
                    request.experiment.resources,
                    request.worker_resources,
                    workspace.metadata / "scheduler-array-tasks.sh",
                    request.compact_plan.worker_count,
                    request.task_slots_per_worker,
                    output_root=(workspace.outputs if request.shard_outputs else None),
                    shard_root=(
                        workspace.outputs / ".rundra-shards"
                        if request.shard_outputs
                        else None
                    ),
                    infrastructure_retry_limit=(
                        worker_policy.infrastructure_retry_limit
                    ),
                    requeue_limit=worker_policy.requeue_limit,
                    scratch=allocation_scratch,
                )
                compact_submission = (
                    cast(
                        CompactDependencyScheduler, self._scheduler
                    ).submit_compact_array_afterok(
                        compact_request, preparation_reference
                    )
                    if preparation_reference is not None
                    else self._scheduler.submit_compact_array(compact_request)
                )
                submission_references = compact_submission.references
                task_native_ids = None
            elif request.plan.strategy in {SLURM_ARRAY, SCHEDULER_ARRAY}:
                scheduler_group = SchedulerGroup(
                    tuple(
                        SchedulerUnit(unit.task_id, unit.command, unit.resources)
                        for unit in scheduled_units
                    ),
                    scratch=allocation_scratch,
                )
                if not isinstance(self._scheduler, ArrayScheduler):
                    raise TypeError(
                        "Configured scheduler does not support mapped arrays"
                    )
                array_request = SchedulerArrayRequest(
                    scheduler_group,
                    request.plan.array_mapping,
                    workspace.metadata
                    / (
                        "slurm-array-tasks.sh"
                        if request.plan.strategy == SLURM_ARRAY
                        else "scheduler-array-tasks.sh"
                    ),
                    allow_duplicate_seeds=request.plan.version == 3,
                    max_concurrent_jobs=request.max_concurrent_jobs,
                    max_workers=request.max_workers,
                    task_slots_per_worker=request.task_slots_per_worker,
                    output_root=(workspace.outputs if request.shard_outputs else None),
                    shard_root=(
                        workspace.outputs / ".rundra-shards"
                        if request.shard_outputs
                        else None
                    ),
                    worker_resources=request.worker_resources,
                )
                submission = (
                    cast(DependencyScheduler, self._scheduler).submit_array_afterok(
                        array_request,
                        preparation_reference,
                    )
                    if preparation_reference is not None
                    else self._scheduler.submit_array(array_request)
                )
                submission_references = submission.references
                task_native_ids = submission.task_native_ids
            else:
                scheduler_group = SchedulerGroup(
                    tuple(
                        SchedulerUnit(unit.task_id, unit.command, unit.resources)
                        for unit in scheduled_units
                    ),
                    scratch=allocation_scratch,
                )
                submission = (
                    cast(DependencyScheduler, self._scheduler).submit_afterok(
                        scheduler_group,
                        preparation_reference,
                    )
                    if preparation_reference is not None
                    else self._scheduler.submit(scheduler_group)
                )
                submission_references = submission.references
                task_native_ids = submission.task_native_ids
            if task_native_ids is not None and set(task_native_ids) != {
                unit.task_id for unit in units
            }:
                raise ValueError(
                    "Scheduler submission did not map every planned Task exactly"
                )
            scheduler_job_ids = tuple(
                reference.native_id for reference in submission_references
            )
            if request.compact_plan is not None:
                assert self._task_store is not None
                assert compact_submission is not None
                self._task_store.initialize_compact_submission(
                    run_id,
                    compact_submission.worker_native_ids,
                    scheduler_job_ids=scheduler_job_ids,
                )
            if pending_receipt is not None:
                assert submission_receipts is not None
                if request.compact_plan is not None:
                    submission_receipts.complete_compact(
                        pending_receipt, scheduler_job_ids, self._clock()
                    )
                else:
                    submission_receipts.complete(
                        pending_receipt,
                        scheduler_job_ids,
                        submission.task_native_ids,
                        self._clock(),
                    )
        except SchedulerSubmissionFailure as error:
            if error.outcome is SchedulerSubmissionOutcome.REJECTED:
                if pending_receipt is not None:
                    assert submission_receipts is not None
                    try:
                        submission_receipts.reject(
                            pending_receipt,
                            backend=error.backend,
                            phase=error.phase,
                            failure_classification="scheduler_rejected",
                            exit_code=error.exit_code,
                            updated_at=self._clock(),
                        )
                    finally:
                        self._fail_before_completion(
                            record, "SCHEDULER_SUBMISSION_FAILED"
                        )
                else:
                    self._fail_before_completion(record, "SCHEDULER_SUBMISSION_FAILED")
                raise OrchestrationError(
                    code="SCHEDULER_SUBMISSION_FAILED",
                    message=f"Run {run_id} scheduler submission failed: {error}",
                    run_id=run_id,
                ) from error
            if pending_receipt is not None:
                assert submission_receipts is not None
                submission_receipts.mark_uncertain(
                    pending_receipt,
                    backend=error.backend,
                    phase=error.phase,
                    failure_classification="scheduler_outcome_uncertain",
                    exit_code=error.exit_code,
                    updated_at=self._clock(),
                )
            raise OrchestrationError(
                code="SUBMISSION_OUTCOME_UNKNOWN",
                message=(
                    f"Run {run_id} scheduler submission outcome is unknown: "
                    f"{error}. Retry only with 'rundr resume {run_id}'."
                ),
                run_id=run_id,
            ) from error
        except Exception as error:
            if pending_receipt is not None:
                assert submission_receipts is not None
                submission_receipts.mark_uncertain(
                    pending_receipt,
                    backend=record.run.target.scheduler.kind,
                    phase="orchestration",
                    failure_classification="unclassified_exception",
                    exit_code=None,
                    updated_at=self._clock(),
                )
                raise OrchestrationError(
                    code="SUBMISSION_OUTCOME_UNKNOWN",
                    message=(
                        f"Run {run_id} scheduler submission outcome is unknown: "
                        f"{error}. Retry only with 'rundr resume {run_id}'."
                    ),
                    run_id=run_id,
                ) from error
            self._fail_before_completion(record, "SCHEDULER_SUBMISSION_FAILED")
            raise OrchestrationError(
                code="SCHEDULER_SUBMISSION_FAILED",
                message=f"Run {run_id} scheduler submission failed: {error}",
                run_id=run_id,
            ) from error

        bundled = request.compact_plan is not None or (
            task_native_ids is not None
            and len(set(task_native_ids.values())) < len(task_native_ids)
        )
        if request.compact_plan is not None and not record.is_compact:
            try:
                assert self._task_store is not None
                compact = _compact_record(
                    record, request.compact_plan, self._task_store
                )
                if not isinstance(self.store, CompactRunStore):
                    raise RunStoreError(
                        "Configured Run store does not support compact persistence"
                    )
                self.store.compact(compact, expected=record)
                record = compact
            except RunStoreError as error:
                raise OrchestrationError(
                    code="TASK_STATE_PERSISTENCE_FAILED",
                    message=f"Run {run_id} compact Task state failed: {error}",
                    run_id=run_id,
                ) from error
        updated = replace(
            _with_execution_state(record, ExecutionState.SUBMITTED),
            scheduler_job_ids=tuple(
                reference.native_id for reference in submission_references
            ),
            task_scheduler_ids=({} if record.is_compact else task_native_ids or {}),
            submitted_at=submission_started_at,
            scheduler_metadata={
                **record.scheduler_metadata,
                **(
                    {
                        "bundle_status_root": str(workspace.metadata / "bundle-status"),
                        "max_concurrent_jobs": request.max_concurrent_jobs or 0,
                        "max_workers": request.max_workers or 0,
                        "task_slots_per_worker": request.task_slots_per_worker,
                        "requested_workers": request.requested_workers or 0,
                        "requested_task_slots_per_worker": (
                            request.requested_task_slots_per_worker or 0
                        ),
                        "result_shards": request.shard_outputs,
                        **(
                            {
                                "result_shard_root": str(
                                    workspace.outputs / ".rundra-shards"
                                )
                            }
                            if request.shard_outputs
                            else {}
                        ),
                    }
                    if bundled
                    else {}
                ),
            },
        )
        self.store.update(updated, expected=record)
        record = updated
        self._report(
            ProgressPhase.SUBMIT,
            4,
            "scheduler_jobs="
            f"{','.join(reference.native_id for reference in submission_references)} "
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
                transport=self._transport,
                clock=self._clock,
                progress=self._progress,
                task_store=self._task_store,
            )
            record = lifecycle.wait(record)
            command_result = None
            if len(units) == 1:
                assert task_native_ids is not None
                task_reference = SchedulerReference(task_native_ids[units[0].task_id])
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

        with self.store.operation_lock(run_id):
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
                            f"Run {run_id} completed but logs could not be persisted: "
                            f"{error}"
                        ),
                        run_id=run_id,
                    ) from error
                updated = replace(record, artifacts=(*record.artifacts, *log_artifacts))
                self.store.update(updated, expected=record)
                record = updated
            updated = replace(
                record,
                run=replace(record.run, retrieval_state=RetrievalState.PENDING),
                task_retrieval_states=(
                    {}
                    if record.is_compact
                    else {task.id: RetrievalState.PENDING for task in record.run.tasks}
                ),
            )
            if record.is_compact:
                assert self._task_store is not None
                self._task_store.set_all_retrieval(
                    record.run.id, RetrievalState.PENDING
                )
            self.store.update(updated, expected=record)
            record = updated
            retrieval_destination = (
                record.retrieval_destination or request.fetch_destination
            )
            self._report(
                ProgressPhase.RETRIEVE,
                5 + len(units),
                f"destination={retrieval_destination}",
                run_id,
                len(units),
            )
            try:
                fetched = self._stager.fetch(
                    FetchRequest(
                        workspace=workspace,
                        patterns=(
                            (".rundra-shards/*.tar", ".rundra-shards/*.sha256")
                            if request.shard_outputs
                            else _fetch_patterns(request.experiment.outputs, units)
                        ),
                        destination=retrieval_destination,
                        mode=request.fetch_mode,
                    )
                )
                fetched_artifacts = _fetched_task_artifacts(fetched.artifacts, units)
                if record.is_compact and request.shard_outputs:
                    assert self._task_store is not None
                    shard_paths = tuple(
                        Path(artifact.path)
                        for artifact in fetched_artifacts
                        if artifact.kind is ArtifactKind.OUTPUT_SHARD
                        and str(artifact.path).endswith(".tar")
                    )
                    if shard_paths:
                        self._task_store.ingest_result_shards(
                            record.run.id,
                            _verified_result_shard_rows(record, shard_paths),
                        )
            except Exception as error:
                failed = replace(
                    record,
                    run=replace(record.run, retrieval_state=RetrievalState.FAILED),
                    task_retrieval_states=(
                        {}
                        if record.is_compact
                        else {
                            task.id: RetrievalState.FAILED for task in record.run.tasks
                        }
                    ),
                )
                if record.is_compact:
                    assert self._task_store is not None
                    self._task_store.set_all_retrieval(
                        record.run.id, RetrievalState.FAILED
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
                task_retrieval_states=(
                    {}
                    if record.is_compact
                    else {
                        task.id: RetrievalState.SUCCEEDED for task in record.run.tasks
                    }
                ),
                artifacts=(*record.artifacts, *fetched_artifacts),
            )
            if record.is_compact:
                assert self._task_store is not None
                self._task_store.set_all_retrieval(
                    record.run.id, RetrievalState.SUCCEEDED
                )
            self.store.update(updated, expected=record)
            self._report(
                ProgressPhase.RETRIEVE,
                6 + len(units),
                (
                    f"artifacts={len(fetched_artifacts)} "
                    f"state={updated.run.retrieval_state.value}"
                ),
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
        direct_compact = _is_direct_compact_request(request)
        if (
            len(units) > 1
            and request.plan.strategy
            not in {
                SLURM_ARRAY,
                SCHEDULER_ARRAY,
            }
            and not direct_compact
        ):
            raise OrchestrationError(
                code="UNSUPPORTED_TASK_COUNT",
                message=(
                    "Multi-Task execution requires a Slurm array or PBS "
                    "scheduler array plan"
                ),
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
        if direct_compact:
            assert request.compact_plan is not None
            if request.compact_plan.experiment_name != request.experiment.name:
                raise OrchestrationError(
                    code="PLAN_MISMATCH",
                    message="Compact plan and experiment names do not match",
                )
            return
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
        if _is_direct_compact_request(request):
            assert request.compact_plan is not None
            assert request.compact_plan.task_space is not None
            assert request.compact_plan.retrieval_policy is not None
            assert self._task_store is not None
            compact_run = CompactRun(
                id=run_id,
                experiment_name=request.experiment.name,
                target=request.plan.target,
                tasks=(),
                created_at=self._clock(),
            )
            return RunRecord(
                format_version=6,
                framework_version=self._framework_version,
                run=compact_run,
                experiment=request.experiment,
                source_root=request.source_root,
                retrieval_destination=_retrieval_destination(request.fetch_destination),
                fetch_mode=request.fetch_mode,
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
                scheduler_metadata=_execution_storage_metadata(request),
                task_space=request.compact_plan.task_space,
                execution_strategy=request.compact_plan.strategy,
                retrieval_policy=request.compact_plan.retrieval_policy,
                task_state_store=PurePath(self._task_store.path(run_id).name),
            )
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
            format_version=6,
            framework_version=self._framework_version,
            run=run,
            experiment=request.experiment,
            source_root=request.source_root,
            retrieval_destination=_retrieval_destination(request.fetch_destination),
            fetch_mode=request.fetch_mode,
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
            scheduler_metadata=_execution_storage_metadata(request),
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
            task_retrieval_states=(
                {}
                if record.is_compact
                else {task.id: RetrievalState.PENDING for task in record.run.tasks}
            ),
        )
        if record.is_compact:
            assert self._task_store is not None
            self._task_store.set_all_retrieval(record.run.id, RetrievalState.PENDING)
        self.store.update(pending, expected=record)
        self.store.update(
            replace(
                pending,
                run=replace(pending.run, retrieval_state=RetrievalState.FAILED),
                task_retrieval_states=(
                    {}
                    if record.is_compact
                    else {task.id: RetrievalState.FAILED for task in pending.run.tasks}
                ),
            ),
            expected=pending,
        )
        if record.is_compact:
            assert self._task_store is not None
            self._task_store.set_all_retrieval(record.run.id, RetrievalState.FAILED)


def _execution_storage_metadata(
    request: RunExecutionRequest,
) -> dict[str, NativeValue]:
    policy = request.plan.target.execution_storage
    if policy is None:
        return {}
    resources = request.plan.units[0].resources
    active_environment = (
        policy.gpu_environment
        if resources.gpus_per_task > 0
        else policy.cpu_environment
    )
    return {
        "execution_storage.type": "slurm_scratch",
        "execution_storage.cpu_environment": policy.cpu_environment,
        "execution_storage.gpu_environment": policy.gpu_environment,
        "execution_storage.active_environment": active_environment,
        "execution_storage.stage_image": policy.stage_image,
        "execution_storage.copy_back": policy.copy_back,
    }


def _compact_record(
    record: RunRecord,
    plan: ExecutionPlan,
    task_store: SqliteTaskStore,
) -> RunRecord:
    assert plan.task_space is not None
    assert plan.retrieval_policy is not None
    return _compact_record_from_metadata(
        record,
        task_space=plan.task_space,
        execution_strategy=plan.strategy,
        retrieval_policy=plan.retrieval_policy,
        task_store=task_store,
    )


def _compact_record_from_metadata(
    record: RunRecord,
    *,
    task_space: TaskSpace,
    execution_strategy: str,
    retrieval_policy: str,
    task_store: SqliteTaskStore,
) -> RunRecord:
    run = CompactRun(
        id=record.run.id,
        experiment_name=record.run.experiment_name,
        target=record.run.target,
        tasks=(),
        created_at=record.run.created_at,
        state=record.run.state,
        retrieval_state=record.run.retrieval_state,
    )
    return replace(
        record,
        format_version=(
            record.format_version if record.format_version in {5, 6} else 4
        ),
        run=run,
        task_array_mapping=(),
        task_scheduler_ids={},
        task_native_states={},
        task_retrieval_states={},
        task_exit_codes={},
        task_space=task_space,
        execution_strategy=execution_strategy,
        retrieval_policy=retrieval_policy,
        task_state_store=PurePath(task_store.path(record.run.id).name),
    )


def _retrieval_destination(destination: PurePath) -> PurePath:
    return PurePath(Path(str(destination)).resolve())


def _compact_observed_record(
    record: RunRecord,
    scheduler: Scheduler,
    transport: Transport | None,
    task_store: SqliteTaskStore,
    observed_at: datetime,
) -> RunRecord:
    if record.task_space is None or record.task_state_store is None:
        raise ValueError("Compact Run is missing TaskSpace persistence metadata")
    if record.task_state_store.name != task_store.path(record.run.id).name:
        raise ValueError("Compact Run references another Task state sidecar")
    states = task_store.all_states(record.run.id)
    reference_ids = tuple(
        dict.fromkeys(
            state.scheduler_id for state in states if state.scheduler_id is not None
        )
    )
    if not reference_ids:
        raise ValueError("Compact Run has no scheduler Task identities")
    references = tuple(SchedulerReference(value) for value in reference_ids)
    scheduler_observations = scheduler.query(references)
    _validate_observations(scheduler_observations, references)
    by_native = {
        observation.reference.native_id: observation
        for observation in scheduler_observations
    }
    started, finished = _compact_bundle_events(record, reference_ids, transport)
    updated_states: list[TaskState] = []
    for state in states:
        if state.execution_state in _TERMINAL_STATES:
            updated_states.append(state)
            continue
        assert state.scheduler_id is not None
        observation = by_native[state.scheduler_id]
        task_id = state.coordinate.task_id
        if task_id in finished:
            attempt, code, _, _ = finished[task_id]
            execution = ExecutionState.SUCCEEDED if code == 0 else ExecutionState.FAILED
            native_state = (
                "BUNDLED_TASK_SUCCEEDED"
                if code == 0
                else (
                    "BUNDLE_RETRY_EXHAUSTED" if code == 125 else "BUNDLED_TASK_FAILED"
                )
            )
            updated_states.append(
                replace(
                    state,
                    execution_state=execution,
                    native_state=native_state,
                    exit_code=code,
                    attempt=attempt,
                )
            )
        elif task_id in started:
            attempt, _, _ = started[task_id]
            updated_states.append(
                replace(
                    state,
                    execution_state=ExecutionState.RUNNING,
                    native_state="BUNDLED_TASK_RUNNING",
                    attempt=attempt,
                )
            )
        elif observation.state in {
            ExecutionState.SUBMITTED,
            ExecutionState.QUEUED,
            ExecutionState.RUNNING,
        }:
            updated_states.append(
                replace(
                    state,
                    execution_state=ExecutionState.QUEUED,
                    native_state=observation.native_state,
                )
            )
        elif observation.state is ExecutionState.CANCELLED:
            updated_states.append(
                replace(
                    state,
                    execution_state=ExecutionState.CANCELLED,
                    native_state=observation.native_state,
                )
            )
        else:
            updated_states.append(
                replace(
                    state,
                    execution_state=ExecutionState.FAILED,
                    native_state="BUNDLE_JOURNAL_MISSING",
                    exit_code=125,
                )
            )
    task_store.update_batch(record.run.id, tuple(updated_states))
    execution_state = aggregate_execution_state(
        tuple(state.execution_state for state in updated_states)
    )
    worker_states = tuple(observation.state for observation in scheduler_observations)
    workers_terminal = all(state in _TERMINAL_STATES for state in worker_states)
    if not workers_terminal and execution_state in _TERMINAL_STATES:
        # FINISH is emitted before the worker seals and publishes its result
        # shard. Keep the Run active until the scheduler confirms that every
        # worker has crossed that publication barrier.
        execution_state = ExecutionState.RUNNING
    elif ExecutionState.FAILED in worker_states:
        execution_state = ExecutionState.FAILED
    elif (
        ExecutionState.CANCELLED in worker_states
        and execution_state is ExecutionState.SUCCEEDED
    ):
        execution_state = ExecutionState.CANCELLED
    scheduler_metadata = dict(record.scheduler_metadata)
    scheduler_metadata["running_tasks"] = sum(
        state.execution_state is ExecutionState.RUNNING for state in updated_states
    )
    scheduler_metadata["active_workers"] = sum(
        observation.state is ExecutionState.RUNNING
        for observation in scheduler_observations
    )
    scheduler_metadata.pop("throughput_tasks_per_second", None)
    scheduler_metadata.pop("eta_seconds", None)
    estimate = _progress_estimate(
        len(updated_states),
        tuple(value[1] for value in started.values()),
        tuple(value[2] for value in finished.values()),
    )
    if estimate is not None:
        (
            scheduler_metadata["throughput_tasks_per_second"],
            scheduler_metadata["eta_seconds"],
        ) = estimate
    nodes = tuple(
        str(node)
        for observation in scheduler_observations
        if (node := observation.metadata.get("allocated_nodes")) is not None
    )
    started_values = tuple(
        observation.started_at
        for observation in scheduler_observations
        if observation.started_at is not None
    )
    scheduler_native_states = tuple(
        dict.fromkeys(
            observation.native_state for observation in scheduler_observations
        )
    )
    task_native_state = _aggregate_native_counts(
        {
            value: sum(item.native_state == value for item in updated_states)
            for value in {item.native_state for item in updated_states}
        }
    )
    if any(
        state in {ExecutionState.FAILED, ExecutionState.CANCELLED}
        for state in worker_states
    ):
        task_native_state = ""
    terminal = execution_state in _TERMINAL_STATES
    return replace(
        record,
        run=replace(record.run, state=execution_state),
        allocated_nodes=tuple(dict.fromkeys((*record.allocated_nodes, *nodes))),
        started_at=record.started_at
        or (min(started_values) if started_values else None),
        completed_at=(record.completed_at or observed_at if terminal else None),
        native_state=(
            task_native_state
            or (
                scheduler_native_states[0]
                if len(scheduler_native_states) == 1
                else "MIXED"
            )
        ),
        scheduler_metadata=scheduler_metadata,
    )


def _compact_bundle_events(
    record: RunRecord,
    reference_ids: tuple[str, ...],
    transport: Transport | None,
) -> tuple[
    dict[TaskId, tuple[int, int, str]],
    dict[TaskId, tuple[int, int, int, str]],
]:
    status_value = record.scheduler_metadata.get("bundle_status_root")
    if type(status_value) is not str or not status_value.startswith("/"):
        raise ValueError("Compact worker-pool Run has no bundle status root")
    if transport is None:
        raise ValueError("Compact worker-pool reconciliation requires transport")
    result = transport.run(
        Command(
            (
                "/bin/sh",
                "-c",
                (
                    'status=$1; shift; for native do aggregate="$status/$native.tsv"; '
                    'if [ -f "$aggregate" ]; then cat -- "$aggregate"; continue; fi; '
                    'for path in "$status/$native".lane-*.tsv '
                    '"$status/$native".lane-*.tsv.*; do '
                    '[ -f "$path" ] && cat -- "$path"; done; done; :'
                ),
                "rundra-compact-bundle-status",
                status_value,
                *reference_ids,
            )
        )
    )
    if result.exit_code != 0:
        raise ValueError("Could not read compact bundled Task journals")
    assert record.task_space is not None
    started: dict[TaskId, tuple[int, int, str]] = {}
    finished: dict[TaskId, tuple[int, int, int, str]] = {}
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if fields in (
            ["RUNDRA_TASK_EVENTS", "1"],
            ["RUNDRA_TASK_EVENTS", "2"],
        ):
            continue
        if len(fields) in {4, 5} and fields[0] == "START":
            task_id = TaskId(fields[1])
            ordinal = int(task_id.value.removeprefix("task_"))
            attempt = int(fields[2]) if len(fields) == 5 else 0
            timestamp_index = 3 if len(fields) == 5 else 2
            if ordinal >= record.task_space.task_count or attempt < 0:
                raise ValueError("Compact Task START event is invalid")
            start_event = (
                attempt,
                int(fields[timestamp_index]),
                fields[timestamp_index + 1],
            )
            previous_start = started.get(task_id)
            if (
                previous_start is not None
                and start_event[0] == previous_start[0]
                and start_event != previous_start
            ):
                raise ValueError("Compact Task START events conflict")
            if previous_start is None or start_event[0] > previous_start[0]:
                started[task_id] = start_event
            continue
        if len(fields) in {5, 6} and fields[0] == "FINISH":
            task_id = TaskId(fields[1])
            ordinal = int(task_id.value.removeprefix("task_"))
            attempt = int(fields[2]) if len(fields) == 6 else 0
            code_index = 3 if len(fields) == 6 else 2
            code = int(fields[code_index])
            if (
                ordinal >= record.task_space.task_count
                or attempt < 0
                or not 0 <= code <= 255
            ):
                raise ValueError("Compact Task FINISH event is invalid")
            finish_event = (
                attempt,
                code,
                int(fields[code_index + 1]),
                fields[code_index + 2],
            )
            previous_finish = finished.get(task_id)
            if (
                previous_finish is not None
                and finish_event[0] == previous_finish[0]
                and finish_event != previous_finish
            ):
                raise ValueError("Compact Task FINISH events conflict")
            if previous_finish is None or finish_event[0] > previous_finish[0]:
                finished[task_id] = finish_event
            continue
        if len(fields) == 2:
            task_id = TaskId(fields[0])
            ordinal = int(task_id.value.removeprefix("task_"))
            code = int(fields[1])
            if ordinal >= record.task_space.task_count or not 0 <= code <= 255:
                raise ValueError("Compact legacy Task event is invalid")
            previous_finish = finished.get(task_id)
            if previous_finish is not None and previous_finish[1] != code:
                raise ValueError("Compact legacy Task outcome conflicts")
            if previous_finish is None:
                finished[task_id] = (0, code, 0, "unknown")
            continue
        raise ValueError("Compact bundled Task journal is malformed")
    return started, finished


def _with_execution_state(record: RunRecord, state: ExecutionState) -> RunRecord:
    tasks = tuple(replace(task, state=state) for task in record.run.tasks)
    return replace(record, run=replace(record.run, tasks=tasks, state=state))


def _aggregate_native_counts(counts: Mapping[str | None, int]) -> str | None:
    observed = {value for value, count in counts.items() if count > 0}
    if not observed or None in observed:
        return None
    return next(iter(observed)) if len(observed) == 1 else "MIXED"


def _progress_estimate(
    total: int,
    started_at: tuple[int, ...],
    finished_at: tuple[int, ...],
) -> tuple[float, float] | None:
    if total < 1 or len(finished_at) >= total or not started_at:
        return None
    required = max(_MIN_ETA_SAMPLE_COUNT, ceil(total * _MIN_ETA_SAMPLE_FRACTION))
    if len(finished_at) < required:
        return None
    elapsed = max(finished_at) - min(started_at)
    if elapsed < _MIN_ETA_WINDOW_SECONDS:
        return None
    throughput = len(finished_at) / elapsed
    return throughput, (total - len(finished_at)) / throughput


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
        replace(
            task,
            state=_monotonic_observed_state(task.state, observations[task.id].state),
        )
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
    artifact_keys = {(artifact.kind, artifact.task_id) for artifact in record.artifacts}
    for task_id, observation in task_observations:
        for metadata_name, kind in (
            ("stdout_path", ArtifactKind.STDOUT),
            ("stderr_path", ArtifactKind.STDERR),
        ):
            path = observation.metadata.get(metadata_name)
            key = (kind, task_id)
            if type(path) is str and key not in artifact_keys:
                artifacts.append(Artifact(kind, PurePosixPath(path), task_id=task_id))
                artifact_keys.add(key)
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


def _monotonic_observed_state(
    current: ExecutionState,
    observed: ExecutionState,
) -> ExecutionState:
    if current is ExecutionState.RUNNING and observed in {
        ExecutionState.SUBMITTED,
        ExecutionState.QUEUED,
    }:
        return current
    if current is ExecutionState.QUEUED and observed is ExecutionState.SUBMITTED:
        return current
    return observed


def _apply_bundle_journals(
    record: RunRecord,
    task_observations: tuple[tuple[TaskId, SchedulerObservation], ...],
    transport: Transport | None,
) -> RunRecord:
    status_value = record.scheduler_metadata.get("bundle_status_root")
    if status_value is None:
        return record
    if type(status_value) is not str or not status_value.startswith("/"):
        raise ValueError("Run bundle status root is invalid")
    if transport is None:
        raise ValueError("Bundled Run reconciliation requires its transport")
    observations = dict(task_observations)
    terminal_references = tuple(
        dict.fromkeys(
            record.task_scheduler_ids[task_id]
            for task_id, observation in task_observations
            if observation.state
            in {
                ExecutionState.SUBMITTED,
                ExecutionState.QUEUED,
                ExecutionState.RUNNING,
                ExecutionState.SUCCEEDED,
                ExecutionState.FAILED,
                ExecutionState.CANCELLED,
            }
        )
    )
    journal_codes: dict[TaskId, int] = {}
    journal_started: dict[TaskId, tuple[int, str]] = {}
    journal_finished: dict[TaskId, tuple[int, int, str]] = {}
    if terminal_references:
        result = transport.run(
            Command(
                (
                    "/bin/sh",
                    "-c",
                    (
                        'status=$1; shift; for native do aggregate="$status/$native.tsv"; '
                        'if [ -f "$aggregate" ]; then cat -- "$aggregate"; continue; fi; '
                        'for path in "$status/$native".lane-*.tsv '
                        '"$status/$native".lane-*.tsv.*; do '
                        '[ -f "$path" ] && cat -- "$path"; done; done; :'
                    ),
                    "rundra-bundle-status",
                    status_value,
                    *terminal_references,
                )
            )
        )
        if result.exit_code != 0:
            raise ValueError("Could not read bundled Task status journals")
        known = {task.id for task in record.run.tasks}
        for line in result.stdout.splitlines():
            fields = line.split("\t")
            if fields == ["RUNDRA_TASK_EVENTS", "1"]:
                continue
            if len(fields) == 4 and fields[0] == "START":
                task_id = TaskId(fields[1])
                if task_id not in known:
                    raise ValueError("Bundled Task START event is invalid")
                start_event = (int(fields[2]), fields[3])
                previous = journal_started.get(task_id)
                if previous is not None and previous != start_event:
                    raise ValueError("Bundled Task START events conflict")
                journal_started[task_id] = start_event
                continue
            if len(fields) == 5 and fields[0] == "FINISH":
                task_id = TaskId(fields[1])
                if task_id not in known:
                    raise ValueError("Bundled Task FINISH event is invalid")
                code = int(fields[2])
                finish_event = (code, int(fields[3]), fields[4])
                previous_finish = journal_finished.get(task_id)
                if previous_finish is not None and previous_finish != finish_event:
                    raise ValueError("Bundled Task FINISH events conflict")
                existing_code = journal_codes.get(task_id)
                if existing_code is not None and existing_code != code:
                    raise ValueError("Bundled Task exit outcomes conflict")
                journal_finished[task_id] = finish_event
                journal_codes[task_id] = code
                continue
            if len(fields) != 2:
                raise ValueError("Bundled Task status journal is malformed")
            task_id = TaskId(fields[0])
            if task_id not in known:
                raise ValueError("Bundled Task status journal has invalid identities")
            try:
                code = int(fields[1])
            except ValueError as error:
                raise ValueError("Bundled Task exit code is malformed") from error
            if not 0 <= code <= 255:
                raise ValueError("Bundled Task exit code is outside shell range")
            existing_code = journal_codes.get(task_id)
            if existing_code is not None and existing_code != code:
                raise ValueError("Bundled Task exit outcomes conflict")
            journal_codes[task_id] = code
    tasks = []
    exit_codes = dict(record.task_exit_codes)
    native_states = dict(record.task_native_states)
    for task in record.run.tasks:
        observation = observations[task.id]
        if task.id in journal_codes:
            code = journal_codes[task.id]
            state = ExecutionState.SUCCEEDED if code == 0 else ExecutionState.FAILED
            tasks.append(replace(task, state=state))
            exit_codes[task.id] = code
            native_states[task.id] = (
                "BUNDLED_TASK_SUCCEEDED" if code == 0 else "BUNDLED_TASK_FAILED"
            )
            continue
        if task.id in journal_started:
            tasks.append(replace(task, state=ExecutionState.RUNNING))
            native_states[task.id] = "BUNDLED_TASK_RUNNING"
            continue
        if observation.state in {
            ExecutionState.SUBMITTED,
            ExecutionState.QUEUED,
            ExecutionState.RUNNING,
        }:
            tasks.append(replace(task, state=ExecutionState.QUEUED))
            continue
        if observation.state is ExecutionState.CANCELLED:
            tasks.append(replace(task, state=ExecutionState.CANCELLED))
            continue
        tasks.append(replace(task, state=ExecutionState.FAILED))
        exit_codes[task.id] = 125
        native_states[task.id] = "BUNDLE_JOURNAL_MISSING"
    task_tuple = tuple(tasks)
    scheduler_metadata = dict(record.scheduler_metadata)
    scheduler_metadata["running_tasks"] = sum(
        task.state is ExecutionState.RUNNING for task in task_tuple
    )
    scheduler_metadata["active_workers"] = len(
        {
            observation.reference.native_id
            for _, observation in task_observations
            if observation.state is ExecutionState.RUNNING
        }
    )
    scheduler_metadata.pop("throughput_tasks_per_second", None)
    scheduler_metadata.pop("eta_seconds", None)
    estimate = _progress_estimate(
        len(task_tuple),
        tuple(value[0] for value in journal_started.values()),
        tuple(value[1] for value in journal_finished.values()),
    )
    if estimate is not None:
        (
            scheduler_metadata["throughput_tasks_per_second"],
            scheduler_metadata["eta_seconds"],
        ) = estimate
    native_state = _aggregate_native_counts(
        {
            value: sum(item == value for item in native_states.values())
            for value in set(native_states.values())
        }
    )
    return replace(
        record,
        run=replace(
            record.run,
            tasks=task_tuple,
            state=aggregate_execution_state(tuple(task.state for task in task_tuple)),
        ),
        task_exit_codes=exit_codes,
        task_native_states=native_states,
        native_state=native_state or record.native_state,
        scheduler_metadata=scheduler_metadata,
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


def _allocation_scratch(
    request: RunExecutionRequest,
    workspace: StagedWorkspace,
    task_count: int,
) -> AllocationScratch | None:
    policy = request.plan.target.execution_storage
    if policy is None:
        return None
    container = request.experiment.container
    image = None
    if container is not None:
        image = (
            container.image
            if container.image.is_absolute()
            else workspace.source / container.image
        )
    return AllocationScratch(
        workspace.root,
        policy,
        image_path=image,
        task_directories=task_count > 1 or request.compact_plan is not None,
    )


def _compact_container_request(
    experiment: ExperimentSpec,
    unit: ExecutionUnit,
    workspace: StagedWorkspace,
) -> ContainerRequest:
    container = experiment.container
    command = Command(
        tuple(
            argument.replace(
                "{config}", str(_CONTAINER_INPUTS / f"{unit.task_id}.yaml")
            ).replace("{seed}", _COMPACT_SEED)
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
            BindMount(
                workspace.outputs / _COMPACT_TASK_ID,
                _CONTAINER_OUTPUTS,
                read_only=False,
            ),
            BindMount(
                workspace.runtime / _COMPACT_TASK_ID,
                _CONTAINER_RUNTIME,
                read_only=False,
            ),
        ),
    )


def _compact_parameter_units(request: RunExecutionRequest) -> tuple[ExecutionUnit, ...]:
    assert request.compact_plan is not None
    assert request.compact_plan.task_space is not None
    if request.compact_configs:
        seed = request.compact_plan.task_space.seeds.start
        seed_count = request.compact_plan.task_space.seeds.count
        return tuple(
            ExecutionUnit(
                task_id=TaskId.from_ordinal(ordinal * seed_count),
                seed=seed,
                config=expanded.config,
                command=request.experiment.command,
                resources=request.experiment.resources,
                parameter_set=expanded.parameter_set,
            )
            for ordinal, expanded in enumerate(request.compact_configs)
        )
    seed_count = request.compact_plan.task_space.seeds.count
    return tuple(
        request.plan.units[ordinal * seed_count]
        for ordinal in range(request.compact_plan.task_space.parameter_set_count)
    )


def _is_direct_compact_request(request: RunExecutionRequest) -> bool:
    return request.compact_plan is not None and request.plan == request.compact_plan


def _compact_task_manifest(
    plan: ExecutionPlan,
    units: tuple[ExecutionUnit, ...],
) -> str:
    assert plan.task_space is not None
    return json.dumps(
        {
            "schema_version": 2,
            "task_space": {
                "parameter_set_count": plan.task_space.parameter_set_count,
                "seeds": {
                    "start": plan.task_space.seeds.start,
                    "stop": plan.task_space.seeds.stop,
                    "step": plan.task_space.seeds.step,
                },
                "task_count": plan.task_space.task_count,
            },
            "parameter_sets": [
                {
                    "ordinal": ordinal,
                    "config": f"input/{unit.task_id}.yaml",
                    "config_sha256": hashlib.sha256(
                        unit.config.content.encode("utf-8")
                    ).hexdigest(),
                    "parameter_set": (
                        None
                        if unit.parameter_set is None
                        else {
                            "id": unit.parameter_set.id,
                            "choices": dict(unit.parameter_set.choices),
                        }
                    ),
                }
                for ordinal, unit in enumerate(units)
            ],
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
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
        ArtifactKind.REFERENCE_MANIFEST,
        ArtifactKind.OUTPUT_SHARD,
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
        if (
            artifact.kind is ArtifactKind.RAW_RESULT
            and ".rundra-shards" in artifact.path.parts
        ):
            result.append(replace(artifact, kind=ArtifactKind.OUTPUT_SHARD))
            continue
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


def _verified_result_shard_rows(
    record: RunRecord, shard_paths: tuple[Path, ...]
) -> Iterable[tuple[str, Mapping[TaskId, int]]]:
    configured_host = record.run.target.transport.options.get("host")
    controller_hostname = configured_host if isinstance(configured_host, str) else None
    for shard in shard_paths:
        index = read_verified_shard_index(
            shard, controller_hostname=controller_hostname
        )
        yield (
            shard.name,
            {TaskId(task_id): code for task_id, code in index.task_exit_codes.items()},
        )
