from __future__ import annotations

import os
import secrets
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path, PurePath
from time import monotonic, sleep
from types import MappingProxyType

from rundra.adapters import (
    ApptainerRuntime,
    LocalPurger,
    LocalScheduler,
    LocalStager,
    LocalTransport,
    NativeRuntime,
    PurgeError,
    RemoteApptainerRuntime,
    RsyncStager,
    RsyncUploadError,
    SharedStager,
    SSHPurger,
    SSHTransport,
)
from rundra.config.errors import ConfigError
from rundra.config.experiments import load_experiment
from rundra.config.launch import (
    LaunchResolutionError,
    LaunchValues,
    ProjectLaunchConfig,
    ResolvedLaunch,
    discover_project_launch,
    discover_user_launch,
    resolve_launch,
)
from rundra.config.sweeps import load_sweep_config
from rundra.config.targets import (
    builtin_targets_source,
    load_targets,
    load_targets_config,
)
from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    Command,
    ExperimentSpec,
    RunId,
    Target,
    TaskId,
)
from rundra.domain.parameters import ParameterSet
from rundra.domain.preparation import (
    PREPARE_LOCATIONS,
    PreparationConfig,
    PreparationImageDefinition,
    PreparationPlan,
    PreparationRecord,
    PreparationSourceWorkingTree,
    PreparationStorageConfig,
)
from rundra.domain.purge import (
    PurgeAttempt,
    PurgeOutcome,
    PurgeReceipt,
    PurgeRequest,
    PurgeResult,
    PurgeScope,
)
from rundra.domain.records import RunRecord
from rundra.domain.scaling import (
    ExecutionPolicy,
    SeedRange,
    TaskCoordinate,
    WorkerPoolPolicy,
)
from rundra.domain.states import (
    ExecutionState,
    RetrievalState,
    aggregate_retrieval_state,
)
from rundra.domain.sweeps import ExpandedConfig, SweepExpansion
from rundra.orchestration.models import ExecutionPlan, PlanningError
from rundra.orchestration.planner import (
    compact_seed_range,
    create_plan,
    create_scalable_plan,
    create_sweep_plan,
    expand_seeds,
    validate_task_confirmation,
)
from rundra.orchestration.preparation import (
    PreparationError,
    RemotePreparationSpec,
    create_remote_preparation_spec,
    prepare_local,
    prepare_source_snapshot,
    probe_remote_preparation_cache,
    read_remote_preparation_result,
    remote_builder_version,
    remote_platform_fingerprint,
    remote_preparation_record,
    select_remote_preparation_location,
)
from rundra.orchestration.progress import ProgressEvent, ProgressObserver, ProgressPhase
from rundra.orchestration.service import (
    _BUNDLE_JOURNAL_READ_UNAVAILABLE,
    _DEFAULT_QUERY_FAILURE_LIMIT,
    OrchestrationError,
    OrchestrationService,
    RunExecutionRequest,
    SchedulerLifecycleService,
)
from rundra.orchestration.shards import (
    extract_shard,
    read_verified_shard_index,
)
from rundra.persistence import (
    PurgeReceiptStore,
    RunNotFoundError,
    RunStore,
    RunStoreConflictError,
    RunStoreError,
    SqliteTaskStore,
    SubmissionReceiptStore,
    TaskState,
)
from rundra.ports import (
    ContainerRuntime,
    FetchRequest,
    Scheduler,
    StagedWorkspace,
    Stager,
    Transport,
)
from rundra.provenance import GitProvenanceCapture
from rundra.results import OperationError, OperationResult
from rundra.scheduler_registry import (
    remote_scheduler_kinds,
    scheduler_capabilities,
    scheduler_for_target,
    validate_scheduler_resources,
)
from rundra.schema_versions import (
    PLAN_SCHEMA,
    RUN_LIST_SCHEMA,
    STATUS_SCHEMA,
    TASKS_SCHEMA,
)
from rundra.sync import (
    SourceSnapshotPreview,
    SourceSnapshotPreviewError,
    SyncExclusionError,
    preview_source_snapshot,
)

_STATUS_JOURNAL_READ_RETRIES = 1
_STATUS_JOURNAL_RETRY_DELAY_SECONDS = 0.2
_SHARD_VISIBILITY_ATTEMPTS = 5
_DEFAULT_PREPARATION_STORAGE = PreparationStorageConfig()
LAST_RUN_SELECTOR = "__rundra_last_run__"
_COMPACT_EXECUTION_THRESHOLD = 1000


def _report_progress(
    observer: ProgressObserver | None,
    phase: ProgressPhase,
    completed: int,
    message: str,
    run_id: RunId | None = None,
    task_total: int = 0,
) -> None:
    if observer is not None:
        observer(ProgressEvent(phase, completed, 6 + task_total, message, run_id))


def _target_progress_message(target: Target) -> str:
    return (
        f"target={target.name} "
        f"backends={target.transport.kind}/{target.scheduler.kind}/"
        f"{target.staging.kind}/{target.container.kind}"
    )


@dataclass(frozen=True, slots=True)
class ValidationValue:
    source: Path
    experiment: ExperimentSpec
    project: ProjectLaunchConfig | None = None


@dataclass(frozen=True, slots=True)
class TargetsValue:
    source: Path
    targets: Mapping[str, Target]
    format_version: int = 2


@dataclass(frozen=True, slots=True)
class RunValue:
    record: RunRecord
    launch: LaunchResolutionValue | None = None

    def __post_init__(self) -> None:
        if self.launch is not None and type(self.launch) is not LaunchResolutionValue:
            raise TypeError("RunValue launch must be LaunchResolutionValue or None")

    @property
    def run_id(self) -> RunId:
        return self.record.run.id

    @property
    def exit_code(self) -> int:
        return (
            2
            if self.record.run.state
            in {ExecutionState.FAILED, ExecutionState.CANCELLED}
            else 0
        )

    @property
    def seed(self) -> int:
        if len(self.record.run.tasks) != 1:
            raise ValueError("RunValue has no singular seed for a replicated Run")
        return self.seeds[0]

    @property
    def seeds(self) -> tuple[int, ...]:
        return tuple(task.seed for task in self.record.run.tasks)


@dataclass(frozen=True, slots=True)
class TaskStatusValue:
    task_id: TaskId
    seed: int
    state: ExecutionState
    retrieval_state: RetrievalState
    native_id: str | None = None
    native_state: str | None = None
    exit_code: int | None = None
    parameter_set: ParameterSet | None = None

    def __post_init__(self) -> None:
        if type(self.task_id) is not TaskId:
            raise TypeError("TaskStatusValue task_id must be a TaskId")
        if type(self.seed) is not int:
            raise TypeError("TaskStatusValue seed must be an integer")
        if type(self.state) is not ExecutionState:
            raise TypeError("TaskStatusValue state must be an ExecutionState")
        if type(self.retrieval_state) is not RetrievalState:
            raise TypeError("TaskStatusValue retrieval_state must be a RetrievalState")
        for name in ("native_id", "native_state"):
            value = getattr(self, name)
            if value is not None and (
                type(value) is not str or not value.strip() or "\x00" in value
            ):
                raise ValueError(f"TaskStatusValue {name} must be safe or None")
        if self.exit_code is not None and type(self.exit_code) is not int:
            raise TypeError("TaskStatusValue exit_code must be an integer or None")
        if (
            self.parameter_set is not None
            and type(self.parameter_set) is not ParameterSet
        ):
            raise TypeError(
                "TaskStatusValue parameter_set must be a ParameterSet or None"
            )


@dataclass(frozen=True, slots=True)
class PreparationStatusValue:
    scheduler_id: str | None
    state: str | None
    native_state: str | None
    location: str

    def __post_init__(self) -> None:
        for name in ("scheduler_id", "state", "native_state"):
            value = getattr(self, name)
            if value is not None and (
                type(value) is not str or not value.strip() or "\x00" in value
            ):
                raise ValueError(f"PreparationStatusValue {name} must be safe or None")
        if self.location not in {"local", "target"}:
            raise ValueError("PreparationStatusValue location is unsupported")


@dataclass(frozen=True, slots=True)
class SubmissionRecoveryValue:
    """Result of safely recovering or finding one scheduler submission."""

    record: RunRecord
    action: str

    def __post_init__(self) -> None:
        if type(self.record) is not RunRecord:
            raise TypeError("Submission recovery requires a RunRecord")
        if self.action not in {"resumed", "found", "resolved_not_submitted"}:
            raise ValueError("Submission recovery action is unsupported")


@dataclass(frozen=True, slots=True)
class StatusValue:
    run_id: RunId
    experiment: str
    target: str
    state: ExecutionState
    retrieval_state: RetrievalState
    task_counts: Mapping[str, int]
    native_state: str | None = None
    scheduler_job_ids: tuple[str, ...] = ()
    task_details: tuple[TaskStatusValue, ...] = ()
    preparation: PreparationStatusValue | None = None
    format_version: int = 1
    worker_count: int | None = None
    task_slots_per_worker: int | None = None
    active_workers: int | None = None
    throughput_tasks_per_second: float | None = None
    eta_seconds: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "task_counts", MappingProxyType(dict(self.task_counts))
        )
        if self.native_state is not None and (
            type(self.native_state) is not str or not self.native_state.strip()
        ):
            raise ValueError("StatusValue native_state must be nonblank or None")
        if not isinstance(self.scheduler_job_ids, Sequence) or isinstance(
            self.scheduler_job_ids, (str, bytes)
        ):
            raise TypeError("StatusValue scheduler_job_ids must be a sequence")
        scheduler_job_ids = tuple(self.scheduler_job_ids)
        if any(type(value) is not str or not value for value in scheduler_job_ids):
            raise ValueError("StatusValue scheduler_job_ids must be nonempty strings")
        object.__setattr__(self, "scheduler_job_ids", scheduler_job_ids)
        if not isinstance(self.task_details, Sequence) or isinstance(
            self.task_details, (str, bytes)
        ):
            raise TypeError("StatusValue task_details must be a sequence")
        task_details = tuple(self.task_details)
        if any(type(value) is not TaskStatusValue for value in task_details):
            raise TypeError("StatusValue task_details must contain TaskStatusValues")
        if len({value.task_id for value in task_details}) != len(task_details):
            raise ValueError("StatusValue task_details must contain unique Task IDs")
        if task_details and sum(self.task_counts.values()) != len(task_details):
            raise ValueError("StatusValue task counts must match task_details")
        object.__setattr__(self, "task_details", task_details)
        if (
            self.preparation is not None
            and type(self.preparation) is not PreparationStatusValue
        ):
            raise TypeError(
                "StatusValue preparation must be PreparationStatusValue or None"
            )
        if self.format_version not in STATUS_SCHEMA.supported:
            raise ValueError("StatusValue format_version is unsupported")
        for name in ("worker_count", "task_slots_per_worker"):
            item = getattr(self, name)
            if item is not None and (type(item) is not int or item < 1):
                raise ValueError(f"StatusValue {name} must be positive or None")
        if self.active_workers is not None and (
            type(self.active_workers) is not int or self.active_workers < 0
        ):
            raise ValueError("StatusValue active_workers must be non-negative or None")
        for name in ("throughput_tasks_per_second", "eta_seconds"):
            item = getattr(self, name)
            if item is not None and (type(item) not in (int, float) or item < 0):
                raise ValueError(f"StatusValue {name} must be non-negative or None")


@dataclass(frozen=True, slots=True)
class WaitValue:
    status: StatusValue
    terminal: bool
    timed_out: bool
    elapsed_seconds: float

    def __post_init__(self) -> None:
        if type(self.status) is not StatusValue:
            raise TypeError("WaitValue status must be a StatusValue")
        if type(self.terminal) is not bool or type(self.timed_out) is not bool:
            raise TypeError("WaitValue flags must be booleans")
        if self.terminal and self.timed_out:
            raise ValueError("A terminal wait cannot be timed out")
        if type(self.elapsed_seconds) is not float or self.elapsed_seconds < 0:
            raise ValueError("WaitValue elapsed_seconds must be non-negative")


@dataclass(frozen=True, slots=True)
class AwaitRunsValue:
    statuses: tuple[StatusValue, ...]
    until: str
    condition_met: bool
    timed_out: bool
    elapsed_seconds: float
    fail_on_run_failure: bool = False

    def __post_init__(self) -> None:
        if not self.statuses:
            raise ValueError("AwaitRunsValue statuses must not be empty")
        run_ids = tuple(str(status.run_id) for status in self.statuses)
        if len(set(run_ids)) != len(run_ids):
            raise ValueError("AwaitRunsValue statuses must have unique Run IDs")
        if self.until not in {"all", "any"}:
            raise ValueError("AwaitRunsValue until must be 'all' or 'any'")
        if type(self.condition_met) is not bool or type(self.timed_out) is not bool:
            raise TypeError("AwaitRunsValue flags must be booleans")
        if self.condition_met and self.timed_out:
            raise ValueError("AwaitRunsValue cannot be complete and timed out")
        if type(self.elapsed_seconds) is not float or self.elapsed_seconds < 0:
            raise ValueError("AwaitRunsValue elapsed_seconds must be non-negative")
        if type(self.fail_on_run_failure) is not bool:
            raise TypeError("AwaitRunsValue fail_on_run_failure must be a boolean")

    @property
    def exit_code(self) -> int:
        failed_states = {ExecutionState.FAILED, ExecutionState.CANCELLED}
        if self.fail_on_run_failure and any(
            status.state in failed_states for status in self.statuses
        ):
            return 2
        return 0


@dataclass(frozen=True, slots=True)
class CancelValue:
    status: StatusValue

    def __post_init__(self) -> None:
        if type(self.status) is not StatusValue:
            raise TypeError("CancelValue status must be a StatusValue")


@dataclass(frozen=True, slots=True)
class ListRunsValue:
    runs: tuple[StatusValue, ...]
    offset: int = 0
    limit: int = 100
    total: int | None = None
    include_tasks: bool = False
    format_version: int = 2

    def __post_init__(self) -> None:
        runs = tuple(self.runs)
        if any(type(item) is not StatusValue for item in runs):
            raise TypeError("ListRunsValue runs must contain StatusValues")
        if type(self.offset) is not int or self.offset < 0:
            raise ValueError("Run list offset must be non-negative")
        if type(self.limit) is not int or not 1 <= self.limit <= 1000:
            raise ValueError("Run list limit must be between 1 and 1000")
        total = len(runs) if self.total is None else self.total
        if type(total) is not int or total < len(runs):
            raise ValueError("Run list total must cover the returned page")
        if type(self.include_tasks) is not bool:
            raise TypeError("Run list include_tasks must be bool")
        if self.format_version not in RUN_LIST_SCHEMA.supported:
            raise ValueError("Run list format_version is unsupported")
        object.__setattr__(self, "runs", runs)
        object.__setattr__(self, "total", total)

    @property
    def next_offset(self) -> int | None:
        next_offset = self.offset + len(self.runs)
        assert self.total is not None
        return next_offset if next_offset < self.total else None


@dataclass(frozen=True, slots=True)
class InspectValue:
    record: RunRecord
    retention: PurgeReceipt | None = None


@dataclass(frozen=True, slots=True)
class PurgeValue:
    run_id: RunId
    scope: PurgeScope
    dry_run: bool
    result: PurgeResult
    receipt: PurgeReceipt | None
    receipt_path: PurePath
    format_version: int = 1


@dataclass(frozen=True, slots=True)
class TasksValue:
    run_id: RunId
    total: int
    offset: int
    limit: int
    tasks: tuple[TaskState, ...]
    format_version: int = 4

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise TypeError("TasksValue run_id must be a RunId")
        if type(self.total) is not int or self.total < 1:
            raise ValueError("TasksValue total must be positive")
        if type(self.offset) is not int or self.offset < 0:
            raise ValueError("TasksValue offset must be non-negative")
        if type(self.limit) is not int or self.limit < 1:
            raise ValueError("TasksValue limit must be positive")
        if any(type(item) is not TaskState for item in self.tasks):
            raise TypeError("TasksValue tasks must contain TaskState values")
        if (
            type(self.format_version) is not int
            or self.format_version not in TASKS_SCHEMA.supported
        ):
            raise ValueError("TasksValue format_version must be supported")


@dataclass(frozen=True, slots=True)
class LogsValue:
    run_id: RunId
    task_id: TaskId
    stdout: str
    stderr: str
    stdout_path: PurePath
    stderr_path: PurePath
    format_version: int = 1


@dataclass(frozen=True, slots=True)
class PreparationLogsValue:
    run_id: RunId
    scheduler_id: str | None
    stdout: str
    stderr: str
    stdout_path: PurePath
    stderr_path: PurePath
    format_version: int = 2


@dataclass(frozen=True, slots=True)
class FetchValue:
    run_id: RunId
    destination: PurePath
    retrieval_state: RetrievalState
    artifacts: tuple[Artifact, ...]
    task_ids: tuple[TaskId, ...] = ()
    format_version: int = 1


type LaunchOutputValue = str | int | None


@dataclass(frozen=True, slots=True)
class LaunchResolutionValue:
    profile: str | None
    values: Mapping[str, LaunchOutputValue]
    sources: Mapping[str, str]

    def __post_init__(self) -> None:
        if self.profile is not None and (
            type(self.profile) is not str or not self.profile
        ):
            raise ValueError("Launch profile must be nonblank or None")
        if not isinstance(self.values, Mapping) or not isinstance(
            self.sources, Mapping
        ):
            raise TypeError("Launch resolution values and sources must be mappings")
        values = dict(self.values)
        sources = dict(self.sources)
        if any(
            type(name) is not str or type(value) not in (str, int, type(None))
            for name, value in values.items()
        ):
            raise TypeError("Launch resolution values are invalid")
        if set(values) != set(sources) or any(
            type(name) is not str or type(source) is not str or not source
            for name, source in sources.items()
        ):
            raise ValueError("Launch resolution sources must match values")
        object.__setattr__(self, "values", MappingProxyType(values))
        object.__setattr__(self, "sources", MappingProxyType(sources))


@dataclass(frozen=True, slots=True)
class PlanValue:
    plan: ExecutionPlan
    launch: LaunchResolutionValue | None = None
    source_snapshot: SourceSnapshotPreview | None = None
    format_version: int = PLAN_SCHEMA.current

    def __post_init__(self) -> None:
        if type(self.plan) is not ExecutionPlan:
            raise TypeError("PlanValue plan must be an ExecutionPlan")
        if self.launch is not None and type(self.launch) is not LaunchResolutionValue:
            raise TypeError("PlanValue launch must be LaunchResolutionValue or None")
        if (
            self.source_snapshot is not None
            and type(self.source_snapshot) is not SourceSnapshotPreview
        ):
            raise TypeError(
                "PlanValue source_snapshot must be SourceSnapshotPreview or None"
            )
        if self.format_version not in PLAN_SCHEMA.supported:
            raise ValueError("PlanValue format_version must be supported")


@dataclass(frozen=True, slots=True)
class ResolvedRunInputs:
    config: Path
    seeds: tuple[int, ...] | SeedRange
    target: str
    targets_file: Path
    source_root: Path
    destination: Path
    data_dir: Path
    resolution: ResolvedLaunch
    preparation: PreparationConfig | None = None
    mutable_source: bool = False
    prepare_location: str = "auto"
    rebuild: bool = False
    rebuild_image: bool = False
    offline: bool = False
    preparation_storage: PreparationStorageConfig = PreparationStorageConfig()
    sweep: SweepExpansion | None = None
    workers: int | None = None
    task_slots_per_worker: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "config",
            "targets_file",
            "source_root",
            "destination",
            "data_dir",
        ):
            if not isinstance(getattr(self, name), Path):
                raise TypeError(f"ResolvedRunInputs {name} must be a Path")
        if type(self.seeds) is SeedRange:
            pass
        elif (
            not isinstance(self.seeds, tuple)
            or not self.seeds
            or any(type(seed) is not int for seed in self.seeds)
            or len(set(self.seeds)) != len(self.seeds)
        ):
            raise TypeError(
                "ResolvedRunInputs seeds must be a SeedRange or unique integers"
            )
        if type(self.target) is not str or not self.target:
            raise ValueError("ResolvedRunInputs target must be nonblank")
        if type(self.resolution) is not ResolvedLaunch:
            raise TypeError("ResolvedRunInputs resolution must be ResolvedLaunch")
        if type(self.preparation_storage) is not PreparationStorageConfig:
            raise TypeError("ResolvedRunInputs preparation storage is invalid")
        if self.sweep is not None and type(self.sweep) is not SweepExpansion:
            raise TypeError("ResolvedRunInputs sweep is invalid")
        for name in ("workers", "task_slots_per_worker"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"ResolvedRunInputs {name} must be positive or None")
        _validate_preparation_inputs(
            self.preparation,
            self.mutable_source,
            self.prepare_location,
            self.rebuild,
            self.rebuild_image,
            self.offline,
        )

    @property
    def preparation_plan(self) -> PreparationPlan | None:
        return _preparation_plan(
            self.preparation,
            source_root=(
                self.source_root
                if self.mutable_source
                or (
                    self.preparation is not None
                    and type(self.preparation.source) is PreparationSourceWorkingTree
                )
                else None
            ),
            location=self.prepare_location,
            rebuild=self.rebuild,
            rebuild_image=self.rebuild_image,
            offline=self.offline,
        )

    @property
    def launch(self) -> LaunchResolutionValue:
        """Return public metadata for values consumed by synchronous run."""
        base = _launch_resolution_value(
            self.resolution,
            (
                "config",
                "target",
                "targets_file",
                "source_root",
                "destination",
                "data_dir",
                "workers",
                "task_slots_per_worker",
                "fetch_mode",
            ),
        )
        if self.seed is not None:
            seed_source = (
                "config"
                if self.sweep is not None and self.sweep.seeds == (self.seed,)
                else self.resolution.sources.get("seed", "generated")
            )
            return LaunchResolutionValue(
                base.profile,
                {**base.values, "seed": self.seed},
                {
                    **base.sources,
                    "seed": seed_source,
                },
            )
        seeds_source = (
            "config"
            if self.sweep is not None and self.sweep.seeds == self.seeds
            else "cli"
        )
        first = self.seeds.start if isinstance(self.seeds, SeedRange) else self.seeds[0]
        last = self.seeds.stop if isinstance(self.seeds, SeedRange) else self.seeds[-1]
        return LaunchResolutionValue(
            base.profile,
            {**base.values, "seeds": f"{first}:{last}"},
            {**base.sources, "seeds": seeds_source},
        )

    @property
    def seed(self) -> int | None:
        """Return the single seed when this is not a replicated launch."""
        if isinstance(self.seeds, SeedRange):
            return self.seeds.start if self.seeds.count == 1 else None
        return self.seeds[0] if len(self.seeds) == 1 else None

    @property
    def seed_count(self) -> int:
        """Return the number of resolved seeds without expanding a range."""
        return (
            self.seeds.count if isinstance(self.seeds, SeedRange) else len(self.seeds)
        )


@dataclass(frozen=True, slots=True)
class ResolvedPlanInputs:
    config: Path
    target: str
    targets_file: Path
    seed: int | None
    seeds: str | None
    resolution: ResolvedLaunch
    preparation: PreparationConfig | None = None
    source_root: Path | None = None
    prepare_location: str = "auto"
    rebuild: bool = False
    rebuild_image: bool = False
    offline: bool = False
    sweep: SweepExpansion | None = None
    workers: int | None = None
    task_slots_per_worker: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.config, Path) or not isinstance(self.targets_file, Path):
            raise TypeError("ResolvedPlanInputs paths must be Paths")
        if type(self.target) is not str or not self.target:
            raise ValueError("ResolvedPlanInputs target must be nonblank")
        if self.seed is not None and type(self.seed) is not int:
            raise TypeError("ResolvedPlanInputs seed must be an integer or None")
        if self.seeds is not None and type(self.seeds) is not str:
            raise TypeError("ResolvedPlanInputs seeds must be a string or None")
        if (self.seed is None) == (self.seeds is None):
            raise ValueError("ResolvedPlanInputs requires exactly one seed form")
        if type(self.resolution) is not ResolvedLaunch:
            raise TypeError("ResolvedPlanInputs resolution must be ResolvedLaunch")
        if self.source_root is not None and not isinstance(self.source_root, Path):
            raise TypeError("ResolvedPlanInputs source_root must be a Path or None")
        if self.sweep is not None and type(self.sweep) is not SweepExpansion:
            raise TypeError("ResolvedPlanInputs sweep is invalid")
        for name in ("workers", "task_slots_per_worker"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"ResolvedPlanInputs {name} must be positive or None")
        _validate_preparation_inputs(
            self.preparation,
            self.source_root is not None,
            self.prepare_location,
            self.rebuild,
            self.rebuild_image,
            self.offline,
        )

    @property
    def preparation_plan(self) -> PreparationPlan | None:
        return _preparation_plan(
            self.preparation,
            source_root=self.source_root,
            location=self.prepare_location,
            rebuild=self.rebuild,
            rebuild_image=self.rebuild_image,
            offline=self.offline,
        )

    @property
    def launch(self) -> LaunchResolutionValue:
        """Return public metadata for values consumed by non-submitting plan."""
        if self.seeds is not None:
            base = _launch_resolution_value(
                self.resolution,
                (
                    "config",
                    "target",
                    "targets_file",
                    "workers",
                    "task_slots_per_worker",
                ),
            )
            seeds_source = (
                "config"
                if self.sweep is not None
                and self.sweep.seeds is not None
                and self.seeds == f"{self.sweep.seeds[0]}:{self.sweep.seeds[-1]}"
                else "cli"
            )
            return LaunchResolutionValue(
                base.profile,
                {**base.values, "seeds": self.seeds},
                {**base.sources, "seeds": seeds_source},
            )
        return _launch_resolution_value(
            self.resolution,
            (
                "config",
                "seed",
                "target",
                "targets_file",
                "workers",
                "task_slots_per_worker",
                "fetch_mode",
            ),
        )


def _validate_preparation_inputs(
    preparation: PreparationConfig | None,
    mutable_source: bool,
    location: str,
    rebuild: bool,
    rebuild_image: bool,
    offline: bool,
) -> None:
    if preparation is not None and type(preparation) is not PreparationConfig:
        raise TypeError("preparation must be PreparationConfig or None")
    if type(mutable_source) is not bool:
        raise TypeError("mutable_source must be a boolean")
    if location not in PREPARE_LOCATIONS:
        raise ValueError("prepare_location must be auto, local, or target")
    if any(type(value) is not bool for value in (rebuild, rebuild_image, offline)):
        raise TypeError("preparation flags must be booleans")


def _preparation_plan(
    recipe: PreparationConfig | None,
    *,
    source_root: Path | None,
    location: str,
    rebuild: bool,
    rebuild_image: bool,
    offline: bool,
) -> PreparationPlan | None:
    if recipe is None:
        return None
    recipe_working_tree = type(recipe.source) is PreparationSourceWorkingTree
    source_mode = (
        "working_tree" if source_root is not None or recipe_working_tree else "git"
    )
    actions = [
        "snapshot_working_tree"
        if source_mode == "working_tree"
        else "reuse_source_snapshot",
        "use_verified_image_candidate",
        "reuse_image_cache",
    ]
    if source_mode == "git" and not offline:
        actions.append("fetch_git_commit")
    if type(recipe.image) is PreparationImageDefinition:
        if not rebuild_image:
            actions.append("reuse_definition_image_cache")
        actions.append("build_definition_image")
    elif not offline:
        actions.extend(("transfer_verified_image", "pull_image"))
    if recipe.build is not None:
        if not rebuild:
            actions.append("reuse_build_cache")
        actions.append("build_application")
    return PreparationPlan(
        recipe=recipe,
        source_mode=source_mode,
        source_root=source_root,
        requested_location=location,
        rebuild=rebuild,
        rebuild_image=rebuild_image,
        offline=offline,
        possible_actions=tuple(actions),
    )


def _validate_preparation_compatibility(
    experiment: ExperimentSpec, preparation: PreparationConfig
) -> None:
    if experiment.container is None:
        raise PlanningError(
            code="PREPARATION_CONTAINER_REQUIRED",
            message="Project preparation requires an experiment container",
        )
    image = experiment.container.image
    if image.is_absolute() or image != preparation.image.name:
        raise PlanningError(
            code="PREPARATION_IMAGE_MISMATCH",
            message="Experiment container.image must equal the preparation image name",
            details={
                "actual": str(image),
                "expected": str(preparation.image.name),
            },
        )


def validate_operation(source: Path) -> OperationResult[ValidationValue]:
    try:
        experiment = load_experiment(source)
        project = discover_project_launch(source)
        if project is not None and project.preparation is not None:
            _validate_preparation_compatibility(experiment, project.preparation)
        return OperationResult.success(
            "validate", ValidationValue(source, experiment, project)
        )
    except ConfigError as error:
        return OperationResult.failure("validate", _config_error(error))
    except PlanningError as error:
        return OperationResult.failure(
            "validate", OperationError(error.code, error.message, error.details)
        )


def plan_operation(
    experiment_source: Path,
    config_source: Path,
    targets_source: Path,
    target_name: str,
    *,
    seed: object = None,
    seeds: object = None,
    launch: LaunchResolutionValue | None = None,
    preparation: PreparationPlan | None = None,
    sweep: SweepExpansion | None = None,
    execution_strategy: str = "auto",
    retrieval_policy: str = "manifest",
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
    source_root: Path | None = None,
) -> OperationResult[PlanValue]:
    try:
        experiment = load_experiment(experiment_source)
        expansion = sweep or load_sweep_config(config_source)
        config = expansion.configs[0].config
        targets_config = load_targets_config(targets_source)
        targets = targets_config.targets
        if target_name not in targets:
            return OperationResult.failure(
                "plan",
                OperationError(
                    "TARGET_NOT_FOUND",
                    f"Target '{target_name}' is not defined",
                    {"source": str(targets_source), "target": target_name},
                ),
            )
        target = targets[target_name]
        if preparation is not None:
            _validate_preparation_compatibility(experiment, preparation.recipe)
            target_storage = targets_config.preparation.get(
                target_name, PreparationStorageConfig()
            )
            selected_location = (
                "local"
                if target.transport.kind == "local" and target.scheduler.kind == "local"
                else select_remote_preparation_location(
                    preparation, target_storage.definition_build
                )
            )
            preparation = replace(
                preparation,
                selected_location=selected_location,
            )
        unsupported = _unsupported_execution_target(target, experiment)
        if unsupported is not None:
            return OperationResult.failure("plan", unsupported)
        policy, policy_version = _effective_execution_policy(
            targets_config.execution.get(target_name),
            experiment,
            target,
            targets_source,
            targets_config.version,
        )
        if policy is not None:
            plan = _create_effective_scaling_plan(
                experiment,
                expansion.configs,
                target,
                compact_seed_range(seed=seed, seeds=seeds),
                policy,
                policy_version,
                strategy=execution_strategy,
                retrieval_policy=retrieval_policy,
                preparation=preparation,
                workers=workers,
                task_slots_per_worker=task_slots_per_worker,
            )
        else:
            seed_values = expand_seeds(seed=seed, seeds=seeds)
            plan = (
                create_sweep_plan(
                    experiment,
                    expansion.configs,
                    target,
                    seeds=seed_values,
                    preparation=preparation,
                )
                if expansion.is_sweep
                else create_plan(
                    experiment,
                    config,
                    target,
                    seeds=seed_values,
                    preparation=preparation,
                )
            )
        source_snapshot = None
        if preparation is None or source_root is not None:
            effective_source_root = source_root or experiment_source.parent
            workspace_root = (
                Path(str(target.workspace)) if target.staging.kind == "local" else None
            )
            source_snapshot = preview_source_snapshot(
                effective_source_root,
                experiment.sync_excludes,
                workspace_root=workspace_root,
            )
        return OperationResult.success(
            "plan",
            PlanValue(plan, launch, source_snapshot),
        )
    except ConfigError as error:
        return OperationResult.failure("plan", _config_error(error))
    except PlanningError as error:
        return OperationResult.failure(
            "plan", OperationError(error.code, error.message, error.details)
        )
    except (SourceSnapshotPreviewError, SyncExclusionError) as error:
        return OperationResult.failure(
            "plan",
            OperationError(
                "SOURCE_SNAPSHOT_PREVIEW_FAILED",
                str(error),
                {"source": str(source_root or experiment_source.parent)},
            ),
        )


def _create_effective_scaling_plan(
    experiment: ExperimentSpec,
    configs: Sequence[ExpandedConfig],
    target: Target,
    seeds: SeedRange,
    policy: ExecutionPolicy,
    targets_version: int,
    *,
    strategy: str = "auto",
    retrieval_policy: str = "manifest",
    preparation: PreparationPlan | None = None,
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
) -> ExecutionPlan:
    return create_scalable_plan(
        experiment,
        configs,
        target,
        seeds=seeds,
        policy=policy,
        strategy=strategy,
        retrieval_policy=retrieval_policy,
        preparation=preparation,
        version=(
            8
            if targets_version >= 8
            else 7
            if targets_version >= 7
            else 6
            if targets_version >= 6
            else 5
            if targets_version >= 4
            else 4
        ),
        workers=workers,
        task_slots_per_worker=task_slots_per_worker,
    )


def _effective_execution_policy(
    policy: ExecutionPolicy | None,
    experiment: ExperimentSpec,
    target: Target,
    targets_source: Path,
    targets_version: int,
) -> tuple[ExecutionPolicy | None, int]:
    """Size the framework-owned local target to the client's usable CPUs."""

    if target.transport.kind != "local" or target.scheduler.kind != "local":
        return policy, targets_version
    if (
        policy is not None
        and targets_source.resolve() != builtin_targets_source().resolve()
    ):
        return policy, targets_version
    if (
        policy is None
        and targets_version < 6
        and targets_source.resolve() != builtin_targets_source().resolve()
    ):
        return None, targets_version
    try:
        available_cpus = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        available_cpus = os.cpu_count() or 1
    logical_task_cpus = experiment.resources.tasks * experiment.resources.cpus_per_task
    task_slots = max(1, available_cpus // logical_task_cpus)
    if policy is None:
        policy = ExecutionPolicy(
            hard_task_limit=100_000,
            confirmation_threshold=1_000,
            max_active_tasks=task_slots,
            max_concurrent_jobs=task_slots,
            max_array_size=1_000,
            output_shard_tasks=1_000,
            automatic_retrieval_threshold=1_000,
            worker_pool=WorkerPoolPolicy(
                activation_threshold=2,
                default_workers=1,
                max_workers=task_slots,
                task_slots_per_worker=task_slots,
                max_task_slots_per_worker=task_slots,
                tasks_per_lease=10,
                infrastructure_retry_limit=0,
                requeue_limit=0,
            ),
        )
        return policy, max(targets_version, 6)
    task_slots = min(task_slots, policy.hard_task_limit)
    worker_pool = replace(
        policy.worker_pool,
        default_workers=1,
        max_workers=task_slots,
        task_slots_per_worker=task_slots,
        max_task_slots_per_worker=task_slots,
    )
    return (
        replace(
            policy,
            max_active_tasks=task_slots,
            max_concurrent_jobs=task_slots,
            worker_pool=worker_pool,
        ),
        max(targets_version, 6),
    )


def _seed_range_from_values(values: Sequence[int]) -> SeedRange:
    normalized = tuple(values)
    if not normalized:
        raise PlanningError(code="SEED_REQUIRED", message="Provide one seed or range")
    if any(type(value) is not int for value in normalized):
        raise PlanningError(code="INVALID_SEED", message="seed must be an integer")
    if len(normalized) > 1 and any(
        right != left + 1
        for left, right in zip(normalized, normalized[1:], strict=False)
    ):
        raise PlanningError(
            code="INVALID_SEED_RANGE",
            message="scalable execution requires a contiguous seed range",
        )
    return SeedRange(normalized[0], normalized[-1])


def targets_operation(source: Path) -> OperationResult[TargetsValue]:
    try:
        return OperationResult.success(
            "targets", TargetsValue(source, load_targets(source))
        )
    except ConfigError as error:
        return OperationResult.failure("targets", _config_error(error))


def resolve_plan_inputs_operation(
    experiment_source: Path,
    *,
    config: Path | None = None,
    seed: int | None = None,
    seeds: str | None = None,
    target: str | None = None,
    targets_file: Path | None = None,
    project_file: Path | None = None,
    profile: str | None = None,
    user_config_source: Path | None = None,
    random_seed: bool = False,
    seed_factory: Callable[[], int] | None = None,
    source_root: Path | None = None,
    prepare_location: str = "auto",
    rebuild: bool = False,
    rebuild_image: bool = False,
    offline: bool = False,
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
    fetch_mode: str | None = None,
) -> OperationResult[ResolvedPlanInputs]:
    """Resolve plan inputs without submitting, staging, or mutating a workspace."""
    if type(random_seed) is not bool:
        raise TypeError("random_seed must be a boolean")
    requested_seed_forms = sum(
        (seed is not None, seeds is not None, random_seed), start=0
    )
    if requested_seed_forms > 1:
        return OperationResult.failure(
            "plan",
            OperationError(
                "SEED_CONFLICT",
                "--seed, --seeds, and --random-seed are mutually exclusive",
            ),
        )
    try:
        discovered_project = discover_project_launch(
            experiment_source, project_file=project_file
        )
        fully_explicit = (
            all(value is not None for value in (config, target, targets_file))
            and requested_seed_forms == 1
        )
        project = (
            discovered_project
            if (
                not fully_explicit
                or project_file is not None
                or profile is not None
                or (discovered_project is not None and discovered_project.version >= 2)
            )
            else None
        )
        use_defaults = not fully_explicit or project is not None or profile is not None
        user = discover_user_launch(user_config_source) if use_defaults else None
        resolved = resolve_launch(
            cli=LaunchValues(
                config=config,
                seed=seed,
                target=target,
                source_root=source_root,
                targets_file=targets_file,
                workers=workers,
                task_slots_per_worker=task_slots_per_worker,
                fetch_mode=fetch_mode,
            ),
            project=project,
            user=user,
            builtins=LaunchValues(
                config=experiment_source.expanduser().resolve().parent / "config.yaml",
                target="local",
                source_root=experiment_source.expanduser().resolve().parent,
                targets_file=Path("~/.config/rundra/targets.yaml").expanduser(),
            ),
            profile=profile,
        )
        resolved = _select_builtin_local_target(resolved)
    except ConfigError as error:
        return OperationResult.failure("plan", _config_error(error))
    except LaunchResolutionError as error:
        return OperationResult.failure(
            "plan", OperationError(error.code, error.message)
        )
    missing = tuple(
        name for name in ("config", "target") if getattr(resolved.values, name) is None
    )
    if missing:
        return OperationResult.failure(
            "plan",
            OperationError(
                "LAUNCH_VALUE_REQUIRED",
                f"Launch values could not resolve: {', '.join(missing)}",
                {"fields": missing},
            ),
        )
    values = resolved.values
    assert values.config is not None
    sweep = None
    if values.config.is_file():
        try:
            sweep = load_sweep_config(values.config)
        except ConfigError as error:
            return OperationResult.failure("plan", _config_error(error))
    resolved_seed = values.seed
    if requested_seed_forms == 0 and sweep is not None and sweep.seeds is not None:
        if len(sweep.seeds) == 1:
            resolved_seed = sweep.seeds[0]
            values = replace(values, seed=resolved_seed)
            resolved = ResolvedLaunch(
                values,
                {**resolved.sources, "seed": "config"},
                resolved.profile,
            )
        else:
            seeds = f"{sweep.seeds[0]}:{sweep.seeds[-1]}"
            resolved_seed = None
    if seeds is not None:
        resolved_seed = None
        resolved = ResolvedLaunch(
            replace(values, seed=None),
            {
                name: source
                for name, source in resolved.sources.items()
                if name != "seed"
            },
            resolved.profile,
        )
    elif resolved_seed is None or random_seed:
        generator = seed_factory or (lambda: secrets.randbits(63))
        resolved_seed = generator()
        if (
            type(resolved_seed) is not int
            or resolved_seed < 0
            or resolved_seed >= 2**63
        ):
            return OperationResult.failure(
                "plan",
                OperationError(
                    "SEED_GENERATION_FAILED",
                    "Seed generator did not return a non-negative 63-bit integer",
                ),
            )
        values = replace(values, seed=resolved_seed)
        resolved = ResolvedLaunch(
            values,
            {**resolved.sources, "seed": "generated"},
            resolved.profile,
        )
    assert values.config is not None
    assert values.target is not None
    assert values.targets_file is not None
    return OperationResult.success(
        "plan",
        ResolvedPlanInputs(
            config=values.config,
            target=values.target,
            targets_file=values.targets_file,
            seed=resolved_seed,
            seeds=seeds,
            resolution=resolved,
            preparation=project.preparation if project is not None else None,
            source_root=(
                source_root
                if source_root is not None
                else values.source_root
                if project is not None
                and project.preparation is not None
                and type(project.preparation.source) is PreparationSourceWorkingTree
                else None
            ),
            prepare_location=prepare_location,
            rebuild=rebuild,
            rebuild_image=rebuild_image,
            offline=offline,
            sweep=sweep,
            workers=values.workers,
            task_slots_per_worker=values.task_slots_per_worker,
        ),
    )


def resolve_run_inputs_operation(
    experiment_source: Path,
    *,
    config: Path | None = None,
    seed: int | None = None,
    seeds: str | None = None,
    target: str | None = None,
    targets_file: Path | None = None,
    source_root: Path | None = None,
    destination: Path | None = None,
    data_dir: Path | None = None,
    project_file: Path | None = None,
    profile: str | None = None,
    user_config_source: Path | None = None,
    random_seed: bool = False,
    seed_factory: Callable[[], int] | None = None,
    operation: str = "run",
    prepare_location: str = "auto",
    rebuild: bool = False,
    rebuild_image: bool = False,
    offline: bool = False,
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
    fetch_mode: str | None = None,
) -> OperationResult[ResolvedRunInputs]:
    """Resolve run or submit inputs without planning or executing work."""
    if type(random_seed) is not bool:
        raise TypeError("random_seed must be a boolean")
    try:
        explicit_seeds: tuple[int, ...] | SeedRange | None = (
            compact_seed_range(seeds=seeds) if seeds is not None else None
        )
        cli_values = LaunchValues(
            config=config,
            seed=seed,
            target=target,
            source_root=source_root,
            destination=destination,
            targets_file=targets_file,
            data_dir=data_dir,
            workers=workers,
            task_slots_per_worker=task_slots_per_worker,
            fetch_mode=fetch_mode,
        )
        discovered_project = discover_project_launch(
            experiment_source, project_file=project_file
        )
        fully_explicit = all(
            getattr(cli_values, field) is not None
            for field in (
                "config",
                "target",
                "source_root",
                "destination",
                "targets_file",
                "data_dir",
            )
        ) and (cli_values.seed is not None or explicit_seeds is not None or random_seed)
        project = (
            discovered_project
            if (
                not fully_explicit
                or project_file is not None
                or profile is not None
                or (discovered_project is not None and discovered_project.version >= 2)
            )
            else None
        )
        use_defaults = not fully_explicit or project is not None or profile is not None
        user = discover_user_launch(user_config_source) if use_defaults else None
        builtins = LaunchValues(
            config=experiment_source.expanduser().resolve().parent / "config.yaml",
            target="local",
            source_root=(
                project.project_root
                if project is not None
                else experiment_source.expanduser().resolve().parent
            ),
            targets_file=Path("~/.config/rundra/targets.yaml").expanduser(),
            data_dir=Path("~/.local/share/rundra/runs").expanduser(),
        )
        resolved = resolve_launch(
            cli=cli_values,
            project=project,
            user=user,
            builtins=builtins,
            profile=profile,
        )
        resolved = _select_builtin_local_target(resolved)
        if resolved.values.destination is None and resolved.values.config is not None:
            destination_root = (
                project.project_root
                if project is not None
                else experiment_source.expanduser().resolve().parent
            )
            derived_destination = (
                destination_root / "retrieved" / resolved.values.config.stem
            ).resolve()
            resolved = ResolvedLaunch(
                replace(resolved.values, destination=derived_destination),
                {**resolved.sources, "destination": "built_in"},
                resolved.profile,
            )
    except ConfigError as error:
        return OperationResult.failure(operation, _config_error(error))
    except PlanningError as error:
        return OperationResult.failure(
            operation, OperationError(error.code, error.message, error.details)
        )
    except LaunchResolutionError as error:
        return OperationResult.failure(
            operation, OperationError(error.code, error.message)
        )
    if sum((seed is not None, seeds is not None, random_seed)) > 1:
        return OperationResult.failure(
            operation,
            OperationError(
                "SEED_CONFLICT",
                "--seed, --seeds, and --random-seed are mutually exclusive",
            ),
        )
    missing = tuple(
        name for name in ("config", "target") if getattr(resolved.values, name) is None
    )
    if missing:
        return OperationResult.failure(
            operation,
            OperationError(
                "LAUNCH_VALUE_REQUIRED",
                f"Launch values could not resolve: {', '.join(missing)}",
                {"fields": missing},
            ),
        )
    values = resolved.values
    assert values.config is not None
    sweep = None
    if values.config.is_file():
        try:
            sweep = load_sweep_config(values.config)
        except ConfigError as error:
            return OperationResult.failure(operation, _config_error(error))
    if (
        explicit_seeds is None
        and seed is None
        and not random_seed
        and sweep is not None
        and sweep.seeds is not None
    ):
        explicit_seeds = sweep.seeds
        values = replace(values, seed=None)
        resolved = ResolvedLaunch(
            values,
            {
                name: source
                for name, source in resolved.sources.items()
                if name != "seed"
            },
            resolved.profile,
        )
    if explicit_seeds is None and (values.seed is None or random_seed):
        generator = seed_factory or (lambda: secrets.randbits(63))
        generated_seed = generator()
        if (
            type(generated_seed) is not int
            or generated_seed < 0
            or generated_seed >= 2**63
        ):
            return OperationResult.failure(
                operation,
                OperationError(
                    "SEED_GENERATION_FAILED",
                    "Seed generator did not return a non-negative 63-bit integer",
                ),
            )
        values = replace(values, seed=generated_seed)
        resolved = ResolvedLaunch(
            values,
            {**resolved.sources, "seed": "generated"},
            resolved.profile,
        )
    assert values.config is not None
    assert values.seed is not None or explicit_seeds is not None
    assert values.target is not None
    assert values.targets_file is not None
    assert values.source_root is not None
    assert values.destination is not None
    assert values.data_dir is not None
    resolved_seeds = explicit_seeds
    if resolved_seeds is None:
        assert values.seed is not None
        resolved_seeds = (values.seed,)
    return OperationResult.success(
        operation,
        ResolvedRunInputs(
            config=values.config,
            seeds=resolved_seeds,
            target=values.target,
            targets_file=values.targets_file,
            source_root=values.source_root,
            destination=values.destination,
            data_dir=values.data_dir,
            resolution=resolved,
            preparation=project.preparation if project is not None else None,
            mutable_source=source_root is not None,
            prepare_location=prepare_location,
            rebuild=rebuild,
            rebuild_image=rebuild_image,
            offline=offline,
            preparation_storage=(
                user.preparation if user is not None else PreparationStorageConfig()
            ),
            sweep=sweep,
            workers=values.workers,
            task_slots_per_worker=values.task_slots_per_worker,
        ),
    )


def _select_builtin_local_target(resolved: ResolvedLaunch) -> ResolvedLaunch:
    """Bind the built-in local target to its packaged definition as one layer."""
    if (
        resolved.values.target != "local"
        or resolved.sources.get("target") != "built_in"
    ):
        return resolved
    return ResolvedLaunch(
        replace(resolved.values, targets_file=builtin_targets_source()),
        {**resolved.sources, "targets_file": "built_in"},
        resolved.profile,
    )


def _remote_preparation_inputs(
    plan: PreparationPlan,
    experiment: ExperimentSpec,
    target: Target,
    source_root: Path,
    transport: Transport,
    local_storage: PreparationStorageConfig,
    target_storage: PreparationStorageConfig,
) -> tuple[Path, ExperimentSpec, PreparationRecord, RemotePreparationSpec]:
    image = plan.recipe.image
    source = prepare_source_snapshot(
        plan,
        source_root=source_root,
        excludes=experiment.sync_excludes,
        cache_root=(
            None
            if local_storage.cache_root is None
            else Path(str(local_storage.cache_root))
        ),
    )
    fingerprint = remote_platform_fingerprint(transport)
    spec = create_remote_preparation_spec(
        plan,
        source,
        target,
        fingerprint,
        cache_root=target_storage.cache_root,
        image_search_paths=target_storage.image_search_paths,
        definition_build=target_storage.definition_build,
        builder_version=(
            remote_builder_version(
                transport,
                str(target.container.options.get("executable", "apptainer")),
            )
            if type(image) is PreparationImageDefinition
            else None
        ),
    )
    record = remote_preparation_record(spec, target)
    assert experiment.container is not None
    effective_experiment = replace(
        experiment,
        container=replace(experiment.container, image=record.image_path),
    )
    return source.root, effective_experiment, record, spec


def _cached_remote_preparation_inputs(
    plan: PreparationPlan,
    experiment: ExperimentSpec,
    target: Target,
    transport: Transport,
    target_storage: PreparationStorageConfig,
) -> tuple[ExperimentSpec, PreparationRecord, PurePath] | None:
    if type(plan.recipe.image) is PreparationImageDefinition:
        return None
    cached = probe_remote_preparation_cache(
        plan,
        experiment,
        target,
        transport,
        cache_root=target_storage.cache_root,
    )
    if cached is None:
        return None
    assert experiment.container is not None
    effective_experiment = replace(
        experiment,
        container=replace(experiment.container, image=cached.experiment_image),
    )
    return effective_experiment, cached.record, cached.source_root


def _local_remote_preparation_inputs(
    plan: PreparationPlan,
    experiment: ExperimentSpec,
    target: Target,
    source_root: Path,
    project_root: Path,
    stager: Stager,
    local_storage: PreparationStorageConfig,
    target_storage: PreparationStorageConfig,
) -> tuple[Path, ExperimentSpec, PreparationRecord]:
    if not isinstance(stager, (RsyncStager, SharedStager)):
        raise PreparationError(
            "Local-to-target preparation requires rsync or shared staging"
        )
    prepared = prepare_local(
        plan,
        experiment,
        target,
        project_root=project_root,
        source_root=source_root,
        cache_root=(
            None
            if local_storage.cache_root is None
            else Path(str(local_storage.cache_root))
        ),
        image_search_paths=tuple(
            Path(str(path)) for path in local_storage.image_search_paths
        ),
        definition_build=target_storage.definition_build,
    )
    target_cache = (
        target.workspace / "cache"
        if target_storage.cache_root is None
        else target_storage.cache_root
    )
    assert prepared.record.image_sha256 is not None
    target_image = target_cache / "images" / f"{prepared.record.image_sha256}.sif"
    try:
        image_action = stager.publish_verified_file(
            Path(str(prepared.record.image_path)),
            target_image,
            prepared.record.image_sha256,
        )
    except RsyncUploadError as error:
        raise PreparationError(str(error)) from error
    assert prepared.experiment.container is not None
    effective_experiment = replace(
        prepared.experiment,
        container=replace(prepared.experiment.container, image=target_image),
    )
    record = replace(
        prepared.record,
        image_path=target_image,
        image_action=image_action,
        resolution_location="local",
        builder_location="local",
    )
    return prepared.source_root, effective_experiment, record


def run_operation(
    experiment_source: Path,
    config_source: Path,
    targets_source: Path,
    target_name: str,
    source_root: Path,
    destination: Path,
    store: RunStore,
    *,
    seed: object = None,
    seeds: object = None,
    launch: LaunchResolutionValue | None = None,
    preparation: PreparationPlan | None = None,
    preparation_storage: PreparationStorageConfig = _DEFAULT_PREPARATION_STORAGE,
    progress: ProgressObserver | None = None,
    sweep: SweepExpansion | None = None,
    confirm_tasks: int | None = None,
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
    task_store: SqliteTaskStore | None = None,
) -> OperationResult[RunValue]:
    try:
        _report_progress(
            progress,
            ProgressPhase.RESOLVE,
            0,
            f"experiment={experiment_source} config={config_source}",
        )
        experiment = load_experiment(experiment_source)
        expansion = sweep or load_sweep_config(config_source)
        config = expansion.configs[0].config
        task_total = _execution_seed_count(seed=seed, seeds=seeds) * len(
            expansion.configs
        )
        targets_config = load_targets_config(targets_source)
        targets = targets_config.targets
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
        policy, policy_version = _effective_execution_policy(
            targets_config.execution.get(target_name),
            experiment,
            target,
            targets_source,
            targets_config.version,
        )
        if policy is not None:
            validate_task_confirmation(
                task_total,
                policy,
                confirm_tasks,
            )
        _report_progress(
            progress,
            ProgressPhase.RESOLVE,
            1,
            _target_progress_message(target),
            task_total=task_total,
        )
        target_storage = targets_config.preparation.get(
            target_name, PreparationStorageConfig()
        )
        if preparation is not None:
            _report_progress(
                progress,
                ProgressPhase.PREPARE,
                1,
                f"location={preparation.requested_location} source={preparation.source_mode} rebuild={preparation.rebuild} offline={preparation.offline}",
                task_total=task_total,
            )
            _validate_preparation_compatibility(experiment, preparation.recipe)
        unsupported = _unsupported_execution_target(target, experiment)
        if unsupported is not None:
            return OperationResult.failure("run", unsupported)
        effective_experiment = experiment
        effective_source_root = source_root
        preparation_record = None
        remote_preparation = None
        remote_source_root = None
        transport, stager, runtime, scheduler = _execution_adapters(target)
        if preparation is not None:
            if target.transport.kind == "local" and target.scheduler.kind == "local":
                prepared = prepare_local(
                    preparation,
                    experiment,
                    target,
                    project_root=experiment_source.expanduser().resolve().parent,
                    source_root=source_root,
                    cache_root=(
                        Path(str(target_storage.cache_root))
                        if target_storage.cache_root is not None
                        else Path(str(preparation_storage.cache_root))
                        if preparation_storage.cache_root is not None
                        else None
                    ),
                    image_search_paths=tuple(
                        Path(str(path))
                        for path in (
                            *target_storage.image_search_paths,
                            *preparation_storage.image_search_paths,
                        )
                    ),
                    definition_build=target_storage.definition_build,
                )
                effective_experiment = prepared.experiment
                effective_source_root = prepared.source_root
                preparation_record = prepared.record
            else:
                if (
                    select_remote_preparation_location(
                        preparation, target_storage.definition_build
                    )
                    == "local"
                ):
                    (
                        effective_source_root,
                        effective_experiment,
                        preparation_record,
                    ) = _local_remote_preparation_inputs(
                        preparation,
                        experiment,
                        target,
                        source_root,
                        experiment_source.expanduser().resolve().parent,
                        stager,
                        preparation_storage,
                        target_storage,
                    )
                else:
                    cached = _cached_remote_preparation_inputs(
                        preparation,
                        experiment,
                        target,
                        transport,
                        target_storage,
                    )
                    if cached is not None:
                        (
                            effective_experiment,
                            preparation_record,
                            remote_source_root,
                        ) = cached
                    else:
                        (
                            effective_source_root,
                            effective_experiment,
                            preparation_record,
                            remote_preparation,
                        ) = _remote_preparation_inputs(
                            preparation,
                            experiment,
                            target,
                            source_root,
                            transport,
                            preparation_storage,
                            target_storage,
                        )
            assert preparation_record is not None
            _report_progress(
                progress,
                ProgressPhase.PREPARE,
                2,
                "source_action={} image_action={} builder={}".format(
                    preparation_record.source_action,
                    preparation_record.image_action,
                    preparation_record.builder_location or "not_requested",
                ),
                task_total=task_total,
            )
        else:
            _report_progress(
                progress,
                ProgressPhase.PREPARE,
                2,
                "not configured",
                task_total=task_total,
            )
        scaling_plan = (
            _create_effective_scaling_plan(
                effective_experiment,
                expansion.configs,
                target,
                _execution_seed_range(seed=seed, seeds=seeds),
                policy,
                policy_version,
                preparation=preparation,
                workers=workers,
                task_slots_per_worker=task_slots_per_worker,
            )
            if policy is not None
            else None
        )
        direct_compact = (
            task_store is not None
            and scaling_plan is not None
            and scaling_plan.strategy == "worker-pool"
            and scheduler_capabilities(target.scheduler.kind).compact_worker_pool
            and task_total >= _COMPACT_EXECUTION_THRESHOLD
        )
        if direct_compact:
            assert scaling_plan is not None
            plan = scaling_plan
        else:
            seed_values = _execution_seed_values(seed=seed, seeds=seeds)
            plan = (
                create_sweep_plan(
                    effective_experiment,
                    expansion.configs,
                    target,
                    seeds=seed_values,
                    preparation=preparation,
                )
                if expansion.is_sweep
                else create_plan(
                    effective_experiment,
                    config,
                    target,
                    seeds=seed_values,
                    preparation=preparation,
                )
            )
        service = OrchestrationService(
            store=store,
            stager=stager,
            runtime=runtime,
            scheduler=scheduler,
            transport=transport,
            framework_version=version("rundra"),
            provenance=GitProvenanceCapture(),
            progress=progress,
            task_store=task_store,
        )
        result = service.execute_one(
            RunExecutionRequest(
                plan=plan,
                experiment=effective_experiment,
                source_root=effective_source_root,
                fetch_destination=destination,
                fetch_mode=(
                    str(launch.values.get("fetch_mode", "auto"))
                    if launch is not None
                    else "auto"
                ),
                experiment_source=experiment_source,
                preparation=preparation_record,
                remote_preparation=remote_preparation,
                remote_source_root=remote_source_root,
                max_concurrent_jobs=(
                    policy.max_concurrent_jobs if policy is not None else None
                ),
                max_workers=(
                    scaling_plan.worker_count if scaling_plan is not None else None
                ),
                task_slots_per_worker=(
                    scaling_plan.task_slots_per_worker
                    if scaling_plan is not None
                    and scaling_plan.task_slots_per_worker is not None
                    else 1
                ),
                worker_resources=(
                    scaling_plan.worker_resources if scaling_plan is not None else None
                ),
                requested_workers=(
                    scaling_plan.requested_workers if scaling_plan is not None else None
                ),
                requested_task_slots_per_worker=(
                    scaling_plan.requested_task_slots_per_worker
                    if scaling_plan is not None
                    else None
                ),
                shard_outputs=(
                    target_name in targets_config.execution
                    and task_total
                    >= targets_config.execution[target_name].output_shard_tasks
                ),
                compact_plan=(scaling_plan if direct_compact else None),
                compact_configs=(expansion.configs if direct_compact else ()),
            )
        )
        record = result.record
        if remote_preparation is not None:
            preparation_result = read_remote_preparation_result(
                transport, result.workspace
            )
            if preparation_result is not None:
                assert record.preparation is not None
                updated = replace(
                    record,
                    preparation=replace(
                        record.preparation,
                        image_action=preparation_result.image_action,
                        build_action=preparation_result.build_action,
                        build_outputs=preparation_result.outputs,
                    ),
                )
                store.update(updated, expected=record)
                record = updated
        _report_progress(
            progress,
            ProgressPhase.COMPLETE,
            6,
            f"run={record.run.id} state={record.run.state.value} retrieval={record.run.retrieval_state.value}",
            record.run.id,
            task_total=task_total,
        )
        return OperationResult.success("run", RunValue(record, launch))
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
    except PreparationError as error:
        return OperationResult.failure(
            "run", OperationError("PREPARATION_FAILED", str(error))
        )


def submit_operation(
    experiment_source: Path,
    config_source: Path,
    targets_source: Path,
    target_name: str,
    source_root: Path,
    destination: Path,
    store: RunStore,
    *,
    seed: object = None,
    seeds: object = None,
    launch: LaunchResolutionValue | None = None,
    preparation: PreparationPlan | None = None,
    preparation_storage: PreparationStorageConfig = _DEFAULT_PREPARATION_STORAGE,
    progress: ProgressObserver | None = None,
    sweep: SweepExpansion | None = None,
    confirm_tasks: int | None = None,
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
    submission_receipts: SubmissionReceiptStore | None = None,
    task_store: SqliteTaskStore | None = None,
) -> OperationResult[RunValue]:
    try:
        _report_progress(
            progress,
            ProgressPhase.RESOLVE,
            0,
            f"experiment={experiment_source} config={config_source}",
        )
        experiment = load_experiment(experiment_source)
        expansion = sweep or load_sweep_config(config_source)
        config = expansion.configs[0].config
        task_total = _execution_seed_count(seed=seed, seeds=seeds) * len(
            expansion.configs
        )
        targets_config = load_targets_config(targets_source)
        targets = targets_config.targets
        if target_name not in targets:
            return OperationResult.failure(
                "submit",
                OperationError(
                    "TARGET_NOT_FOUND",
                    f"Target '{target_name}' is not defined",
                    {"source": str(targets_source), "target": target_name},
                ),
            )
        target = targets[target_name]
        policy, policy_version = _effective_execution_policy(
            targets_config.execution.get(target_name),
            experiment,
            target,
            targets_source,
            targets_config.version,
        )
        if policy is not None:
            validate_task_confirmation(
                task_total,
                policy,
                confirm_tasks,
            )
        _report_progress(
            progress,
            ProgressPhase.RESOLVE,
            1,
            _target_progress_message(target),
            task_total=task_total,
        )
        target_storage = targets_config.preparation.get(
            target_name, PreparationStorageConfig()
        )
        if preparation is not None:
            _report_progress(
                progress,
                ProgressPhase.PREPARE,
                1,
                f"location={preparation.requested_location} source={preparation.source_mode} rebuild={preparation.rebuild} offline={preparation.offline}",
                task_total=task_total,
            )
            _validate_preparation_compatibility(experiment, preparation.recipe)
        unsupported = _unsupported_execution_target(
            target, experiment, asynchronous=True
        )
        if unsupported is not None:
            return OperationResult.failure("submit", unsupported)
        effective_experiment = experiment
        effective_source_root = source_root
        preparation_record = None
        remote_preparation = None
        remote_source_root = None
        transport, stager, runtime, scheduler = _execution_adapters(target)
        if preparation is not None:
            if (
                target.transport.kind != "ssh"
                or target.scheduler.kind not in remote_scheduler_kinds()
                or not scheduler_capabilities(target.scheduler.kind).dependencies
            ):
                return OperationResult.failure(
                    "submit",
                    OperationError(
                        "ASYNC_UNAVAILABLE",
                        "Prepared submit requires an SSH scheduler target",
                        {"target": target.name},
                    ),
                )
            if (
                select_remote_preparation_location(
                    preparation, target_storage.definition_build
                )
                == "local"
            ):
                (
                    effective_source_root,
                    effective_experiment,
                    preparation_record,
                ) = _local_remote_preparation_inputs(
                    preparation,
                    experiment,
                    target,
                    source_root,
                    experiment_source.expanduser().resolve().parent,
                    stager,
                    preparation_storage,
                    target_storage,
                )
            else:
                cached = _cached_remote_preparation_inputs(
                    preparation,
                    experiment,
                    target,
                    transport,
                    target_storage,
                )
                if cached is not None:
                    (
                        effective_experiment,
                        preparation_record,
                        remote_source_root,
                    ) = cached
                else:
                    (
                        effective_source_root,
                        effective_experiment,
                        preparation_record,
                        remote_preparation,
                    ) = _remote_preparation_inputs(
                        preparation,
                        experiment,
                        target,
                        source_root,
                        transport,
                        preparation_storage,
                        target_storage,
                    )
            assert preparation_record is not None
            _report_progress(
                progress,
                ProgressPhase.PREPARE,
                2,
                "source_action={} image_action={} builder={}".format(
                    preparation_record.source_action,
                    preparation_record.image_action,
                    preparation_record.builder_location or "not_requested",
                ),
                task_total=task_total,
            )
        else:
            _report_progress(
                progress,
                ProgressPhase.PREPARE,
                2,
                "not configured",
                task_total=task_total,
            )
        scaling_plan = (
            _create_effective_scaling_plan(
                effective_experiment,
                expansion.configs,
                target,
                _execution_seed_range(seed=seed, seeds=seeds),
                policy,
                policy_version,
                preparation=preparation,
                workers=workers,
                task_slots_per_worker=task_slots_per_worker,
            )
            if policy is not None
            else None
        )
        direct_compact = (
            task_store is not None
            and scaling_plan is not None
            and scaling_plan.strategy == "worker-pool"
            and scheduler_capabilities(target.scheduler.kind).compact_worker_pool
            and task_total >= _COMPACT_EXECUTION_THRESHOLD
        )
        if direct_compact:
            assert scaling_plan is not None
            plan = scaling_plan
        else:
            seed_values = _execution_seed_values(seed=seed, seeds=seeds)
            plan = (
                create_sweep_plan(
                    effective_experiment,
                    expansion.configs,
                    target,
                    seeds=seed_values,
                    preparation=preparation,
                )
                if expansion.is_sweep
                else create_plan(
                    effective_experiment,
                    config,
                    target,
                    seeds=seed_values,
                    preparation=preparation,
                )
            )
        service = OrchestrationService(
            store=store,
            stager=stager,
            runtime=runtime,
            scheduler=scheduler,
            transport=transport,
            framework_version=version("rundra"),
            provenance=GitProvenanceCapture(),
            progress=progress,
            submission_receipts=submission_receipts,
            task_store=task_store,
        )
        result = service.submit_one(
            RunExecutionRequest(
                plan=plan,
                experiment=effective_experiment,
                source_root=effective_source_root,
                fetch_destination=destination,
                fetch_mode=(
                    str(launch.values.get("fetch_mode", "auto"))
                    if launch is not None
                    else "auto"
                ),
                experiment_source=experiment_source,
                preparation=preparation_record,
                remote_preparation=remote_preparation,
                remote_source_root=remote_source_root,
                max_concurrent_jobs=(
                    policy.max_concurrent_jobs if policy is not None else None
                ),
                max_workers=(
                    scaling_plan.worker_count if scaling_plan is not None else None
                ),
                task_slots_per_worker=(
                    scaling_plan.task_slots_per_worker
                    if scaling_plan is not None
                    and scaling_plan.task_slots_per_worker is not None
                    else 1
                ),
                worker_resources=(
                    scaling_plan.worker_resources if scaling_plan is not None else None
                ),
                requested_workers=(
                    scaling_plan.requested_workers if scaling_plan is not None else None
                ),
                requested_task_slots_per_worker=(
                    scaling_plan.requested_task_slots_per_worker
                    if scaling_plan is not None
                    else None
                ),
                shard_outputs=(
                    target_name in targets_config.execution
                    and task_total
                    >= targets_config.execution[target_name].output_shard_tasks
                ),
                compact_plan=(scaling_plan if direct_compact else None),
                compact_configs=(expansion.configs if direct_compact else ()),
            )
        )
        _report_progress(
            progress,
            ProgressPhase.COMPLETE,
            6,
            f"run={result.record.run.id} state={result.record.run.state.value} submission durable",
            result.record.run.id,
        )
        return OperationResult.success("submit", RunValue(result.record, launch))
    except ConfigError as error:
        return OperationResult.failure("submit", _config_error(error))
    except PlanningError as error:
        return OperationResult.failure(
            "submit", OperationError(error.code, error.message, error.details)
        )
    except OrchestrationError as error:
        details = {} if error.run_id is None else {"run_id": str(error.run_id)}
        return OperationResult.failure(
            "submit", OperationError(error.code, error.message, details)
        )
    except RunStoreError as error:
        return OperationResult.failure(
            "submit", OperationError("RUN_STORE_ERROR", str(error))
        )
    except PreparationError as error:
        return OperationResult.failure(
            "submit", OperationError("PREPARATION_FAILED", str(error))
        )


def resume_operation(
    run_id: str,
    store: RunStore,
    receipts: SubmissionReceiptStore,
) -> OperationResult[SubmissionRecoveryValue]:
    """Recover a completed scheduler receipt without submitting another job."""
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("resume", error)
    assert record is not None
    try:
        transport, stager, runtime, scheduler = _execution_adapters(record.run.target)
        service = OrchestrationService(
            store=store,
            stager=stager,
            runtime=runtime,
            scheduler=scheduler,
            transport=transport,
            framework_version=version("rundra"),
            submission_receipts=receipts,
        )
        recovered, action = service.recover_submission(record.run.id)
        return OperationResult.success(
            "resume", SubmissionRecoveryValue(recovered, action)
        )
    except OrchestrationError as recovery_error:
        return OperationResult.failure(
            "resume",
            OperationError(
                recovery_error.code,
                recovery_error.message,
                {"run_id": str(record.run.id)},
            ),
        )
    except RunStoreError as store_error:
        return OperationResult.failure(
            "resume", OperationError("RUN_STORE_ERROR", str(store_error))
        )


def resolve_submission_operation(
    run_id: str,
    store: RunStore,
    receipts: SubmissionReceiptStore,
    *,
    not_submitted: bool,
    confirmation: str,
) -> OperationResult[SubmissionRecoveryValue]:
    """Persist an explicit operator resolution for an uncertain submission."""
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("resolve-submission", error)
    assert record is not None
    if not not_submitted:
        return OperationResult.failure(
            "resolve-submission",
            OperationError(
                "SUBMISSION_RESOLUTION_REQUIRED",
                "Resolution requires the explicit --not-submitted assertion",
                {"run_id": str(record.run.id)},
            ),
        )
    try:
        confirmed_run_id = RunId(confirmation)
    except (TypeError, ValueError) as confirmation_error:
        return OperationResult.failure(
            "resolve-submission",
            OperationError(
                "INVALID_CONFIRMATION_RUN_ID",
                str(confirmation_error),
                {"run_id": str(record.run.id)},
            ),
        )
    try:
        transport, stager, runtime, scheduler = _execution_adapters(record.run.target)
        service = OrchestrationService(
            store=store,
            stager=stager,
            runtime=runtime,
            scheduler=scheduler,
            transport=transport,
            framework_version=version("rundra"),
            submission_receipts=receipts,
        )
        resolved = service.resolve_submission(
            record.run.id,
            confirmation=confirmed_run_id,
        )
        return OperationResult.success(
            "resolve-submission",
            SubmissionRecoveryValue(resolved, "resolved_not_submitted"),
        )
    except OrchestrationError as resolution_error:
        return OperationResult.failure(
            "resolve-submission",
            OperationError(
                resolution_error.code,
                resolution_error.message,
                {"run_id": str(record.run.id)},
            ),
        )
    except RunStoreError as store_error:
        return OperationResult.failure(
            "resolve-submission", OperationError("RUN_STORE_ERROR", str(store_error))
        )


def status_operation(
    run_id: str,
    store: RunStore,
    *,
    scheduler: Scheduler | None = None,
    transport: Transport | None = None,
    task_store: SqliteTaskStore | None = None,
    retry_sleeper: Callable[[float], None] = sleep,
    journal_read_retries: int = _STATUS_JOURNAL_READ_RETRIES,
) -> OperationResult[StatusValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("status", error)
    assert record is not None
    if scheduler_capabilities(
        record.run.target.scheduler.kind
    ).detached_submission and record.run.state not in {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }:
        attempts = 0
        while True:
            attempts += 1
            try:
                active_scheduler = scheduler or _record_scheduler(record)
                record = SchedulerLifecycleService(
                    store=store,
                    scheduler=active_scheduler,
                    transport=transport or _record_ssh_transport(record),
                    task_store=task_store,
                ).refresh(record)
                break
            except RunStoreError as store_error:
                return OperationResult.failure(
                    "status", _run_store_operation_error(store_error, record.run.id)
                )
            except OrchestrationError as orchestration_error:
                retryable = orchestration_error.code == _BUNDLE_JOURNAL_READ_UNAVAILABLE
                if retryable and attempts <= journal_read_retries:
                    retry_sleeper(_STATUS_JOURNAL_RETRY_DELAY_SECONDS)
                    try:
                        record = store.load(record.run.id)
                    except RunStoreError as store_error:
                        return OperationResult.failure(
                            "status",
                            _run_store_operation_error(store_error, record.run.id),
                        )
                    continue
                return OperationResult.failure(
                    "status",
                    OperationError(
                        "SCHEDULER_QUERY_FAILED",
                        (
                            f"Run {record.run.id} scheduler query failed: "
                            f"{orchestration_error}"
                        ),
                        {
                            "run_id": str(record.run.id),
                            "retryable": retryable,
                            "attempts": attempts,
                        },
                    ),
                )
            except (RuntimeError, TypeError, ValueError) as query_error:
                return OperationResult.failure(
                    "status",
                    OperationError(
                        "SCHEDULER_QUERY_FAILED",
                        f"Run {record.run.id} scheduler query failed: {query_error}",
                        {
                            "run_id": str(record.run.id),
                            "retryable": False,
                            "attempts": attempts,
                        },
                    ),
                )
    try:
        record = _finalize_remote_preparation(record, store, transport=transport)
    except RunStoreError as store_error:
        return OperationResult.failure(
            "status", _run_store_operation_error(store_error, record.run.id)
        )
    except (PreparationError, RuntimeError, TypeError, ValueError) as error:
        return OperationResult.failure(
            "status",
            OperationError(
                "PREPARATION_PROVENANCE_FAILED",
                f"Run {record.run.id} preparation provenance failed: {error}",
                {"run_id": str(record.run.id)},
            ),
        )
    try:
        counts = (
            task_store.counts(record.run.id).execution
            if record.is_compact and task_store is not None
            else None
        )
    except RunStoreError as store_error:
        return OperationResult.failure(
            "status", _run_store_operation_error(store_error, record.run.id)
        )
    return OperationResult.success("status", _status_value(record, counts))


def wait_operation(
    run_id: str,
    store: RunStore,
    *,
    timeout: float | None = None,
    poll_interval: float = 2.0,
    scheduler: Scheduler | None = None,
    transport: Transport | None = None,
    task_store: SqliteTaskStore | None = None,
    progress: ProgressObserver | None = None,
    sleeper: Callable[[float], None] = sleep,
    monotonic_clock: Callable[[], float] = monotonic,
    query_failure_limit: int = _DEFAULT_QUERY_FAILURE_LIMIT,
) -> OperationResult[WaitValue]:
    if timeout is not None and (type(timeout) not in (int, float) or timeout < 0):
        return OperationResult.failure(
            "wait",
            OperationError("INVALID_TIMEOUT", "Wait timeout must be non-negative"),
        )
    if type(poll_interval) not in (int, float) or poll_interval <= 0:
        return OperationResult.failure(
            "wait",
            OperationError(
                "INVALID_POLL_INTERVAL", "Wait poll interval must be positive"
            ),
        )
    if type(query_failure_limit) is not int or query_failure_limit < 1:
        return OperationResult.failure(
            "wait",
            OperationError(
                "INVALID_QUERY_FAILURE_LIMIT",
                "Wait query failure limit must be positive",
            ),
        )
    selected_record, selected_error = _load_record(run_id, store)
    if selected_error is not None:
        return OperationResult.failure("wait", selected_error)
    assert selected_record is not None
    resolved_run_id = str(selected_record.run.id)
    started = monotonic_clock()
    terminal_states = {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }
    consecutive_query_failures = 0
    last_value: StatusValue | None = None
    while True:
        status = status_operation(
            resolved_run_id,
            store,
            scheduler=scheduler,
            transport=transport,
            task_store=task_store,
            retry_sleeper=sleeper,
        )
        if not status.ok:
            assert status.error is not None
            if status.error.code == "RUN_STORE_CONFLICT":
                continue
            if status.error.details.get("retryable") is True:
                consecutive_query_failures += 1
                elapsed = max(0.0, monotonic_clock() - started)
                if timeout is not None and elapsed >= float(timeout):
                    if last_value is not None:
                        return OperationResult.success(
                            "wait", WaitValue(last_value, False, True, float(elapsed))
                        )
                    return OperationResult.failure("wait", status.error)
                if consecutive_query_failures < query_failure_limit:
                    delay = float(poll_interval)
                    if timeout is not None:
                        delay = min(delay, max(0.0, float(timeout) - elapsed))
                    sleeper(delay)
                    continue
            return OperationResult.failure("wait", status.error)
        assert status.value is not None
        value = status.value
        consecutive_query_failures = 0
        last_value = value
        elapsed = max(0.0, monotonic_clock() - started)
        terminal = value.state in terminal_states
        total = sum(value.task_counts.values())
        complete = sum(
            value.task_counts.get(state.value, 0)
            for state in (
                ExecutionState.SUCCEEDED,
                ExecutionState.FAILED,
                ExecutionState.CANCELLED,
            )
        )
        _report_progress(
            progress,
            ProgressPhase.WAIT,
            min((6 if terminal else 5) + complete, 6 + total),
            f"run={value.state.value} terminal={terminal}",
            value.run_id,
            task_total=total,
        )
        if terminal:
            return OperationResult.success(
                "wait", WaitValue(value, True, False, float(elapsed))
            )
        if timeout is not None and elapsed >= float(timeout):
            return OperationResult.success(
                "wait", WaitValue(value, False, True, float(elapsed))
            )
        delay = float(poll_interval)
        if timeout is not None:
            delay = min(delay, max(0.0, float(timeout) - elapsed))
        sleeper(delay)


def await_runs_operation(
    run_ids: Sequence[str],
    store: RunStore,
    *,
    until: str = "all",
    timeout: float | None = None,
    poll_interval: float = 15.0,
    fail_on_run_failure: bool = False,
    scheduler: Scheduler | None = None,
    transport: Transport | None = None,
    task_store: SqliteTaskStore | None = None,
    sleeper: Callable[[float], None] = sleep,
    monotonic_clock: Callable[[], float] = monotonic,
    query_failure_limit: int = _DEFAULT_QUERY_FAILURE_LIMIT,
) -> OperationResult[AwaitRunsValue]:
    if isinstance(run_ids, (str, bytes)) or not isinstance(run_ids, Sequence):
        return OperationResult.failure(
            "await",
            OperationError("INVALID_RUN_IDS", "Run IDs must be a sequence of strings"),
        )
    selected = tuple(run_ids)
    if not selected:
        return OperationResult.failure(
            "await",
            OperationError("RUN_IDS_REQUIRED", "At least one Run ID is required"),
        )
    if any(not isinstance(run_id, str) for run_id in selected):
        return OperationResult.failure(
            "await", OperationError("INVALID_RUN_IDS", "Every Run ID must be a string")
        )
    if LAST_RUN_SELECTOR in selected:
        return OperationResult.failure(
            "await",
            OperationError(
                "EXPLICIT_RUN_IDS_REQUIRED",
                "Aggregate waiting requires explicit Run IDs; --last is not supported",
            ),
        )
    if len(set(selected)) != len(selected):
        return OperationResult.failure(
            "await", OperationError("DUPLICATE_RUN_ID", "Run IDs must be unique")
        )
    if until not in {"all", "any"}:
        return OperationResult.failure(
            "await",
            OperationError("INVALID_AWAIT_CONDITION", "--until must be 'all' or 'any'"),
        )
    if timeout is not None and (type(timeout) not in (int, float) or timeout < 0):
        return OperationResult.failure(
            "await",
            OperationError("INVALID_TIMEOUT", "Await timeout must be non-negative"),
        )
    if type(poll_interval) not in (int, float) or poll_interval <= 0:
        return OperationResult.failure(
            "await",
            OperationError(
                "INVALID_POLL_INTERVAL", "Await poll interval must be positive"
            ),
        )
    if type(query_failure_limit) is not int or query_failure_limit < 1:
        return OperationResult.failure(
            "await",
            OperationError(
                "INVALID_QUERY_FAILURE_LIMIT",
                "Await query failure limit must be positive",
            ),
        )

    for run_id in selected:
        _, error = _load_record(run_id, store)
        if error is not None:
            return OperationResult.failure("await", error)

    started = monotonic_clock()
    terminal_states = {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }
    consecutive_query_failures = 0
    last_statuses: tuple[StatusValue, ...] | None = None
    while True:
        statuses: list[StatusValue] = []
        retry_snapshot = False
        for run_id in selected:
            status = status_operation(
                run_id,
                store,
                scheduler=scheduler,
                transport=transport,
                task_store=task_store,
                retry_sleeper=sleeper,
            )
            if not status.ok:
                assert status.error is not None
                if status.error.code == "RUN_STORE_CONFLICT":
                    retry_snapshot = True
                    break
                if status.error.details.get("retryable") is True:
                    consecutive_query_failures += 1
                    elapsed = max(0.0, monotonic_clock() - started)
                    if (
                        timeout is not None
                        and elapsed >= float(timeout)
                        and last_statuses is not None
                    ):
                        return OperationResult.success(
                            "await",
                            AwaitRunsValue(
                                last_statuses,
                                until,
                                False,
                                True,
                                float(elapsed),
                                fail_on_run_failure,
                            ),
                        )
                    if consecutive_query_failures < query_failure_limit:
                        retry_snapshot = True
                        break
                return OperationResult.failure("await", status.error)
            assert status.value is not None
            statuses.append(replace(status.value, task_details=()))
        if retry_snapshot:
            delay = float(poll_interval)
            elapsed = max(0.0, monotonic_clock() - started)
            if timeout is not None:
                delay = min(delay, max(0.0, float(timeout) - elapsed))
            sleeper(delay)
            continue

        elapsed = max(0.0, monotonic_clock() - started)
        consecutive_query_failures = 0
        last_statuses = tuple(statuses)
        terminal = tuple(status.state in terminal_states for status in statuses)
        condition_met = all(terminal) if until == "all" else any(terminal)
        value = AwaitRunsValue(
            tuple(statuses),
            until,
            condition_met,
            False,
            float(elapsed),
            fail_on_run_failure,
        )
        if condition_met:
            return OperationResult.success("await", value)
        if timeout is not None and elapsed >= float(timeout):
            return OperationResult.success("await", replace(value, timed_out=True))
        delay = float(poll_interval)
        if timeout is not None:
            delay = min(delay, max(0.0, float(timeout) - elapsed))
        sleeper(delay)


def cancel_operation(
    run_id: str,
    store: RunStore,
    *,
    scheduler: Scheduler | None = None,
    task_store: SqliteTaskStore | None = None,
) -> OperationResult[CancelValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("cancel", error)
    assert record is not None
    if not scheduler_capabilities(record.run.target.scheduler.kind).detached_submission:
        return OperationResult.failure(
            "cancel",
            OperationError(
                "CANCEL_UNSUPPORTED",
                f"Run {record.run.id} does not use an asynchronous scheduler",
                {"run_id": str(record.run.id)},
            ),
        )
    try:
        active_scheduler = scheduler or _record_scheduler(record)
        record = SchedulerLifecycleService(
            store=store,
            scheduler=active_scheduler,
            transport=_record_ssh_transport(record),
            task_store=task_store,
        ).cancel(record)
    except OrchestrationError as orchestration_error:
        return OperationResult.failure(
            "cancel",
            OperationError(
                orchestration_error.code,
                orchestration_error.message,
                {"run_id": str(record.run.id)},
            ),
        )
    except RunStoreError as store_error:
        return OperationResult.failure(
            "cancel", _run_store_operation_error(store_error, record.run.id)
        )
    return OperationResult.success("cancel", CancelValue(_status_value(record)))


def list_runs_operation(
    store: RunStore,
    *,
    task_store: SqliteTaskStore | None = None,
    offset: int = 0,
    limit: int = 100,
    include_tasks: bool = False,
) -> OperationResult[ListRunsValue]:
    if type(offset) is not int or offset < 0:
        return OperationResult.failure(
            "list",
            OperationError("INVALID_RUN_OFFSET", "Run offset must be non-negative"),
        )
    if type(limit) is not int or not 1 <= limit <= 1000:
        return OperationResult.failure(
            "list",
            OperationError("INVALID_RUN_LIMIT", "Run limit must be between 1 and 1000"),
        )
    try:
        records = store.list()
        page = records[offset : offset + limit]
        return OperationResult.success(
            "list",
            ListRunsValue(
                tuple(
                    status if include_tasks else replace(status, task_details=())
                    for status in (
                        _status_value(
                            record,
                            (
                                task_store.counts(record.run.id).execution
                                if record.is_compact and task_store is not None
                                else None
                            ),
                        )
                        for record in page
                    )
                ),
                offset,
                limit,
                len(records),
                include_tasks,
            ),
        )
    except RunStoreError as error:
        return OperationResult.failure(
            "list", OperationError("RUN_STORE_ERROR", str(error))
        )


def inspect_operation(
    run_id: str,
    store: RunStore,
    *,
    receipts: PurgeReceiptStore | None = None,
) -> OperationResult[InspectValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("inspect", error)
    assert record is not None
    try:
        retention = None if receipts is None else receipts.load(record.run.id)
    except RunStoreError as receipt_error:
        return OperationResult.failure(
            "inspect", OperationError("PURGE_RECEIPT_INVALID", str(receipt_error))
        )
    return OperationResult.success("inspect", InspectValue(record, retention))


def purge_operation(
    run_id: str,
    store: RunStore,
    receipts: PurgeReceiptStore,
    *,
    workspace: bool = False,
    confirm: str | None = None,
    dry_run: bool = False,
    purger: LocalPurger | SSHPurger | None = None,
    scheduler: Scheduler | None = None,
    transport: Transport | None = None,
) -> OperationResult[PurgeValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("purge", error)
    assert record is not None
    if not dry_run and confirm != str(record.run.id):
        return OperationResult.failure(
            "purge",
            OperationError(
                "PURGE_CONFIRMATION_REQUIRED",
                "Purge requires --confirm with the exact Run ID",
                {"run_id": str(record.run.id)},
            ),
        )
    with store.operation_lock(record.run.id):
        try:
            record = store.load(record.run.id)
        except RunStoreError as store_error:
            return OperationResult.failure(
                "purge", _run_store_operation_error(store_error, record.run.id)
            )
        if (
            record.run.state
            not in {
                ExecutionState.SUCCEEDED,
                ExecutionState.FAILED,
                ExecutionState.CANCELLED,
            }
            and scheduler_capabilities(
                record.run.target.scheduler.kind
            ).detached_submission
        ):
            try:
                active_scheduler = scheduler or _record_scheduler(record)
                record = SchedulerLifecycleService(
                    store=store,
                    scheduler=active_scheduler,
                    transport=transport or _record_ssh_transport(record),
                ).refresh(record)
            except (
                OrchestrationError,
                RunStoreError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as refresh_error:
                return OperationResult.failure(
                    "purge",
                    OperationError(
                        "SCHEDULER_QUERY_FAILED",
                        f"Run {record.run.id} state refresh failed: {refresh_error}",
                        {"run_id": str(record.run.id)},
                    ),
                )
        if record.run.state not in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }:
            return OperationResult.failure(
                "purge",
                OperationError(
                    "RUN_NOT_TERMINAL",
                    f"Run {record.run.id} must be terminal before purge",
                    {"run_id": str(record.run.id), "state": record.run.state.value},
                ),
            )
        scope = PurgeScope.WORKSPACE if workspace else PurgeScope.OUTPUTS
        request = PurgeRequest(
            record.run.id,
            _record_workspace(record).root,
            record.run.target.workspace,
            scope,
        )
        try:
            active_purger = purger or _record_purger(record)
            planned = active_purger.purge(request, dry_run=True)
        except (
            OSError,
            PurgeError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as purge_error:
            return OperationResult.failure(
                "purge",
                OperationError(
                    "PURGE_FAILED",
                    f"Run {record.run.id} purge validation failed: {purge_error}",
                    {"run_id": str(record.run.id), "scope": scope.value},
                ),
            )
        if dry_run:
            try:
                receipt = receipts.load(record.run.id)
            except RunStoreError as receipt_error:
                return OperationResult.failure(
                    "purge",
                    OperationError("PURGE_RECEIPT_INVALID", str(receipt_error)),
                )
            return OperationResult.success(
                "purge",
                PurgeValue(
                    record.run.id,
                    scope,
                    True,
                    planned,
                    receipt,
                    receipts.path(record.run.id),
                ),
            )
        pending = PurgeAttempt(
            secrets.token_hex(16),
            datetime.now(UTC),
            None,
            scope,
            planned.backend,
            planned.path,
            planned.tombstone,
            PurgeOutcome.PENDING,
        )
        receipt_started = False
        try:
            receipts.append(record.run.id, pending)
            receipt_started = True
            result = active_purger.purge(request)
            completed = replace(
                pending,
                finished_at=datetime.now(UTC),
                outcome=result.outcome,
            )
            receipt = receipts.replace_last(record.run.id, completed)
        except (OSError, PurgeError, RunStoreError, RuntimeError) as purge_error:
            failed = replace(
                pending,
                finished_at=datetime.now(UTC),
                outcome=PurgeOutcome.FAILED,
                error_code="PURGE_FAILED",
            )
            if receipt_started:
                try:
                    receipts.replace_last(record.run.id, failed)
                except RunStoreError:
                    pass
            return OperationResult.failure(
                "purge",
                OperationError(
                    "PURGE_FAILED",
                    f"Run {record.run.id} purge failed: {purge_error}",
                    {"run_id": str(record.run.id), "scope": scope.value},
                ),
            )
        return OperationResult.success(
            "purge",
            PurgeValue(
                record.run.id,
                scope,
                False,
                result,
                receipt,
                receipts.path(record.run.id),
            ),
        )


def tasks_operation(
    run_id: str,
    store: RunStore,
    task_store: SqliteTaskStore,
    *,
    offset: int = 0,
    limit: int = 100,
) -> OperationResult[TasksValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("tasks", error)
    assert record is not None
    if not record.is_compact:
        try:
            materialized_page = _materialized_task_page(
                record, offset=offset, limit=limit
            )
        except ValueError as task_error:
            return OperationResult.failure(
                "tasks",
                OperationError(
                    "TASK_STATE_ERROR",
                    str(task_error),
                    {"run_id": str(record.run.id)},
                ),
            )
        return OperationResult.success(
            "tasks",
            TasksValue(
                record.run.id,
                len(record.run.tasks),
                offset,
                limit,
                materialized_page,
                format_version=record.format_version,
            ),
        )
    if record.task_state_store is None:
        return OperationResult.failure(
            "tasks",
            OperationError(
                "TASK_STATE_MISMATCH",
                f"Run {record.run.id} has no compact Task state sidecar",
                {"run_id": str(record.run.id)},
            ),
        )
    if record.task_state_store.name != task_store.path(record.run.id).name:
        return OperationResult.failure(
            "tasks",
            OperationError(
                "TASK_STATE_MISMATCH",
                f"Run {record.run.id} references another Task state sidecar",
                {"run_id": str(record.run.id)},
            ),
        )
    try:
        page = task_store.page(record.run.id, offset=offset, limit=limit)
    except (RunStoreError, TypeError, ValueError) as task_error:
        return OperationResult.failure(
            "tasks",
            OperationError(
                "TASK_STATE_ERROR",
                str(task_error),
                {"run_id": str(record.run.id)},
            ),
        )
    return OperationResult.success(
        "tasks",
        TasksValue(
            record.run.id,
            page.total,
            page.offset,
            page.limit,
            page.tasks,
            format_version=record.format_version,
        ),
    )


def _materialized_task_page(
    record: RunRecord,
    *,
    offset: int,
    limit: int,
) -> tuple[TaskState, ...]:
    if type(offset) is not int or offset < 0:
        raise ValueError("Task page offset must be a non-negative integer")
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Task page limit must be between 1 and 1000")
    stop = min(len(record.run.tasks), offset + limit)
    if offset >= stop:
        return ()
    retrieval_states = _task_retrieval_states(record)
    parameter_ordinals: dict[str, int] = {}
    seed_ordinals: dict[int, int] = {}
    page: list[TaskState] = []
    for ordinal, task in enumerate(record.run.tasks[:stop]):
        parameter_key = task.parameter_set.id if task.parameter_set is not None else ""
        parameter_ordinal = parameter_ordinals.setdefault(
            parameter_key, len(parameter_ordinals)
        )
        seed_ordinal = seed_ordinals.get(parameter_ordinal, 0)
        seed_ordinals[parameter_ordinal] = seed_ordinal + 1
        if ordinal < offset:
            continue
        page.append(
            TaskState(
                TaskCoordinate(
                    task.id,
                    ordinal,
                    parameter_ordinal,
                    seed_ordinal,
                    task.seed,
                ),
                execution_state=task.state,
                retrieval_state=retrieval_states[task.id],
                scheduler_id=record.task_scheduler_ids.get(task.id),
                native_state=record.task_native_states.get(task.id),
                exit_code=record.task_exit_codes.get(task.id),
            )
        )
    return tuple(page)


def logs_operation(
    run_id: str,
    store: RunStore,
    *,
    task: str | None = None,
    preparation: bool = False,
    scheduler: Scheduler | None = None,
    transport: Transport | None = None,
) -> OperationResult[LogsValue | PreparationLogsValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("logs", error)
    assert record is not None
    if (
        not preparation
        and record.run.target.scheduler.kind != "local"
        and record.run.state
        not in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
    ):
        try:
            active_scheduler = scheduler or _record_scheduler(record)
            record = SchedulerLifecycleService(
                store=store,
                scheduler=active_scheduler,
                transport=transport or _record_ssh_transport(record),
            ).refresh(record)
        except RunStoreError as store_error:
            return OperationResult.failure(
                "logs", _run_store_operation_error(store_error, record.run.id)
            )
        except (OrchestrationError, RuntimeError, TypeError, ValueError) as error:
            return OperationResult.failure(
                "logs",
                OperationError(
                    "SCHEDULER_QUERY_FAILED",
                    f"Run {record.run.id} scheduler query failed: {error}",
                    {"run_id": str(record.run.id)},
                ),
            )
    if preparation:
        prepared = record.preparation
        if prepared is None or len(prepared.logs) != 2:
            return OperationResult.failure(
                "logs",
                OperationError(
                    "PREPARATION_LOGS_UNAVAILABLE",
                    f"Preparation logs are unavailable for Run {record.run.id}",
                    {"run_id": str(record.run.id)},
                ),
            )
        stdout_path, stderr_path = prepared.logs
        try:
            if record.run.target.transport.kind == "ssh":
                active_transport = transport or _record_ssh_transport(record)
                stdout_text = _read_remote_log(active_transport, stdout_path)
                stderr_text = _read_remote_log(active_transport, stderr_path)
            else:
                stdout_text = Path(str(stdout_path)).read_text(encoding="utf-8")
                stderr_text = Path(str(stderr_path)).read_text(encoding="utf-8")
        except (OSError, RuntimeError) as error:
            return OperationResult.failure(
                "logs",
                OperationError(
                    "LOG_READ_FAILED",
                    f"Could not read preparation logs for Run {record.run.id}: {error}",
                    {"run_id": str(record.run.id)},
                ),
            )
        return OperationResult.success(
            "logs",
            PreparationLogsValue(
                record.run.id,
                prepared.builder_scheduler_id,
                stdout_text,
                stderr_text,
                stdout_path,
                stderr_path,
                format_version=record.format_version,
            ),
        )
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
        if record.run.target.transport.kind == "ssh":
            active_transport = transport or _record_ssh_transport(record)
            stdout_text = _read_remote_log(active_transport, stdout.path)
            stderr_text = _read_remote_log(active_transport, stderr.path)
        else:
            stdout_text = Path(str(stdout.path)).read_text(encoding="utf-8")
            stderr_text = Path(str(stderr.path)).read_text(encoding="utf-8")
    except (OSError, RuntimeError) as error:
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
            format_version=record.format_version,
        ),
    )


def fetch_operation(
    run_id: str,
    store: RunStore,
    destination: Path | None = None,
    *,
    tasks: Sequence[str] | None = None,
    stager: Stager | None = None,
    mode: str | None = None,
    extract: bool = False,
    progress: ProgressObserver | None = None,
    task_store: SqliteTaskStore | None = None,
) -> OperationResult[FetchValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("fetch", error)
    assert record is not None
    effective_mode = mode
    if effective_mode is None:
        stored_mode = record.fetch_mode
        if stored_mode is None:
            legacy_mode = record.scheduler_metadata.get("fetch_mode", "auto")
            stored_mode = legacy_mode if type(legacy_mode) is str else "auto"
        effective_mode = stored_mode
    effective_destination = (
        destination.resolve()
        if destination is not None
        else _default_fetch_destination(record)
    )
    with store.operation_lock(record.run.id):
        return _fetch_operation_locked(
            str(record.run.id),
            store,
            effective_destination,
            tasks=tasks,
            stager=stager,
            mode=effective_mode,
            extract=extract,
            progress=progress,
            task_store=task_store,
        )


def _default_fetch_destination(record: RunRecord) -> Path:
    if record.retrieval_destination is not None:
        return Path(str(record.retrieval_destination))
    project_root = (
        Path(str(record.experiment_source)).parent
        if record.experiment_source is not None
        else Path(str(record.source_root))
    )
    config_stem = (
        record.run.tasks[0].config.source.stem
        if record.run.tasks
        else record.experiment_source.stem
        if record.experiment_source is not None
        else record.experiment.name
    )
    return project_root / "retrieved" / config_stem


def _fetch_operation_locked(
    run_id: str,
    store: RunStore,
    destination: Path,
    *,
    tasks: Sequence[str] | None = None,
    stager: Stager | None = None,
    mode: str = "auto",
    extract: bool = False,
    progress: ProgressObserver | None = None,
    task_store: SqliteTaskStore | None = None,
) -> OperationResult[FetchValue]:
    record, error = _load_record(run_id, store)
    if error is not None:
        return OperationResult.failure("fetch", error)
    assert record is not None
    sharded = record.scheduler_metadata.get("result_shards") is True
    if mode == "archive" and not sharded:
        return OperationResult.failure(
            "fetch",
            OperationError(
                "SHARDS_UNAVAILABLE",
                "Archive retrieval requires a Run with sealed result shards",
                {"run_id": str(record.run.id)},
            ),
        )
    effective_mode = mode
    if mode == "auto":
        if extract and sharded:
            effective_mode = "archive"
        elif record.run.target.staging.kind == "shared":
            effective_mode = "reference"
        elif record.run.target.staging.kind == "rsync" and record.run.state in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }:
            effective_mode = "auto"
        else:
            effective_mode = "copy"
    if effective_mode == "reference" and record.run.state not in {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    }:
        return OperationResult.failure(
            "fetch",
            OperationError(
                "RUN_NOT_TERMINAL",
                "Reference retrieval requires a terminal Run",
                {"run_id": str(record.run.id), "state": record.run.state.value},
            ),
        )
    compact_all = record.is_compact and sharded and tasks is None
    selected = () if compact_all else _selected_task_ids(record, tasks)
    if isinstance(selected, OperationError):
        return OperationResult.failure("fetch", selected)
    if record.is_compact and task_store is None:
        return OperationResult.failure(
            "fetch",
            OperationError(
                "TASK_STATE_UNAVAILABLE",
                f"Run {record.run.id} requires its compact Task state",
                {"run_id": str(record.run.id)},
            ),
        )
    if record.is_compact and sharded:
        assert task_store is not None
        return _fetch_compact_shards_locked(
            record,
            store,
            destination,
            selected=selected,
            select_all=compact_all,
            stager=stager,
            mode=effective_mode,
            extract=extract,
            progress=progress,
            task_store=task_store,
        )
    try:
        retrieval_states = _task_retrieval_states(record, task_store)
    except RunStoreError as store_error:
        return OperationResult.failure(
            "fetch", _run_store_operation_error(store_error, record.run.id)
        )
    transitioning = tuple(
        task_id
        for task_id in selected
        if retrieval_states[task_id] is not RetrievalState.SUCCEEDED
    )
    _report_progress(
        progress,
        ProgressPhase.RETRIEVE,
        5 + len(selected) - len(transitioning),
        (f"mode={effective_mode} destination={destination} tasks={len(selected)}"),
        record.run.id,
        task_total=len(selected),
    )
    if transitioning:
        if record.is_compact:
            assert task_store is not None
            task_store.set_retrieval(
                record.run.id, transitioning, RetrievalState.PENDING
            )
        for task_id in transitioning:
            retrieval_states[task_id] = RetrievalState.PENDING
        pending = replace(
            record,
            run=replace(
                record.run,
                retrieval_state=aggregate_retrieval_state(
                    tuple(retrieval_states.values())
                ),
            ),
            task_retrieval_states=({} if record.is_compact else retrieval_states),
        )
        try:
            store.update(pending, expected=record)
        except RunStoreError as store_error:
            return OperationResult.failure(
                "fetch", _run_store_operation_error(store_error, record.run.id)
            )
        record = pending
    if extract and sharded:
        extraction_pending = _with_extraction_state(
            record, destination, RetrievalState.PENDING
        )
        try:
            store.update(extraction_pending, expected=record)
        except RunStoreError as store_error:
            return OperationResult.failure(
                "fetch", _run_store_operation_error(store_error, record.run.id)
            )
        record = extraction_pending
    workspace = _record_workspace(record)
    try:
        active_stager = stager or _record_stager(record)
        fetched = active_stager.fetch(
            FetchRequest(
                workspace,
                _selected_fetch_patterns(record, selected),
                destination,
                effective_mode,
            )
        )
    except (OSError, RuntimeError, ValueError) as error:
        if transitioning:
            if record.is_compact:
                assert task_store is not None
                task_store.set_retrieval(
                    record.run.id, transitioning, RetrievalState.FAILED
                )
            for task_id in transitioning:
                retrieval_states[task_id] = RetrievalState.FAILED
            try:
                failed = replace(
                    record,
                    run=replace(
                        record.run,
                        retrieval_state=aggregate_retrieval_state(
                            tuple(retrieval_states.values())
                        ),
                    ),
                    task_retrieval_states=(
                        {} if record.is_compact else retrieval_states
                    ),
                )
                if extract and sharded:
                    failed = _with_extraction_state(
                        failed, destination, RetrievalState.FAILED
                    )
                store.update(failed, expected=record)
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
    try:
        artifacts = _selected_fetch_artifacts(record, selected, fetched.artifacts)
        if extract and sharded:
            artifacts = (
                *artifacts,
                *_extract_fetched_shards(record, destination, artifacts, selected),
            )
    except ValueError as error:
        if transitioning:
            if record.is_compact:
                assert task_store is not None
                task_store.set_retrieval(
                    record.run.id, transitioning, RetrievalState.FAILED
                )
            for task_id in transitioning:
                retrieval_states[task_id] = RetrievalState.FAILED
            try:
                failed = replace(
                    record,
                    run=replace(
                        record.run,
                        retrieval_state=aggregate_retrieval_state(
                            tuple(retrieval_states.values())
                        ),
                    ),
                    task_retrieval_states=(
                        {} if record.is_compact else retrieval_states
                    ),
                )
                if extract and sharded:
                    failed = _with_extraction_state(
                        failed, destination, RetrievalState.FAILED
                    )
                store.update(failed, expected=record)
            except RunStoreError:
                pass
        return OperationResult.failure(
            "fetch",
            OperationError(
                "RESULT_RETRIEVAL_FAILED",
                f"Run {record.run.id} returned invalid fetched artifacts: {error}",
                {"run_id": str(record.run.id)},
            ),
        )
    for task_id in selected:
        retrieval_states[task_id] = RetrievalState.SUCCEEDED
    if record.is_compact:
        assert task_store is not None
        task_store.set_retrieval(record.run.id, selected, RetrievalState.SUCCEEDED)
    retrieval_state = aggregate_retrieval_state(tuple(retrieval_states.values()))
    merged = _merge_artifacts(record.artifacts, artifacts)
    succeeded = replace(
        record,
        run=replace(record.run, retrieval_state=retrieval_state),
        task_retrieval_states=({} if record.is_compact else retrieval_states),
        artifacts=merged,
    )
    if extract and sharded:
        succeeded = _with_extraction_state(
            succeeded, destination, RetrievalState.SUCCEEDED
        )
    try:
        store.update(succeeded, expected=record)
    except RunStoreError as error:
        return OperationResult.failure(
            "fetch", _run_store_operation_error(error, record.run.id)
        )
    _report_progress(
        progress,
        ProgressPhase.COMPLETE,
        6 + len(selected),
        f"retrieval={retrieval_state.value} destination={destination}",
        record.run.id,
        task_total=len(selected),
    )
    return OperationResult.success(
        "fetch",
        FetchValue(
            record.run.id,
            destination,
            retrieval_state,
            artifacts,
            selected,
            format_version=record.format_version,
        ),
    )


def _fetch_compact_shards_locked(
    record: RunRecord,
    store: RunStore,
    destination: Path,
    *,
    selected: tuple[TaskId, ...],
    select_all: bool,
    stager: Stager | None,
    mode: str,
    extract: bool,
    progress: ProgressObserver | None,
    task_store: SqliteTaskStore,
) -> OperationResult[FetchValue]:
    assert record.task_space is not None
    task_total = record.task_space.task_count if select_all else len(selected)
    counts = task_store.counts(record.run.id)
    transitioning: tuple[TaskId, ...] = ()
    if select_all:
        transition_count = task_total - counts.retrieval[RetrievalState.SUCCEEDED]
    else:
        states = tuple(
            task_store.get(record.run.id, int(task_id.value.removeprefix("task_")))
            for task_id in selected
        )
        transitioning = tuple(
            state.coordinate.task_id
            for state in states
            if state.retrieval_state is not RetrievalState.SUCCEEDED
        )
        transition_count = len(transitioning)
    _report_progress(
        progress,
        ProgressPhase.RETRIEVE,
        5 + task_total - transition_count,
        f"mode={mode} destination={destination} tasks={task_total}",
        record.run.id,
        task_total=task_total,
    )
    if transition_count:
        try:
            if select_all:
                task_store.prepare_all_retrieval(record.run.id)
            else:
                task_store.set_retrieval(
                    record.run.id, transitioning, RetrievalState.PENDING
                )
            pending = replace(
                record,
                run=replace(
                    record.run,
                    retrieval_state=_compact_retrieval_state(task_store, record.run.id),
                ),
            )
            store.update(pending, expected=record)
            record = pending
        except RunStoreError as error:
            return OperationResult.failure(
                "fetch", _run_store_operation_error(error, record.run.id)
            )
    if extract:
        extraction_pending = _with_extraction_state(
            record, destination, RetrievalState.PENDING
        )
        try:
            store.update(extraction_pending, expected=record)
        except RunStoreError as error:
            return OperationResult.failure(
                "fetch", _run_store_operation_error(error, record.run.id)
            )
        record = extraction_pending
    workspace = _record_workspace(record)
    try:
        active_stager = stager or _record_stager(record)
        artifacts, shard_rows = _fetch_complete_compact_shards(
            record,
            active_stager,
            workspace,
            destination,
            mode=mode,
            selected=selected,
            select_all=select_all,
            task_total=task_total,
        )
        if extract:
            artifacts = (
                *artifacts,
                *_extract_fetched_shards(
                    record,
                    destination,
                    artifacts,
                    None if select_all else selected,
                ),
            )
        task_store.ingest_result_shards(
            record.run.id,
            shard_rows,
            selected=None if select_all else selected,
        )
    except (OSError, RuntimeError, ValueError) as error:
        try:
            if select_all:
                task_store.fail_pending_retrieval(record.run.id)
            elif transitioning:
                task_store.set_retrieval(
                    record.run.id, transitioning, RetrievalState.FAILED
                )
            failed = replace(
                record,
                run=replace(
                    record.run,
                    retrieval_state=_compact_retrieval_state(task_store, record.run.id),
                ),
            )
            if extract:
                failed = _with_extraction_state(
                    failed, destination, RetrievalState.FAILED
                )
            store.update(failed, expected=record)
        except RunStoreError:
            pass
        return OperationResult.failure(
            "fetch",
            OperationError(
                "RESULT_RETRIEVAL_FAILED",
                f"Run {record.run.id} result shard ingestion failed: {error}",
                {"run_id": str(record.run.id)},
            ),
        )
    retrieval_state = _compact_retrieval_state(task_store, record.run.id)
    succeeded = replace(
        record,
        run=replace(record.run, retrieval_state=retrieval_state),
        artifacts=_merge_artifacts(record.artifacts, artifacts),
    )
    if extract:
        succeeded = _with_extraction_state(
            succeeded, destination, RetrievalState.SUCCEEDED
        )
    try:
        store.update(succeeded, expected=record)
    except RunStoreError as error:
        return OperationResult.failure(
            "fetch", _run_store_operation_error(error, record.run.id)
        )
    _report_progress(
        progress,
        ProgressPhase.COMPLETE,
        6 + task_total,
        f"retrieval={retrieval_state.value} destination={destination}",
        record.run.id,
        task_total=task_total,
    )
    return OperationResult.success(
        "fetch",
        FetchValue(
            record.run.id,
            destination,
            retrieval_state,
            artifacts,
            selected,
            format_version=record.format_version,
        ),
    )


def _fetch_complete_compact_shards(
    record: RunRecord,
    stager: Stager,
    workspace: StagedWorkspace,
    destination: Path,
    *,
    mode: str,
    selected: tuple[TaskId, ...],
    select_all: bool,
    task_total: int,
) -> tuple[tuple[Artifact, ...], tuple[tuple[str, Mapping[TaskId, int]], ...]]:
    artifacts: tuple[Artifact, ...] = ()
    for attempt in range(_SHARD_VISIBILITY_ATTEMPTS):
        fetched = stager.fetch(
            FetchRequest(
                workspace,
                (".rundra-shards/*.tar", ".rundra-shards/*.sha256"),
                destination,
                mode,
            )
        )
        artifacts = _merge_artifacts(
            artifacts,
            _selected_fetch_artifacts(record, selected, fetched.artifacts),
        )
        shard_paths = tuple(
            Path(artifact.path)
            for artifact in artifacts
            if artifact.kind is ArtifactKind.OUTPUT_SHARD
            and str(artifact.path).endswith(".tar")
        )
        rows = tuple(_verified_shard_rows(record, shard_paths))
        covered: set[TaskId] = set()
        for _name, task_exit_codes in rows:
            overlap = covered.intersection(task_exit_codes)
            if overlap:
                raise ValueError(
                    "Result shards contain duplicate Task "
                    f"{min(str(task_id) for task_id in overlap)}"
                )
            covered.update(task_exit_codes)
        complete = (
            len(covered) == task_total
            if select_all
            else set(selected).issubset(covered)
        )
        if complete:
            return artifacts, rows
        if attempt + 1 < _SHARD_VISIBILITY_ATTEMPTS:
            sleep(0.25 * (2**attempt))
            continue
        raise ValueError(
            f"Result shards cover {len(covered)} of {task_total} requested Tasks"
        )
    raise AssertionError("compact shard visibility attempts must be positive")


def _compact_retrieval_state(
    task_store: SqliteTaskStore, run_id: RunId
) -> RetrievalState:
    counts = task_store.counts(run_id).retrieval
    total = sum(counts.values())
    if counts[RetrievalState.SUCCEEDED] == total:
        return RetrievalState.SUCCEEDED
    if counts[RetrievalState.FAILED]:
        return RetrievalState.FAILED
    if counts[RetrievalState.NOT_REQUESTED] == total:
        return RetrievalState.NOT_REQUESTED
    return RetrievalState.PENDING


def _with_extraction_state(
    record: RunRecord,
    destination: Path,
    state: RetrievalState,
) -> RunRecord:
    return replace(
        record,
        scheduler_metadata={
            **record.scheduler_metadata,
            "extraction_state": state.value,
            "extraction_destination": str(destination),
        },
    )


def _load_record(
    value: object,
    store: RunStore,
) -> tuple[RunRecord | None, OperationError | None]:
    if type(value) is not str:
        return None, OperationError(
            "INVALID_RUN_ID",
            "Run selector must be a Run ID or --last",
            {"run_id": str(value)},
        )
    if value == LAST_RUN_SELECTOR:
        try:
            records = store.list()
        except RunStoreError as error:
            return None, OperationError("RUN_STORE_ERROR", str(error))
        if not records:
            return None, OperationError(
                "RUN_NOT_FOUND",
                "No registered Runs are available; submit or run an experiment first",
                {"selector": "last"},
            )
        return max(
            records,
            key=lambda record: (record.run.created_at, str(record.run.id)),
        ), None
    try:
        run_id = RunId(value)
    except ValueError as error:
        return None, OperationError("INVALID_RUN_ID", str(error), {"run_id": value})
    try:
        return store.load(run_id), None
    except RunNotFoundError as error:
        return None, OperationError("RUN_NOT_FOUND", str(error), {"run_id": value})
    except RunStoreError as error:
        return None, OperationError("RUN_STORE_ERROR", str(error), {"run_id": value})


def _run_store_operation_error(error: RunStoreError, run_id: RunId) -> OperationError:
    if isinstance(error, RunStoreConflictError):
        return OperationError(
            "RUN_STORE_CONFLICT",
            f"Run {run_id} changed concurrently; retry the operation",
            {"run_id": str(run_id)},
        )
    return OperationError("RUN_STORE_ERROR", str(error), {"run_id": str(run_id)})


def _status_value(
    record: RunRecord,
    compact_counts: Mapping[ExecutionState, int] | None = None,
) -> StatusValue:
    counts = (
        {state.value: count for state, count in compact_counts.items() if count}
        if compact_counts is not None
        else {}
    )
    if compact_counts is None:
        for task in record.run.tasks:
            counts[task.state.value] = counts.get(task.state.value, 0) + 1
    retrieval_states = _task_retrieval_states(record)
    return StatusValue(
        run_id=record.run.id,
        experiment=record.run.experiment_name,
        target=record.run.target.name,
        state=record.run.state,
        retrieval_state=record.run.retrieval_state,
        task_counts=counts,
        native_state=record.native_state,
        scheduler_job_ids=record.scheduler_job_ids,
        task_details=tuple(
            TaskStatusValue(
                task_id=task.id,
                seed=task.seed,
                state=task.state,
                retrieval_state=retrieval_states[task.id],
                native_id=record.task_scheduler_ids.get(task.id),
                native_state=record.task_native_states.get(task.id),
                exit_code=record.task_exit_codes.get(task.id),
                parameter_set=task.parameter_set,
            )
            for task in record.run.tasks
        ),
        preparation=(
            None
            if record.preparation is None
            else PreparationStatusValue(
                scheduler_id=record.preparation.builder_scheduler_id,
                state=record.preparation.builder_status,
                native_state=record.preparation.builder_state,
                location=record.preparation.builder_location
                or record.preparation.resolution_location,
            )
        ),
        format_version=record.format_version,
        worker_count=(
            int(record.scheduler_metadata["max_workers"])
            if type(record.scheduler_metadata.get("max_workers")) is int
            and int(record.scheduler_metadata["max_workers"]) > 0
            else None
        ),
        task_slots_per_worker=(
            int(record.scheduler_metadata["task_slots_per_worker"])
            if type(record.scheduler_metadata.get("task_slots_per_worker")) is int
            and int(record.scheduler_metadata["task_slots_per_worker"]) > 0
            else None
        ),
        active_workers=(
            int(record.scheduler_metadata["active_workers"])
            if type(record.scheduler_metadata.get("active_workers")) is int
            else None
        ),
        throughput_tasks_per_second=(
            float(record.scheduler_metadata["throughput_tasks_per_second"])
            if type(record.scheduler_metadata.get("throughput_tasks_per_second"))
            in (int, float)
            else None
        ),
        eta_seconds=(
            float(record.scheduler_metadata["eta_seconds"])
            if type(record.scheduler_metadata.get("eta_seconds")) in (int, float)
            else None
        ),
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
    compact_member = (
        record.is_compact
        and record.task_space is not None
        and int(selected.value.removeprefix("task_")) < record.task_space.task_count
    )
    if not compact_member and selected not in {task.id for task in record.run.tasks}:
        return OperationError(
            "TASK_NOT_FOUND",
            f"Task {selected} does not belong to Run {record.run.id}",
            {"run_id": str(record.run.id), "task_id": str(selected)},
        )
    return selected


def _selected_task_ids(
    record: RunRecord, values: Sequence[str] | None
) -> tuple[TaskId, ...] | OperationError:
    if values is None:
        if record.is_compact and record.task_space is not None:
            return tuple(
                record.task_space.coordinate(ordinal).task_id
                for ordinal in range(record.task_space.task_count)
            )
        return tuple(task.id for task in record.run.tasks)
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        return OperationError("INVALID_TASK_ID", "Task selectors must be a sequence")
    if not values:
        return OperationError("TASK_REQUIRED", "Select at least one Task to fetch")
    selected: list[TaskId] = []
    for value in values:
        task_id = _selected_task_id(record, value)
        if isinstance(task_id, OperationError):
            return task_id
        if task_id in selected:
            return OperationError(
                "DUPLICATE_TASK",
                f"Task {task_id} was selected more than once",
                {"run_id": str(record.run.id), "task_id": str(task_id)},
            )
        selected.append(task_id)
    return tuple(selected)


def _task_retrieval_states(
    record: RunRecord, task_store: SqliteTaskStore | None = None
) -> dict[TaskId, RetrievalState]:
    if record.is_compact:
        if task_store is None:
            return {}
        return {
            state.coordinate.task_id: state.retrieval_state
            for state in task_store.all_states(record.run.id)
        }
    if record.task_retrieval_states:
        return dict(record.task_retrieval_states)
    return {task.id: record.run.retrieval_state for task in record.run.tasks}


def _selected_fetch_patterns(
    record: RunRecord, task_ids: tuple[TaskId, ...]
) -> tuple[str, ...]:
    if record.scheduler_metadata.get("result_shards") is True:
        return (".rundra-shards/*.tar", ".rundra-shards/*.sha256")
    if len(record.run.tasks) == 1:
        return record.experiment.outputs
    return tuple(
        f"{task_id}/{pattern}"
        for task_id in task_ids
        for pattern in record.experiment.outputs
    )


def _selected_fetch_artifacts(
    record: RunRecord,
    task_ids: tuple[TaskId, ...],
    artifacts: tuple[Artifact, ...],
) -> tuple[Artifact, ...]:
    allowed = {
        ArtifactKind.RAW_RESULT,
        ArtifactKind.STDOUT,
        ArtifactKind.STDERR,
        ArtifactKind.SCHEDULER_METADATA,
        ArtifactKind.REFERENCE_MANIFEST,
        ArtifactKind.OUTPUT_SHARD,
    }
    selected = set(task_ids)
    result: list[Artifact] = []
    for artifact in artifacts:
        if (
            artifact.kind is ArtifactKind.RAW_RESULT
            and ".rundra-shards" in artifact.path.parts
        ):
            artifact = replace(artifact, kind=ArtifactKind.OUTPUT_SHARD)
        if artifact.kind not in allowed:
            raise ValueError(f"unsupported artifact kind {artifact.kind.value}")
        task_id = artifact.task_id
        if task_id is None and artifact.kind is ArtifactKind.RAW_RESULT:
            if len(record.run.tasks) == 1:
                task_id = record.run.tasks[0].id
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
                        f"raw artifact path {artifact.path} does not identify a Task"
                    )
            artifact = replace(artifact, task_id=task_id)
        if task_id is not None and task_id not in selected:
            continue
        result.append(artifact)
    return tuple(result)


def _extract_fetched_shards(
    record: RunRecord,
    destination: Path,
    artifacts: tuple[Artifact, ...],
    selected: tuple[TaskId, ...] | None,
) -> tuple[Artifact, ...]:
    configured_host = record.run.target.transport.options.get("host")
    controller_hostname = configured_host if isinstance(configured_host, str) else None
    shard_paths = tuple(
        Path(artifact.path)
        for artifact in artifacts
        if artifact.kind is ArtifactKind.OUTPUT_SHARD
        and str(artifact.path).endswith(".tar")
    )
    selected_names = (
        None if selected is None else {str(task_id): task_id for task_id in selected}
    )
    covered: set[str] = set()
    extracted_artifacts: list[Artifact] = []
    output_root = destination / "output"
    for shard in shard_paths:
        index = read_verified_shard_index(
            shard, controller_hostname=controller_hostname
        )
        shard_tasks = tuple(
            task_id
            for task_id in index.task_exit_codes
            if selected_names is None or task_id in selected_names
        )
        if not shard_tasks:
            continue
        for path in extract_shard(
            shard,
            output_root,
            task_ids=shard_tasks,
            controller_hostname=controller_hostname,
        ):
            task_name = path.relative_to(output_root).parts[0]
            task_id = (
                TaskId(task_name)
                if selected_names is None
                else selected_names[task_name]
            )
            extracted_artifacts.append(
                Artifact(
                    ArtifactKind.RAW_RESULT,
                    path,
                    task_id=task_id,
                    size_bytes=path.stat().st_size,
                )
            )
        covered.update(shard_tasks)
    missing = set() if selected_names is None else set(selected_names) - covered
    if missing:
        raise ValueError(f"Result shards do not contain selected Task {min(missing)}")
    return tuple(extracted_artifacts)


def _verified_shard_rows(
    record: RunRecord, shard_paths: Sequence[Path]
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


def _record_workspace(record: RunRecord) -> StagedWorkspace:
    if record.run.target.staging.kind == "local":
        root: PurePath = (
            Path(str(record.run.target.workspace)).expanduser().resolve()
            / "runs"
            / str(record.run.id)
        )
    else:
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


def _finalize_remote_preparation(
    record: RunRecord,
    store: RunStore,
    *,
    transport: Transport | None = None,
) -> RunRecord:
    preparation = record.preparation
    if (
        preparation is None
        or preparation.builder_status != ExecutionState.SUCCEEDED.value
        or preparation.image_action != "resolve_in_preparation_job"
        or record.run.target.transport.kind != "ssh"
    ):
        return record
    active_transport = transport or _record_ssh_transport(record)
    result = read_remote_preparation_result(
        active_transport,
        _record_workspace(record),
    )
    if result is None:
        raise PreparationError("Completed preparation manifest is unavailable")
    updated = replace(
        record,
        preparation=replace(
            preparation,
            image_action=result.image_action,
            build_action=result.build_action,
            build_outputs=result.outputs,
        ),
    )
    store.update(updated, expected=record)
    return updated


def _record_stager(record: RunRecord) -> Stager:
    if record.run.target.staging.kind == "local":
        return LocalStager()
    if record.run.target.staging.kind == "rsync":
        transport = _record_ssh_transport(record)
        host = record.run.target.transport.options.get("host")
        if type(host) is not str:
            raise ValueError("Persisted rsync target host is unavailable")
        executable, config_file = _target_ssh_selection(record.run.target)
        return RsyncStager(
            transport,
            host=host,
            ssh_executable=executable,
            ssh_config_file=config_file,
        )
    if record.run.target.staging.kind == "shared":
        root = record.run.target.staging.options.get("root")
        if type(root) is not str:
            raise ValueError("Persisted shared staging root is unavailable")
        return SharedStager(PurePath(root))
    raise ValueError(
        f"Persisted staging backend {record.run.target.staging.kind!r} cannot fetch"
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


def _record_purger(record: RunRecord) -> LocalPurger | SSHPurger:
    if record.run.target.staging.kind in {"local", "shared"}:
        return LocalPurger()
    if record.run.target.transport.kind == "ssh":
        return SSHPurger(_record_ssh_transport(record))
    raise ValueError(
        f"Persisted target {record.run.target.name!r} cannot purge Run data"
    )


def _launch_resolution_value(
    resolved: ResolvedLaunch,
    fields: tuple[str, ...],
) -> LaunchResolutionValue:
    values: dict[str, LaunchOutputValue] = {}
    sources: dict[str, str] = {}
    for field in fields:
        value = getattr(resolved.values, field)
        if value is None and field in {
            "workers",
            "task_slots_per_worker",
            "fetch_mode",
        }:
            continue
        if value is None or field not in resolved.sources:
            raise ValueError(f"Resolved launch field is unavailable: {field}")
        values[field] = str(value) if isinstance(value, Path) else value
        sources[field] = resolved.sources[field]
    return LaunchResolutionValue(resolved.profile, values, sources)


def _execution_seed_values(*, seed: object, seeds: object) -> tuple[int, ...]:
    if type(seeds) is SeedRange:
        return tuple(seeds.at(ordinal) for ordinal in range(seeds.count))
    if isinstance(seeds, tuple):
        if seed is not None:
            raise PlanningError(
                code="SEED_CONFLICT",
                message="seed and seeds are mutually exclusive",
            )
        if not seeds or any(type(value) is not int for value in seeds):
            raise PlanningError(
                code="INVALID_SEED_RANGE",
                message="resolved seeds must be a nonempty integer tuple",
            )
        return seeds
    return expand_seeds(seed=seed, seeds=seeds)


def _execution_seed_range(*, seed: object, seeds: object) -> SeedRange:
    if type(seeds) is SeedRange:
        if seed is not None:
            raise PlanningError(
                code="SEED_CONFLICT", message="seed and seeds are mutually exclusive"
            )
        return seeds
    if isinstance(seeds, tuple):
        if seed is not None:
            raise PlanningError(
                code="SEED_CONFLICT", message="seed and seeds are mutually exclusive"
            )
        return _seed_range_from_values(seeds)
    return compact_seed_range(seed=seed, seeds=seeds)


def _execution_seed_count(*, seed: object, seeds: object) -> int:
    if type(seeds) is SeedRange:
        if seed is not None:
            raise PlanningError(
                code="SEED_CONFLICT", message="seed and seeds are mutually exclusive"
            )
        return seeds.count
    if isinstance(seeds, tuple):
        if seed is not None:
            raise PlanningError(
                code="SEED_CONFLICT", message="seed and seeds are mutually exclusive"
            )
        if not seeds:
            raise PlanningError(
                code="SEED_REQUIRED", message="Provide one seed or range"
            )
        return len(seeds)
    return compact_seed_range(seed=seed, seeds=seeds).count


def _unsupported_execution_target(
    target: Target,
    experiment: ExperimentSpec,
    *,
    asynchronous: bool = False,
) -> OperationError | None:
    actual = (
        target.transport.kind,
        target.scheduler.kind,
        target.staging.kind,
        target.container.kind,
    )
    if actual[:3] == ("local", "local", "local") and actual[3] in {
        "apptainer",
        "native",
    }:
        if asynchronous:
            return OperationError(
                "ASYNC_UNAVAILABLE",
                (
                    "Asynchronous submit requires SSH with a detached scheduler, "
                    "rsync or shared staging, and Apptainer"
                ),
                {"target": target.name},
            )
        if actual[3] == "native" and experiment.container is not None:
            return OperationError(
                "CONTAINER_CONFLICT",
                "Native target cannot satisfy an experiment container request",
                {"target": target.name},
            )
        if actual[3] == "apptainer" and experiment.container is None:
            return OperationError(
                "CONTAINER_REQUIRED",
                "Apptainer target requires an experiment container image",
                {"target": target.name},
            )
    elif actual in {
        ("ssh", "slurm", "rsync", "apptainer"),
        ("ssh", "slurm", "shared", "apptainer"),
        ("ssh", "pbs", "rsync", "apptainer"),
        ("ssh", "pbs", "shared", "apptainer"),
        ("ssh", "htcondor", "rsync", "apptainer"),
        ("ssh", "htcondor", "shared", "apptainer"),
    }:
        if experiment.container is None:
            return OperationError(
                "CONTAINER_REQUIRED",
                "The remote scheduler path requires an experiment container image",
                {"target": target.name},
            )
    else:
        return OperationError(
            "TARGET_UNSUPPORTED",
            f"Execution does not support target '{target.name}'",
            {"target": target.name},
        )

    resources = experiment.resources
    if target.scheduler.kind == "local" and resources.native:
        return OperationError(
            "NATIVE_OPTIONS_UNSUPPORTED",
            "Local execution does not accept backend-native resource options",
            {"target": target.name},
        )
    if target.scheduler.kind != "local":
        try:
            validate_scheduler_resources(target.scheduler.kind, resources)
        except (TypeError, ValueError, RuntimeError) as error:
            return OperationError(
                "NATIVE_OPTIONS_UNSUPPORTED",
                str(error),
                {"target": target.name, "scheduler": target.scheduler.kind},
            )

    container_gpu = (
        experiment.container.gpu if experiment.container is not None else False
    )
    requested_gpus = resources.gpus_per_task
    if requested_gpus > 0 and not container_gpu:
        return OperationError(
            "GPU_CONFIGURATION_MISMATCH",
            "GPU resources require container.gpu: true for device passthrough",
            {"gpus_per_task": requested_gpus, "target": target.name},
        )
    if container_gpu and requested_gpus == 0:
        return OperationError(
            "GPU_CONFIGURATION_MISMATCH",
            "container.gpu: true requires a positive resources.gpus_per_task request",
            {"gpus_per_task": requested_gpus, "target": target.name},
        )
    return None


def _execution_adapters(
    target: Target,
) -> tuple[Transport, Stager, ContainerRuntime, Scheduler]:
    container_executable = _target_container_executable(target)
    if target.transport.kind == "local":
        transport = LocalTransport()
        runtime: ContainerRuntime = (
            NativeRuntime()
            if target.container.kind == "native"
            else ApptainerRuntime(container_executable)
        )
        return transport, LocalStager(), runtime, LocalScheduler(transport)
    host = target.transport.options.get("host")
    if type(host) is not str:
        raise ValueError("SSH target host is unavailable")
    executable, config_file = _target_ssh_selection(target)
    remote_transport = SSHTransport(
        host, executable=executable, config_file=config_file
    )
    stager: Stager
    if target.staging.kind == "shared":
        root = target.staging.options.get("root")
        if type(root) is not str:
            raise ValueError("Shared staging root is unavailable")
        stager = SharedStager(PurePath(root))
    else:
        stager = RsyncStager(
            remote_transport,
            host=host,
            ssh_executable=executable,
            ssh_config_file=config_file,
        )
    scheduler = scheduler_for_target(
        target,
        remote_transport,
        log_directory=target.workspace / ".rundra-scheduler-logs",
    )
    return (
        remote_transport,
        stager,
        RemoteApptainerRuntime(remote_transport, container_executable),
        scheduler,
    )


def _record_scheduler(record: RunRecord) -> Scheduler:
    transport = _record_ssh_transport(record)
    return scheduler_for_target(
        record.run.target,
        transport,
        log_directory=record.run.target.workspace / ".rundra-scheduler-logs",
    )


def _record_ssh_transport(record: RunRecord) -> SSHTransport:
    host = record.run.target.transport.options.get("host")
    if type(host) is not str:
        raise ValueError("Persisted SSH target host is unavailable")
    executable, config_file = _target_ssh_selection(record.run.target)
    return SSHTransport(host, executable=executable, config_file=config_file)


def _target_ssh_selection(target: Target) -> tuple[str, PurePath | None]:
    executable = target.transport.options.get("executable", "ssh")
    config_file = target.transport.options.get("config_file")
    if type(executable) is not str:
        raise ValueError("SSH target executable is invalid")
    if config_file is not None and type(config_file) is not str:
        raise ValueError("SSH target config_file is invalid")
    return executable, None if config_file is None else PurePath(config_file)


def _target_container_executable(target: Target) -> str:
    executable = target.container.options.get("executable", "apptainer")
    if type(executable) is not str:
        raise ValueError("Container target executable is invalid")
    return executable


def _read_remote_log(transport: Transport, path: PurePath) -> str:
    result = transport.run(Command(("cat", "--", str(path))))
    if result.exit_code != 0:
        raise RuntimeError(
            f"Remote log {path} is unavailable (exit code {result.exit_code})"
        )
    return result.stdout


def _config_error(error: ConfigError) -> OperationError:
    return OperationError(
        error.code,
        error.message,
        {"source": str(error.source), "path": error.path},
    )
