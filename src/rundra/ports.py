from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from math import isfinite
from pathlib import PurePath
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import (
    Artifact,
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    NativeValue,
    ResourceRequest,
    RunId,
    Target,
    TaskId,
)
from rundra.domain.scaling import TaskSpace
from rundra.domain.states import ExecutionState
from rundra.domain.storage import SlurmScratchPolicy


@dataclass(frozen=True, slots=True)
class SchedulerPartition:
    """Bounded scheduler partition information for operator diagnostics."""

    name: str
    default: bool
    availability: str
    max_walltime_seconds: int | None
    max_walltime_raw: str
    gres: str
    node_count: int | None = None
    cpu_allocated: int | None = None
    cpu_idle: int | None = None
    cpu_other: int | None = None
    cpu_total: int | None = None

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name.strip():
            raise ValueError("Scheduler partition name must be nonblank")
        if type(self.default) is not bool:
            raise TypeError("Scheduler partition default must be bool")
        for field_name in ("availability", "max_walltime_raw", "gres"):
            if type(getattr(self, field_name)) is not str:
                raise TypeError(f"Scheduler partition {field_name} must be a string")
        if self.max_walltime_seconds is not None and (
            type(self.max_walltime_seconds) is not int or self.max_walltime_seconds < 1
        ):
            raise ValueError("Scheduler partition walltime must be positive or None")
        for field_name in (
            "node_count",
            "cpu_allocated",
            "cpu_idle",
            "cpu_other",
            "cpu_total",
        ):
            value = getattr(self, field_name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(
                    f"Scheduler partition {field_name} must be nonnegative or None"
                )
        cpu_values = (
            self.cpu_allocated,
            self.cpu_idle,
            self.cpu_other,
            self.cpu_total,
        )
        if any(value is None for value in cpu_values) != all(
            value is None for value in cpu_values
        ):
            raise ValueError("Scheduler partition CPU observations must be complete")
        if self.cpu_total is not None:
            assert self.cpu_allocated is not None
            assert self.cpu_idle is not None
            assert self.cpu_other is not None
            if self.cpu_allocated + self.cpu_idle + self.cpu_other != self.cpu_total:
                raise ValueError(
                    "Scheduler partition CPU observations are inconsistent"
                )

    @property
    def utilization_percent(self) -> int | None:
        if self.cpu_total in {None, 0}:
            return None
        assert self.cpu_allocated is not None
        return (100 * self.cpu_allocated) // self.cpu_total


@runtime_checkable
class SchedulerInventoryProvider(Protocol):
    """Optional read-only scheduler partition inventory capability."""

    def inventory(self) -> tuple[SchedulerPartition, ...]: ...


class SchedulerSubmissionOutcome(StrEnum):
    """Portable classification for a failed scheduler submission attempt."""

    REJECTED = "rejected"
    UNCERTAIN = "uncertain"


class SchedulerSubmissionFailure(RuntimeError):
    """Safe scheduler-submission failure exposed across adapter boundaries."""

    def __init__(
        self,
        message: str,
        *,
        backend: str,
        phase: str,
        outcome: SchedulerSubmissionOutcome,
        exit_code: int | None = None,
    ) -> None:
        if type(message) is not str or not message.strip():
            raise ValueError("Scheduler submission failure message must be nonblank")
        for name, value in (("backend", backend), ("phase", phase)):
            if type(value) is not str or not value.strip() or "\x00" in value:
                raise ValueError(
                    f"Scheduler submission failure {name} must be nonblank and safe"
                )
        if type(outcome) is not SchedulerSubmissionOutcome:
            raise TypeError("Scheduler submission outcome must be portable")
        if exit_code is not None and type(exit_code) is not int:
            raise TypeError("Scheduler submission exit_code must be an integer or None")
        self.backend = backend
        self.phase = phase
        self.outcome = outcome
        self.exit_code = exit_code
        super().__init__(message)


def _freeze_metadata(
    value: Mapping[str, NativeValue],
) -> Mapping[str, NativeValue]:
    if not isinstance(value, Mapping) or any(
        type(key) is not str
        or not key.strip()
        or "\x00" in key
        or type(item) not in (str, int, float, bool)
        for key, item in value.items()
    ):
        raise TypeError("Adapter metadata must map strings to scalar values")
    if any(type(item) is float and not isfinite(item) for item in value.values()):
        raise ValueError("Adapter metadata float values must be finite")
    return MappingProxyType(dict(value))


def _freeze_artifacts(value: object) -> tuple[Artifact, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("Artifacts must be a sequence")
    artifacts = tuple(value)
    if any(type(artifact) is not Artifact for artifact in artifacts):
        raise TypeError("Artifacts must contain only Artifact values")
    return artifacts


@dataclass(frozen=True, slots=True)
class CapabilityCheck:
    """Successful availability check for a configured adapter."""

    name: str
    version: str | None = None

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name.strip():
            raise ValueError("CapabilityCheck name must be a nonblank string")
        if self.version is not None and type(self.version) is not str:
            raise TypeError("CapabilityCheck version must be a string or None")


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Captured outcome of one argument-vector command."""

    command: Command
    exit_code: int
    stdout: str
    stderr: str
    started_at: datetime
    finished_at: datetime

    def __post_init__(self) -> None:
        if type(self.command) is not Command:
            raise TypeError("CommandResult command must be a Command")
        if type(self.exit_code) is not int:
            raise TypeError("CommandResult exit_code must be an integer")
        if type(self.stdout) is not str or type(self.stderr) is not str:
            raise TypeError("CommandResult output must be strings")
        for name in ("started_at", "finished_at"):
            value = getattr(self, name)
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError(f"CommandResult {name} must be timezone-aware")
        if self.finished_at < self.started_at:
            raise ValueError("CommandResult cannot finish before it starts")


@dataclass(frozen=True, slots=True)
class SchedulerUnit:
    """Minimal normalized command and resources for one logical Task."""

    task_id: TaskId
    command: Command
    resources: ResourceRequest

    def __post_init__(self) -> None:
        if type(self.task_id) is not TaskId:
            raise TypeError("SchedulerUnit task_id must be a TaskId")
        if type(self.command) is not Command:
            raise TypeError("SchedulerUnit command must be a Command")
        if type(self.resources) is not ResourceRequest:
            raise TypeError("SchedulerUnit resources must be a ResourceRequest")


@dataclass(frozen=True, slots=True)
class AllocationScratch:
    """Immutable shared-to-allocation storage mapping for one scheduler job."""

    shared_root: PurePath
    policy: SlurmScratchPolicy
    image_path: PurePath | None = None
    task_directories: bool = True

    def __post_init__(self) -> None:
        if (
            not isinstance(self.shared_root, PurePath)
            or not self.shared_root.is_absolute()
            or self.shared_root == PurePath("/")
            or "\x00" in str(self.shared_root)
        ):
            raise ValueError("Allocation scratch shared_root must be absolute and safe")
        if type(self.policy) is not SlurmScratchPolicy:
            raise TypeError("Allocation scratch policy must be a SlurmScratchPolicy")
        if self.image_path is not None and (
            not isinstance(self.image_path, PurePath)
            or not self.image_path.is_absolute()
            or self.image_path == PurePath("/")
            or "\x00" in str(self.image_path)
        ):
            raise ValueError("Allocation scratch image_path must be absolute and safe")
        if type(self.task_directories) is not bool:
            raise TypeError("Allocation scratch task_directories must be bool")


@dataclass(frozen=True, slots=True)
class SchedulerGroup:
    """One nonempty scheduler submission with explicit logical Task members."""

    units: tuple[SchedulerUnit, ...]
    scratch: AllocationScratch | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.units, Sequence) or isinstance(self.units, (str, bytes)):
            raise TypeError("SchedulerGroup units must be a sequence")
        units = tuple(self.units)
        if any(type(unit) is not SchedulerUnit for unit in units):
            raise TypeError("SchedulerGroup units must contain SchedulerUnits")
        if not units:
            raise ValueError("SchedulerGroup must contain at least one unit")
        if len({unit.task_id for unit in units}) != len(units):
            raise ValueError("SchedulerGroup Task IDs must be unique")
        if self.scratch is not None and type(self.scratch) is not AllocationScratch:
            raise TypeError("SchedulerGroup scratch must be AllocationScratch or None")
        object.__setattr__(self, "units", units)


@dataclass(frozen=True, slots=True)
class SchedulerArrayRequest:
    """Backend-neutral request to submit an explicitly mapped Task array."""

    group: SchedulerGroup
    mapping: tuple[ArrayTaskMapping, ...]
    manifest_path: PurePath
    allow_duplicate_seeds: bool = False
    max_concurrent_jobs: int | None = None
    max_workers: int | None = None
    task_slots_per_worker: int = 1
    output_root: PurePath | None = None
    shard_root: PurePath | None = None
    worker_resources: ResourceRequest | None = None
    completion_observer: Callable[[TaskId, int, int], None] | None = None

    def __post_init__(self) -> None:
        if type(self.group) is not SchedulerGroup:
            raise TypeError("SchedulerArrayRequest group must be a SchedulerGroup")
        if len(self.group.units) < 2:
            raise ValueError("SchedulerArrayRequest requires at least two Tasks")
        if not isinstance(self.mapping, Sequence) or isinstance(
            self.mapping, (str, bytes)
        ):
            raise TypeError("SchedulerArrayRequest mapping must be a sequence")
        mapping = tuple(self.mapping)
        if any(type(item) is not ArrayTaskMapping for item in mapping):
            raise TypeError(
                "SchedulerArrayRequest mapping must contain ArrayTaskMappings"
            )
        if tuple(item.task_id for item in mapping) != tuple(
            unit.task_id for unit in self.group.units
        ):
            raise ValueError(
                "SchedulerArrayRequest mapping must match SchedulerGroup order"
            )
        if type(self.allow_duplicate_seeds) is not bool:
            raise TypeError("SchedulerArrayRequest allow_duplicate_seeds must be bool")
        if not self.allow_duplicate_seeds and len(
            {item.seed for item in mapping}
        ) != len(mapping):
            raise ValueError("SchedulerArrayRequest mapping seeds must be unique")
        if not isinstance(self.manifest_path, PurePath):
            raise TypeError("SchedulerArrayRequest manifest_path must be a path")
        if not self.manifest_path.is_absolute() or "\x00" in str(self.manifest_path):
            raise ValueError(
                "SchedulerArrayRequest manifest_path must be absolute and safe"
            )
        if self.max_concurrent_jobs is not None and (
            type(self.max_concurrent_jobs) is not int or self.max_concurrent_jobs < 1
        ):
            raise ValueError(
                "SchedulerArrayRequest max_concurrent_jobs must be positive or None"
            )
        if self.max_workers is not None and (
            type(self.max_workers) is not int or self.max_workers < 1
        ):
            raise ValueError(
                "SchedulerArrayRequest max_workers must be positive or None"
            )
        if (
            type(self.task_slots_per_worker) is not int
            or self.task_slots_per_worker < 1
        ):
            raise ValueError(
                "SchedulerArrayRequest task_slots_per_worker must be positive"
            )
        if (self.output_root is None) != (self.shard_root is None):
            raise ValueError("SchedulerArrayRequest shard paths must be set together")
        if (
            self.worker_resources is not None
            and type(self.worker_resources) is not ResourceRequest
        ):
            raise TypeError(
                "SchedulerArrayRequest worker_resources must be a ResourceRequest"
            )
        if self.completion_observer is not None and not callable(
            self.completion_observer
        ):
            raise TypeError(
                "SchedulerArrayRequest completion_observer must be callable or None"
            )
        for name in ("output_root", "shard_root"):
            path = getattr(self, name)
            if path is not None and (
                not isinstance(path, PurePath)
                or not path.is_absolute()
                or path == PurePath("/")
                or "\x00" in str(path)
            ):
                raise ValueError(f"SchedulerArrayRequest {name} must be absolute")
        object.__setattr__(self, "mapping", mapping)


@dataclass(frozen=True, slots=True)
class CompactSchedulerArrayRequest:
    """Constant-size worker-pool request for one logical TaskSpace."""

    task_space: TaskSpace
    commands: tuple[Command, ...]
    resources: ResourceRequest
    worker_resources: ResourceRequest
    manifest_path: PurePath
    worker_count: int
    task_slots_per_worker: int = 1
    output_root: PurePath | None = None
    shard_root: PurePath | None = None
    infrastructure_retry_limit: int = 0
    requeue_limit: int = 0
    scratch: AllocationScratch | None = None

    def __post_init__(self) -> None:
        if type(self.task_space) is not TaskSpace:
            raise TypeError("Compact scheduler request requires a TaskSpace")
        commands = tuple(self.commands)
        if len(commands) != self.task_space.parameter_set_count or any(
            type(command) is not Command for command in commands
        ):
            raise ValueError("Compact scheduler commands must match the parameter sets")
        if (
            type(self.resources) is not ResourceRequest
            or type(self.worker_resources) is not ResourceRequest
        ):
            raise TypeError("Compact scheduler resources must be ResourceRequests")
        if (
            not isinstance(self.manifest_path, PurePath)
            or not self.manifest_path.is_absolute()
            or "\x00" in str(self.manifest_path)
        ):
            raise ValueError(
                "Compact scheduler manifest path must be absolute and safe"
            )
        for name in ("worker_count", "task_slots_per_worker"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= self.task_space.task_count:
                raise ValueError(f"Compact scheduler {name} must fit the TaskSpace")
        if self.worker_count * self.task_slots_per_worker > self.task_space.task_count:
            raise ValueError("Compact scheduler capacity exceeds the TaskSpace")
        for name in ("infrastructure_retry_limit", "requeue_limit"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"Compact scheduler {name} must be non-negative")
        if (self.output_root is None) != (self.shard_root is None):
            raise ValueError("Compact scheduler shard paths must be set together")
        for name in ("output_root", "shard_root"):
            path = getattr(self, name)
            if path is not None and (
                not isinstance(path, PurePath)
                or not path.is_absolute()
                or path == PurePath("/")
                or "\x00" in str(path)
            ):
                raise ValueError(f"Compact scheduler {name} must be absolute")
        if self.scratch is not None and type(self.scratch) is not AllocationScratch:
            raise TypeError(
                "Compact scheduler scratch must be AllocationScratch or None"
            )
        object.__setattr__(self, "commands", commands)


@dataclass(frozen=True, slots=True)
class SchedulerReference:
    """Opaque scheduler identity kept separate from Run and Task IDs."""

    native_id: str

    def __post_init__(self) -> None:
        if type(self.native_id) is not str:
            raise TypeError("SchedulerReference native_id must be a string")
        if not self.native_id.strip() or "\x00" in self.native_id:
            raise ValueError("SchedulerReference native_id must be nonblank and safe")


@dataclass(frozen=True, slots=True)
class SchedulerSubmission:
    """Scheduler reference plus its explicit logical Task mapping."""

    reference: SchedulerReference
    task_native_ids: Mapping[TaskId, str]
    additional_references: tuple[SchedulerReference, ...] = ()

    def __post_init__(self) -> None:
        if type(self.reference) is not SchedulerReference:
            raise TypeError("SchedulerSubmission reference must be a reference")
        if not isinstance(self.task_native_ids, Mapping):
            raise TypeError("Scheduler Task mapping must be a mapping")
        mapping = dict(self.task_native_ids)
        if any(
            type(task_id) is not TaskId or type(native_id) is not str
            for task_id, native_id in mapping.items()
        ):
            raise TypeError("Scheduler Task mapping must map TaskIds to native IDs")
        if not mapping:
            raise ValueError("Scheduler Task mapping must not be empty")
        if any(
            not native_id.strip() or "\x00" in native_id
            for native_id in mapping.values()
        ):
            raise ValueError("Scheduler Task native IDs must be nonblank and safe")
        object.__setattr__(self, "task_native_ids", MappingProxyType(mapping))
        if self.reference in self.additional_references:
            raise ValueError("Additional scheduler references must exclude the primary")
        if len(set(self.additional_references)) != len(self.additional_references):
            raise ValueError("Additional scheduler references must be unique")

    @property
    def references(self) -> tuple[SchedulerReference, ...]:
        """Return every scheduler root created for this logical submission."""

        return (self.reference, *self.additional_references)


@dataclass(frozen=True, slots=True)
class CompactSchedulerSubmission:
    """Bounded scheduler roots and worker identities for a TaskSpace."""

    reference: SchedulerReference
    task_space: TaskSpace
    worker_native_ids: tuple[str, ...]
    additional_references: tuple[SchedulerReference, ...] = ()

    def __post_init__(self) -> None:
        if type(self.reference) is not SchedulerReference:
            raise TypeError("Compact submission reference must be a reference")
        if type(self.task_space) is not TaskSpace:
            raise TypeError("Compact submission requires a TaskSpace")
        workers = tuple(self.worker_native_ids)
        if not workers or any(
            type(value) is not str or not value.strip() or "\x00" in value
            for value in workers
        ):
            raise ValueError("Compact submission worker identities must be safe")
        if len(set(workers)) != len(workers):
            raise ValueError("Compact submission worker identities must be unique")
        references = tuple(self.additional_references)
        if any(type(item) is not SchedulerReference for item in references):
            raise TypeError("Compact additional references must be references")
        if self.reference in references or len(set(references)) != len(references):
            raise ValueError("Compact additional references must be distinct")
        object.__setattr__(self, "worker_native_ids", workers)
        object.__setattr__(self, "additional_references", references)

    @property
    def references(self) -> tuple[SchedulerReference, ...]:
        return (self.reference, *self.additional_references)


@dataclass(frozen=True, slots=True)
class SchedulerObservation:
    """Portable state with separately preserved native accounting data."""

    reference: SchedulerReference
    state: ExecutionState
    native_state: str
    exit_code: int | None = None
    metadata: Mapping[str, NativeValue] = field(default_factory=dict)
    result: CommandResult | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        if type(self.reference) is not SchedulerReference:
            raise TypeError("SchedulerObservation reference must be a reference")
        if type(self.state) is not ExecutionState:
            raise TypeError("SchedulerObservation state must be portable")
        if self.state in {ExecutionState.CREATED, ExecutionState.STAGING}:
            raise ValueError("SchedulerObservation state must be scheduler-owned")
        if (
            type(self.native_state) is not str
            or not self.native_state.strip()
            or "\x00" in self.native_state
        ):
            raise ValueError(
                "SchedulerObservation native_state must be nonblank and safe"
            )
        if self.exit_code is not None and type(self.exit_code) is not int:
            raise TypeError("SchedulerObservation exit_code must be an integer or None")
        if self.result is not None and type(self.result) is not CommandResult:
            raise TypeError(
                "SchedulerObservation result must be a CommandResult or None"
            )
        if (
            self.result is not None
            and self.exit_code is not None
            and self.result.exit_code != self.exit_code
        ):
            raise ValueError(
                "SchedulerObservation result and exit_code must describe the same exit"
            )
        started_at = self.started_at
        finished_at = self.finished_at
        for name, value in (
            ("started_at", started_at),
            ("finished_at", finished_at),
        ):
            if value is not None and (
                not isinstance(value, datetime) or value.utcoffset() is None
            ):
                raise ValueError(
                    f"SchedulerObservation {name} must be timezone-aware or None"
                )
        if self.result is not None:
            if started_at is None:
                started_at = self.result.started_at
            elif started_at != self.result.started_at:
                raise ValueError(
                    "SchedulerObservation result and started_at must agree"
                )
            if finished_at is None:
                finished_at = self.result.finished_at
            elif finished_at != self.result.finished_at:
                raise ValueError(
                    "SchedulerObservation result and finished_at must agree"
                )
        if (
            started_at is not None
            and finished_at is not None
            and finished_at < started_at
        ):
            raise ValueError("SchedulerObservation cannot finish before it starts")
        terminal = self.state in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
        if not terminal and (
            self.exit_code is not None
            or self.result is not None
            or finished_at is not None
        ):
            raise ValueError(
                "Nonterminal SchedulerObservation cannot have an exit, result, "
                "or finish time"
            )
        if self.state is ExecutionState.SUCCEEDED and self.exit_code not in {None, 0}:
            raise ValueError("Successful SchedulerObservation cannot have nonzero exit")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "finished_at", finished_at)
        object.__setattr__(self, "metadata", _freeze_metadata(self.metadata))


@dataclass(frozen=True, slots=True)
class StageRequest:
    """Semantic inputs needed to create one isolated Run workspace."""

    run_id: RunId
    experiment: ExperimentSpec
    config: ConfigSnapshot
    target: Target
    source_root: PurePath
    task_ids: tuple[TaskId, ...] = ()
    task_configs: Mapping[TaskId, ConfigSnapshot] = field(default_factory=dict)
    task_manifest: str | None = None
    remote_source_root: PurePath | None = None

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise TypeError("StageRequest run_id must be a RunId")
        if type(self.experiment) is not ExperimentSpec:
            raise TypeError("StageRequest experiment must be an ExperimentSpec")
        if type(self.config) is not ConfigSnapshot:
            raise TypeError("StageRequest config must be a ConfigSnapshot")
        if type(self.target) is not Target:
            raise TypeError("StageRequest target must be a Target")
        if not isinstance(self.source_root, PurePath):
            raise TypeError("StageRequest source_root must be a PurePath")
        if not isinstance(self.task_ids, Sequence) or isinstance(
            self.task_ids, (str, bytes)
        ):
            raise TypeError("StageRequest task_ids must be a sequence")
        task_ids = tuple(self.task_ids)
        if any(type(task_id) is not TaskId for task_id in task_ids):
            raise TypeError("StageRequest task_ids must contain only TaskIds")
        if len(set(task_ids)) != len(task_ids):
            raise ValueError("StageRequest task_ids must be unique")
        task_configs = dict(self.task_configs)
        if any(
            type(task_id) is not TaskId or type(config) is not ConfigSnapshot
            for task_id, config in task_configs.items()
        ):
            raise TypeError("StageRequest task_configs must map TaskIds to configs")
        if task_configs and set(task_configs) != set(task_ids):
            raise ValueError("StageRequest task_configs must match task_ids")
        if self.task_manifest is not None and type(self.task_manifest) is not str:
            raise TypeError("StageRequest task_manifest must be a string or None")
        if self.remote_source_root is not None and (
            not isinstance(self.remote_source_root, PurePath)
            or not self.remote_source_root.is_absolute()
            or self.remote_source_root == PurePath("/")
            or "\x00" in str(self.remote_source_root)
        ):
            raise ValueError(
                "StageRequest remote_source_root must be an absolute safe path or None"
            )
        object.__setattr__(self, "task_ids", task_ids)
        object.__setattr__(self, "task_configs", MappingProxyType(task_configs))


@dataclass(frozen=True, slots=True)
class StagedWorkspace:
    """Semantic paths returned by a staging adapter."""

    root: PurePath
    source: PurePath
    inputs: PurePath
    config: PurePath
    runtime: PurePath
    outputs: PurePath
    logs: PurePath
    metadata: PurePath
    artifacts: tuple[Artifact, ...] = ()
    task_configs: Mapping[TaskId, PurePath] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "root",
            "source",
            "inputs",
            "config",
            "runtime",
            "outputs",
            "logs",
            "metadata",
        ):
            if not isinstance(getattr(self, name), PurePath):
                raise TypeError(f"StagedWorkspace {name} must be a PurePath")
        object.__setattr__(self, "artifacts", _freeze_artifacts(self.artifacts))
        task_configs = dict(self.task_configs)
        if any(
            type(task_id) is not TaskId or not isinstance(path, PurePath)
            for task_id, path in task_configs.items()
        ):
            raise TypeError("StagedWorkspace task_configs must map TaskIds to paths")
        object.__setattr__(self, "task_configs", MappingProxyType(task_configs))

    def for_task(self, task_id: TaskId) -> TaskWorkspace:
        """Derive isolated mutable paths for one logical Task."""
        if type(task_id) is not TaskId:
            raise TypeError("StagedWorkspace task_id must be a TaskId")
        name = str(task_id)
        return TaskWorkspace(
            task_id=task_id,
            source=self.source,
            config=self.task_configs.get(task_id, self.config),
            runtime=self.runtime / name,
            outputs=self.outputs / name,
            stdout=self.logs / f"{name}.stdout",
            stderr=self.logs / f"{name}.stderr",
            metadata=self.metadata / name,
        )


@dataclass(frozen=True, slots=True)
class TaskWorkspace:
    """Shared immutable inputs and isolated mutable paths for one Task."""

    task_id: TaskId
    source: PurePath
    config: PurePath
    runtime: PurePath
    outputs: PurePath
    stdout: PurePath
    stderr: PurePath
    metadata: PurePath

    def __post_init__(self) -> None:
        if type(self.task_id) is not TaskId:
            raise TypeError("TaskWorkspace task_id must be a TaskId")
        for name in (
            "source",
            "config",
            "runtime",
            "outputs",
            "stdout",
            "stderr",
            "metadata",
        ):
            if not isinstance(getattr(self, name), PurePath):
                raise TypeError(f"TaskWorkspace {name} must be a PurePath")


@dataclass(frozen=True, slots=True)
class FetchRequest:
    workspace: StagedWorkspace
    patterns: tuple[str, ...]
    destination: PurePath
    mode: str = "copy"

    def __post_init__(self) -> None:
        if type(self.workspace) is not StagedWorkspace:
            raise TypeError("FetchRequest workspace must be a StagedWorkspace")
        if not isinstance(self.destination, PurePath):
            raise TypeError("FetchRequest destination must be a PurePath")
        if not isinstance(self.patterns, Sequence) or isinstance(
            self.patterns, (str, bytes)
        ):
            raise TypeError("FetchRequest patterns must be a sequence")
        patterns = tuple(self.patterns)
        if any(type(pattern) is not str or not pattern for pattern in patterns):
            raise ValueError("FetchRequest patterns must be nonempty strings")
        if any(
            PurePath(pattern).is_absolute() or ".." in PurePath(pattern).parts
            for pattern in patterns
        ):
            raise ValueError("FetchRequest patterns must be safe relative patterns")
        if self.mode not in {"auto", "copy", "reference", "archive"}:
            raise ValueError(
                "FetchRequest mode must be auto, copy, reference, or archive"
            )
        object.__setattr__(self, "patterns", patterns)


@dataclass(frozen=True, slots=True)
class FetchResult:
    artifacts: tuple[Artifact, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifacts", _freeze_artifacts(self.artifacts))


@dataclass(frozen=True, slots=True)
class BindMount:
    """Portable host-to-container path mapping with explicit access mode."""

    source: PurePath
    destination: PurePath
    read_only: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.source, PurePath):
            raise TypeError("BindMount source must be a PurePath")
        if not isinstance(self.destination, PurePath):
            raise TypeError("BindMount destination must be a PurePath")
        if type(self.read_only) is not bool:
            raise TypeError("BindMount read_only must be a boolean")


@dataclass(frozen=True, slots=True)
class ContainerRequest:
    """Normalized input for pure execution-runtime command construction."""

    command: Command
    image: PurePath | None
    gpu: bool
    binds: tuple[BindMount, ...] = ()

    def __post_init__(self) -> None:
        if type(self.command) is not Command:
            raise TypeError("ContainerRequest command must be a Command")
        if self.image is not None and not isinstance(self.image, PurePath):
            raise TypeError("ContainerRequest image must be a PurePath or None")
        if type(self.gpu) is not bool:
            raise TypeError("ContainerRequest gpu must be a boolean")
        if not isinstance(self.binds, Sequence) or isinstance(self.binds, (str, bytes)):
            raise TypeError("ContainerRequest binds must be a sequence")
        binds = tuple(self.binds)
        if any(type(bind) is not BindMount for bind in binds):
            raise TypeError("ContainerRequest binds must contain only BindMount values")
        if len({bind.destination for bind in binds}) != len(binds):
            raise ValueError(
                "ContainerRequest binds require unique container destinations"
            )
        object.__setattr__(self, "binds", binds)


@runtime_checkable
class Transport(Protocol):
    def check(self) -> CapabilityCheck: ...

    def run(self, command: Command) -> CommandResult: ...


@runtime_checkable
class Scheduler(Protocol):
    def submit(self, group: SchedulerGroup) -> SchedulerSubmission: ...

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]: ...

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]: ...


@runtime_checkable
class ArrayScheduler(Protocol):
    def submit_array(self, request: SchedulerArrayRequest) -> SchedulerSubmission: ...


@runtime_checkable
class CompactArrayScheduler(Protocol):
    def submit_compact_array(
        self, request: CompactSchedulerArrayRequest
    ) -> CompactSchedulerSubmission: ...


@runtime_checkable
class DependencyScheduler(Protocol):
    """Scheduler extension for framework-owned successful-job dependencies."""

    def submit_afterok(
        self,
        group: SchedulerGroup,
        dependency: SchedulerReference,
    ) -> SchedulerSubmission: ...

    def submit_array_afterok(
        self,
        request: SchedulerArrayRequest,
        dependency: SchedulerReference,
    ) -> SchedulerSubmission: ...


@runtime_checkable
class CompactDependencyScheduler(Protocol):
    def submit_compact_array_afterok(
        self,
        request: CompactSchedulerArrayRequest,
        dependency: SchedulerReference,
    ) -> CompactSchedulerSubmission: ...


@runtime_checkable
class Stager(Protocol):
    def stage(self, request: StageRequest) -> StagedWorkspace: ...

    def fetch(self, request: FetchRequest) -> FetchResult: ...


@runtime_checkable
class ContainerRuntime(Protocol):
    def check(self) -> CapabilityCheck: ...

    def build_command(self, request: ContainerRequest) -> Command: ...


@runtime_checkable
class ContainerRuntimeIdentityProvider(Protocol):
    """Optional execution-time extension for durable runtime provenance."""

    def identity(self) -> CapabilityCheck: ...
