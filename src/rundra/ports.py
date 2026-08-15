from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite
from pathlib import PurePath
from types import MappingProxyType
from typing import Protocol, runtime_checkable

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
from rundra.domain.states import ExecutionState


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
class SchedulerGroup:
    """One nonempty scheduler submission with explicit logical Task members."""

    units: tuple[SchedulerUnit, ...]

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
        object.__setattr__(self, "units", units)


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

    def for_task(self, task_id: TaskId) -> TaskWorkspace:
        """Derive isolated mutable paths for one logical Task."""
        if type(task_id) is not TaskId:
            raise TypeError("StagedWorkspace task_id must be a TaskId")
        name = str(task_id)
        return TaskWorkspace(
            task_id=task_id,
            source=self.source,
            config=self.config,
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
class Stager(Protocol):
    def stage(self, request: StageRequest) -> StagedWorkspace: ...

    def fetch(self, request: FetchRequest) -> FetchResult: ...


@runtime_checkable
class ContainerRuntime(Protocol):
    def check(self) -> CapabilityCheck: ...

    def build_command(self, request: ContainerRequest) -> Command: ...
