from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from math import isfinite
from pathlib import PurePath
from types import MappingProxyType
from uuid import uuid4

from rundra.domain.parameters import ParameterSet
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.domain.storage import SlurmScratchPolicy

_RUN_ID_PATTERN = re.compile(r"run_[0-9a-f]{32}\Z")
_TASK_ID_PATTERN = re.compile(r"task_[0-9]{6,}\Z")

type NativeValue = str | int | float | bool

_NATIVE_VALUE_TYPES = (str, int, float, bool)


def _freeze_string_sequence(value: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{field_name} must be a sequence of strings")
    frozen = tuple(value)
    if any(type(item) is not str for item in frozen):
        raise TypeError(f"{field_name} must contain only strings")
    return frozen


def _freeze_scalar_options(
    options: Mapping[str, NativeValue],
    *,
    field_name: str,
) -> Mapping[str, NativeValue]:
    if not isinstance(options, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    frozen: dict[str, NativeValue] = {}
    for key, value in options.items():
        if type(key) is not str or type(value) not in _NATIVE_VALUE_TYPES:
            raise TypeError(f"{field_name} must map strings to scalar native values")
        if type(value) is float and not isfinite(value):
            raise ValueError(f"{field_name} float values must be finite")
        frozen[key] = value
    return MappingProxyType(frozen)


def _freeze_native_options(
    options: Mapping[str, Mapping[str, NativeValue]],
) -> Mapping[str, Mapping[str, NativeValue]]:
    if not isinstance(options, Mapping):
        raise TypeError("native must be a mapping")
    frozen: dict[str, Mapping[str, NativeValue]] = {}
    for backend, backend_options in options.items():
        if type(backend) is not str:
            raise TypeError("native backend names must be strings")
        frozen[backend] = _freeze_scalar_options(
            backend_options,
            field_name="native options",
        )
    return MappingProxyType(frozen)


@dataclass(frozen=True, slots=True)
class Command:
    """An immutable argument-vector command and its portable execution context."""

    argv: tuple[str, ...]
    environment: Mapping[str, str] = field(default_factory=dict)
    working_directory: PurePath | None = None

    def __post_init__(self) -> None:
        argv = _freeze_string_sequence(self.argv, field_name="Command argv")
        if not argv or any(not argument for argument in argv):
            raise ValueError("Command argv must contain only non-empty arguments")
        if not isinstance(self.environment, Mapping) or any(
            type(key) is not str or type(value) is not str
            for key, value in self.environment.items()
        ):
            raise TypeError("Command environment must map strings to strings")
        if self.working_directory is not None and not isinstance(
            self.working_directory, PurePath
        ):
            raise TypeError("Command working_directory must be a PurePath")
        object.__setattr__(self, "argv", argv)
        object.__setattr__(
            self,
            "environment",
            MappingProxyType(dict(self.environment)),
        )


@dataclass(frozen=True, slots=True)
class ResourceRequest:
    """Portable resources plus explicit, namespaced backend-native options."""

    nodes: int = 1
    tasks: int = 1
    cpus_per_task: int = 1
    gpus_per_task: int = 0
    memory_bytes: int | None = None
    walltime: timedelta | None = None
    native: Mapping[str, Mapping[str, NativeValue]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("nodes", "tasks", "cpus_per_task"):
            value = getattr(self, name)
            if type(value) is not int:
                raise TypeError(f"{name} must be an integer")
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if type(self.gpus_per_task) is not int:
            raise TypeError("gpus_per_task must be an integer")
        if self.gpus_per_task < 0:
            raise ValueError("gpus_per_task must be non-negative")
        if self.memory_bytes is not None:
            if type(self.memory_bytes) is not int:
                raise TypeError("memory_bytes must be an integer when provided")
            if self.memory_bytes <= 0:
                raise ValueError("memory_bytes must be positive when provided")
        if self.walltime is not None:
            if type(self.walltime) is not timedelta:
                raise TypeError("walltime must be a timedelta when provided")
            if self.walltime <= timedelta(0):
                raise ValueError("walltime must be positive when provided")
        object.__setattr__(self, "native", _freeze_native_options(self.native))


@dataclass(frozen=True, slots=True)
class ContainerSpec:
    """Portable container requirements declared by an experiment."""

    image: PurePath
    gpu: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.image, PurePath):
            raise TypeError("ContainerSpec image must be a PurePath")
        if type(self.gpu) is not bool:
            raise TypeError("ContainerSpec gpu must be a boolean")


@dataclass(frozen=True, slots=True)
class ExperimentSpec:
    """Portable scientific definition for executing one experiment task."""

    version: int
    name: str
    command: Command
    resources: ResourceRequest
    container: ContainerSpec | None = None
    outputs: tuple[str, ...] = ()
    sync_excludes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.version) is not int:
            raise TypeError("ExperimentSpec version must be an integer")
        if self.version < 1:
            raise ValueError("ExperimentSpec version must be positive")
        if type(self.name) is not str:
            raise TypeError("ExperimentSpec name must be a string")
        if not self.name.strip():
            raise ValueError("ExperimentSpec name must not be blank")
        if type(self.command) is not Command:
            raise TypeError("ExperimentSpec command must be a Command")
        if type(self.resources) is not ResourceRequest:
            raise TypeError("ExperimentSpec resources must be a ResourceRequest")
        if self.container is not None and type(self.container) is not ContainerSpec:
            raise TypeError("ExperimentSpec container must be a ContainerSpec or None")
        object.__setattr__(
            self,
            "outputs",
            _freeze_string_sequence(self.outputs, field_name="ExperimentSpec outputs"),
        )
        object.__setattr__(
            self,
            "sync_excludes",
            _freeze_string_sequence(
                self.sync_excludes,
                field_name="ExperimentSpec sync_excludes",
            ),
        )


class ArtifactKind(StrEnum):
    """Raw execution artifact categories managed by the framework."""

    SOURCE_SNAPSHOT = "source_snapshot"
    EFFECTIVE_CONFIG = "effective_config"
    STDOUT = "stdout"
    STDERR = "stderr"
    RAW_RESULT = "raw_result"
    SCHEDULER_METADATA = "scheduler_metadata"
    PROVENANCE_METADATA = "provenance_metadata"
    REFERENCE_MANIFEST = "reference_manifest"
    OUTPUT_SHARD = "output_shard"


@dataclass(frozen=True, slots=True)
class Artifact:
    """A file or directory associated with raw experiment execution."""

    kind: ArtifactKind
    path: PurePath
    task_id: TaskId | None = None
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        if type(self.kind) is not ArtifactKind:
            raise TypeError("Artifact kind must be an ArtifactKind")
        if not isinstance(self.path, PurePath):
            raise TypeError("Artifact path must be a PurePath")
        if self.task_id is not None and type(self.task_id) is not TaskId:
            raise TypeError("Artifact task_id must be a TaskId or None")
        if self.size_bytes is not None:
            if type(self.size_bytes) is not int:
                raise TypeError("Artifact size_bytes must be an integer when provided")
            if self.size_bytes < 0:
                raise ValueError("size_bytes must be non-negative when provided")


@dataclass(frozen=True, slots=True)
class BackendConfig:
    """A target-selected backend kind and its site-specific scalar options."""

    kind: str
    options: Mapping[str, NativeValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.kind) is not str:
            raise TypeError("BackendConfig kind must be a string")
        if not self.kind.strip():
            raise ValueError("BackendConfig kind must not be blank")
        object.__setattr__(
            self,
            "options",
            _freeze_scalar_options(self.options, field_name="BackendConfig options"),
        )


@dataclass(frozen=True, slots=True)
class Target:
    """Site configuration kept separate from scientific experiment intent."""

    name: str
    transport: BackendConfig
    scheduler: BackendConfig
    staging: BackendConfig
    container: BackendConfig
    workspace: PurePath
    execution_storage: SlurmScratchPolicy | None = None

    def __post_init__(self) -> None:
        if type(self.name) is not str:
            raise TypeError("Target name must be a string")
        if not self.name.strip():
            raise ValueError("Target name must not be blank")
        for field_name in ("transport", "scheduler", "staging", "container"):
            if type(getattr(self, field_name)) is not BackendConfig:
                raise TypeError(f"Target {field_name} must be a BackendConfig")
        if not isinstance(self.workspace, PurePath):
            raise TypeError("Target workspace must be a PurePath")
        if self.execution_storage is not None and type(
            self.execution_storage
        ) is not SlurmScratchPolicy:
            raise TypeError(
                "Target execution_storage must be a SlurmScratchPolicy or None"
            )


@dataclass(frozen=True, slots=True)
class ConfigSnapshot:
    """The exact opaque scientific configuration supplied to a Task."""

    source: PurePath
    content: str

    def __post_init__(self) -> None:
        if not isinstance(self.source, PurePath):
            raise TypeError("ConfigSnapshot source must be a PurePath")
        if type(self.content) is not str:
            raise TypeError("ConfigSnapshot content must be a string")


@dataclass(frozen=True, slots=True)
class Task:
    """One logical experiment execution, independent from scheduler indexing."""

    id: TaskId
    run_id: RunId
    experiment_name: str
    config: ConfigSnapshot
    seed: int
    resources: ResourceRequest
    state: ExecutionState = ExecutionState.CREATED
    parameter_set: ParameterSet | None = None

    def __post_init__(self) -> None:
        if type(self.id) is not TaskId:
            raise TypeError("Task id must be a TaskId")
        if type(self.run_id) is not RunId:
            raise TypeError("Task run_id must be a RunId")
        if type(self.experiment_name) is not str:
            raise TypeError("Task experiment_name must be a string")
        if not self.experiment_name.strip():
            raise ValueError("Task experiment_name must not be blank")
        if type(self.config) is not ConfigSnapshot:
            raise TypeError("Task config must be a ConfigSnapshot")
        if type(self.resources) is not ResourceRequest:
            raise TypeError("Task resources must be a ResourceRequest")
        if (
            self.parameter_set is not None
            and type(self.parameter_set) is not ParameterSet
        ):
            raise TypeError("Task parameter_set must be a ParameterSet or None")
        if type(self.state) is not ExecutionState:
            raise TypeError("Task state must be an ExecutionState")
        if type(self.seed) is not int:
            raise TypeError("Task seed must be an explicit integer")


@dataclass(frozen=True, slots=True)
class Run:
    """A logical submission grouping one or more Tasks."""

    id: RunId
    experiment_name: str
    target: Target
    tasks: tuple[Task, ...]
    created_at: datetime
    state: ExecutionState = ExecutionState.CREATED
    retrieval_state: RetrievalState = RetrievalState.NOT_REQUESTED

    def __post_init__(self) -> None:
        if type(self.id) is not RunId:
            raise TypeError("Run id must be a RunId")
        if type(self.experiment_name) is not str:
            raise TypeError("Run experiment_name must be a string")
        if not self.experiment_name.strip():
            raise ValueError("Run experiment_name must not be blank")
        if type(self.target) is not Target:
            raise TypeError("Run target must be a Target")
        if type(self.state) is not ExecutionState:
            raise TypeError("Run state must be an ExecutionState")
        if type(self.retrieval_state) is not RetrievalState:
            raise TypeError("Run retrieval_state must be a RetrievalState")
        if not isinstance(self.tasks, Sequence) or isinstance(self.tasks, (str, bytes)):
            raise TypeError("Run tasks must be a sequence of Tasks")
        tasks = tuple(self.tasks)
        if any(type(task) is not Task for task in tasks):
            raise TypeError("Run tasks must contain only Tasks")
        if not tasks:
            raise ValueError("Run must contain at least one Task")
        if any(task.run_id != self.id for task in tasks):
            raise ValueError("Every Task must reference the same Run ID")
        if any(task.experiment_name != self.experiment_name for task in tasks):
            raise ValueError("Every Task must reference the same experiment as its Run")
        if len({task.id for task in tasks}) != len(tasks):
            raise ValueError("Run must contain unique Task IDs")
        expected_ids = tuple(TaskId.from_ordinal(index) for index in range(len(tasks)))
        if tuple(task.id for task in tasks) != expected_ids:
            raise ValueError(
                "Run Tasks must use contiguous ordinal IDs in requested seed order"
            )
        parameterized = tuple(task.parameter_set is not None for task in tasks)
        if any(parameterized) and not all(parameterized):
            raise ValueError("Run Tasks must consistently define parameter sets")
        if not any(parameterized):
            if len({task.seed for task in tasks}) != len(tasks):
                raise ValueError("Run must contain unique Task seeds")
            effective_config = tasks[0].config
            if any(task.config != effective_config for task in tasks[1:]):
                raise ValueError("Run Tasks must share one effective config")
        if not isinstance(self.created_at, datetime):
            raise TypeError("Run created_at must be a datetime")
        if self.created_at.utcoffset() is None:
            raise ValueError("Run creation time must be timezone-aware")
        object.__setattr__(self, "tasks", tasks)


@dataclass(frozen=True, slots=True)
class RunId:
    """Framework-generated identifier for a logical Run."""

    value: str

    def __post_init__(self) -> None:
        if type(self.value) is not str:
            raise TypeError("RunId value must be a string")
        if _RUN_ID_PATTERN.fullmatch(self.value) is None:
            msg = "Run ID must match 'run_' followed by 32 lowercase hex digits"
            raise ValueError(msg)

    @classmethod
    def new(cls) -> RunId:
        """Create a collision-resistant, filesystem-safe Run identifier."""
        return cls(f"run_{uuid4().hex}")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class TaskId:
    """Stable identifier for a Task within a Run."""

    value: str

    def __post_init__(self) -> None:
        if type(self.value) is not str:
            raise TypeError("TaskId value must be a string")
        if _TASK_ID_PATTERN.fullmatch(self.value) is None:
            msg = "Task ID must match 'task_' followed by at least six digits"
            raise ValueError(msg)

    @classmethod
    def from_ordinal(cls, ordinal: int) -> TaskId:
        """Create the deterministic Task identifier for a zero-based ordinal."""
        if type(ordinal) is not int:
            raise TypeError("Task ordinal must be an integer")
        if ordinal < 0:
            raise ValueError("Task ordinal must be non-negative")
        return cls(f"task_{ordinal:06d}")

    def __str__(self) -> str:
        return self.value
