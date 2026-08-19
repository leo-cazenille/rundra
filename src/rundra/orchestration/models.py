from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import (
    Command,
    ConfigSnapshot,
    ResourceRequest,
    Target,
    TaskId,
)
from rundra.domain.parameters import ParameterSet
from rundra.domain.preparation import PreparationPlan
from rundra.domain.scaling import ExecutionPolicy, TaskSpace

ONE_UNIT_PER_TASK = "one_unit_per_task"
SLURM_ARRAY = "slurm_array"
SCHEDULER_ARRAY = "scheduler_array"
MULTI_ARRAY = "multi-array"
WORKER_POOL = "worker-pool"
_EXECUTION_STRATEGIES = frozenset(
    {ONE_UNIT_PER_TASK, SLURM_ARRAY, SCHEDULER_ARRAY, MULTI_ARRAY, WORKER_POOL}
)


class PlanningError(Exception):
    """A deterministic, structured failure to construct an execution plan."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: Mapping[str, str | int | bool] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.details = MappingProxyType(dict(details or {}))
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ExecutionUnit:
    """One logical, scheduler-independent unit in an inspectable plan."""

    task_id: TaskId
    seed: int
    config: ConfigSnapshot
    command: Command
    resources: ResourceRequest
    parameter_set: ParameterSet | None = None

    def __post_init__(self) -> None:
        if type(self.task_id) is not TaskId:
            raise TypeError("ExecutionUnit task_id must be a TaskId")
        if type(self.seed) is not int:
            raise TypeError("ExecutionUnit seed must be an integer")
        if type(self.config) is not ConfigSnapshot:
            raise TypeError("ExecutionUnit config must be a ConfigSnapshot")
        if type(self.command) is not Command:
            raise TypeError("ExecutionUnit command must be a Command")
        if type(self.resources) is not ResourceRequest:
            raise TypeError("ExecutionUnit resources must be a ResourceRequest")
        if (
            self.parameter_set is not None
            and type(self.parameter_set) is not ParameterSet
        ):
            raise TypeError(
                "ExecutionUnit parameter_set must be a ParameterSet or None"
            )


@dataclass(frozen=True, slots=True)
class ExecutionGroup:
    """An ordered group of logical Tasks sharing one submission strategy."""

    task_ids: tuple[TaskId, ...]

    def __post_init__(self) -> None:
        task_ids = tuple(self.task_ids)
        if any(type(task_id) is not TaskId for task_id in task_ids):
            raise TypeError("ExecutionGroup task_ids must contain only TaskIds")
        if not task_ids:
            raise ValueError("ExecutionGroup must contain at least one Task ID")
        if len(set(task_ids)) != len(task_ids):
            raise ValueError("ExecutionGroup Task IDs must be unique")
        object.__setattr__(self, "task_ids", task_ids)


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """A pure prospective plan; it is not a Run and submits nothing."""

    version: int
    experiment_name: str
    target: Target
    units: tuple[ExecutionUnit, ...]
    groups: tuple[ExecutionGroup, ...]
    array_mapping: tuple[ArrayTaskMapping, ...]
    strategy: str = ONE_UNIT_PER_TASK
    preparation: PreparationPlan | None = None
    task_space: TaskSpace | None = None
    execution_policy: ExecutionPolicy | None = None
    retrieval_policy: str | None = None
    scheduler_batches: int | None = None
    worker_count: int | None = None
    task_slots_per_worker: int | None = None
    concurrent_task_capacity: int | None = None
    max_lane_depth: int | None = None
    worker_resources: ResourceRequest | None = None
    staging_backend: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version < 1:
            raise ValueError("ExecutionPlan version must be a positive integer")
        if self.version == 1 and self.preparation is not None:
            raise ValueError("ExecutionPlan v1 cannot contain preparation")
        if self.version == 2 and type(self.preparation) is not PreparationPlan:
            raise ValueError("ExecutionPlan v2 requires preparation")
        if self.version == 3 and any(unit.parameter_set is None for unit in self.units):
            raise ValueError("ExecutionPlan v3 requires parameterized units")
        if self.version in {4, 5}:
            if type(self.task_space) is not TaskSpace:
                raise ValueError("Scalable ExecutionPlan requires a TaskSpace")
            if type(self.execution_policy) is not ExecutionPolicy:
                raise ValueError("Scalable ExecutionPlan requires an execution policy")
            if self.strategy not in {MULTI_ARRAY, WORKER_POOL}:
                raise ValueError("Scalable ExecutionPlan requires a scalable strategy")
            if self.retrieval_policy not in {"all", "manifest", "none"}:
                raise ValueError(
                    "Scalable ExecutionPlan retrieval policy is unsupported"
                )
            if type(self.scheduler_batches) is not int or self.scheduler_batches < 1:
                raise ValueError(
                    "Scalable ExecutionPlan scheduler_batches must be positive"
                )
            if self.strategy == WORKER_POOL:
                if type(self.worker_count) is not int or self.worker_count < 1:
                    raise ValueError(
                        "worker-pool plans require a positive worker_count"
                    )
            elif self.worker_count is not None:
                raise ValueError("multi-array plans cannot define worker_count")
            if self.version == 5:
                if (
                    type(self.task_slots_per_worker) is not int
                    or self.task_slots_per_worker < 1
                ):
                    raise ValueError(
                        "ExecutionPlan v5 task_slots_per_worker must be positive"
                    )
                if (
                    type(self.concurrent_task_capacity) is not int
                    or self.concurrent_task_capacity < 1
                ):
                    raise ValueError(
                        "ExecutionPlan v5 concurrent_task_capacity must be positive"
                    )
                if type(self.max_lane_depth) is not int or self.max_lane_depth < 1:
                    raise ValueError("ExecutionPlan v5 max_lane_depth must be positive")
                if type(self.worker_resources) is not ResourceRequest:
                    raise TypeError("ExecutionPlan v5 requires worker_resources")
            elif any(
                value is not None
                for value in (
                    self.task_slots_per_worker,
                    self.concurrent_task_capacity,
                    self.max_lane_depth,
                    self.worker_resources,
                )
            ):
                raise ValueError("ExecutionPlan v4 cannot contain v5 scaling fields")
        elif any(
            value is not None
            for value in (
                self.task_space,
                self.execution_policy,
                self.retrieval_policy,
                self.scheduler_batches,
                self.worker_count,
                self.task_slots_per_worker,
                self.concurrent_task_capacity,
                self.max_lane_depth,
                self.worker_resources,
            )
        ):
            raise ValueError("ExecutionPlan v1-v3 cannot contain scaling fields")
        if self.version not in {1, 2, 3, 4, 5}:
            raise ValueError("ExecutionPlan version is unsupported")
        if type(self.experiment_name) is not str or not self.experiment_name.strip():
            raise ValueError("ExecutionPlan experiment_name must be nonblank")
        if type(self.target) is not Target:
            raise TypeError("ExecutionPlan target must be a Target")
        if self.strategy not in _EXECUTION_STRATEGIES:
            raise ValueError("ExecutionPlan strategy is unsupported")
        units = tuple(self.units)
        if any(type(unit) is not ExecutionUnit for unit in units):
            raise TypeError("ExecutionPlan units must contain only ExecutionUnits")
        if not units:
            raise ValueError("ExecutionPlan must contain at least one unit")
        if len({unit.task_id for unit in units}) != len(units):
            raise ValueError("ExecutionPlan unit Task IDs must be unique")
        expected_ids = tuple(TaskId.from_ordinal(index) for index in range(len(units)))
        if tuple(unit.task_id for unit in units) != expected_ids:
            raise ValueError("ExecutionPlan units must use contiguous ordinal Task IDs")
        if self.version < 3 and len({unit.seed for unit in units}) != len(units):
            raise ValueError("ExecutionPlan unit seeds must be unique")
        effective_config = units[0].config
        if self.version < 3 and any(
            unit.config != effective_config for unit in units[1:]
        ):
            raise ValueError("ExecutionPlan units must share one effective config")
        groups = tuple(self.groups)
        if any(type(group) is not ExecutionGroup for group in groups):
            raise TypeError("ExecutionPlan groups must contain ExecutionGroups")
        grouped_ids = tuple(task_id for group in groups for task_id in group.task_ids)
        unit_ids = tuple(unit.task_id for unit in units)
        if self.version >= 4 and groups:
            raise ValueError(
                "Scalable ExecutionPlan cannot materialize execution groups"
            )
        if self.version < 4 and grouped_ids != unit_ids:
            raise ValueError(
                "ExecutionPlan groups must partition Task IDs in plan order"
            )
        if (
            self.version < 4
            and self.strategy == ONE_UNIT_PER_TASK
            and any(len(group.task_ids) != 1 for group in groups)
        ):
            raise ValueError(
                "one_unit_per_task strategy requires singleton execution groups"
            )
        if self.version < 4 and self.strategy == SLURM_ARRAY:
            if self.target.scheduler.kind != "slurm":
                raise ValueError("slurm_array strategy requires a Slurm target")
            if len(units) < 2 or len(groups) != 1:
                raise ValueError(
                    "slurm_array strategy requires one multi-Task execution group"
                )
            if any(unit.resources != units[0].resources for unit in units[1:]):
                raise ValueError("slurm_array strategy requires uniform Task resources")
        if self.version < 4 and self.strategy == SCHEDULER_ARRAY:
            if self.target.scheduler.kind != "pbs":
                raise ValueError("scheduler_array strategy requires a PBS target")
            if len(units) < 2 or len(groups) != 1:
                raise ValueError(
                    "scheduler_array strategy requires one multi-Task execution group"
                )
            if any(unit.resources != units[0].resources for unit in units[1:]):
                raise ValueError(
                    "scheduler_array strategy requires uniform Task resources"
                )
        array_mapping = tuple(self.array_mapping)
        if any(type(item) is not ArrayTaskMapping for item in array_mapping):
            raise TypeError(
                "ExecutionPlan array_mapping must contain ArrayTaskMappings"
            )
        if self.version >= 4 and array_mapping:
            raise ValueError(
                "Scalable ExecutionPlan cannot materialize an array mapping"
            )
        if self.strategy == ONE_UNIT_PER_TASK and array_mapping:
            raise ValueError(
                "one_unit_per_task strategy cannot define an array mapping"
            )
        if self.strategy in {SLURM_ARRAY, SCHEDULER_ARRAY}:
            expected_mapping = tuple(
                ArrayTaskMapping(unit.task_id, unit.seed, index)
                for index, unit in enumerate(units)
            )
            if array_mapping != expected_mapping:
                raise ValueError(
                    "ExecutionPlan array mapping must match Task order and seeds"
                )
        object.__setattr__(self, "units", units)
        object.__setattr__(self, "groups", groups)
        object.__setattr__(self, "array_mapping", array_mapping)
        object.__setattr__(self, "staging_backend", self.target.staging.kind)
