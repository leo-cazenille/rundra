from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from rundra.domain.models import (
    Command,
    ConfigSnapshot,
    ResourceRequest,
    Target,
    TaskId,
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


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """A pure prospective plan; it is not a Run and submits nothing."""

    version: int
    experiment_name: str
    target: Target
    units: tuple[ExecutionUnit, ...]
    strategy: str = "one_unit_per_task"
    staging_backend: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version < 1:
            raise ValueError("ExecutionPlan version must be a positive integer")
        if type(self.experiment_name) is not str or not self.experiment_name.strip():
            raise ValueError("ExecutionPlan experiment_name must be nonblank")
        if type(self.target) is not Target:
            raise TypeError("ExecutionPlan target must be a Target")
        if type(self.strategy) is not str or not self.strategy.strip():
            raise ValueError("ExecutionPlan strategy must be nonblank")
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
        if len({unit.seed for unit in units}) != len(units):
            raise ValueError("ExecutionPlan unit seeds must be unique")
        effective_config = units[0].config
        if any(unit.config != effective_config for unit in units[1:]):
            raise ValueError("ExecutionPlan units must share one effective config")
        object.__setattr__(self, "units", units)
        object.__setattr__(self, "staging_backend", self.target.staging.kind)
