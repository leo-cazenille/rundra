from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class PlacementTargetDecision:
    target: str
    accepted: bool
    reason: str
    partition: str | None = None
    utilization_percent: int | None = None
    idle_cpus: int | None = None
    planned_capacity: int | None = None
    usable_capacity: int | None = None
    assigned_seed_start: int | None = None
    assigned_seed_stop: int | None = None

    def __post_init__(self) -> None:
        if type(self.target) is not str or not self.target.strip():
            raise ValueError("Placement decision target must be nonblank")
        if type(self.accepted) is not bool:
            raise TypeError("Placement decision accepted must be bool")
        if type(self.reason) is not str or not self.reason.strip():
            raise ValueError("Placement decision reason must be nonblank")
        for name in (
            "utilization_percent",
            "idle_cpus",
            "planned_capacity",
            "usable_capacity",
        ):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"Placement decision {name} must be nonnegative")
        assigned_start = self.assigned_seed_start
        assigned_stop = self.assigned_seed_stop
        if (assigned_start is None) != (assigned_stop is None):
            raise ValueError("Placement seed assignment must be complete or absent")
        if (
            assigned_start is not None
            and assigned_stop is not None
            and assigned_stop < assigned_start
        ):
            raise ValueError("Placement seed assignment is invalid")


@dataclass(frozen=True, slots=True)
class PlacementDecision:
    policy: str
    strategy: str
    observed_at: datetime
    targets: tuple[PlacementTargetDecision, ...]

    def __post_init__(self) -> None:
        if type(self.policy) is not str or not self.policy.strip():
            raise ValueError("Placement policy name must be nonblank")
        if self.strategy != "available_capacity":
            raise ValueError("Placement strategy is unsupported")
        if (
            not isinstance(self.observed_at, datetime)
            or self.observed_at.utcoffset() is None
        ):
            raise ValueError("Placement observation time must be timezone-aware")
        targets = tuple(self.targets)
        if not targets or any(
            type(item) is not PlacementTargetDecision for item in targets
        ):
            raise ValueError("Placement decision targets are invalid")
        if len({item.target for item in targets}) != len(targets):
            raise ValueError("Placement decision targets must be unique")
        object.__setattr__(self, "targets", targets)

    @property
    def selected_targets(self) -> tuple[str, ...]:
        return tuple(
            item.target
            for item in self.targets
            if item.accepted and item.assigned_seed_start is not None
        )
