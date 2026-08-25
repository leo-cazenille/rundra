from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class SlurmPartitionRoute:
    """One operator-declared Slurm partition compatibility route."""

    name: str
    partition: str
    resource_class: str
    max_walltime: timedelta

    def __post_init__(self) -> None:
        for field_name in ("name", "partition"):
            value = getattr(self, field_name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"Partition route {field_name} must be nonblank")
        if self.resource_class not in {"cpu", "gpu"}:
            raise ValueError("Partition route resource_class must be cpu or gpu")
        if type(self.max_walltime) is not timedelta or self.max_walltime <= timedelta():
            raise ValueError("Partition route max_walltime must be positive")


@dataclass(frozen=True, slots=True)
class SlurmPartitionPolicy:
    """Immutable ordered partition routes owned by one Slurm target."""

    routes: tuple[SlurmPartitionRoute, ...]

    def __post_init__(self) -> None:
        routes = tuple(self.routes)
        if not routes:
            raise ValueError("Slurm partition policy requires at least one route")
        if any(type(route) is not SlurmPartitionRoute for route in routes):
            raise TypeError("Slurm partition policy routes are invalid")
        if len({route.name for route in routes}) != len(routes):
            raise ValueError("Slurm partition route names must be unique")
        if len({route.partition for route in routes}) != len(routes):
            raise ValueError("Slurm partition names must be unique")
        object.__setattr__(self, "routes", routes)
