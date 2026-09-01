from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from rundra.config._schema import (
    ConfigPath,
    check_fields,
    expect_integer,
    expect_mapping,
    expect_string,
    expect_string_list,
    fail,
)
_PLACEMENT_FIELDS = frozenset(
    {
        "candidates",
        "strategy",
        "max_utilization_percent",
        "minimum_idle_cpus",
        "max_targets",
    }
)


@dataclass(frozen=True, slots=True)
class PlacementPolicy:
    """Project-owned constraints for one-shot automatic target placement."""

    name: str
    candidates: tuple[str, ...] = ()
    strategy: str = "available_capacity"
    max_utilization_percent: int = 90
    minimum_idle_cpus: int = 1
    max_targets: int | None = None

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name.strip():
            raise ValueError("PlacementPolicy name must be nonblank")
        candidates = tuple(self.candidates)
        if any(type(item) is not str or not item.strip() for item in candidates):
            raise ValueError("PlacementPolicy candidates must be nonblank strings")
        if len(set(candidates)) != len(candidates):
            raise ValueError("PlacementPolicy candidates must be unique")
        if self.strategy != "available_capacity":
            raise ValueError("PlacementPolicy strategy is unsupported")
        if (
            type(self.max_utilization_percent) is not int
            or not 1 <= self.max_utilization_percent <= 100
        ):
            raise ValueError("Placement utilization threshold must be from 1 to 100")
        if type(self.minimum_idle_cpus) is not int or self.minimum_idle_cpus < 1:
            raise ValueError("Placement minimum_idle_cpus must be positive")
        if self.max_targets is not None and (
            type(self.max_targets) is not int or self.max_targets < 1
        ):
            raise ValueError("Placement max_targets must be positive or None")
        object.__setattr__(self, "candidates", candidates)


def automatic_placement_policy() -> PlacementPolicy:
    """Return the conservative built-in policy used by ``--placement auto``."""
    return PlacementPolicy("auto")


def parse_placement_policies(
    value: object, *, source: Path
) -> Mapping[str, PlacementPolicy]:
    raw = expect_mapping(value, source=source, path=("placements",))
    policies: dict[str, PlacementPolicy] = {}
    for name, item in raw.items():
        expect_string(name, source=source, path=("placements", name), nonblank=True)
        policies[name] = _parse_policy(item, name, source, ("placements", name))
    return MappingProxyType(policies)


def _parse_policy(
    value: object,
    name: str,
    source: Path,
    path: ConfigPath,
) -> PlacementPolicy:
    document = expect_mapping(value, source=source, path=path)
    check_fields(
        document,
        allowed=_PLACEMENT_FIELDS,
        required=frozenset(),
        source=source,
        path=path,
    )
    strategy = (
        expect_string(
            document["strategy"], source=source, path=(*path, "strategy")
        )
        if "strategy" in document
        else "available_capacity"
    )
    if strategy != "available_capacity":
        fail(
            source=source,
            path=(*path, "strategy"),
            code="INVALID_VALUE",
            message="strategy must be available_capacity",
        )
    candidates = tuple(
        expect_string_list(
            document.get("candidates", []),
            source=source,
            path=(*path, "candidates"),
        )
    )
    if len(set(candidates)) != len(candidates):
        fail(
            source=source,
            path=(*path, "candidates"),
            code="DUPLICATE_VALUE",
            message="Placement candidate targets must be unique",
        )
    max_utilization_percent = expect_integer(
        document.get("max_utilization_percent", 90),
        source=source,
        path=(*path, "max_utilization_percent"),
        minimum=1,
    )
    if max_utilization_percent > 100:
        fail(
            source=source,
            path=(*path, "max_utilization_percent"),
            code="INVALID_VALUE",
            message="Value must be at most 100",
        )
    return PlacementPolicy(
        name=name,
        candidates=candidates,
        strategy=strategy,
        max_utilization_percent=max_utilization_percent,
        minimum_idle_cpus=expect_integer(
            document.get("minimum_idle_cpus", 1),
            source=source,
            path=(*path, "minimum_idle_cpus"),
            minimum=1,
        ),
        max_targets=(
            expect_integer(
                document["max_targets"],
                source=source,
                path=(*path, "max_targets"),
                minimum=1,
            )
            if "max_targets" in document
            else None
        ),
    )
