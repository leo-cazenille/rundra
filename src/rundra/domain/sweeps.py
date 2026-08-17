from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType

from rundra.domain.models import ConfigSnapshot


@dataclass(frozen=True, slots=True)
class ParameterSet:
    id: str
    choices: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.id.startswith("parameter_set_") or not self.id[14:].isdigit():
            raise ValueError("ParameterSet id must be a stable ordinal identifier")
        if not isinstance(self.choices, Mapping) or not self.choices:
            raise ValueError("ParameterSet choices must be a nonempty mapping")
        object.__setattr__(self, "choices", MappingProxyType(dict(self.choices)))


@dataclass(frozen=True, slots=True)
class ExpandedConfig:
    config: ConfigSnapshot
    parameter_set: ParameterSet | None = None

    @property
    def sha256(self) -> str:
        return sha256(self.config.content.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class SweepExpansion:
    configs: tuple[ExpandedConfig, ...]
    seeds: tuple[int, ...] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.configs, Sequence) or not self.configs:
            raise ValueError("SweepExpansion requires at least one config")
        configs = tuple(self.configs)
        if any(type(config) is not ExpandedConfig for config in configs):
            raise TypeError("SweepExpansion configs must contain ExpandedConfig values")
        if self.seeds is not None:
            seeds = tuple(self.seeds)
            if not seeds or any(type(seed) is not int or seed < 0 for seed in seeds):
                raise ValueError("SweepExpansion seeds must be non-negative integers")
            if len(set(seeds)) != len(seeds):
                raise ValueError("SweepExpansion seeds must be unique")
            object.__setattr__(self, "seeds", seeds)
        object.__setattr__(self, "configs", configs)

    @property
    def is_sweep(self) -> bool:
        return any(config.parameter_set is not None for config in self.configs)
