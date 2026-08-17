from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256

from rundra.domain.models import ConfigSnapshot
from rundra.domain.parameters import ParameterSet


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
