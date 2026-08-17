from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class ParameterSet:
    id: str
    choices: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.id.startswith("parameter_set_") or not self.id[14:].isdigit():
            raise ValueError("ParameterSet id must be a stable ordinal identifier")
        if not isinstance(self.choices, Mapping) or not self.choices:
            raise ValueError("ParameterSet choices must be a nonempty mapping")
        choices = dict(self.choices)
        if any(type(name) is not str or not name for name in choices):
            raise ValueError("ParameterSet choice names must be nonempty strings")
        if any(not _json_value(value) for value in choices.values()):
            raise TypeError("ParameterSet choices must contain finite JSON values")
        object.__setattr__(self, "choices", MappingProxyType(choices))


def _json_value(value: object) -> bool:
    if value is None or type(value) in {str, bool, int}:
        return True
    if type(value) is float:
        return isfinite(value)
    if isinstance(value, list):
        return all(_json_value(item) for item in value)
    if isinstance(value, Mapping):
        return all(
            type(key) is str and _json_value(item) for key, item in value.items()
        )
    return False
