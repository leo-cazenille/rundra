from __future__ import annotations

import copy
import itertools
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from rundra.config._yaml import read_yaml_document
from rundra.config.errors import ConfigError
from rundra.domain.models import ConfigSnapshot
from rundra.domain.parameters import ParameterSet
from rundra.domain.sweeps import ExpandedConfig, SweepExpansion

_SEEDS = re.compile(r"([0-9]+):([0-9]+)\Z")
_MARKERS = frozenset(
    {"batch_options", "batch_options_range", "batch_hierarchical_options"}
)
type PathPart = str | int


@dataclass(frozen=True, slots=True)
class _Factor:
    path: tuple[PathPart, ...]
    name: str
    values: tuple[tuple[str, object], ...]
    hierarchical: bool = False


def load_sweep_config(source: Path) -> SweepExpansion:
    """Load one ordinary config or deterministically materialize an opted-in sweep."""
    normalized = source.expanduser().resolve()
    document = read_yaml_document(normalized)
    if not isinstance(document, dict):
        _fail(normalized, (), "INVALID_TYPE", "Scientific config must be a mapping")
    metadata = document.get("_rundr")
    if metadata is None:
        content = normalized.read_text(encoding="utf-8")
        return SweepExpansion((ExpandedConfig(ConfigSnapshot(source, content)),))
    seeds = _parse_metadata(metadata, normalized)
    materialized = copy.deepcopy(document)
    materialized.pop("_rundr")
    factors: list[_Factor] = []
    _collect_factors(materialized, (), factors, normalized)
    factors.sort(key=lambda factor: factor.hierarchical)
    if not factors:
        content = yaml.safe_dump(materialized, sort_keys=False)
        return SweepExpansion((ExpandedConfig(ConfigSnapshot(source, content)),), seeds)
    names = [factor.name for factor in factors]
    if len(set(names)) != len(names):
        _fail(
            normalized,
            ("_rundr",),
            "DUPLICATE_PARAMETER_NAME",
            "Sweep parameter names must be unique",
        )
    configs: list[ExpandedConfig] = []
    for ordinal, selections in enumerate(
        itertools.product(*(factor.values for factor in factors))
    ):
        effective = copy.deepcopy(materialized)
        choices: dict[str, object] = {}
        for factor, (label, value) in zip(factors, selections, strict=True):
            choices[factor.name] = copy.deepcopy(
                label if factor.hierarchical else value
            )
            if factor.hierarchical:
                target = _get(effective, factor.path)
                if not isinstance(target, dict) or not isinstance(value, dict):
                    raise AssertionError("validated hierarchical sweep changed shape")
                target.pop("batch_hierarchical_options")
                target.update(copy.deepcopy(value))
            else:
                _set(effective, factor.path, copy.deepcopy(value))
        content = yaml.safe_dump(effective, sort_keys=False)
        configs.append(
            ExpandedConfig(
                ConfigSnapshot(source, content),
                ParameterSet(f"parameter_set_{ordinal:06d}", choices),
            )
        )
    return SweepExpansion(tuple(configs), seeds)


def _parse_metadata(value: object, source: Path) -> tuple[int, ...] | None:
    if not isinstance(value, dict):
        _fail(source, ("_rundr",), "INVALID_TYPE", "_rundr must be a mapping")
    unknown = set(value) - {"version", "seeds"}
    if unknown:
        field = sorted(unknown)[0]
        _fail(source, ("_rundr", field), "UNKNOWN_FIELD", "Unknown _rundr field")
    if value.get("version") != 1:
        _fail(
            source,
            ("_rundr", "version"),
            "UNSUPPORTED_VERSION",
            "_rundr.version must be 1",
        )
    if "seeds" not in value:
        return None
    raw = value["seeds"]
    if type(raw) is int:
        if raw < 0:
            _fail(
                source,
                ("_rundr", "seeds"),
                "INVALID_SEEDS",
                "Seed must be non-negative",
            )
        return (raw,)
    if type(raw) is not str or (match := _SEEDS.fullmatch(raw)) is None:
        _fail(
            source,
            ("_rundr", "seeds"),
            "INVALID_SEEDS",
            "Seeds must be an integer or inclusive START:STOP string",
        )
    start, stop = (int(part) for part in match.groups())
    if stop < start:
        _fail(source, ("_rundr", "seeds"), "INVALID_SEEDS", "Seed stop precedes start")
    return tuple(range(start, stop + 1))


def _collect_factors(
    node: object,
    path: tuple[PathPart, ...],
    factors: list[_Factor],
    source: Path,
) -> None:
    if isinstance(node, dict):
        present = _MARKERS.intersection(node)
        if len(present) > 1:
            _fail(
                source,
                path,
                "CONFLICTING_SWEEP_MARKERS",
                "Only one sweep marker is allowed per value",
            )
        if "batch_options" in node:
            values = node["batch_options"]
            if not isinstance(values, list) or not values:
                _fail(
                    source,
                    (*path, "batch_options"),
                    "INVALID_SWEEP_OPTIONS",
                    "batch_options must be a nonempty list",
                )
            name = _path_name(path)
            factors.append(
                _Factor(
                    path,
                    name,
                    tuple((str(index), value) for index, value in enumerate(values)),
                )
            )
            return
        if "batch_options_range" in node:
            values = _expand_range(node["batch_options_range"], source, path)
            name = _path_name(path)
            factors.append(
                _Factor(
                    path,
                    name,
                    tuple((str(index), value) for index, value in enumerate(values)),
                )
            )
            return
        if "batch_hierarchical_options" in node:
            raw = node["batch_hierarchical_options"]
            if not isinstance(raw, dict):
                _fail(
                    source,
                    (*path, "batch_hierarchical_options"),
                    "INVALID_TYPE",
                    "batch_hierarchical_options must be a mapping",
                )
            name_value = raw.get("name")
            if name_value is not None and (
                type(name_value) is not str or not name_value.strip()
            ):
                _fail(
                    source,
                    (*path, "batch_hierarchical_options", "name"),
                    "INVALID_VALUE",
                    "hierarchical name must be nonblank",
                )
            options = tuple(
                (name, value)
                for name, value in raw.items()
                if name not in {"name", "default"}
            )
            if not options or any(
                type(name) is not str or not isinstance(value, dict)
                for name, value in options
            ):
                _fail(
                    source,
                    (*path, "batch_hierarchical_options"),
                    "INVALID_SWEEP_OPTIONS",
                    "hierarchical choices must be named mappings",
                )
            factors.append(_Factor(path, name_value or _path_name(path), options, True))
            for key, child in node.items():
                if key != "batch_hierarchical_options":
                    _collect_factors(child, (*path, key), factors, source)
            return
        for key, value in node.items():
            _collect_factors(value, (*path, key), factors, source)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            _collect_factors(value, (*path, index), factors, source)


def _expand_range(
    value: object, source: Path, path: tuple[PathPart, ...]
) -> tuple[int | float, ...]:
    marker_path = (*path, "batch_options_range")
    if not isinstance(value, dict) or set(value) - {
        "start",
        "stop",
        "step",
        "inclusive",
        "type",
    }:
        _fail(
            source,
            marker_path,
            "INVALID_SWEEP_RANGE",
            "Invalid batch_options_range fields",
        )
    if not {"start", "stop", "step"}.issubset(value):
        _fail(
            source,
            marker_path,
            "INVALID_SWEEP_RANGE",
            "Range requires start, stop, and step",
        )
    start, stop, step = value["start"], value["stop"], value["step"]
    if any(type(item) not in {int, float} for item in (start, stop, step)) or step == 0:
        _fail(
            source,
            marker_path,
            "INVALID_SWEEP_RANGE",
            "Range values must be numeric with a nonzero step",
        )
    kind = value.get("type")
    if kind is None:
        kind = (
            "int"
            if all(float(item).is_integer() for item in (start, stop, step))
            else "float"
        )
    if kind not in {"int", "float"} or type(value.get("inclusive", False)) is not bool:
        _fail(
            source,
            marker_path,
            "INVALID_SWEEP_RANGE",
            "Range type or inclusive flag is invalid",
        )
    inclusive = value.get("inclusive", False)
    result: list[int | float] = []
    current = float(start)
    epsilon = 1e-12
    while (
        (
            current <= float(stop) + epsilon
            if step > 0 and inclusive
            else current < float(stop) - epsilon
        )
        if step > 0
        else (
            current >= float(stop) - epsilon
            if inclusive
            else current > float(stop) + epsilon
        )
    ):
        result.append(int(round(current)) if kind == "int" else round(current, 12))
        current += float(step)
    if not result:
        _fail(source, marker_path, "INVALID_SWEEP_RANGE", "Range produced no values")
    return tuple(result)


def _get(document: object, path: tuple[PathPart, ...]) -> Any:
    current = document
    for part in path:
        current = current[part]
    return current


def _set(document: object, path: tuple[PathPart, ...], value: object) -> None:
    if not path:
        raise ValueError("Sweep marker cannot replace the config root")
    parent = _get(document, path[:-1])
    parent[path[-1]] = value


def _path_name(path: tuple[PathPart, ...]) -> str:
    if not path:
        raise ValueError("Sweep marker cannot occur at the config root")
    return ".".join(str(part) for part in path)


def _fail(
    source: Path,
    path: tuple[PathPart, ...],
    code: str,
    message: str,
) -> None:
    raise ConfigError(code=code, message=message, source=source, path=path)
