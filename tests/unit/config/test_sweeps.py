from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rundra.config.errors import ConfigError
from rundra.config.sweeps import load_sweep_config


def test_sweep_expands_simple_range_and_hierarchical_options(tmp_path: Path) -> None:
    source = tmp_path / "sweep.yaml"
    source.write_text(
        """_rundr:
  version: 1
  seeds: "2:3"
robots:
  nb:
    default_option: 10
    batch_options: [10, 20]
parameters:
  speed:
    batch_options_range: {start: 1, stop: 3, step: 1}
  batch_hierarchical_options:
    name: regime
    default: {run: 1, tumble: 1}
    ballistic: {run: 20, tumble: 1}
    long_tumble: {run: 1, tumble: 20}
""",
        encoding="utf-8",
    )

    expansion = load_sweep_config(source)

    assert expansion.seeds == (2, 3)
    assert expansion.is_sweep
    assert len(expansion.configs) == 8
    first = expansion.configs[0]
    assert first.parameter_set is not None
    assert first.parameter_set.id == "parameter_set_000000"
    assert dict(first.parameter_set.choices) == {
        "robots.nb": 10,
        "parameters.speed": 1,
        "regime": "ballistic",
    }
    effective = yaml.safe_load(first.config.content)
    assert effective["robots"]["nb"] == 10
    assert effective["parameters"] == {"speed": 1, "run": 20, "tumble": 1}
    assert "_rundr" not in effective


def test_rundr_seed_metadata_without_parameters_is_stripped(tmp_path: Path) -> None:
    source = tmp_path / "config.yaml"
    source.write_text("_rundr: {version: 1, seeds: 7}\nvalue: 3\n", encoding="utf-8")

    expansion = load_sweep_config(source)

    assert expansion.seeds == (7,)
    assert not expansion.is_sweep
    assert yaml.safe_load(expansion.configs[0].config.content) == {"value": 3}


def test_sweep_rejects_unknown_metadata_and_empty_options(tmp_path: Path) -> None:
    unknown = tmp_path / "unknown.yaml"
    unknown.write_text("_rundr: {version: 1, extra: true}\n", encoding="utf-8")
    empty = tmp_path / "empty.yaml"
    empty.write_text(
        "_rundr: {version: 1}\nvalue: {batch_options: []}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Unknown _rundr field"):
        load_sweep_config(unknown)
    with pytest.raises(ConfigError, match="nonempty list"):
        load_sweep_config(empty)
