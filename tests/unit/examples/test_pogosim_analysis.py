from __future__ import annotations

import runpy
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from unittest.mock import patch

_ROOT = Path(__file__).parents[3]
_PYARROW = ModuleType("pyarrow")
_PYARROW.__path__ = []  # type: ignore[attr-defined]
_FEATHER = ModuleType("pyarrow.feather")
_PYARROW.feather = _FEATHER  # type: ignore[attr-defined]
with patch.dict(
    sys.modules,
    {"pyarrow": _PYARROW, "pyarrow.feather": _FEATHER},
):
    _NAMESPACE = runpy.run_path(
        str(_ROOT / "examples/pogosim-shoal/analysis/analyze_msd.py")
    )
_manifest_tasks = cast(
    Callable[[dict[str, Any]], list[dict[str, Any]]],
    _NAMESPACE["_manifest_tasks"],
)
_checkpoint_targets = cast(
    Callable[[float], tuple[float, ...]],
    _NAMESPACE["checkpoint_targets"],
)


def test_materialized_task_manifest_is_preserved() -> None:
    tasks = [{"task_id": "task_000000", "seed": 7}]

    assert _manifest_tasks({"tasks": tasks}) is tasks


def test_compact_task_manifest_expands_parameter_major_order() -> None:
    manifest = {
        "schema_version": 2,
        "task_space": {
            "parameter_set_count": 2,
            "seeds": {"start": 3, "stop": 5, "step": 1},
            "task_count": 6,
        },
        "parameter_sets": [
            {
                "ordinal": 0,
                "parameter_set": {
                    "id": "parameter_set_000000",
                    "choices": {"regime": "ballistic"},
                },
            },
            {
                "ordinal": 1,
                "parameter_set": {
                    "id": "parameter_set_000001",
                    "choices": {"regime": "long_tumble"},
                },
            },
        ],
    }

    tasks = _manifest_tasks(manifest)

    assert [(task["task_id"], task["seed"]) for task in tasks] == [
        ("task_000000", 3),
        ("task_000001", 4),
        ("task_000002", 5),
        ("task_000003", 3),
        ("task_000004", 4),
        ("task_000005", 5),
    ]
    assert tasks[0]["parameter_set"]["choices"] == {"regime": "ballistic"}
    assert tasks[3]["parameter_set"]["choices"] == {"regime": "long_tumble"}


def test_checkpoint_targets_follow_simulation_duration() -> None:
    assert _checkpoint_targets(119.2)[-1] == 120.0
    assert _checkpoint_targets(299.0)[-1] == 300.0
    assert _checkpoint_targets(595.0)[-1] == 600.0
