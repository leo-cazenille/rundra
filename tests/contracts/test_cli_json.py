from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["shoal-run", *arguments],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    "arguments, contract",
    [
        (
            ("validate", "examples/minimal/experiment.yaml", "--json"),
            "validate-success-v1.json",
        ),
        (
            (
                "plan",
                "examples/minimal/experiment.yaml",
                "--config",
                "examples/minimal/config.yaml",
                "--seeds",
                "0:1",
                "--target",
                "local",
                "--targets-file",
                "examples/minimal/targets.yaml",
                "--json",
            ),
            "plan-success-v1.json",
        ),
        (
            (
                "targets",
                "--targets-file",
                "examples/minimal/targets.yaml",
                "--json",
            ),
            "targets-success-v1.json",
        ),
        (
            ("validate", "missing-experiment.yaml", "--json"),
            "error-v1.json",
        ),
    ],
)
def test_cli_json_matches_checked_contract(
    arguments: tuple[str, ...], contract: str
) -> None:
    result = _run(*arguments)
    expected = json.loads((_ROOT / "docs" / "schemas" / contract).read_text())

    assert result.returncode == (0 if expected["ok"] else 1)
    assert result.stderr == ""
    assert json.loads(result.stdout) == expected


def test_plan_json_is_byte_for_byte_deterministic() -> None:
    arguments = (
        "plan",
        "examples/minimal/experiment.yaml",
        "--config",
        "examples/minimal/config.yaml",
        "--seed",
        "7",
        "--target",
        "local",
        "--targets-file",
        "examples/minimal/targets.yaml",
        "--json",
    )

    assert _run(*arguments).stdout == _run(*arguments).stdout


def test_human_commands_use_the_same_operations_and_separate_errors() -> None:
    valid = _run("validate", "examples/minimal/experiment.yaml")
    planned = _run(
        "plan",
        "examples/minimal/experiment.yaml",
        "--config",
        "examples/minimal/config.yaml",
        "--seed",
        "7",
        "--target",
        "local",
        "--targets-file",
        "examples/minimal/targets.yaml",
    )
    targets = _run("targets", "--targets-file", "examples/minimal/targets.yaml")
    invalid = _run("validate", "missing-experiment.yaml")

    assert valid.returncode == planned.returncode == targets.returncode == 0
    assert "Valid experiment: minimal" in valid.stdout
    assert "1 task(s)" in planned.stdout and "Seeds: 7" in planned.stdout
    assert "local: local / local" in targets.stdout
    assert invalid.returncode == 1 and invalid.stdout == ""
    assert "CONFIG_NOT_FOUND" in invalid.stderr
