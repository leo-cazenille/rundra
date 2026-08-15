from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["rundr", *arguments],
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


def test_slurm_array_plan_json_exposes_explicit_task_seed_index_mapping(
    tmp_path: Path,
) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        """version: 1
targets:
  shoal:
    transport: {type: ssh, host: fishvision}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /tmp/rundra-array-plan
""",
        encoding="utf-8",
    )

    completed = _run(
        "plan",
        "examples/minimal/experiment.yaml",
        "--config",
        "examples/minimal/config.yaml",
        "--seeds",
        "7:9",
        "--target",
        "shoal",
        "--targets-file",
        str(targets),
        "--json",
    )
    document = json.loads(completed.stdout)

    assert completed.returncode == 0
    assert document["plan"]["strategy"] == "slurm_array"
    assert document["plan"]["groups"] == [
        {
            "task_ids": [
                "task_000000",
                "task_000001",
                "task_000002",
            ]
        }
    ]
    assert document["plan"]["array_mapping"] == [
        {"task_id": "task_000000", "seed": 7, "array_index": 0},
        {"task_id": "task_000001", "seed": 8, "array_index": 1},
        {"task_id": "task_000002", "seed": 9, "array_index": 2},
    ]
    assert "run_id" not in document["plan"]
    assert "scheduler_job_id" not in document["plan"]


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
