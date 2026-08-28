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
                "plan",
                "examples/shoal/cpu/experiment.yaml",
                "--config",
                "examples/minimal/sweep.yaml",
                "--seeds",
                "0:1",
                "--target",
                "shoal",
                "--targets-file",
                "examples/shoal/targets.yaml",
                "--json",
            ),
            "plan-success-v3.json",
        ),
        (
            (
                "targets",
                "--targets-file",
                "examples/minimal/targets.yaml",
                "--json",
            ),
            "targets-success-v2.json",
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
    actual = json.loads(result.stdout)

    assert result.returncode == (0 if expected["ok"] else 1)
    assert result.stderr == ""
    if expected["operation"] == "plan" and expected["ok"]:
        snapshot = actual.pop("source_snapshot")
        assert actual.pop("format_version") == 10
        assert snapshot["file_count"] >= 1
        assert snapshot["size_bytes"] >= 1
        assert snapshot["source_root"]
        actual["format_version"] = expected["format_version"]
    assert actual == expected


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
        "examples/shoal/cpu/experiment.yaml",
        "--config",
        "examples/shoal/cpu/config.yaml",
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
    assert document["plan"]["resources"]["gpus_per_task"] == 0
    assert document["plan"]["native_options"] == {}
    assert document["plan"]["staging"] == {
        "backend": "rsync",
        "effective_config": "rsync_upload",
        "inputs_sealed": True,
        "results": "rsync_download",
        "source": "rsync_upload",
        "workspace_root": "/tmp/rundra-array-plan",
    }
    assert document["plan"]["validation"] == {
        "native_options": "validated",
        "resources": "validated",
        "target_capabilities": "validated",
    }
    assert document["plan"]["safety"] == {
        "contacts_target": False,
        "creates_run": False,
        "creates_workspace": False,
        "submits": False,
    }
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
    assert "Resources: nodes=1" in planned.stdout
    assert "Native options: none" in planned.stdout
    assert "Safety: validated offline" in planned.stdout
    assert "local: local / local" in targets.stdout
    assert invalid.returncode == 1 and invalid.stdout == ""
    assert "CONFIG_NOT_FOUND" in invalid.stderr


def test_json_is_a_common_option_before_or_after_the_command() -> None:
    before = _run("--json", "validate", "examples/minimal/experiment.yaml")
    after = _run("validate", "examples/minimal/experiment.yaml", "--json")

    assert before.returncode == after.returncode == 0
    assert before.stderr == after.stderr == ""
    assert before.stdout == after.stdout


def test_json_usage_failure_uses_the_common_error_envelope_and_exit_one() -> None:
    result = _run("--json", "status")
    expected = json.loads(
        (_ROOT / "docs/schemas/cli-usage-error-v1.json").read_text(encoding="utf-8")
    )

    assert result.returncode == 1
    assert result.stderr == ""
    assert json.loads(result.stdout) == expected


def test_human_usage_failure_uses_the_same_error_code() -> None:
    result = _run("status")

    assert result.returncode == 1
    assert result.stdout == ""
    assert "CLI_USAGE_ERROR" in result.stderr
    assert "run_id" in result.stderr


def test_unknown_command_is_a_structured_cli_operation_error() -> None:
    result = _run("--json", "unknown")
    document = json.loads(result.stdout)

    assert result.returncode == 1
    assert result.stderr == ""
    assert document["operation"] == "cli"
    assert document["error"]["code"] == "CLI_USAGE_ERROR"


def test_json_without_a_command_is_a_structured_usage_error() -> None:
    result = _run("--json")
    document = json.loads(result.stdout)

    assert result.returncode == 1
    assert result.stderr == ""
    assert document == {
        "error": {
            "code": "CLI_USAGE_ERROR",
            "details": {"command": "cli"},
            "message": "a command is required",
        },
        "format_version": 1,
        "ok": False,
        "operation": "cli",
    }


def test_no_arguments_remain_successful_human_help() -> None:
    result = _run()

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith("usage: rundr")
