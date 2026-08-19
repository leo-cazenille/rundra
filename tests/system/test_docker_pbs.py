from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.docker_pbs

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/system/docker_slurm"
TARGETS = Path(os.environ.get("RUNDRA_DOCKER_PBS_TARGETS_FILE", "/missing"))
TARGET = "docker-pbs"


def _rundr(
    *arguments: str, expected: int = 0
) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    executable = shutil.which("rundr")
    assert executable is not None
    completed = subprocess.run(
        (executable, *arguments, "--json"),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == expected, completed.stderr + completed.stdout
    return completed, json.loads(completed.stdout)


def _common(tmp_path: Path, config: str, destination: str) -> tuple[str, ...]:
    return (
        str(FIXTURE / "experiment.yaml"),
        "--config",
        str(FIXTURE / config),
        "--target",
        TARGET,
        "--targets-file",
        str(TARGETS),
        "--source-root",
        str(FIXTURE),
        "--destination",
        str(tmp_path / destination),
        "--data-dir",
        str(tmp_path / "records"),
    )


def test_openpbs_array_success_retrieval_and_compute_placement(tmp_path: Path) -> None:
    _, payload = _rundr(
        "run", *_common(tmp_path, "config.yaml", "success"), "--seeds", "0:7"
    )
    run = payload["run"]
    assert run["state"] == "SUCCEEDED"
    assert run["retrieval_state"] == "SUCCEEDED"
    assert run["tasks"] == 8
    assert len(run["scheduler_job_ids"]) == 1
    texts = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in (tmp_path / "success").rglob("*")
        if path.is_file() and path.stat().st_size < 1_000_000
    )
    hosts = set(re.findall(r"compute[12]", texts))
    assert hosts
    assert hosts <= {"compute1", "compute2"}


def test_openpbs_array_partial_failure_retrieves_completed_outputs(
    tmp_path: Path,
) -> None:
    _, payload = _rundr(
        "run",
        *_common(tmp_path, "failure.yaml", "failure"),
        "--seeds",
        "0:7",
        expected=2,
    )
    run = payload["run"]
    assert run["state"] == "FAILED"
    assert run["retrieval_state"] == "SUCCEEDED"
    assert run["tasks"] == 8
    assert any(code != 0 for code in run["task_exit_codes"].values())
    assert any(path.is_file() for path in (tmp_path / "failure").rglob("*"))


def test_openpbs_array_cancellation_is_durable(tmp_path: Path) -> None:
    _, submitted = _rundr(
        "submit",
        *_common(tmp_path, "cancel.yaml", "cancelled"),
        "--seeds",
        "0:3",
    )
    run_id = submitted["run"]["run_id"]
    _, cancelled = _rundr("cancel", run_id, "--data-dir", str(tmp_path / "records"))
    assert cancelled["cancel"]["state"] == "CANCELLED"
    _, status = _rundr("status", run_id, "--data-dir", str(tmp_path / "records"))
    assert status["status"]["state"] == "CANCELLED"
