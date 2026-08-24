from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.docker_htcondor
ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/system/docker_htcondor"
TARGETS = Path(os.environ.get("RUNDRA_DOCKER_HTCONDOR_TARGETS_FILE", "/missing"))


def test_htcondor_array_lifecycle_retrieval_and_compute_placement(
    tmp_path: Path,
) -> None:
    executable = shutil.which("rundr")
    assert executable is not None
    completed = subprocess.run(
        (
            executable,
            "run",
            str(FIXTURE / "experiment.yaml"),
            "--config",
            str(FIXTURE / "config.yaml"),
            "--seeds",
            "0:7",
            "--target",
            "docker-htcondor",
            "--targets-file",
            str(TARGETS),
            "--source-root",
            str(FIXTURE),
            "--destination",
            str(tmp_path / "retrieved"),
            "--data-dir",
            str(tmp_path / "records"),
            "--json",
        ),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    run = json.loads(completed.stdout)["run"]
    assert run["state"] == "SUCCEEDED"
    assert run["retrieval_state"] == "SUCCEEDED"
    assert run["tasks"] == 8
    assert len(run["scheduler_job_ids"]) == 1
    hosts = {
        path.read_text(encoding="utf-8").strip()
        for path in (tmp_path / "retrieved").rglob("hostname.txt")
    }
    assert hosts and hosts <= {"execute1", "execute2"}
