from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.docker_slurm
_ROOT = Path(__file__).parents[2]
_SOURCE = Path(__file__).with_name("docker_slurm")


def _rundr(*arguments: str, timeout: float = 1200) -> dict[str, object]:
    completed = subprocess.run(
        (sys.executable, "-m", "rundra", *arguments, "--json"),
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=os.environ.copy(),
    )
    document = json.loads(completed.stdout)
    assert completed.returncode == 0, completed.stderr or document
    assert document["ok"] is True
    return document


def test_docker_slurm_runs_one_thousand_tasks_on_compute_nodes(
    tmp_path: Path,
    docker_slurm_targets_source: Path,
    docker_slurm_target_name: str,
) -> None:
    target_options = (
        "--targets-file",
        str(docker_slurm_targets_source),
    )
    store_options = (
        "--data-dir",
        str(tmp_path / "records"),
    )
    _rundr(
        "doctor",
        str(_SOURCE / "experiment.yaml"),
        "--target",
        docker_slurm_target_name,
        "--targets-file",
        str(docker_slurm_targets_source),
        "--connect",
    )
    submitted = _rundr(
        "submit",
        str(_SOURCE / "experiment.yaml"),
        "--config",
        str(_SOURCE / "config.yaml"),
        "--seeds",
        "0:999",
        "--target",
        docker_slurm_target_name,
        "--source-root",
        str(_SOURCE),
        "--confirm-tasks",
        "1000",
        *target_options,
        *store_options,
    )
    run = submitted["run"]
    assert isinstance(run, dict)
    run_id = str(run["run_id"])
    assert len(run["scheduler_job_ids"]) <= 2

    waited = _rundr("wait", run_id, "--timeout", "1200", *store_options)
    wait = waited["wait"]
    assert isinstance(wait, dict) and wait["terminal"] is True
    fetched = _rundr(
        "fetch",
        run_id,
        "--destination",
        str(tmp_path / "retrieved"),
        "--extract",
        *store_options,
    )
    fetch = fetched["fetch"]
    assert isinstance(fetch, dict)
    assert fetch["retrieval_state"] == "SUCCEEDED"

    results = sorted((tmp_path / "retrieved").glob("output/task_*/results/result.json"))
    assert len(results) == 1000
    documents = [json.loads(path.read_text(encoding="utf-8")) for path in results]
    assert {item["seed"] for item in documents} == set(range(1000))
    assert {item["host"] for item in documents} <= {"compute1", "compute2"}
