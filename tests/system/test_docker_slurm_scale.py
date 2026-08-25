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
_PREPARED = _SOURCE / "prepared"


def _rundr(
    *arguments: str,
    timeout: float = 1200,
    expected_returncode: int = 0,
) -> dict[str, object]:
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
    assert completed.returncode == expected_returncode, completed.stderr or document
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
    diagnosed = _rundr(
        "doctor",
        "--target",
        docker_slurm_target_name,
        "--targets-file",
        str(docker_slurm_targets_source),
        "--connect",
        "--scheduler-probe",
    )
    doctor = diagnosed["doctor"]
    assert isinstance(doctor, dict)
    checks = doctor["checks"]
    assert isinstance(checks, list)
    scheduler_probe = next(
        check
        for check in checks
        if isinstance(check, dict) and check.get("name") == "scheduler_probe"
    )
    assert scheduler_probe["status"] == "pass"
    assert "allocation-local scratch" in str(scheduler_probe["message"])
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

    inspected = _rundr("inspect", run_id, *store_options)
    record = inspected["record"]
    assert isinstance(record, dict)
    scheduler_metadata = record["scheduler_metadata"]
    assert isinstance(scheduler_metadata, dict)
    assert scheduler_metadata["execution_storage.active_environment"] == (
        "SLURM_TMPDIR"
    )

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


def test_docker_slurm_bounded_array_preserves_partial_failure(
    tmp_path: Path,
    docker_slurm_targets_source: Path,
    docker_slurm_target_name: str,
) -> None:
    destination = tmp_path / "retrieved"
    document = _rundr(
        "run",
        str(_SOURCE / "experiment.yaml"),
        "--config",
        str(_SOURCE / "failure.yaml"),
        "--seeds",
        "0:7",
        "--target",
        docker_slurm_target_name,
        "--targets-file",
        str(docker_slurm_targets_source),
        "--source-root",
        str(_SOURCE),
        "--destination",
        str(destination),
        "--data-dir",
        str(tmp_path / "records"),
        expected_returncode=2,
    )

    run = document["run"]
    assert isinstance(run, dict)
    assert run["state"] == "FAILED"
    assert run["retrieval_state"] == "SUCCEEDED"
    assert len(run["scheduler_job_ids"]) == 1
    results = sorted(destination.glob("output/task_*/results/result.json"))
    assert len(results) == 8


def test_docker_slurm_builds_image_and_application_in_allocation_scratch(
    tmp_path: Path,
    docker_slurm_targets_source: Path,
    docker_slurm_target_name: str,
) -> None:
    data_dir = tmp_path / "records"
    submitted = _rundr(
        "submit",
        str(_PREPARED / "experiment.yaml"),
        "--project-file",
        str(_PREPARED / "rundra.yaml"),
        "--config",
        str(_PREPARED / "config.yaml"),
        "--seed",
        "41",
        "--target",
        docker_slurm_target_name,
        "--targets-file",
        str(docker_slurm_targets_source),
        "--source-root",
        str(_PREPARED),
        "--data-dir",
        str(data_dir),
    )
    run = submitted["run"]
    assert isinstance(run, dict)
    run_id = str(run["run_id"])

    waited = _rundr("wait", run_id, "--timeout", "600", "--data-dir", str(data_dir))
    wait = waited["wait"]
    assert isinstance(wait, dict) and wait["terminal"] is True
    status = wait["status"]
    assert isinstance(status, dict) and status["state"] == "SUCCEEDED"

    inspected = _rundr("inspect", run_id, "--data-dir", str(data_dir))
    record = inspected["record"]
    assert isinstance(record, dict)
    preparation = record["preparation"]
    assert isinstance(preparation, dict)
    assert preparation["image_action"] == "build_definition_image"
    assert preparation["build_action"] == "build_and_publish"
    assert preparation["builder_scheduler_id"] is not None
    scheduler_metadata = record["scheduler_metadata"]
    assert isinstance(scheduler_metadata, dict)
    assert scheduler_metadata["execution_storage.type"] == "slurm_scratch"
    assert scheduler_metadata["execution_storage.active_environment"] == (
        "SLURM_TMPDIR"
    )
    assert scheduler_metadata["execution_storage.stage_image"] is True
    assert scheduler_metadata["execution_storage.copy_back"] == "task"

    destination = tmp_path / "prepared-retrieved"
    fetched = _rundr(
        "fetch",
        run_id,
        "--destination",
        str(destination),
        "--extract",
        "--data-dir",
        str(data_dir),
    )
    fetch = fetched["fetch"]
    assert isinstance(fetch, dict) and fetch["retrieval_state"] == "SUCCEEDED"
    result = json.loads(
        (destination / "output/results/result.json").read_text(encoding="utf-8")
    )
    assert result == {"seed": 41}


def test_docker_slurm_cancels_bounded_array(
    tmp_path: Path,
    docker_slurm_targets_source: Path,
    docker_slurm_target_name: str,
) -> None:
    data_dir = tmp_path / "records"
    submitted = _rundr(
        "submit",
        str(_SOURCE / "experiment.yaml"),
        "--config",
        str(_SOURCE / "cancel.yaml"),
        "--seeds",
        "0:7",
        "--target",
        docker_slurm_target_name,
        "--targets-file",
        str(docker_slurm_targets_source),
        "--source-root",
        str(_SOURCE),
        "--data-dir",
        str(data_dir),
    )
    run = submitted["run"]
    assert isinstance(run, dict)
    run_id = str(run["run_id"])

    _rundr("cancel", run_id, "--data-dir", str(data_dir))
    waited = _rundr(
        "wait",
        run_id,
        "--timeout",
        "120",
        "--data-dir",
        str(data_dir),
    )
    wait = waited["wait"]
    assert isinstance(wait, dict)
    status = wait["status"]
    assert isinstance(status, dict)
    assert status["state"] == "CANCELLED"
