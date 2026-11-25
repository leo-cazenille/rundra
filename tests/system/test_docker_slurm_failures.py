from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from rundra.cli.operations import _record_scheduler
from rundra.domain.models import RunId
from rundra.domain.states import ExecutionState
from rundra.persistence import JsonRunStore
from rundra.ports import SchedulerReference

pytestmark = pytest.mark.docker_slurm
_INPUTS = Path(__file__).with_name("docker_slurm")


@pytest.mark.parametrize(
    ("scenario", "wrapper_state", "task_native", "diagnostic"),
    [
        (
            "scratch",
            ExecutionState.FAILED,
            "BUNDLE_JOURNAL_MISSING",
            "SCRATCH_SETUP_FAILED",
        ),
        ("container", ExecutionState.SUCCEEDED, "BUNDLED_TASK_FAILED", None),
        (
            "required",
            ExecutionState.SUCCEEDED,
            "BUNDLED_TASK_FAILED",
            "REQUIRED_OUTPUT_MISSING",
        ),
    ],
)
def test_docker_slurm_failure_matrix(
    tmp_path: Path,
    docker_slurm_targets_source: Path,
    docker_slurm_target_name: str,
    scenario: str,
    wrapper_state: ExecutionState,
    task_native: str,
    diagnostic: str | None,
) -> None:
    definitions = yaml.safe_load(
        docker_slurm_targets_source.read_text(encoding="utf-8")
    )
    target = definitions["targets"][docker_slurm_target_name]
    target["execution"]["worker_pool"]["activation_threshold"] = 2
    target["execution"]["worker_pool"]["default_workers"] = 1
    if scenario == "container":
        del target["execution_storage"]
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        yaml.safe_dump(
            {
                "version": definitions["version"],
                "targets": {docker_slurm_target_name: target},
            }
        ),
        encoding="utf-8",
    )
    definition = yaml.safe_load(
        (_INPUTS / "experiment.yaml").read_text(encoding="utf-8")
    )
    if scenario in {"scratch", "container"}:
        definition["container"]["image"] = "/cluster/absent-rundra-diagnostic.sif"
    else:
        definition["version"] = 2
        definition["outputs"]["required"] = ["results/not-produced.json"]
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(yaml.safe_dump(definition), encoding="utf-8")
    records = tmp_path / "records"

    def rundr(*args: str) -> dict:
        completed = subprocess.run(
            (
                sys.executable,
                "-m",
                "rundra",
                *args,
                "--data-dir",
                str(records),
                "--json",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=240,
        )
        assert completed.returncode == 0, completed.stderr or completed.stdout
        return json.loads(completed.stdout)

    run_id = rundr(
        "submit",
        str(experiment),
        "--target",
        docker_slurm_target_name,
        "--targets-file",
        str(targets),
        "--source-root",
        str(_INPUTS),
        "--config",
        str(_INPUTS / "config.yaml"),
        "--seeds",
        "0:1",
    )["run"]["run_id"]
    result = rundr(
        "await", run_id, "--diagnostics", "--timeout", "180", "--poll-interval", "1"
    )
    assert result["await"]["condition_met"]
    status = result["await"]["runs"][0]
    assert status["state"] == "FAILED"
    assert status["diagnostics"]["task_counts"] == {"FAILED": 2}
    events = status["diagnostics"]["events"]
    assert {event["code"] for event in events} == (
        {diagnostic} if diagnostic else set()
    )
    record = JsonRunStore(records).load(RunId(run_id))
    assert set(record.task_native_states.values()) == {task_native}
    references = tuple(
        SchedulerReference(value)
        for value in sorted(set(record.task_scheduler_ids.values()))
    )
    wrappers = _record_scheduler(record).query(references)
    assert {item.state for item in wrappers} == {wrapper_state}
