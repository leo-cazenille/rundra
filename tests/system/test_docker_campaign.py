from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.docker_slurm
_ROOT = Path(__file__).parents[2]
_SOURCE = Path(__file__).with_name("docker_slurm")
_TARGETS = ("docker-campaign-a", "docker-campaign-b")


def _rundr(
    *arguments: str,
    timeout: float = 600,
) -> dict[str, Any]:
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


def _matching_strings(value: object, prefix: str) -> set[str]:
    if isinstance(value, str):
        return {value} if value.startswith(prefix) else set()
    if isinstance(value, list):
        return set().union(*(_matching_strings(item, prefix) for item in value))
    if isinstance(value, dict):
        return set().union(
            *(_matching_strings(item, prefix) for item in value.values())
        )
    return set()


def test_docker_campaign_runs_children_concurrently_and_fetches_all_results(
    tmp_path: Path,
    docker_slurm_targets_source: Path,
) -> None:
    data_dir = tmp_path / "records"
    config = tmp_path / "config.yaml"
    config.write_text("failure_seed: -1\nsleep_seconds: 5\n", encoding="utf-8")
    campaign = tmp_path / "campaign.yaml"
    campaign.write_text(
        f"""\
kind: campaign
version: 1
name: docker-two-targets
experiment: {json.dumps(str(_SOURCE / "experiment.yaml"))}
on_submit_failure: cancel
launches:
  - name: cluster-a
    target: {_TARGETS[0]}
    config: {json.dumps(str(config))}
    source_root: {json.dumps(str(_SOURCE))}
    destination: {json.dumps(str(tmp_path / "declared-a"))}
    seeds: "0:3"
    workers: 1
    task_slots_per_worker: 1
  - name: cluster-b
    target: {_TARGETS[1]}
    config: {json.dumps(str(config))}
    source_root: {json.dumps(str(_SOURCE))}
    destination: {json.dumps(str(tmp_path / "declared-b"))}
    seeds: "4:7"
    workers: 1
    task_slots_per_worker: 1
""",
        encoding="utf-8",
    )
    target_options = (
        "--targets-file",
        str(docker_slurm_targets_source),
    )
    store_options = (
        "--data-dir",
        str(data_dir),
    )

    planned = _rundr("plan", str(campaign), *target_options)
    assert _matching_strings(planned, "docker-campaign-") == set(_TARGETS)

    submitted = _rundr("submit", str(campaign), *target_options, *store_options)
    campaign_ids = _matching_strings(submitted, "campaign_")
    run_ids = _matching_strings(submitted, "run_")
    assert len(campaign_ids) == 1
    assert len(run_ids) == 2
    campaign_id = campaign_ids.pop()

    deadline = time.monotonic() + 30
    concurrent = False
    while time.monotonic() < deadline:
        states = {
            str(_rundr("status", run_id, *store_options)["status"]["state"])
            for run_id in run_ids
        }
        if states == {"RUNNING"}:
            concurrent = True
            break
        time.sleep(0.5)
    assert concurrent, "both campaign child Runs were not RUNNING concurrently"

    waited = _rundr(
        "wait",
        campaign_id,
        "--timeout",
        "300",
        *store_options,
    )
    assert "SUCCEEDED" in _matching_strings(waited, "SUCCEEDED")

    destination = tmp_path / "retrieved"
    _rundr(
        "fetch",
        campaign_id,
        "--destination",
        str(destination),
        "--mode",
        "copy",
        *store_options,
    )

    expected_seeds = {"cluster-a": set(range(4)), "cluster-b": set(range(4, 8))}
    for launch, seeds in expected_seeds.items():
        paths = sorted((destination / launch).glob("output/task_*/results/result.json"))
        assert len(paths) == 4
        documents = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
        assert {item["seed"] for item in documents} == seeds
        assert {item["host"] for item in documents} <= {"compute1", "compute2"}

    targets = set()
    for run_id in run_ids:
        inspected = _rundr("inspect", run_id, *store_options)
        record = inspected["record"]
        assert record["run"]["state"] == "SUCCEEDED"
        assert record["scheduler_job_ids"]
        targets.add(record["run"]["target"]["name"])
    assert targets == set(_TARGETS)

    tasks = _rundr("tasks", campaign_id, "--limit", "8", *store_options)
    selectors = _matching_strings(tasks, "cluster-")
    assert {selector.split("/", 1)[0] for selector in selectors if "/" in selector} == {
        "cluster-a",
        "cluster-b",
    }
