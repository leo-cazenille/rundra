from __future__ import annotations

import json
import math
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from rundra.domain.models import RunId, Target
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_multiprocessing]
_ROOT = Path(__file__).parents[2]
_EXAMPLE = _ROOT / "examples/python-multiprocessing"
_SHOAL_HOSTS = {f"shoal{index}" for index in range(1, 9)}
_TASK_COUNT = 20


def _invoke(arguments: tuple[str, ...], *, timeout: float = 900) -> dict[str, object]:
    completed = subprocess.run(
        (sys.executable, "-m", "rundra", *arguments, "--json"),
        cwd=_ROOT,
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        shell=False,
        timeout=timeout,
    )
    try:
        document = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        pytest.fail(
            f"rundr {arguments[0]} returned invalid JSON: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
        raise AssertionError from error
    assert completed.returncode == 0, completed.stderr or document
    assert isinstance(document, dict)
    assert document["ok"] is True
    return document


def _prepare_source(root: Path, image: Path) -> Path:
    source = root / "source"
    shutil.copytree(_EXAMPLE, source)
    experiment_source = source / "experiment-shoal.yaml"
    document = yaml.safe_load(experiment_source.read_text(encoding="utf-8"))
    document["container"]["image"] = str(image)
    experiment_source.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    return source


def _prepare_target(source: Path, destination: Path, target_name: str) -> Path:
    document = yaml.safe_load(source.read_text(encoding="utf-8"))
    assert document["version"] >= 6
    execution = document["targets"][target_name]["execution"]
    workers = execution["worker_pool"]
    assert workers["max_workers"] >= 2
    assert workers["max_task_slots_per_worker"] >= 10
    assert execution["max_active_tasks"] >= _TASK_COUNT
    workers["activation_threshold"] = 2
    destination.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return destination


def test_shoal_runs_bounded_python_processes_on_two_full_nodes(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_image: Path,
) -> None:
    del shoal_target
    source = _prepare_source(tmp_path, shoal_cpu_image)
    targets = _prepare_target(
        shoal_targets_source,
        tmp_path / "targets.yaml",
        shoal_target_name,
    )
    records = tmp_path / "records"
    destination = tmp_path / "retrieved"
    common = (
        "--config",
        str(source / "config.json"),
        "--seeds",
        "0:19",
        "--target",
        shoal_target_name,
        "--targets-file",
        str(targets),
        "--source-root",
        str(source),
        "--workers",
        "2",
        "--task-slots-per-worker",
        "10",
    )

    planned = _invoke(("plan", str(source / "experiment-shoal.yaml"), *common))
    plan = planned["plan"]
    assert isinstance(plan, dict)
    assert plan["strategy"] == "worker-pool"
    scheduling = plan["scheduling"]
    assert isinstance(scheduling, dict)
    assert scheduling["worker_count"] == 2
    assert scheduling["task_slots_per_worker"] == 10
    assert scheduling["concurrent_task_capacity"] == _TASK_COUNT
    worker_resources = scheduling["worker_resources"]
    assert isinstance(worker_resources, dict)
    assert worker_resources["nodes"] == 1
    assert worker_resources["tasks"] == 10
    assert worker_resources["cpus_per_task"] == 4
    assert worker_resources["memory_bytes"] == 10 * 256 * 1024**2

    submitted = _invoke(
        (
            "submit",
            str(source / "experiment-shoal.yaml"),
            *common,
            "--confirm-tasks",
            str(_TASK_COUNT),
            "--data-dir",
            str(records),
        )
    )
    run = submitted["run"]
    assert isinstance(run, dict)
    run_id = RunId(str(run["run_id"]))

    waited = _invoke(
        ("wait", str(run_id), "--timeout", "900", "--data-dir", str(records))
    )
    wait = waited["wait"]
    assert isinstance(wait, dict)
    assert wait["terminal"] is True
    status = wait["status"]
    assert isinstance(status, dict)
    assert status["state"] == "SUCCEEDED"
    assert status["tasks"] == {"total": _TASK_COUNT, "succeeded": _TASK_COUNT}

    fetched = _invoke(
        (
            "fetch",
            str(run_id),
            "--destination",
            str(destination),
            "--extract",
            "--data-dir",
            str(records),
        )
    )
    fetch = fetched["fetch"]
    assert isinstance(fetch, dict)
    assert fetch["retrieval_state"] == "SUCCEEDED"

    sources = sorted(destination.glob("output/task_*/results/result.json"))
    assert len(sources) == _TASK_COUNT
    results = [json.loads(path.read_text(encoding="utf-8")) for path in sources]
    assert {item["seed"] for item in results} == set(range(_TASK_COUNT))
    for result in results:
        assert result["processes"] == 4
        assert result["allocated_cpus"] == 4
        assert abs(result["pi_estimate"] - math.pi) <= 1e-10
        partitions = result["partitions"]
        assert len(partitions) == 4
        assert len({item["pid"] for item in partitions}) == 4
    observed_hosts = {str(result["host"]) for result in results}
    assert observed_hosts <= _SHOAL_HOSTS
    assert len(observed_hosts) == 2

    record = JsonRunStore(records).load(run_id)
    assert set(record.allocated_nodes) == observed_hosts
    assert len(record.allocated_nodes) == 2
