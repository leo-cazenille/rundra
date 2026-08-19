from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from rundra.domain.models import RunId, Target
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_scaling]
_REPOSITORY_ROOT = Path(__file__).parents[2]
_SOURCE = _REPOSITORY_ROOT / "examples/shoal/scaling"
_SHOAL_HOSTS = {f"shoal{index}" for index in range(1, 9)}
_LAYOUTS = ((1, 8), (2, 20), (8, 40))


def _invoke_cli(
    arguments: tuple[str, ...], *, timeout: float = 900
) -> dict[str, object]:
    completed = subprocess.run(
        (sys.executable, "-m", "rundra", *arguments, "--json"),
        cwd=_REPOSITORY_ROOT,
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
    shutil.copytree(_SOURCE, source)
    experiment_source = source / "experiment.yaml"
    document = yaml.safe_load(experiment_source.read_text(encoding="utf-8"))
    document["container"]["image"] = str(image)
    experiment_source.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    return source


def _prepare_scaling_target(
    source: Path,
    destination: Path,
    target_name: str,
) -> Path:
    document = yaml.safe_load(source.read_text(encoding="utf-8"))
    assert document["version"] == 6, "Shoal scaling tests require target version 6"
    target = document["targets"][target_name]
    execution = target["execution"]
    worker_pool = execution["worker_pool"]
    assert worker_pool["max_workers"] >= 8
    assert worker_pool["max_task_slots_per_worker"] >= 40
    assert execution["max_active_tasks"] >= 320
    worker_pool["activation_threshold"] = 2
    destination.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return destination


def _assert_plan(
    document: dict[str, object], *, workers: int, slots: int, tasks: int
) -> None:
    plan = document["plan"]
    assert isinstance(plan, dict)
    assert plan["strategy"] == "worker_pool"
    task_space = plan["task_space"]
    assert isinstance(task_space, dict)
    assert task_space["task_count"] == tasks
    assert 1 <= task_space["preview_count"] <= tasks
    scheduling = plan["scheduling"]
    assert isinstance(scheduling, dict)
    assert scheduling["worker_count"] == workers
    assert scheduling["requested_workers"] == workers
    assert scheduling["task_slots_per_worker"] == slots
    assert scheduling["requested_task_slots_per_worker"] == slots
    assert scheduling["concurrent_task_capacity"] == tasks
    assert scheduling["max_lane_depth"] == 1
    resources = scheduling["worker_resources"]
    assert isinstance(resources, dict)
    assert resources["nodes"] == 1
    assert resources["tasks"] == slots
    assert resources["cpus_per_task"] == 1
    assert resources["memory_bytes"] == slots * 32 * 1024**2


@pytest.mark.parametrize(
    ("workers", "slots"),
    _LAYOUTS,
    ids=("one-worker-eight-slots", "two-workers-twenty-slots", "full-eight-by-forty"),
)
def test_shoal_explicit_worker_layouts(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_image: Path,
    workers: int,
    slots: int,
) -> None:
    task_count = workers * slots
    source = _prepare_source(tmp_path, shoal_cpu_image)
    targets_source = _prepare_scaling_target(
        shoal_targets_source,
        tmp_path / "targets.yaml",
        shoal_target_name,
    )
    data_dir = tmp_path / "records"
    destination = tmp_path / "retrieved"
    common = (
        "--config",
        str(source / "config.yaml"),
        "--seeds",
        f"0:{task_count - 1}",
        "--target",
        shoal_target_name,
        "--targets-file",
        str(targets_source),
        "--source-root",
        str(source),
        "--workers",
        str(workers),
        "--task-slots-per-worker",
        str(slots),
    )

    planned = _invoke_cli(("plan", str(source / "experiment.yaml"), *common))
    _assert_plan(planned, workers=workers, slots=slots, tasks=task_count)

    submitted = _invoke_cli(
        (
            "submit",
            str(source / "experiment.yaml"),
            *common,
            "--confirm-tasks",
            str(task_count),
            "--data-dir",
            str(data_dir),
        )
    )
    run = submitted["run"]
    assert isinstance(run, dict)
    assert run["tasks"] == task_count
    scheduler_ids = run["scheduler_job_ids"]
    assert isinstance(scheduler_ids, list) and len(scheduler_ids) == 1
    scheduler_id = str(scheduler_ids[0])
    run_id = RunId(str(run["run_id"]))

    waited = _invoke_cli(
        ("wait", str(run_id), "--timeout", "900", "--data-dir", str(data_dir))
    )
    wait = waited["wait"]
    assert isinstance(wait, dict)
    assert wait["terminal"] is True
    status = wait["status"]
    assert isinstance(status, dict)
    assert status["state"] == "SUCCEEDED"
    assert status["tasks"] == {"total": task_count, "succeeded": task_count}

    fetched = _invoke_cli(
        (
            "fetch",
            str(run_id),
            "--destination",
            str(destination),
            "--extract",
            "--data-dir",
            str(data_dir),
        )
    )
    fetch = fetched["fetch"]
    assert isinstance(fetch, dict)
    assert fetch["retrieval_state"] == "SUCCEEDED"

    result_sources = sorted(destination.glob("output/task_*/results/result.json"))
    assert len(result_sources) == task_count
    results = [json.loads(path.read_text(encoding="utf-8")) for path in result_sources]
    assert {item["seed"] for item in results} == set(range(task_count))
    host_by_seed = {int(item["seed"]): str(item["host"]) for item in results}
    observed_hosts = set(host_by_seed.values())
    assert observed_hosts <= _SHOAL_HOSTS
    for worker_index in range(workers):
        worker_hosts = {
            host_by_seed[seed] for seed in range(worker_index, task_count, workers)
        }
        assert len(worker_hosts) == 1

    journals = {
        path.name
        for path in (destination / "metadata/bundle-status").glob(
            f"{scheduler_id}_*.tsv"
        )
        if ".lane-" not in path.name
    }
    assert journals == {
        f"{scheduler_id}_{worker_index}.tsv" for worker_index in range(workers)
    }

    record = JsonRunStore(data_dir).load(run_id)
    assert set(record.allocated_nodes) == observed_hosts
    if workers == 1:
        assert len(observed_hosts) == 1
    elif workers == 2:
        assert 1 <= len(observed_hosts) <= 2
    else:
        assert workers == 8 and slots == 40
        assert observed_hosts == _SHOAL_HOSTS
        assert all(re.fullmatch(r"shoal[1-8]", host) for host in observed_hosts)
