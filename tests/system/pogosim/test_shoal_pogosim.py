"""Opt-in live Pogosim smoke test for the Shoal Slurm target."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

from rundra.adapters import (
    RemoteApptainerRuntime,
    RemotePreflight,
    RsyncStager,
    SSHTransport,
)
from rundra.cli.operations import plan_operation
from rundra.config.experiments import load_experiment
from rundra.domain.models import RunId, Target
from rundra.orchestration.preflight import PreflightStatus
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_pogosim]

_REPOSITORY_ROOT = Path(__file__).parents[3]
_EXAMPLE_ROOT = _REPOSITORY_ROOT / "examples" / "pogosim-shoal"
_SEEDS = (0, 1, 2)
_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def _required_path(variable: str, *, directory: bool) -> Path:
    value = os.environ.get(variable)
    if value is None or not value.strip():
        pytest.fail(f"{variable} must be set when the Pogosim test is enabled")
    path = Path(value).expanduser()
    exists = path.is_dir() if directory else path.is_file()
    if not exists:
        kind = "directory" if directory else "file"
        pytest.fail(f"{variable} must name an existing {kind}: {path}")
    return path


def _prepare_experiment(root: Path, image: Path) -> Path:
    document = yaml.safe_load(
        (_EXAMPLE_ROOT / "experiment.yaml").read_text(encoding="utf-8")
    )
    document["container"]["image"] = str(image)
    experiment = root / "experiment.yaml"
    experiment.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return experiment


def _invoke_cli(
    arguments: tuple[str, ...],
    *,
    expected_exit: int | tuple[int, ...] = 0,
    timeout: float = 600,
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
    expected_exits = (
        (expected_exit,) if isinstance(expected_exit, int) else expected_exit
    )
    if completed.returncode not in expected_exits:
        pytest.fail(
            f"rundr {arguments[0]} exited {completed.returncode}, expected "
            f"one of {expected_exits}: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        pytest.fail(
            f"rundr {arguments[0]} returned invalid JSON: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
        raise AssertionError from error
    if not isinstance(value, dict):
        pytest.fail(f"rundr {arguments[0]} returned a non-object JSON document")
    return value


def _require_plan_and_preflight(
    experiment: Path,
    source_root: Path,
    targets_source: Path,
    target_name: str,
    target: Target,
) -> None:
    plan_result = plan_operation(
        experiment,
        _EXAMPLE_ROOT / "config.yaml",
        targets_source,
        target_name,
        seeds="0:2",
    )
    if plan_result.error is not None:
        pytest.fail(
            "Pogosim plan failed "
            f"[{plan_result.error.code}]: {plan_result.error.message}"
        )
    assert plan_result.value is not None
    plan = plan_result.value.plan
    assert plan.strategy == "slurm_array"
    assert len(plan.groups) == 1
    assert [unit.seed for unit in plan.units] == list(_SEEDS)
    assert [
        (str(item.task_id), item.seed, item.array_index) for item in plan.array_mapping
    ] == [
        ("task_000000", 0, 0),
        ("task_000001", 1, 1),
        ("task_000002", 2, 2),
    ]

    host = target.transport.options.get("host")
    if type(host) is not str:
        pytest.fail("Shoal SSH target has no string host option")
    transport = SSHTransport(host)
    stager = RsyncStager(transport, host=host)
    report = RemotePreflight(
        target,
        load_experiment(experiment),
        transport,
        rsync_check=stager.check,
        runtime=RemoteApptainerRuntime(transport),
    ).run()
    failures = [
        check for check in report.checks if check.status is not PreflightStatus.PASSED
    ]
    if failures:
        pytest.fail(
            "Pogosim preflight failed: "
            + "; ".join(f"{check.layer}/{check.name}" for check in failures)
        )

    binary = source_root / "examples/run_and_tumble/run_and_tumble"
    if not binary.is_file() or not os.access(binary, os.X_OK):
        pytest.fail(f"Pogosim run_and_tumble is not executable: {binary}")


def _wait_for_terminal(run_id: RunId, data_dir: Path) -> dict[str, object]:
    deadline = time.monotonic() + 15 * 60
    while time.monotonic() < deadline:
        document = _invoke_cli(("status", str(run_id), "--data-dir", str(data_dir)))
        status = document.get("status")
        if not isinstance(status, dict):
            pytest.fail("status result has no status payload")
        if status.get("state") in _TERMINAL_STATES:
            return status
        time.sleep(2)
    pytest.fail(f"Pogosim Run {run_id} did not become terminal within 15 minutes")


def _assert_feather_signature(path: Path) -> None:
    with path.open("rb") as stream:
        header = stream.read(6)
        stream.seek(-6, 2)
        footer = stream.read(6)
    assert header == b"ARROW1", f"missing Arrow header: {path}"
    assert footer == b"ARROW1", f"missing Arrow footer: {path}"


def test_three_seed_pogosim_run_on_shoal(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
) -> None:
    source_root = _required_path("RUNDRA_POGOSIM_SOURCE_ROOT", directory=True)
    image = _required_path("RUNDRA_POGOSIM_IMAGE", directory=False)
    experiment = _prepare_experiment(tmp_path, image)
    data_dir = tmp_path / "records"
    automatic_destination = tmp_path / "automatic-retrieval"
    _require_plan_and_preflight(
        experiment,
        source_root,
        shoal_targets_source,
        shoal_target_name,
        shoal_target,
    )

    submission = _invoke_cli(
        (
            "submit",
            str(experiment),
            "--config",
            str(_EXAMPLE_ROOT / "config.yaml"),
            "--seeds",
            "0:2",
            "--target",
            shoal_target_name,
            "--targets-file",
            str(shoal_targets_source),
            "--source-root",
            str(source_root),
            "--destination",
            str(automatic_destination),
            "--data-dir",
            str(data_dir),
        )
    )
    run = submission.get("run")
    if not isinstance(run, dict):
        pytest.fail("submit result has no Run payload")
    assert run["state"] == "SUBMITTED"
    assert run["seeds"] == list(_SEEDS)
    run_id = RunId(run["run_id"])

    status = _wait_for_terminal(run_id, data_dir)
    assert status["state"] == "SUCCEEDED"
    assert status["tasks"] == {"total": 3, "succeeded": 3}

    record = JsonRunStore(data_dir).load(run_id)
    assert len(record.scheduler_job_ids) == 1
    root_id = record.scheduler_job_ids[0]
    assert record.task_scheduler_ids == {
        task.id: f"{root_id}_{index}" for index, task in enumerate(record.run.tasks)
    }
    assert len(set(record.task_scheduler_ids.values())) == 3

    for index, task in enumerate(record.run.tasks):
        logs = _invoke_cli(
            (
                "logs",
                str(run_id),
                "--task",
                str(index),
                "--data-dir",
                str(data_dir),
            )
        )
        assert isinstance(logs.get("logs"), dict)
        assert logs["logs"]["task_id"] == str(task.id)

    destination = tmp_path / "retrieved"
    fetched = _invoke_cli(
        (
            "fetch",
            str(run_id),
            "--destination",
            str(destination),
            "--data-dir",
            str(data_dir),
        )
    )
    assert isinstance(fetched.get("fetch"), dict)
    feather_files = sorted(destination.rglob("data.feather"))
    assert len(feather_files) == 3
    for feather_file in feather_files:
        _assert_feather_signature(feather_file)
