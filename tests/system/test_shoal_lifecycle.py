from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path, PurePosixPath

import pytest
import yaml

from rundra.adapters import RemoteApptainerRuntime, RsyncStager, SSHTransport
from rundra.cli.operations import plan_operation
from rundra.config.experiments import load_experiment
from rundra.domain.models import RunId, Target
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.preflight import PreflightStatus, RemotePreflight
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_lifecycle]
_REPOSITORY_ROOT = Path(__file__).parents[2]
_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}


def _prepare_source(root: Path, image: Path) -> Path:
    source = root / "source"
    shutil.copytree(_REPOSITORY_ROOT / "examples/shoal/lifecycle", source)
    experiment_source = source / "experiment.yaml"
    document = yaml.safe_load(experiment_source.read_text(encoding="utf-8"))
    document["container"]["image"] = str(image)
    experiment_source.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    return source


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
    source: Path,
    targets_source: Path,
    target_name: str,
    target: Target,
) -> None:
    plan = plan_operation(
        source / "experiment.yaml",
        source / "config.yaml",
        targets_source,
        target_name,
        seed=71,
    )
    if plan.error is not None:
        pytest.fail(f"M6.6 plan failed [{plan.error.code}]: {plan.error.message}")
    host = target.transport.options.get("host")
    if type(host) is not str:
        pytest.fail("Shoal SSH target has no string host option")
    transport = SSHTransport(host)
    stager = RsyncStager(transport, host=host)
    report = RemotePreflight(
        target,
        load_experiment(source / "experiment.yaml"),
        transport,
        rsync_check=stager.check,
        runtime=RemoteApptainerRuntime(transport),
    ).run()
    failures = [
        check for check in report.checks if check.status is not PreflightStatus.PASSED
    ]
    if failures:
        pytest.fail(
            "M6.6 preflight failed: "
            + "; ".join(f"{check.layer}/{check.name}" for check in failures)
        )


def _submit(
    source: Path,
    targets_source: Path,
    target_name: str,
    data_dir: Path,
    destination: Path,
    seed: int,
) -> RunId:
    document = _invoke_cli(
        (
            "submit",
            str(source / "experiment.yaml"),
            "--config",
            str(source / "config.yaml"),
            "--seed",
            str(seed),
            "--target",
            target_name,
            "--targets-file",
            str(targets_source),
            "--source-root",
            str(source),
            "--destination",
            str(destination),
            "--data-dir",
            str(data_dir),
        )
    )
    run = document.get("run")
    if not isinstance(run, dict):
        pytest.fail("submit result has no Run payload")
    assert run["state"] == "SUBMITTED"
    return RunId(run["run_id"])


def _status(run_id: RunId, data_dir: Path) -> dict[str, object]:
    document = _invoke_cli(("status", str(run_id), "--data-dir", str(data_dir)))
    status = document.get("status")
    if not isinstance(status, dict):
        pytest.fail("status result has no status payload")
    return status


def _wait_for(
    run_id: RunId,
    data_dir: Path,
    wanted: set[str],
    *,
    timeout: float = 300,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = _status(run_id, data_dir)
        state = status.get("state")
        if state in wanted:
            return status
        time.sleep(1)
    pytest.fail(f"Run {run_id} did not reach one of {sorted(wanted)}")


def _wait_for_started_logs(
    run_id: RunId, data_dir: Path, *, timeout: float = 120
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        document = _invoke_cli(
            ("logs", str(run_id), "--data-dir", str(data_dir)),
            expected_exit=(0, 1),
        )
        logs = document.get("logs")
        if isinstance(logs, dict) and logs.get("stdout") == (
            "RUNDRA_LIFECYCLE_STDOUT started seed=73\n"
        ):
            return logs
        error = document.get("error")
        if not isinstance(error, dict) or error.get("code") != "LOG_READ_FAILED":
            pytest.fail(f"Unexpected pre-cancellation logs result for {run_id}")
        time.sleep(1)
    pytest.fail(f"Run {run_id} did not expose its started log before cancellation")


def _fetch_twice(run_id: RunId, data_dir: Path, destination: Path) -> None:
    arguments = (
        "fetch",
        str(run_id),
        "--destination",
        str(destination),
        "--data-dir",
        str(data_dir),
    )
    first = _invoke_cli(arguments)
    second = _invoke_cli(arguments)
    assert first["fetch"] == second["fetch"]


def test_shoal_disconnected_async_lifecycle_is_repeatable_and_isolated(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_image: Path,
) -> None:
    source = _prepare_source(tmp_path, shoal_cpu_image)
    data_dir = tmp_path / "records"
    _require_plan_and_preflight(
        source, shoal_targets_source, shoal_target_name, shoal_target
    )

    first_id = _submit(
        source,
        shoal_targets_source,
        shoal_target_name,
        data_dir,
        tmp_path / "first-auto",
        71,
    )
    second_id = _submit(
        source,
        shoal_targets_source,
        shoal_target_name,
        data_dir,
        tmp_path / "second-auto",
        72,
    )
    assert first_id != second_id

    store = JsonRunStore(data_dir)
    first_submitted = store.load(first_id)
    second_submitted = store.load(second_id)
    assert first_submitted.run.state is ExecutionState.SUBMITTED
    assert second_submitted.run.state is ExecutionState.SUBMITTED
    assert first_submitted.scheduler_job_ids != second_submitted.scheduler_job_ids

    assert _wait_for(first_id, data_dir, _TERMINAL)["state"] == "SUCCEEDED"
    assert _wait_for(second_id, data_dir, _TERMINAL)["state"] == "SUCCEEDED"

    for run_id, seed, name in (
        (first_id, 71, "first"),
        (second_id, 72, "second"),
    ):
        logs = _invoke_cli(("logs", str(run_id), "--data-dir", str(data_dir)))["logs"]
        assert isinstance(logs, dict)
        assert logs["stdout"] == f"RUNDRA_LIFECYCLE_STDOUT started seed={seed}\n"
        assert logs["stderr"] == f"RUNDRA_LIFECYCLE_STDERR completed seed={seed}\n"

        destination = tmp_path / f"{name}-retrieved"
        _fetch_twice(run_id, data_dir, destination)
        evidence = destination / "output/results/evidence.txt"
        assert evidence.read_text(encoding="utf-8") == (
            f"seed={seed}\n"
            "phase=started\n"
            "config:\n"
            "label: shoal-lifecycle\n"
            "phase=completed\n"
        )

        cancelled = _invoke_cli(("cancel", str(run_id), "--data-dir", str(data_dir)))
        repeated = _invoke_cli(("cancel", str(run_id), "--data-dir", str(data_dir)))
        assert cancelled == repeated
        record = store.load(run_id)
        keys = [
            (artifact.kind, artifact.task_id, artifact.path)
            for artifact in record.artifacts
        ]
        assert len(keys) == len(set(keys))
        assert record.run.retrieval_state is RetrievalState.SUCCEEDED

    first_workspace = PurePosixPath(shoal_target.workspace) / "runs" / str(first_id)
    second_workspace = PurePosixPath(shoal_target.workspace) / "runs" / str(second_id)
    assert first_workspace != second_workspace

    cancel_id = _submit(
        source,
        shoal_targets_source,
        shoal_target_name,
        data_dir,
        tmp_path / "cancel-auto",
        73,
    )
    _wait_for(cancel_id, data_dir, {"RUNNING"})
    started_logs = _wait_for_started_logs(cancel_id, data_dir)
    cancelled = _invoke_cli(
        ("cancel", str(cancel_id), "--data-dir", str(data_dir)), timeout=300
    )
    status = cancelled.get("cancel")
    assert isinstance(status, dict)
    assert status["state"] == "CANCELLED"
    assert _status(cancel_id, data_dir)["state"] == "CANCELLED"
    assert (
        _invoke_cli(("cancel", str(cancel_id), "--data-dir", str(data_dir)))
        == cancelled
    )

    logs = _invoke_cli(("logs", str(cancel_id), "--data-dir", str(data_dir)))["logs"]
    assert isinstance(logs, dict)
    assert logs == started_logs
    cancel_destination = tmp_path / "cancel-retrieved"
    _fetch_twice(cancel_id, data_dir, cancel_destination)
    partial = cancel_destination / "output/results/evidence.txt"
    assert partial.read_text(encoding="utf-8") == (
        "seed=73\nphase=started\nconfig:\nlabel: shoal-lifecycle\n"
    )
    cancelled_record = store.load(cancel_id)
    assert cancelled_record.run.state is ExecutionState.CANCELLED
    assert cancelled_record.run.retrieval_state is RetrievalState.SUCCEEDED
