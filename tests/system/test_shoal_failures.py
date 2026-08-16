from __future__ import annotations

import json
import shutil
import subprocess
import sys
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
from rundra.domain.models import ArtifactKind, Command, RunId, Target
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.preflight import PreflightStatus
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_failure]
_REPOSITORY_ROOT = Path(__file__).parents[2]


def _prepare_failure_source(root: Path, image: Path) -> Path:
    source = root / "source"
    shutil.copytree(_REPOSITORY_ROOT / "examples/shoal/failure", source)
    experiment_source = source / "experiment.yaml"
    document = yaml.safe_load(experiment_source.read_text(encoding="utf-8"))
    document["container"]["image"] = str(image)
    experiment_source.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    return source


def _write_impossible_workspace_target(
    destination: Path,
    targets_source: Path,
    target_name: str,
    workspace_file: Path,
) -> Path:
    document = yaml.safe_load(targets_source.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("targets"), dict):
        pytest.fail("Shoal target file must contain a targets mapping")
    target_document = document["targets"].get(target_name)
    if not isinstance(target_document, dict):
        pytest.fail(f"Shoal target file has no mapping for {target_name!r}")
    target_document["workspace"] = str(workspace_file)
    destination.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return destination


def _invoke_cli(
    arguments: tuple[str, ...], *, timeout: float = 600
) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
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
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        pytest.fail(
            f"rundr {arguments[0]} returned invalid JSON: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
        raise AssertionError from error
    if not isinstance(value, dict):
        pytest.fail(f"rundr {arguments[0]} returned a non-object JSON document")
    return completed, value


def _require_cpu_plan_and_preflight(
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
        seed=29,
    )
    if plan.error is not None:
        pytest.fail(f"M4.5 plan failed [{plan.error.code}]: {plan.error.message}")
    assert plan.value is not None
    resources = plan.value.plan.units[0].resources
    assert resources.nodes == 1
    assert resources.tasks == 1
    assert resources.cpus_per_task == 1
    assert resources.gpus_per_task == 0
    assert resources.memory_bytes == 1024**3
    assert resources.walltime is not None
    assert resources.walltime.total_seconds() == 300

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
            "M4.5 preflight failed: "
            + "; ".join(f"{check.layer}/{check.name}" for check in failures)
        )


def test_shoal_nonzero_exit_preserves_logs_and_partial_result(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_image: Path,
) -> None:
    source = _prepare_failure_source(tmp_path, shoal_cpu_image)
    destination = tmp_path / "retrieved"
    data_dir = tmp_path / "records"
    _require_cpu_plan_and_preflight(
        source, shoal_targets_source, shoal_target_name, shoal_target
    )

    completed, document = _invoke_cli(
        (
            "run",
            str(source / "experiment.yaml"),
            "--config",
            str(source / "config.yaml"),
            "--seed",
            "29",
            "--target",
            shoal_target_name,
            "--targets-file",
            str(shoal_targets_source),
            "--source-root",
            str(source),
            "--destination",
            str(destination),
            "--data-dir",
            str(data_dir),
        )
    )

    assert completed.returncode == 2
    assert completed.stderr == ""
    assert document["ok"] is True
    run_value = document["run"]
    assert isinstance(run_value, dict)
    run_id = RunId(run_value["run_id"])
    assert run_value["state"] == "FAILED"
    assert run_value["retrieval_state"] == "SUCCEEDED"
    record = JsonRunStore(data_dir).load(run_id)

    assert record.run.state is ExecutionState.FAILED
    assert record.run.tasks[0].state is ExecutionState.FAILED
    assert record.run.retrieval_state is RetrievalState.SUCCEEDED
    assert record.task_exit_codes == {record.run.tasks[0].id: 23}
    assert record.native_state == "FAILED"
    assert len(record.scheduler_job_ids) == 1
    assert record.scheduler_job_ids[0].isdigit()
    assert record.allocated_nodes
    assert record.completed_at is not None

    partial = destination / "output/results/partial.txt"
    assert partial.read_text(encoding="utf-8") == (
        "seed=29\nconfig:\nlabel: shoal-failure\nstatus=partial-before-exit\n"
    )
    artifact_kinds = {artifact.kind for artifact in record.artifacts}
    assert {
        ArtifactKind.SOURCE_SNAPSHOT,
        ArtifactKind.EFFECTIVE_CONFIG,
        ArtifactKind.STDOUT,
        ArtifactKind.STDERR,
        ArtifactKind.RAW_RESULT,
    } <= artifact_kinds

    logs_completed, logs_document = _invoke_cli(
        ("logs", str(run_id), "--data-dir", str(data_dir))
    )
    assert logs_completed.returncode == 0
    logs = logs_document["logs"]
    assert isinstance(logs, dict)
    assert logs["stdout"] == "RUNDRA_FAILURE_STDOUT seed=29\n"
    assert logs["stderr"] == "RUNDRA_FAILURE_STDERR deliberate-exit=23\n"


def test_shoal_remote_workspace_failure_is_durable_and_non_submitting(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_image: Path,
) -> None:
    source = _prepare_failure_source(tmp_path, shoal_cpu_image)
    targets_source = _write_impossible_workspace_target(
        tmp_path / "targets.yaml",
        shoal_targets_source,
        shoal_target_name,
        shoal_cpu_image,
    )
    data_dir = tmp_path / "records"
    destination = tmp_path / "retrieved"

    plan = plan_operation(
        source / "experiment.yaml",
        source / "config.yaml",
        targets_source,
        shoal_target_name,
        seed=31,
    )
    if plan.error is not None:
        pytest.fail(
            f"M4.5 infrastructure-failure plan failed "
            f"[{plan.error.code}]: {plan.error.message}"
        )

    host = shoal_target.transport.options.get("host")
    assert type(host) is str
    transport = SSHTransport(host)
    stat_command = Command(("stat", "-c", "%F:%s", "--", str(shoal_cpu_image)))
    before = transport.run(stat_command)
    assert before.exit_code == 0
    assert before.stdout.startswith("regular file:")

    completed, document = _invoke_cli(
        (
            "run",
            str(source / "experiment.yaml"),
            "--config",
            str(source / "config.yaml"),
            "--seed",
            "31",
            "--target",
            shoal_target_name,
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

    assert completed.returncode == 1
    assert completed.stderr == ""
    assert document["ok"] is False
    error = document["error"]
    assert isinstance(error, dict)
    assert error["code"] == "STAGING_FAILED"
    details = error["details"]
    assert isinstance(details, dict)
    run_id = RunId(details["run_id"])
    record = JsonRunStore(data_dir).load(run_id)

    assert record.run.state is ExecutionState.FAILED
    assert record.run.tasks[0].state is ExecutionState.FAILED
    assert record.run.retrieval_state is RetrievalState.NOT_REQUESTED
    assert record.native_state == "STAGING_FAILED"
    assert record.completed_at is not None
    assert record.scheduler_job_ids == ()
    assert record.allocated_nodes == ()
    assert record.task_exit_codes == {}
    assert record.artifacts == ()
    assert not destination.exists()

    after = transport.run(stat_command)
    assert after.exit_code == 0
    assert after.stdout == before.stdout
