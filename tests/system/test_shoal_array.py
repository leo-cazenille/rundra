from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath

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

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_array]
_REPOSITORY_ROOT = Path(__file__).parents[2]
_SEEDS = (40, 41, 42)


def _prepare_array_source(root: Path, image: Path) -> Path:
    source = root / "source"
    shutil.copytree(_REPOSITORY_ROOT / "examples/shoal/array", source)
    experiment_source = source / "experiment.yaml"
    document = yaml.safe_load(experiment_source.read_text(encoding="utf-8"))
    document["container"]["image"] = str(image)
    experiment_source.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    return source


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


def _require_array_plan_and_preflight(
    source: Path,
    targets_source: Path,
    target_name: str,
    target: Target,
) -> None:
    plan_result = plan_operation(
        source / "experiment.yaml",
        source / "config.yaml",
        targets_source,
        target_name,
        seeds="40:42",
    )
    if plan_result.error is not None:
        pytest.fail(
            f"M5.6 plan failed [{plan_result.error.code}]: {plan_result.error.message}"
        )
    assert plan_result.value is not None
    plan = plan_result.value.plan
    assert plan.strategy == "slurm_array"
    assert len(plan.groups) == 1
    assert [unit.seed for unit in plan.units] == list(_SEEDS)
    assert [
        (str(item.task_id), item.seed, item.array_index) for item in plan.array_mapping
    ] == [
        ("task_000000", 40, 0),
        ("task_000001", 41, 1),
        ("task_000002", 42, 2),
    ]
    resources = plan.units[0].resources
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
            "M5.6 preflight failed: "
            + "; ".join(f"{check.layer}/{check.name}" for check in failures)
        )


def test_shoal_array_preserves_task_identity_and_partial_failure(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_image: Path,
) -> None:
    source = _prepare_array_source(tmp_path, shoal_cpu_image)
    destination = tmp_path / "retrieved"
    data_dir = tmp_path / "records"
    _require_array_plan_and_preflight(
        source, shoal_targets_source, shoal_target_name, shoal_target
    )

    completed, document = _invoke_cli(
        (
            "run",
            str(source / "experiment.yaml"),
            "--config",
            str(source / "config.yaml"),
            "--seeds",
            "40:42",
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
    assert run_value["state"] == "FAILED"
    assert run_value["retrieval_state"] == "SUCCEEDED"
    assert run_value["seeds"] == list(_SEEDS)
    run_id = RunId(run_value["run_id"])
    record = JsonRunStore(data_dir).load(run_id)
    tasks = record.run.tasks

    assert record.run.state is ExecutionState.FAILED
    assert [task.seed for task in tasks] == list(_SEEDS)
    assert [task.state for task in tasks] == [
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.SUCCEEDED,
    ]
    assert record.run.retrieval_state is RetrievalState.SUCCEEDED
    assert record.task_retrieval_states == {
        task.id: RetrievalState.SUCCEEDED for task in tasks
    }
    assert len(record.scheduler_job_ids) == 1
    root_id = record.scheduler_job_ids[0]
    assert root_id.isdigit()
    assert record.task_scheduler_ids == {
        task.id: f"{root_id}_{index}" for index, task in enumerate(tasks)
    }
    assert record.task_native_states == {
        tasks[0].id: "COMPLETED",
        tasks[1].id: "FAILED",
        tasks[2].id: "COMPLETED",
    }
    assert record.task_exit_codes == {
        tasks[0].id: 0,
        tasks[1].id: 23,
        tasks[2].id: 0,
    }
    assert [
        (str(item.task_id), item.seed, item.array_index)
        for item in record.task_array_mapping
    ] == [
        ("task_000000", 40, 0),
        ("task_000001", 41, 1),
        ("task_000002", 42, 2),
    ]

    expected_config = "label: shoal-array\nfailure_seed: 41\n"
    for index, (task, seed) in enumerate(zip(tasks, _SEEDS, strict=True)):
        evidence = destination / "output" / str(task.id) / "results/evidence.txt"
        assert evidence.read_text(encoding="utf-8") == (
            f"seed={seed}\nconfig:\n{expected_config}"
        )
        assert any(
            artifact.kind is ArtifactKind.RAW_RESULT
            and artifact.task_id == task.id
            and artifact.path == evidence
            for artifact in record.artifacts
        )

        logs_completed, logs_document = _invoke_cli(
            (
                "logs",
                str(run_id),
                "--task",
                str(index),
                "--data-dir",
                str(data_dir),
            )
        )
        assert logs_completed.returncode == 0
        logs = logs_document["logs"]
        assert isinstance(logs, dict)
        assert logs["task_id"] == str(task.id)
        assert logs["stdout"] == f"RUNDRA_ARRAY_STDOUT seed={seed}\n"
        expected_stderr = (
            f"RUNDRA_ARRAY_STDERR deliberate-exit=23 seed={seed}\n"
            if seed == 41
            else f"RUNDRA_ARRAY_STDERR success seed={seed}\n"
        )
        assert logs["stderr"] == expected_stderr

    status_completed, status_document = _invoke_cli(
        ("status", str(run_id), "--data-dir", str(data_dir))
    )
    assert status_completed.returncode == 0
    status = status_document["status"]
    assert isinstance(status, dict)
    assert status["state"] == "FAILED"
    assert status["tasks"] == {"total": 3, "failed": 1, "succeeded": 2}
    details = status["task_details"]
    assert isinstance(details, list)
    assert [item["seed"] for item in details] == list(_SEEDS)
    assert [item["exit_code"] for item in details] == [0, 23, 0]

    host = shoal_target.transport.options.get("host")
    assert type(host) is str
    manifest_path = (
        PurePosixPath(shoal_target.workspace)
        / "runs"
        / str(run_id)
        / "metadata/slurm-array-tasks.sh"
    )
    sealed = SSHTransport(host).run(Command(("test", "!", "-w", str(manifest_path))))
    assert sealed.exit_code == 0
