from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from rundra.adapters import RemoteApptainerRuntime, RsyncStager, SSHTransport
from rundra.cli.operations import plan_operation
from rundra.config.experiments import load_experiment
from rundra.domain.models import ArtifactKind, RunId, Target
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.preflight import PreflightStatus, RemotePreflight
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_gpu]
_REPOSITORY_ROOT = Path(__file__).parents[2]


def _prepare_source(root: Path, image: Path) -> Path:
    source = root / "source"
    shutil.copytree(_REPOSITORY_ROOT / "examples/shoal/gpu", source)
    experiment_source = source / "experiment.yaml"
    document = yaml.safe_load(experiment_source.read_text(encoding="utf-8"))
    document["container"]["image"] = str(image)
    experiment_source.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    return source


def _run_cli(arguments: tuple[str, ...], *, timeout: float = 600) -> dict[str, object]:
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
    if completed.returncode != 0:
        pytest.fail(
            f"rundr {arguments[0]} failed with exit code {completed.returncode}: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
    value = json.loads(completed.stdout)
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
        seed=23,
    )
    if plan.error is not None:
        pytest.fail(f"M4.4 plan failed [{plan.error.code}]: {plan.error.message}")
    assert plan.value is not None
    resources = plan.value.plan.units[0].resources
    assert resources.nodes == 1
    assert resources.tasks == 1
    assert resources.cpus_per_task == 1
    assert resources.gpus_per_task == 1
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
            "M4.4 preflight failed: "
            + "; ".join(f"{check.layer}/{check.name}" for check in failures)
        )


def test_shoal_gpu_run_verifies_scheduler_allocation_and_container_view(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_gpu_image: Path,
) -> None:
    source = _prepare_source(tmp_path, shoal_gpu_image)
    destination = tmp_path / "retrieved"
    data_dir = tmp_path / "records"
    _require_plan_and_preflight(
        source, shoal_targets_source, shoal_target_name, shoal_target
    )
    run_document = _run_cli(
        (
            "run",
            str(source / "experiment.yaml"),
            "--config",
            str(source / "config.yaml"),
            "--seed",
            "23",
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

    assert run_document["ok"] is True
    run_value = run_document["run"]
    assert isinstance(run_value, dict)
    run_id = RunId(run_value["run_id"])
    record = JsonRunStore(data_dir).load(run_id)

    assert record.run.state is ExecutionState.SUCCEEDED
    assert record.run.retrieval_state is RetrievalState.SUCCEEDED
    task = record.run.tasks[0]
    assert task.seed == 23
    assert task.resources.gpus_per_task == 1
    assert record.experiment.container is not None
    assert record.experiment.container.gpu
    assert record.task_exit_codes == {task.id: 0}
    assert len(record.scheduler_job_ids) == 1
    assert record.scheduler_job_ids[0].isdigit()
    assert record.allocated_nodes
    assert record.native_state == "COMPLETED"

    evidence = (destination / "output/results/evidence.txt").read_text(encoding="utf-8")
    assert "seed=23\n" in evidence
    assert "cuda_visible_devices=\n" not in evidence
    assert evidence.endswith("config:\nlabel: shoal-gpu\n")
    gpu_view = (destination / "output/results/nvidia-smi.txt").read_text(
        encoding="utf-8"
    )
    assert any(line.startswith("GPU ") for line in gpu_view.splitlines())

    artifact_kinds = {artifact.kind for artifact in record.artifacts}
    assert {
        ArtifactKind.SOURCE_SNAPSHOT,
        ArtifactKind.EFFECTIVE_CONFIG,
        ArtifactKind.STDOUT,
        ArtifactKind.STDERR,
        ArtifactKind.RAW_RESULT,
    } <= artifact_kinds
    logs_document = _run_cli(("logs", str(run_id), "--data-dir", str(data_dir)))
    logs = logs_document["logs"]
    assert isinstance(logs, dict)
    assert logs["stdout"] == "RUNDRA_GPU_STDOUT seed=23\n"
    assert logs["stderr"] == "RUNDRA_GPU_STDERR nvidia-container-view-ok\n"
    assert not any(
        key.lower().endswith(("password", "token", "secret"))
        for key in record.scheduler_metadata
    )
