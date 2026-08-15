from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath

import pytest
import yaml

from rundra.adapters import RemoteApptainerRuntime, RsyncStager, SSHTransport
from rundra.cli.operations import plan_operation
from rundra.config.experiments import load_experiment
from rundra.domain.models import ArtifactKind, Command, RunId, Target
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.preflight import PreflightStatus, RemotePreflight
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_cpu]
_REPOSITORY_ROOT = Path(__file__).parents[2]


def _git(source: Path, *arguments: str) -> None:
    completed = subprocess.run(
        ("git", *arguments),
        cwd=source,
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        shell=False,
    )
    if completed.returncode != 0:
        pytest.fail(f"Could not prepare M4.3 Git source: git {arguments[0]} failed")


def _prepare_dirty_source(root: Path, image: Path) -> Path:
    source = root / "source"
    shutil.copytree(_REPOSITORY_ROOT / "examples/shoal/cpu", source)
    _git(source, "init", "--initial-branch", "main")
    _git(source, "config", "user.name", "Rundra System Test")
    _git(source, "config", "user.email", "rundra-system-test@invalid")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "baseline")

    experiment_source = source / "experiment.yaml"
    document = yaml.safe_load(experiment_source.read_text(encoding="utf-8"))
    document["container"]["image"] = str(image)
    experiment_source.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    (source / "payload.txt").write_text("dirty tracked payload\n", encoding="utf-8")
    (source / "untracked.txt").write_text("untracked payload\n", encoding="utf-8")
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
        seed=17,
    )
    if plan.error is not None:
        pytest.fail(f"M4.3 plan failed [{plan.error.code}]: {plan.error.message}")
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
            "M4.3 preflight failed: "
            + "; ".join(f"{check.layer}/{check.name}" for check in failures)
        )


def test_shoal_cpu_run_preserves_dirty_source_and_retrieves_evidence(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_image: Path,
) -> None:
    source = _prepare_dirty_source(tmp_path, shoal_cpu_image)
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
            "17",
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
    assert record.run.tasks[0].seed == 17
    assert record.task_exit_codes == {record.run.tasks[0].id: 0}
    assert len(record.scheduler_job_ids) == 1
    assert record.scheduler_job_ids[0].isdigit()
    assert record.allocated_nodes
    assert record.submitted_at is not None
    assert record.completed_at is not None
    assert record.scheduler_metadata["native_start"]
    assert record.scheduler_metadata["native_end"]

    assert record.git_commit is not None
    assert record.git_branch == "main"
    assert record.git_dirty is True
    assert record.git_diff is not None
    assert "dirty tracked payload" in record.git_diff
    assert record.container_digest is None
    assert record.run.tasks[0].config.content == "label: shoal-cpu\n"

    result = destination / "output/results/evidence.txt"
    assert result.read_text(encoding="utf-8") == (
        "seed=17\n"
        "tracked=dirty tracked payload\n"
        "untracked=untracked payload\n"
        "config:\n"
        "label: shoal-cpu\n"
    )
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
    assert logs["stdout"] == "RUNDRA_CPU_STDOUT seed=17\n"
    assert logs["stderr"] == "RUNDRA_CPU_STDERR source-snapshot-ok\n"

    workspace = PurePosixPath(shoal_target.workspace) / "runs" / str(run_id)
    host = shoal_target.transport.options.get("host")
    assert type(host) is str
    sealed = SSHTransport(host).run(
        Command(("test", "!", "-w", str(workspace / "source")))
    )
    assert sealed.exit_code == 0
    assert not any(
        key.lower().endswith(("password", "token", "secret"))
        for key in record.scheduler_metadata
    )
