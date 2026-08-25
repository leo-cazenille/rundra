from __future__ import annotations

import hashlib
import subprocess
from datetime import timedelta
from pathlib import Path

from rundra.cli.capability_doctor import doctor_operation
from rundra.cli.render import render_json
from rundra.domain.preparation import (
    PreparationConfig,
    PreparationImage,
    PreparationPlan,
    PreparationSourceGit,
    PreparationStorageConfig,
    source_recipe_identity,
)
from rundra.domain.scheduling import SlurmPartitionPolicy, SlurmPartitionRoute
from rundra.ports import SchedulerPartition


def test_doctor_accepts_a_writable_local_target(tmp_path: Path) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 1
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: native}}
    workspace: {tmp_path / "workspace"}
""",
        encoding="utf-8",
    )

    result = doctor_operation(targets, "local")

    assert result.ok and result.value is not None
    assert result.value.ready
    assert result.value.format_version == 3
    assert any(check.name == "workspace" for check in result.value.checks)
    assert not (tmp_path / "workspace").exists()


def test_doctor_reports_missing_target_without_connecting(tmp_path: Path) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text("version: 1\ntargets: {}\n", encoding="utf-8")

    result = doctor_operation(targets, "absent", connect=True)

    assert result.ok and result.value is not None
    assert not result.value.ready
    assert any(check.name == "selected_target" for check in result.value.checks)


def test_bootstrap_doctor_reports_paths_and_generates_codex_profile(
    tmp_path: Path,
) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        "\n".join(
            (
                "version: 1",
                "targets:",
                "  local:",
                "    transport: {type: local}",
                "    scheduler: {type: local}",
                "    staging: {type: local}",
                "    container: {type: native}",
                f"    workspace: {tmp_path / 'workspace'}",
                "",
            )
        ),
        encoding="utf-8",
    )
    data_dir = tmp_path / "records"
    cache = tmp_path / "cache"

    result = doctor_operation(
        targets, None, data_dir=data_dir, cache_root=cache, agent="codex"
    )

    assert result.ok and result.value is not None
    assert result.value.ready
    assert result.value.mode == "bootstrap"
    assert result.value.agent_config is not None
    assert "[permissions.rundra.filesystem]" in result.value.agent_config
    assert str(data_dir) in result.value.agent_config
    assert not data_dir.exists()
    assert not cache.exists()
    document = render_json(result)
    assert '"format_version":3' in document
    assert '"complete":true' in document


def test_doctor_rejects_scheduler_probe_without_write_probe(tmp_path: Path) -> None:
    result = doctor_operation(
        tmp_path / "targets.yaml",
        None,
        scheduler_probe=True,
        write_probe=False,
    )

    assert result.error is not None
    assert result.error.code == "CLI_USAGE_ERROR"


def test_doctor_requires_connect_for_read_only_scheduler_inventory(
    tmp_path: Path,
) -> None:
    result = doctor_operation(
        tmp_path / "targets.yaml",
        None,
        scheduler_inventory=True,
    )

    assert result.error is not None
    assert result.error.code == "CLI_USAGE_ERROR"


def test_scheduler_inventory_validates_configured_routes(monkeypatch) -> None:
    from pathlib import PurePosixPath

    from rundra.cli import capability_doctor
    from rundra.domain.models import BackendConfig, Target

    target = Target(
        "cluster",
        BackendConfig("ssh", {"host": "cluster"}),
        BackendConfig("slurm"),
        BackendConfig("rsync"),
        BackendConfig("apptainer"),
        PurePosixPath("/shared/rundra"),
        partition_policy=SlurmPartitionPolicy(
            (SlurmPartitionRoute("cpu_short", "cpu-short", "cpu", timedelta(hours=1)),)
        ),
    )

    class InventoryScheduler:
        def inventory(self) -> tuple[SchedulerPartition, ...]:
            return (
                SchedulerPartition("cpu-short", True, "up", 3600, "01:00:00", "(null)"),
            )

    monkeypatch.setattr(
        capability_doctor,
        "scheduler_for_target",
        lambda *args, **kwargs: InventoryScheduler(),
    )

    inventory, checks = capability_doctor._scheduler_inventory(target)

    assert len(inventory) == 1
    assert {check.name: check.status for check in checks} == {
        "scheduler_inventory": "pass",
        "partition_route_cpu_short": "pass",
    }


def test_doctor_requires_target_for_local_target_access(tmp_path: Path) -> None:
    result = doctor_operation(
        tmp_path / "targets.yaml",
        None,
        local_target_access=True,
    )

    assert result.error is not None
    assert result.error.code == "CLI_USAGE_ERROR"


def test_doctor_audits_explicit_local_access_to_remote_target(
    tmp_path: Path,
) -> None:
    blocker = tmp_path / "blocked"
    blocker.write_text("not a directory\n", encoding="utf-8")
    workspace = blocker / "workspace"
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 1
targets:
  cluster:
    transport: {{type: ssh, host: cluster}}
    scheduler: {{type: slurm}}
    staging: {{type: rsync}}
    container: {{type: apptainer}}
    workspace: {workspace}
""",
        encoding="utf-8",
    )

    result = doctor_operation(
        targets,
        "cluster",
        local_target_access=True,
        agent="codex",
    )

    assert result.ok and result.value is not None
    assert not result.value.ready
    assert result.value.agent_config is not None
    assert str(workspace) in result.value.agent_config
    assert str(workspace / "cache") in result.value.agent_config
    checks = {check.name: check.status for check in result.value.checks}
    assert checks["local_target_workspace"] == "fail"
    assert checks["local_target_preparation_cache"] == "fail"


def test_doctor_automatically_audits_shared_target_paths(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    workspace = shared / "workspace"
    image_search = shared / "images"
    image_search.mkdir(parents=True)
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 6
targets:
  cluster:
    transport: {{type: ssh, host: cluster}}
    scheduler: {{type: slurm}}
    staging: {{type: shared, root: {shared}}}
    container: {{type: apptainer}}
    workspace: {workspace}
    preparation:
      cache_root: {workspace / "prepared"}
      image_search_paths: [{image_search}]
    execution:
      hard_task_limit: 1000
      confirmation_threshold: 1000
      max_active_tasks: 40
      max_concurrent_jobs: 2
      max_array_size: 1000
      output_shard_tasks: 100
      automatic_retrieval_threshold: 1000
      worker_pool:
        activation_threshold: 100
        default_workers: 1
        max_workers: 2
        default_task_slots_per_worker: 1
        max_task_slots_per_worker: 20
        tasks_per_lease: 20
        infrastructure_retry_limit: 1
        requeue_limit: 2
""",
        encoding="utf-8",
    )

    result = doctor_operation(targets, "cluster", agent="codex")

    assert result.ok and result.value is not None
    assert result.value.ready
    assert result.value.agent_config is not None
    checks = {check.name: check.status for check in result.value.checks}
    assert checks["local_target_workspace"] == "pass"
    assert checks["local_target_preparation_cache"] == "pass"
    assert checks["local_target_image_search_path_0"] == "pass"
    assert str(workspace) in result.value.agent_config
    assert str(workspace / "prepared") in result.value.agent_config
    assert str(image_search) in result.value.agent_config
    assert not workspace.exists()


def test_offline_doctor_reports_missing_pinned_source_and_image(tmp_path: Path) -> None:
    targets = _local_targets(tmp_path)
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(_minimal_experiment(), encoding="utf-8")
    plan, storage = _offline_plan(tmp_path, b"image")

    result = doctor_operation(
        targets,
        "local",
        experiment_source=experiment,
        source_root=tmp_path,
        cache_root=tmp_path / "cache",
        preparation=plan,
        preparation_storage=storage,
        offline=True,
    )

    assert result.ok and result.value is not None
    assert not result.value.ready
    checks = {check.name: check.status for check in result.value.checks}
    assert checks["offline_source_cache"] == "fail"
    assert checks["offline_image_cache"] == "fail"
    assert {action.code for action in result.value.actions} >= {
        "OFFLINE_SOURCE_CACHE_MISS",
        "OFFLINE_IMAGE_CACHE_MISS",
    }


def test_offline_doctor_accepts_exact_pinned_source_and_image(tmp_path: Path) -> None:
    targets = _local_targets(tmp_path)
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(_minimal_experiment(), encoding="utf-8")
    image_bytes = b"verified image"
    plan, storage = _offline_plan(tmp_path, image_bytes, populate=True)

    result = doctor_operation(
        targets,
        "local",
        experiment_source=experiment,
        source_root=tmp_path,
        cache_root=tmp_path / "cache",
        preparation=plan,
        preparation_storage=storage,
        offline=True,
    )

    assert result.ok and result.value is not None
    assert result.value.ready
    checks = {check.name: check.status for check in result.value.checks}
    assert checks["offline_source_cache"] == "pass"
    assert checks["offline_image_cache"] == "pass"


def _local_targets(root: Path) -> Path:
    targets = root / "targets.yaml"
    targets.write_text(
        f"""version: 1
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: native}}
    workspace: {root / "workspace"}
""",
        encoding="utf-8",
    )
    return targets


def _minimal_experiment() -> str:
    return """version: 1
experiment:
  name: offline-doctor
command:
  argv: ["true"]
container:
  image: image.sif
  gpu: false
resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 1
  gpus_per_task: 0
  memory: 1MiB
  walltime: "00:01:00"
outputs:
  include: [result.txt]
"""


def _offline_plan(
    root: Path, image_bytes: bytes, *, populate: bool = False
) -> tuple[PreparationPlan, PreparationStorageConfig]:
    source = root / "origin"
    source.mkdir()
    subprocess.run(("git", "init", "-q", str(source)), check=True)
    (source / "main.py").write_text("print('ready')\n", encoding="utf-8")
    subprocess.run(("git", "-C", str(source), "add", "main.py"), check=True)
    subprocess.run(
        (
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=Rundra Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-q",
            "-m",
            "fixture",
        ),
        check=True,
    )
    revision = subprocess.run(
        ("git", "-C", str(source), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    digest = hashlib.sha256(image_bytes).hexdigest()
    cache = root / "cache"
    if populate:
        repository = (
            cache
            / "git"
            / source_recipe_identity(PreparationSourceGit(source.as_uri(), revision))
        )
        repository.parent.mkdir(parents=True)
        subprocess.run(
            ("git", "clone", "-q", "--bare", str(source), str(repository)), check=True
        )
        image = cache / "images" / f"{digest}.sif"
        image.parent.mkdir(parents=True)
        image.write_bytes(image_bytes)
    recipe = PreparationConfig(
        PreparationSourceGit(source.as_uri(), revision),
        PreparationImage(Path("image.sif"), "library://example/image:1", digest),
        None,
    )
    return (
        PreparationPlan(recipe, "git", None, offline=True),
        PreparationStorageConfig(cache_root=cache),
    )
