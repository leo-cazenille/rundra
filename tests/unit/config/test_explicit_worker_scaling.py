from pathlib import Path

import pytest

from rundra.config.errors import ConfigError
from rundra.config.launch import LaunchValues, load_project_launch, resolve_launch
from rundra.config.targets import load_targets_config


def test_target_v6_separates_worker_defaults_from_ceilings(tmp_path: Path) -> None:
    source = tmp_path / "targets.yaml"
    source.write_text(
        """
version: 6
targets:
  cluster:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /work/rundra
    execution:
      hard_task_limit: 100000
      confirmation_threshold: 10000
      max_active_tasks: 320
      max_concurrent_jobs: 8
      max_array_size: 1001
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 20000
      worker_pool:
        activation_threshold: 10000
        default_workers: 1
        max_workers: 8
        default_task_slots_per_worker: 4
        max_task_slots_per_worker: 40
        tasks_per_lease: 100
        infrastructure_retry_limit: 2
        requeue_limit: 8
""".lstrip(),
        encoding="utf-8",
    )

    config = load_targets_config(source)
    workers = config.execution["cluster"].worker_pool

    assert config.version == 6
    assert workers.default_worker_count == 1
    assert workers.max_workers == 8
    assert workers.task_slots_per_worker == 4
    assert workers.max_slot_count == 40


def test_target_v7_parses_optional_worker_memory_ceiling(tmp_path: Path) -> None:
    source = tmp_path / "targets.yaml"
    source.write_text(
        """
version: 7
targets:
  cluster:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /work/rundra
    execution:
      hard_task_limit: 100000
      confirmation_threshold: 10000
      max_active_tasks: 320
      max_concurrent_jobs: 8
      max_array_size: 1001
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 20000
      max_memory_per_worker: 60GiB
      worker_pool:
        activation_threshold: 10000
        default_workers: 1
        max_workers: 8
        default_task_slots_per_worker: 1
        max_task_slots_per_worker: 40
        tasks_per_lease: 100
        infrastructure_retry_limit: 2
        requeue_limit: 8
""".lstrip(),
        encoding="utf-8",
    )

    config = load_targets_config(source)

    assert config.version == 7
    assert config.execution["cluster"].max_memory_per_worker == 60 * 1024**3


def test_target_v6_rejects_default_capacity_above_policy(tmp_path: Path) -> None:
    source = tmp_path / "targets.yaml"
    source.write_text(
        """
version: 6
targets:
  cluster:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /work/rundra
    execution:
      hard_task_limit: 1000
      confirmation_threshold: 100
      max_active_tasks: 8
      max_concurrent_jobs: 4
      max_array_size: 100
      output_shard_tasks: 100
      automatic_retrieval_threshold: 100
      worker_pool:
        activation_threshold: 10
        default_workers: 4
        max_workers: 4
        default_task_slots_per_worker: 4
        max_task_slots_per_worker: 4
        tasks_per_lease: 10
        infrastructure_retry_limit: 0
        requeue_limit: 0
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="default worker capacity"):
        load_targets_config(source)


def test_project_scale_profile_and_cli_precedence(tmp_path: Path) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(
        """
version: 1
default_profile: full
defaults:
  target: cluster
  workers: 1
  task_slots_per_worker: 4
profiles:
  full:
    workers: 8
    task_slots_per_worker: 40
""".lstrip(),
        encoding="utf-8",
    )
    project = load_project_launch(source)

    resolved = resolve_launch(
        project=project,
        cli=LaunchValues(workers=2),
    )

    assert resolved.values.workers == 2
    assert resolved.values.task_slots_per_worker == 40
    assert resolved.sources["workers"] == "cli"
    assert resolved.sources["task_slots_per_worker"] == "project_profile:full"
