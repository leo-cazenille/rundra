from __future__ import annotations

from pathlib import Path

from rundra.cli.operations import plan_operation
from rundra.cli.render import render_human, result_document


def test_target_v3_plan_emits_compact_scaling_summary(tmp_path: Path) -> None:
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: scalable}
command:
  argv: [simulate, --config, "{config}", --seed, "{seed}"]
container: {image: application.sif}
resources: {}
""",
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text("value: 1\n", encoding="utf-8")
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        """\
version: 3
targets:
  shoal:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /remote/work
    execution:
      hard_task_limit: 100000000
      confirmation_threshold: 10000
      max_active_tasks: 800
      max_array_size: 1001
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 20000
      worker_pool:
        activation_threshold: 100000
        max_workers: 64
        tasks_per_lease: 100
        infrastructure_retry_limit: 2
        requeue_limit: 8
""",
        encoding="utf-8",
    )

    result = plan_operation(
        experiment,
        config,
        targets,
        "shoal",
        seeds="0:19999",
        execution_strategy="auto",
        retrieval_policy="none",
    )

    assert result.ok
    document = result_document(result)
    assert document["format_version"] == 4
    assert document["plan"]["task_space"]["task_count"] == 20_000
    assert document["plan"]["task_space"]["preview_count"] == 10
    assert document["plan"]["scheduling"] == {
        "scheduler_batches": 1,
        "worker_count": 64,
        "max_active_tasks": 800,
        "max_concurrent_jobs": 256,
        "max_array_size": 1001,
    }
    assert document["plan"]["retrieval_policy"] == "none"
    assert "20000 task(s)" in render_human(result)


def test_target_v4_plan_emits_worker_slot_capacity(tmp_path: Path) -> None:
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: scalable}
command:
  argv: [simulate, --config, "{config}", --seed, "{seed}"]
container: {image: application.sif}
resources: {cpus_per_task: 1, memory: 2GiB, walltime: "00:02:00"}
""",
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text("value: 1\n", encoding="utf-8")
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        """\
version: 10
targets:
  shoal:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /remote/work
    execution_storage:
      type: slurm_scratch
      cpu_environment: SLURM_TMPDIR
      gpu_environment: SLURM_GPUTMPDIR
      stage_image: true
      copy_back: task
    execution:
      hard_task_limit: 100000000
      confirmation_threshold: 10000
      max_active_tasks: 320
      max_concurrent_jobs: 8
      max_array_size: 1001
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 20000
      worker_pool:
        activation_threshold: 10000
        max_workers: 8
        task_slots_per_worker: 40
        tasks_per_lease: 100
        infrastructure_retry_limit: 2
        requeue_limit: 8
""",
        encoding="utf-8",
    )

    result = plan_operation(
        experiment,
        config,
        targets,
        "shoal",
        seeds="0:19999",
        execution_strategy="auto",
        retrieval_policy="none",
    )

    assert result.ok
    document = result_document(result)
    assert document["format_version"] == 5
    scheduling = document["plan"]["scheduling"]
    assert scheduling["worker_count"] == 8
    assert scheduling["task_slots_per_worker"] == 40
    assert scheduling["concurrent_task_capacity"] == 320
    assert scheduling["max_lane_depth"] == 63
    assert scheduling["worker_resources"]["tasks"] == 40
    human = render_human(result)
    assert "slots_per_worker=40" in human
    assert "allocation-local Slurm scratch" in human
    assert "cpu=SLURM_TMPDIR, gpu=SLURM_GPUTMPDIR" in human
    assert "outputs copied back after each task" in human
