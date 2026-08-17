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
        execution_strategy="multi-array",
        retrieval_policy="none",
    )

    assert result.ok
    document = result_document(result)
    assert document["format_version"] == 4
    assert document["plan"]["task_space"]["task_count"] == 20_000
    assert document["plan"]["task_space"]["preview_count"] == 10
    assert document["plan"]["scheduling"] == {
        "scheduler_batches": 20,
        "worker_count": None,
        "max_active_tasks": 800,
        "max_array_size": 1001,
    }
    assert document["plan"]["retrieval_policy"] == "none"
    assert "20000 task(s)" in render_human(result)
