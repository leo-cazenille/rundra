from pathlib import Path

import pytest

from rundra.cli import operations
from rundra.orchestration.service import RunExecutionRequest
from rundra.persistence import JsonRunStore, SqliteTaskStore


def test_submit_propagates_scalable_worker_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """
version: 1
experiment: {name: simulation}
command:
  argv: [simulate, --config, "{config}", --seed, "{seed}"]
container: {image: /images/simulation.sif, gpu: false}
resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 1
  gpus_per_task: 0
  memory: 1GiB
  walltime: "00:15:00"
outputs: {include: [result.dat]}
""".lstrip(),
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text("value: 1\n", encoding="utf-8")
    targets = tmp_path / "targets.yaml"
    targets.write_text(
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
      confirmation_threshold: 1000
      max_active_tasks: 320
      max_concurrent_jobs: 8
      max_array_size: 1001
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 20000
      worker_pool:
        activation_threshold: 100
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
    captured: list[RunExecutionRequest] = []

    class CapturingService:
        def __init__(self, **_: object) -> None:
            pass

        def submit_one(self, request: RunExecutionRequest) -> object:
            captured.append(request)
            raise RuntimeError("captured")

    monkeypatch.setattr(operations, "OrchestrationService", CapturingService)
    monkeypatch.setattr(
        operations,
        "_execution_adapters",
        lambda _: (object(), object(), object(), object()),
    )
    monkeypatch.setattr(
        operations,
        "expand_seeds",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("large worker-pool launch expanded its seed range")
        ),
    )

    with pytest.raises(RuntimeError, match="captured"):
        operations.submit_operation(
            experiment,
            config,
            targets,
            "cluster",
            tmp_path,
            tmp_path / "retrieved",
            JsonRunStore(tmp_path / "runs"),
            seeds="0:999",
            workers=8,
            task_slots_per_worker=40,
            confirm_tasks=1000,
            task_store=SqliteTaskStore(tmp_path / "runs"),
        )

    request = captured[0]
    assert request.max_workers == 8
    assert request.task_slots_per_worker == 40
    assert request.requested_workers == 8
    assert request.requested_task_slots_per_worker == 40
    assert request.worker_resources is not None
    assert request.worker_resources.tasks == 40
    assert request.worker_resources.memory_bytes == 40 * 1024**3
    assert request.compact_plan is not None
    assert request.compact_plan.task_space is not None
    assert request.compact_plan.task_space.task_count == 1_000
    assert request.plan is request.compact_plan
    assert len(request.plan.units) == 10
    assert len(request.compact_configs) == 1
