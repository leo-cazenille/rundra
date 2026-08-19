from datetime import timedelta
from pathlib import PurePosixPath

import pytest

from rundra.domain.models import (
    BackendConfig,
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    ResourceRequest,
    Target,
)
from rundra.domain.scaling import ExecutionPolicy, SeedRange, WorkerPoolPolicy
from rundra.domain.sweeps import ExpandedConfig
from rundra.orchestration.models import PlanningError
from rundra.orchestration.planner import create_scalable_plan


def _target() -> Target:
    return Target(
        "cluster",
        BackendConfig("ssh", {"host": "cluster"}),
        BackendConfig("slurm"),
        BackendConfig("rsync"),
        BackendConfig("apptainer"),
        PurePosixPath("/work/rundra"),
    )


def _policy() -> ExecutionPolicy:
    return ExecutionPolicy(
        hard_task_limit=100_000,
        confirmation_threshold=10_000,
        max_active_tasks=320,
        max_array_size=1001,
        output_shard_tasks=1000,
        automatic_retrieval_threshold=20_000,
        max_concurrent_jobs=8,
        worker_pool=WorkerPoolPolicy(
            activation_threshold=10_000,
            max_workers=8,
            tasks_per_lease=100,
            infrastructure_retry_limit=2,
            requeue_limit=8,
            task_slots_per_worker=1,
            default_workers=1,
            max_task_slots_per_worker=40,
        ),
    )


def _spec() -> ExperimentSpec:
    return ExperimentSpec(
        1,
        "simulation",
        Command(("simulate", "--config", "{config}", "--seed", "{seed}")),
        ResourceRequest(
            cpus_per_task=1,
            memory_bytes=1024**3,
            walltime=timedelta(minutes=15),
        ),
    )


def test_v6_explicit_full_scale_reports_exact_worker_resources() -> None:
    plan = create_scalable_plan(
        _spec(),
        (ExpandedConfig(ConfigSnapshot(PurePosixPath("config.yaml"), "x: 1\n")),),
        _target(),
        seeds=SeedRange(0, 4_999),
        policy=_policy(),
        version=6,
        workers=8,
        task_slots_per_worker=40,
    )

    assert plan.version == 6
    assert plan.requested_workers == 8
    assert plan.requested_task_slots_per_worker == 40
    assert plan.worker_count == 8
    assert plan.concurrent_task_capacity == 320
    assert plan.worker_resources == ResourceRequest(
        nodes=1,
        tasks=40,
        cpus_per_task=1,
        memory_bytes=40 * 1024**3,
        walltime=timedelta(hours=4),
    )


def test_v6_rejects_scale_above_target_policy() -> None:
    with pytest.raises(PlanningError) as caught:
        create_scalable_plan(
            _spec(),
            (ExpandedConfig(ConfigSnapshot(PurePosixPath("config.yaml"), "x: 1\n")),),
            _target(),
            seeds=SeedRange(0, 99),
            policy=_policy(),
            version=6,
            workers=9,
            task_slots_per_worker=40,
        )

    assert caught.value.code == "WORKER_LIMIT_EXCEEDED"
