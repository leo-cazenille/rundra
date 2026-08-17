from __future__ import annotations

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
from rundra.orchestration.planner import (
    PlanningError,
    compact_seed_range,
    create_scalable_plan,
    validate_task_confirmation,
)


def _policy() -> ExecutionPolicy:
    return ExecutionPolicy(
        100_000_000,
        10_000,
        800,
        1001,
        1000,
        20_000,
        WorkerPoolPolicy(100_000, 64, 100, 2, 8),
    )


def _target() -> Target:
    return Target(
        "shoal",
        BackendConfig("ssh", {"host": "fishvision"}),
        BackendConfig("slurm"),
        BackendConfig("rsync"),
        BackendConfig("apptainer"),
        PurePosixPath("/remote/work"),
    )


def _spec() -> ExperimentSpec:
    return ExperimentSpec(
        1,
        "large",
        Command(("simulate", "--config", "{config}", "--seed", "{seed}")),
        ResourceRequest(),
    )


def test_scalable_plan_is_constant_size_and_selects_worker_pool() -> None:
    configs = (
        ExpandedConfig(ConfigSnapshot(PurePosixPath("a.yaml"), "mode: a\n")),
        ExpandedConfig(ConfigSnapshot(PurePosixPath("b.yaml"), "mode: b\n")),
    )

    plan = create_scalable_plan(
        _spec(),
        configs,
        _target(),
        seeds=SeedRange(0, 49_999_999),
        policy=_policy(),
    )

    assert plan.version == 4
    assert plan.strategy == "worker-pool"
    assert plan.task_space is not None and plan.task_space.task_count == 100_000_000
    assert len(plan.units) == 10
    assert plan.groups == ()
    assert plan.worker_count == 64
    assert plan.scheduler_batches == 1


def test_scalable_plan_reports_multi_array_batches_without_controller_probe() -> None:
    with pytest.raises(PlanningError) as caught:
        create_scalable_plan(
            _spec(),
            (ExpandedConfig(ConfigSnapshot(PurePosixPath("a.yaml"), "mode: a\n")),),
            _target(),
            seeds=compact_seed_range(seeds="0:19999"),
            policy=_policy(),
            strategy="multi-array",
            retrieval_policy="none",
        )

    assert caught.value.code == "CONCURRENT_JOB_LIMIT_EXCEEDED"


def test_target_limits_and_exact_confirmation_are_enforced() -> None:
    policy = _policy()

    with pytest.raises(PlanningError, match="--confirm-tasks 10000"):
        validate_task_confirmation(10_000, policy, None)
    with pytest.raises(PlanningError, match="--confirm-tasks 10000"):
        validate_task_confirmation(10_000, policy, 9_999)
    validate_task_confirmation(10_000, policy, 10_000)

    with pytest.raises(PlanningError) as caught:
        create_scalable_plan(
            _spec(),
            (ExpandedConfig(ConfigSnapshot(PurePosixPath("a.yaml"), "x: 1\n")),),
            _target(),
            seeds=SeedRange(0, 100_000_000),
            policy=policy,
        )
    assert caught.value.code == "TASK_LIMIT_EXCEEDED"
