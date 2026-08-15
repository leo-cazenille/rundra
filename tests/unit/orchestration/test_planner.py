from pathlib import PurePosixPath

import pytest

from rundra.domain.models import (
    BackendConfig,
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    ResourceRequest,
    RunId,
    Target,
)


def _spec(argv: tuple[str, ...] | None = None) -> ExperimentSpec:
    return ExperimentSpec(
        version=1,
        name="example",
        command=Command(
            argv=argv
            or ("python", "main.py", "--config", "{config}", "--seed", "{seed}")
        ),
        resources=ResourceRequest(cpus_per_task=2),
        outputs=("results/**",),
    )


def _config() -> ConfigSnapshot:
    return ConfigSnapshot(PurePosixPath("configs/test.yaml"), "value: 1\n")


def _target() -> Target:
    local = BackendConfig(kind="local")
    return Target(
        name="local",
        transport=local,
        scheduler=local,
        staging=local,
        container=BackendConfig(kind="apptainer"),
        workspace=PurePosixPath("/tmp/rundra"),
    )


@pytest.mark.parametrize(
    "seed, seeds, expected",
    [
        (17, None, (17,)),
        (None, "0:0", (0,)),
        (None, "0:3", (0, 1, 2, 3)),
        (None, "-2:0", (-2, -1, 0)),
    ],
)
def test_expand_seeds_supports_one_seed_or_an_inclusive_range(
    seed: int | None,
    seeds: str | None,
    expected: tuple[int, ...],
) -> None:
    """Catches nondeterministic or off-by-one seed expansion."""
    from rundra.orchestration.planner import expand_seeds

    assert expand_seeds(seed=seed, seeds=seeds) == expected


@pytest.mark.parametrize(
    "seed, seeds, code",
    [
        (None, None, "SEED_REQUIRED"),
        (1, "1:2", "SEED_CONFLICT"),
        (True, None, "INVALID_SEED"),
        (None, "3:1", "INVALID_SEED_RANGE"),
        (None, "1:2:3", "INVALID_SEED_RANGE"),
        (None, " 1:2", "INVALID_SEED_RANGE"),
        (None, "1:2 ", "INVALID_SEED_RANGE"),
        (None, "+1:2", "INVALID_SEED_RANGE"),
        (None, "1.0:2", "INVALID_SEED_RANGE"),
    ],
)
def test_expand_seeds_rejects_ambiguous_or_invalid_requests(
    seed: object,
    seeds: object,
    code: str,
) -> None:
    """Catches implicit seeds and ambiguous task-set requests."""
    from rundra.orchestration.models import PlanningError
    from rundra.orchestration.planner import expand_seeds

    with pytest.raises(PlanningError) as caught:
        expand_seeds(seed=seed, seeds=seeds)

    assert caught.value.code == code


def test_create_plan_is_deterministic_inspectable_and_does_not_create_a_run() -> None:
    """Catches plans with random Run IDs or unresolved execution placeholders."""
    from rundra.orchestration.planner import create_plan

    first = create_plan(_spec(), _config(), _target(), seeds=(4, 9))
    second = create_plan(_spec(), _config(), _target(), seeds=(4, 9))

    assert first == second
    assert first.version == 1
    assert first.experiment_name == "example"
    assert first.target == _target()
    assert first.strategy == "one_unit_per_task"
    assert first.array_mapping == ()
    assert [group.task_ids for group in first.groups] == [
        (first.units[0].task_id,),
        (first.units[1].task_id,),
    ]
    assert first.staging_backend == "local"
    assert [str(unit.task_id) for unit in first.units] == [
        "task_000000",
        "task_000001",
    ]
    assert [unit.seed for unit in first.units] == [4, 9]
    assert first.units[0].command.argv == (
        "python",
        "main.py",
        "--config",
        "configs/test.yaml",
        "--seed",
        "4",
    )
    assert not hasattr(first, "run_id")
    assert not hasattr(first, "run")


def test_construct_tasks_requires_a_run_id_but_not_a_scheduler_mapping() -> None:
    """Catches equating logical Tasks with scheduler jobs or array indices."""
    from rundra.orchestration.planner import construct_tasks

    run_id = RunId.new()
    tasks = construct_tasks(run_id, _spec(), _config(), seeds=(4, 9))

    assert [task.run_id for task in tasks] == [run_id, run_id]
    assert [task.seed for task in tasks] == [4, 9]
    assert [str(task.id) for task in tasks] == ["task_000000", "task_000001"]
    assert all(not hasattr(task, "scheduler_job_id") for task in tasks)
    assert all(not hasattr(task, "array_index") for task in tasks)


@pytest.mark.parametrize(
    "argv, code",
    [
        (("python", "{config}"), "MISSING_PLACEHOLDER"),
        (("python", "{seed}"), "MISSING_PLACEHOLDER"),
        (("python", "{config}", "{unknown}", "{seed}"), "UNKNOWN_PLACEHOLDER"),
        (("python", "{{config}}", "{seed}"), "UNKNOWN_PLACEHOLDER"),
    ],
)
def test_create_plan_rejects_incomplete_or_unknown_placeholders(
    argv: tuple[str, ...],
    code: str,
) -> None:
    """Catches commands that cannot propagate config and seed deterministically."""
    from rundra.orchestration.models import PlanningError
    from rundra.orchestration.planner import create_plan

    with pytest.raises(PlanningError) as caught:
        create_plan(_spec(argv), _config(), _target(), seeds=(1,))

    assert caught.value.code == code


@pytest.mark.parametrize(
    "seeds, code",
    [
        ((), "INVALID_SEEDS"),
        ((1, 1), "DUPLICATE_SEED"),
        ((1, True), "INVALID_SEEDS"),
    ],
)
def test_create_plan_rejects_empty_duplicate_or_non_integer_seeds(
    seeds: tuple[object, ...], code: str
) -> None:
    """Catches plans whose logical task set is unstable or lacks explicit seeds."""
    from rundra.orchestration.models import PlanningError
    from rundra.orchestration.planner import create_plan

    with pytest.raises(PlanningError, match="seed") as caught:
        create_plan(_spec(), _config(), _target(), seeds=seeds)

    assert caught.value.code == code


def test_create_plan_preserves_requested_seed_order() -> None:
    """Catches sorting seeds and silently changing Task identity."""
    from rundra.orchestration.planner import create_plan

    plan = create_plan(_spec(), _config(), _target(), seeds=(9, -2, 4))

    assert [(str(unit.task_id), unit.seed) for unit in plan.units] == [
        ("task_000000", 9),
        ("task_000001", -2),
        ("task_000002", 4),
    ]


def test_execution_plan_rejects_unstable_ids_duplicate_seeds_and_configs() -> None:
    from dataclasses import replace

    from rundra.orchestration.models import ExecutionPlan
    from rundra.orchestration.planner import create_plan

    plan = create_plan(_spec(), _config(), _target(), seeds=(4, 9))
    first, second = plan.units
    common = {
        "version": plan.version,
        "experiment_name": plan.experiment_name,
        "target": plan.target,
        "groups": plan.groups,
        "array_mapping": plan.array_mapping,
    }

    with pytest.raises(ValueError, match="contiguous ordinal Task IDs"):
        ExecutionPlan(units=(second, first), **common)
    with pytest.raises(ValueError, match="seeds must be unique"):
        ExecutionPlan(units=(first, replace(second, seed=first.seed)), **common)
    with pytest.raises(ValueError, match="one effective config"):
        ExecutionPlan(
            units=(
                first,
                replace(
                    second,
                    config=ConfigSnapshot(PurePosixPath("other.yaml"), "value: 2\n"),
                ),
            ),
            **common,
        )


def test_slurm_multi_task_plan_selects_one_array_group() -> None:
    from dataclasses import replace

    from rundra.domain.models import BackendConfig
    from rundra.orchestration.planner import create_plan

    target = replace(_target(), scheduler=BackendConfig("slurm"))
    plan = create_plan(_spec(), _config(), target, seeds=(4, 9, 12))

    assert plan.strategy == "slurm_array"
    assert len(plan.groups) == 1
    assert plan.groups[0].task_ids == tuple(unit.task_id for unit in plan.units)
    assert [unit.seed for unit in plan.units] == [4, 9, 12]
    assert [
        (str(item.task_id), item.seed, item.array_index) for item in plan.array_mapping
    ] == [
        ("task_000000", 4, 0),
        ("task_000001", 9, 1),
        ("task_000002", 12, 2),
    ]


def test_single_task_slurm_plan_does_not_select_an_array() -> None:
    from dataclasses import replace

    from rundra.domain.models import BackendConfig
    from rundra.orchestration.planner import create_plan

    target = replace(_target(), scheduler=BackendConfig("slurm"))
    plan = create_plan(_spec(), _config(), target, seeds=(4,))

    assert plan.strategy == "one_unit_per_task"
    assert plan.array_mapping == ()
    assert plan.groups[0].task_ids == (plan.units[0].task_id,)


def test_execution_plan_rejects_invalid_group_partitions_and_strategies() -> None:
    from dataclasses import replace

    from rundra.domain.models import BackendConfig, TaskId
    from rundra.orchestration.models import ExecutionGroup, ExecutionPlan
    from rundra.orchestration.planner import create_plan

    local = create_plan(_spec(), _config(), _target(), seeds=(4, 9))
    common = {
        "version": local.version,
        "experiment_name": local.experiment_name,
        "target": local.target,
        "units": local.units,
        "array_mapping": local.array_mapping,
    }

    with pytest.raises(ValueError, match="partition Task IDs in plan order"):
        ExecutionPlan(groups=(ExecutionGroup((TaskId.from_ordinal(1),)),), **common)
    with pytest.raises(ValueError, match="singleton execution groups"):
        ExecutionPlan(
            groups=(ExecutionGroup(tuple(unit.task_id for unit in local.units)),),
            **common,
        )
    with pytest.raises(ValueError, match="requires a Slurm target"):
        ExecutionPlan(groups=local.groups, strategy="slurm_array", **common)

    slurm_target = replace(local.target, scheduler=BackendConfig("slurm"))
    with pytest.raises(ValueError, match="one multi-Task execution group"):
        ExecutionPlan(
            version=local.version,
            experiment_name=local.experiment_name,
            target=slurm_target,
            units=local.units,
            groups=local.groups,
            array_mapping=(),
            strategy="slurm_array",
        )
    with pytest.raises(ValueError, match="unsupported"):
        ExecutionPlan(groups=local.groups, strategy="future", **common)


def test_execution_plan_rejects_array_mapping_that_changes_task_identity() -> None:
    from dataclasses import replace

    from rundra.domain.models import BackendConfig
    from rundra.orchestration.models import ArrayTaskMapping, ExecutionPlan
    from rundra.orchestration.planner import create_plan

    target = replace(_target(), scheduler=BackendConfig("slurm"))
    plan = create_plan(_spec(), _config(), target, seeds=(4, 9))
    first, second = plan.array_mapping
    common = {
        "version": plan.version,
        "experiment_name": plan.experiment_name,
        "target": plan.target,
        "units": plan.units,
        "groups": plan.groups,
        "strategy": plan.strategy,
    }

    with pytest.raises(ValueError, match="match Task order and seeds"):
        ExecutionPlan(array_mapping=(second, first), **common)
    with pytest.raises(ValueError, match="match Task order and seeds"):
        ExecutionPlan(
            array_mapping=(first, replace(second, seed=10)),
            **common,
        )
    with pytest.raises(ValueError, match="match Task order and seeds"):
        ExecutionPlan(
            array_mapping=(first, replace(second, array_index=7)),
            **common,
        )
    with pytest.raises(ValueError, match="non-negative"):
        ArrayTaskMapping(first.task_id, first.seed, -1)


def test_execution_plan_rejects_heterogeneous_array_resources() -> None:
    from dataclasses import replace

    from rundra.domain.models import BackendConfig, ResourceRequest
    from rundra.orchestration.models import ExecutionPlan
    from rundra.orchestration.planner import create_plan

    target = replace(_target(), scheduler=BackendConfig("slurm"))
    plan = create_plan(_spec(), _config(), target, seeds=(4, 9))

    with pytest.raises(ValueError, match="uniform Task resources"):
        ExecutionPlan(
            version=plan.version,
            experiment_name=plan.experiment_name,
            target=plan.target,
            units=(
                plan.units[0],
                replace(plan.units[1], resources=ResourceRequest(cpus_per_task=8)),
            ),
            groups=plan.groups,
            array_mapping=plan.array_mapping,
            strategy=plan.strategy,
        )
