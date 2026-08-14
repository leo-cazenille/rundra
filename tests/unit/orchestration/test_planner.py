from pathlib import PurePosixPath

import pytest

from shoal_run.domain.models import (
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
        workspace=PurePosixPath("/tmp/shoal-run"),
    )


@pytest.mark.parametrize(
    "seed, seeds, expected",
    [(17, None, (17,)), (None, "0:3", (0, 1, 2, 3)), (None, "-2:0", (-2, -1, 0))],
)
def test_expand_seeds_supports_one_seed_or_an_inclusive_range(
    seed: int | None,
    seeds: str | None,
    expected: tuple[int, ...],
) -> None:
    """Catches nondeterministic or off-by-one seed expansion."""
    from shoal_run.orchestration.planner import expand_seeds

    assert expand_seeds(seed=seed, seeds=seeds) == expected


@pytest.mark.parametrize(
    "seed, seeds, code",
    [
        (None, None, "SEED_REQUIRED"),
        (1, "1:2", "SEED_CONFLICT"),
        (True, None, "INVALID_SEED"),
        (None, "3:1", "INVALID_SEED_RANGE"),
        (None, "1:2:3", "INVALID_SEED_RANGE"),
    ],
)
def test_expand_seeds_rejects_ambiguous_or_invalid_requests(
    seed: object,
    seeds: object,
    code: str,
) -> None:
    """Catches implicit seeds and ambiguous task-set requests."""
    from shoal_run.orchestration.models import PlanningError
    from shoal_run.orchestration.planner import expand_seeds

    with pytest.raises(PlanningError) as caught:
        expand_seeds(seed=seed, seeds=seeds)

    assert caught.value.code == code


def test_create_plan_is_deterministic_inspectable_and_does_not_create_a_run() -> None:
    """Catches plans with random Run IDs or unresolved execution placeholders."""
    from shoal_run.orchestration.planner import create_plan

    first = create_plan(_spec(), _config(), _target(), seeds=(4, 9))
    second = create_plan(_spec(), _config(), _target(), seeds=(4, 9))

    assert first == second
    assert first.version == 1
    assert first.experiment_name == "example"
    assert first.target == _target()
    assert first.strategy == "one_unit_per_task"
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
    from shoal_run.orchestration.planner import construct_tasks

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
    from shoal_run.orchestration.models import PlanningError
    from shoal_run.orchestration.planner import create_plan

    with pytest.raises(PlanningError) as caught:
        create_plan(_spec(argv), _config(), _target(), seeds=(1,))

    assert caught.value.code == code


@pytest.mark.parametrize("seeds", [(), (1, 1), (1, True)])
def test_create_plan_rejects_empty_duplicate_or_non_integer_seeds(
    seeds: tuple[object, ...],
) -> None:
    """Catches plans whose logical task set is unstable or lacks explicit seeds."""
    from shoal_run.orchestration.models import PlanningError
    from shoal_run.orchestration.planner import create_plan

    with pytest.raises(PlanningError, match="seed"):
        create_plan(_spec(), _config(), _target(), seeds=seeds)
