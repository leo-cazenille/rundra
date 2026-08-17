from __future__ import annotations

import itertools
import re
from collections.abc import Sequence
from math import ceil
from typing import cast

from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import (
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    RunId,
    Target,
    Task,
    TaskId,
)
from rundra.domain.preparation import PreparationPlan
from rundra.domain.scaling import ExecutionPolicy, SeedRange, TaskSpace
from rundra.domain.sweeps import ExpandedConfig
from rundra.orchestration.models import (
    MULTI_ARRAY,
    ONE_UNIT_PER_TASK,
    SLURM_ARRAY,
    WORKER_POOL,
    ExecutionGroup,
    ExecutionPlan,
    ExecutionUnit,
    PlanningError,
)

_PLACEHOLDER_PATTERN = re.compile(r"\{[^{}]+\}")
_REQUIRED_PLACEHOLDERS = frozenset({"{config}", "{seed}"})
_SEED_RANGE_PATTERN = re.compile(r"(-?[0-9]+):(-?[0-9]+)\Z")
_SCALABLE_STRATEGIES = frozenset({"auto", MULTI_ARRAY, WORKER_POOL})
_RETRIEVAL_POLICIES = frozenset({"all", "manifest", "none"})


def expand_seeds(*, seed: object = None, seeds: object = None) -> tuple[int, ...]:
    """Expand one explicit seed or an inclusive ``START:STOP`` expression."""
    if seed is None and seeds is None:
        raise PlanningError(
            code="SEED_REQUIRED",
            message="Provide exactly one of seed or seeds",
        )
    if seed is not None and seeds is not None:
        raise PlanningError(
            code="SEED_CONFLICT",
            message="seed and seeds are mutually exclusive",
        )
    if seed is not None:
        if type(seed) is not int:
            raise PlanningError(code="INVALID_SEED", message="seed must be an integer")
        return (seed,)
    if type(seeds) is not str:
        raise PlanningError(
            code="INVALID_SEED_RANGE",
            message="seeds must use inclusive START:STOP syntax",
        )
    match = _SEED_RANGE_PATTERN.fullmatch(seeds)
    if match is None:
        raise PlanningError(
            code="INVALID_SEED_RANGE",
            message="seeds must use inclusive START:STOP syntax",
        )
    start, stop = (int(part) for part in match.groups())
    if stop < start:
        raise PlanningError(
            code="INVALID_SEED_RANGE",
            message="seed range stop must not precede start",
            details={"start": start, "stop": stop},
        )
    return tuple(range(start, stop + 1))


def compact_seed_range(*, seed: object = None, seeds: object = None) -> SeedRange:
    """Parse one seed or inclusive range without materializing its members."""

    if seed is None and seeds is None:
        raise PlanningError(code="SEED_REQUIRED", message="Provide one seed or range")
    if seed is not None and seeds is not None:
        raise PlanningError(
            code="SEED_CONFLICT", message="seed and seeds are mutually exclusive"
        )
    if seed is not None:
        if type(seed) is not int:
            raise PlanningError(code="INVALID_SEED", message="seed must be an integer")
        return SeedRange(seed, seed)
    if (
        type(seeds) is not str
        or (match := _SEED_RANGE_PATTERN.fullmatch(seeds)) is None
    ):
        raise PlanningError(
            code="INVALID_SEED_RANGE",
            message="seeds must use inclusive START:STOP syntax",
        )
    start, stop = (int(part) for part in match.groups())
    try:
        return SeedRange(start, stop)
    except ValueError as error:
        raise PlanningError(code="INVALID_SEED_RANGE", message=str(error)) from error


def create_scalable_plan(
    spec: ExperimentSpec,
    configs: Sequence[ExpandedConfig],
    target: Target,
    *,
    seeds: SeedRange,
    policy: ExecutionPolicy,
    strategy: str = "auto",
    retrieval_policy: str = "manifest",
    preparation: PreparationPlan | None = None,
) -> ExecutionPlan:
    """Create a bounded-preview v4 plan without materializing logical Tasks."""

    expanded = tuple(configs)
    if not expanded:
        raise PlanningError(
            code="INVALID_SWEEP", message="config set must not be empty"
        )
    if type(seeds) is not SeedRange:
        raise TypeError("create_scalable_plan seeds must be a SeedRange")
    if type(policy) is not ExecutionPolicy:
        raise TypeError("create_scalable_plan policy must be an ExecutionPolicy")
    if strategy not in _SCALABLE_STRATEGIES:
        raise PlanningError(
            code="INVALID_EXECUTION_STRATEGY",
            message="execution strategy must be auto, multi-array, or worker-pool",
        )
    if retrieval_policy not in _RETRIEVAL_POLICIES:
        raise PlanningError(
            code="INVALID_RETRIEVAL_POLICY",
            message="retrieval policy must be all, manifest, or none",
        )
    _validate_placeholders(spec.command)
    task_space = TaskSpace(len(expanded), seeds)
    if task_space.task_count > policy.hard_task_limit:
        raise PlanningError(
            code="TASK_LIMIT_EXCEEDED",
            message="logical Task count exceeds the target hard limit",
            details={
                "task_count": task_space.task_count,
                "hard_task_limit": policy.hard_task_limit,
            },
        )
    selected = (
        WORKER_POOL
        if strategy == "auto"
        and (
            task_space.task_count > policy.max_concurrent_jobs
            or task_space.task_count >= policy.worker_pool.activation_threshold
        )
        else (MULTI_ARRAY if strategy == "auto" else strategy)
    )
    if selected == MULTI_ARRAY and task_space.task_count > policy.max_concurrent_jobs:
        raise PlanningError(
            code="CONCURRENT_JOB_LIMIT_EXCEEDED",
            message=(
                f"multi-array would submit {task_space.task_count} scheduler elements; "
                f"target limit is {policy.max_concurrent_jobs}; use worker-pool"
            ),
            details={
                "task_count": task_space.task_count,
                "max_concurrent_jobs": policy.max_concurrent_jobs,
            },
        )
    if selected == WORKER_POOL and target.scheduler.kind != "slurm":
        raise PlanningError(
            code="UNSUPPORTED_EXECUTION_STRATEGY",
            message="worker-pool execution requires a Slurm target",
        )
    preview = task_space.page(offset=0, limit=min(task_space.task_count, 10))
    units = tuple(
        ExecutionUnit(
            task_id=item.task_id,
            seed=item.seed,
            config=expanded[item.parameter_set_ordinal].config,
            command=_render_command(
                spec.command,
                expanded[item.parameter_set_ordinal].config,
                item.seed,
            ),
            resources=spec.resources,
            parameter_set=expanded[item.parameter_set_ordinal].parameter_set,
        )
        for item in preview
    )
    if selected == MULTI_ARRAY:
        scheduler_batches = ceil(task_space.task_count / policy.max_array_size)
        worker_count = None
    else:
        leases = ceil(task_space.task_count / policy.worker_pool.tasks_per_lease)
        worker_count = min(
            policy.worker_pool.max_workers,
            policy.max_active_tasks,
            policy.max_concurrent_jobs,
            leases,
        )
        scheduler_batches = 1
    return ExecutionPlan(
        version=4,
        experiment_name=spec.name,
        target=target,
        units=units,
        groups=(),
        array_mapping=(),
        strategy=selected,
        preparation=preparation,
        task_space=task_space,
        execution_policy=policy,
        retrieval_policy=retrieval_policy,
        scheduler_batches=scheduler_batches,
        worker_count=worker_count,
    )


def validate_task_confirmation(
    task_count: int,
    policy: ExecutionPolicy,
    confirmation: int | None,
) -> None:
    """Require an exact count above the target-owned confirmation threshold."""

    if type(task_count) is not int or task_count < 1:
        raise TypeError("task_count must be a positive integer")
    if task_count > policy.hard_task_limit:
        raise PlanningError(
            code="TASK_LIMIT_EXCEEDED",
            message="logical Task count exceeds the target hard limit",
        )
    if task_count < policy.confirmation_threshold:
        return
    if type(confirmation) is not int or confirmation != task_count:
        raise PlanningError(
            code="TASK_CONFIRMATION_REQUIRED",
            message=f"submission requires --confirm-tasks {task_count}",
            details={"task_count": task_count},
        )


def create_plan(
    spec: ExperimentSpec,
    config: ConfigSnapshot,
    target: Target,
    *,
    seeds: Sequence[object],
    preparation: PreparationPlan | None = None,
) -> ExecutionPlan:
    """Create a deterministic plan without creating a Run or calling adapters."""
    normalized_seeds = _validate_seed_set(seeds)
    _validate_placeholders(spec.command)
    units = tuple(
        ExecutionUnit(
            task_id=TaskId.from_ordinal(index),
            seed=seed,
            config=config,
            command=_render_command(spec.command, config, seed),
            resources=spec.resources,
        )
        for index, seed in enumerate(normalized_seeds)
    )
    uses_array = target.scheduler.kind == "slurm" and len(units) > 1
    return ExecutionPlan(
        version=2 if preparation is not None else 1,
        experiment_name=spec.name,
        target=target,
        units=units,
        groups=_execution_groups(target, units),
        array_mapping=(
            tuple(
                ArrayTaskMapping(unit.task_id, unit.seed, index)
                for index, unit in enumerate(units)
            )
            if uses_array
            else ()
        ),
        strategy=SLURM_ARRAY if uses_array else ONE_UNIT_PER_TASK,
        preparation=preparation,
    )


def create_sweep_plan(
    spec: ExperimentSpec,
    configs: Sequence[ExpandedConfig],
    target: Target,
    *,
    seeds: Sequence[object],
    preparation: PreparationPlan | None = None,
) -> ExecutionPlan:
    """Create one deterministic parameter-set by seed execution plan."""
    normalized_seeds = _validate_seed_set(seeds)
    expanded = tuple(configs)
    if not expanded or any(config.parameter_set is None for config in expanded):
        raise PlanningError(
            code="INVALID_SWEEP",
            message="Sweep plans require parameterized effective configs",
        )
    _validate_placeholders(spec.command)
    units = tuple(
        ExecutionUnit(
            task_id=TaskId.from_ordinal(index),
            seed=seed,
            config=expanded_config.config,
            command=_render_command(spec.command, expanded_config.config, seed),
            resources=spec.resources,
            parameter_set=expanded_config.parameter_set,
        )
        for index, (expanded_config, seed) in enumerate(
            itertools.product(expanded, normalized_seeds)
        )
    )
    uses_array = target.scheduler.kind == "slurm" and len(units) > 1
    return ExecutionPlan(
        version=3,
        experiment_name=spec.name,
        target=target,
        units=units,
        groups=_execution_groups(target, units),
        array_mapping=(
            tuple(
                ArrayTaskMapping(unit.task_id, unit.seed, index)
                for index, unit in enumerate(units)
            )
            if uses_array
            else ()
        ),
        strategy=SLURM_ARRAY if uses_array else ONE_UNIT_PER_TASK,
        preparation=preparation,
    )


def construct_tasks(
    run_id: RunId,
    spec: ExperimentSpec,
    config: ConfigSnapshot,
    *,
    seeds: Sequence[object],
) -> tuple[Task, ...]:
    """Construct logical Tasks for a caller-owned Run identifier."""
    normalized_seeds = _validate_seed_set(seeds)
    return tuple(
        Task(
            id=TaskId.from_ordinal(index),
            run_id=run_id,
            experiment_name=spec.name,
            config=config,
            seed=seed,
            resources=spec.resources,
        )
        for index, seed in enumerate(normalized_seeds)
    )


def _validate_seed_set(seeds: Sequence[object]) -> tuple[int, ...]:
    normalized = tuple(seeds)
    if not normalized:
        raise PlanningError(code="INVALID_SEEDS", message="seed set must not be empty")
    if any(type(seed) is not int for seed in normalized):
        raise PlanningError(
            code="INVALID_SEEDS",
            message="every seed must be an integer",
        )
    integer_seeds = cast(tuple[int, ...], normalized)
    if len(set(integer_seeds)) != len(integer_seeds):
        raise PlanningError(
            code="DUPLICATE_SEED",
            message="duplicate seeds are not supported in version 0.1",
        )
    return integer_seeds


def _execution_groups(
    target: Target, units: tuple[ExecutionUnit, ...]
) -> tuple[ExecutionGroup, ...]:
    if target.scheduler.kind == "slurm" and len(units) > 1:
        return (ExecutionGroup(tuple(unit.task_id for unit in units)),)
    return tuple(ExecutionGroup((unit.task_id,)) for unit in units)


def _validate_placeholders(command: Command) -> None:
    joined = "\0".join(command.argv)
    placeholders = frozenset(_PLACEHOLDER_PATTERN.findall(joined))
    unknown = placeholders - _REQUIRED_PLACEHOLDERS
    if unknown or "{" in joined.replace("{config}", "").replace("{seed}", ""):
        raise PlanningError(
            code="UNKNOWN_PLACEHOLDER",
            message="command contains an unsupported placeholder",
        )
    missing = _REQUIRED_PLACEHOLDERS - placeholders
    if missing:
        raise PlanningError(
            code="MISSING_PLACEHOLDER",
            message="command must contain {config} and {seed} placeholders",
        )


def _render_command(
    command: Command,
    config: ConfigSnapshot,
    seed: int,
) -> Command:
    return Command(
        argv=tuple(
            argument.replace("{config}", str(config.source)).replace(
                "{seed}", str(seed)
            )
            for argument in command.argv
        ),
        environment=command.environment,
        working_directory=command.working_directory,
    )
