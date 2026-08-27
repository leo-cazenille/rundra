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
    ResourceRequest,
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
    WORKER_POOL,
    ExecutionGroup,
    ExecutionPlan,
    ExecutionUnit,
    PlanningError,
)
from rundra.orchestration.routing import route_scheduler_resources
from rundra.scheduler_registry import scheduler_capabilities
from rundra.schema_versions import PLAN_SCHEMA

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
    version: int = 4,
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
) -> ExecutionPlan:
    """Create a bounded-preview scalable plan without materializing logical Tasks."""

    expanded = tuple(configs)
    if not expanded:
        raise PlanningError(
            code="INVALID_SWEEP", message="config set must not be empty"
        )
    if type(seeds) is not SeedRange:
        raise TypeError("create_scalable_plan seeds must be a SeedRange")
    if type(policy) is not ExecutionPolicy:
        raise TypeError("create_scalable_plan policy must be an ExecutionPolicy")
    if version not in PLAN_SCHEMA.supported or version < 4:
        raise ValueError("create_scalable_plan version must be from 4 through 9")
    for name, value in (
        ("workers", workers),
        ("task_slots_per_worker", task_slots_per_worker),
    ):
        if value is not None and (type(value) is not int or value < 1):
            raise PlanningError(
                code="INVALID_EXECUTION_SCALE",
                message=f"{name} must be a positive integer",
            )
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
    effective_resources, _ = route_scheduler_resources(spec.resources, target)
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
            workers is not None
            or task_slots_per_worker is not None
            or task_space.task_count > policy.max_concurrent_jobs
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
    if selected == MULTI_ARRAY and (
        workers is not None or task_slots_per_worker is not None
    ):
        raise PlanningError(
            code="EXECUTION_SCALE_REQUIRES_WORKER_POOL",
            message="worker scale options require worker-pool execution",
        )
    capabilities = scheduler_capabilities(target.scheduler.kind)
    if selected == WORKER_POOL and not capabilities.compact_worker_pool:
        raise PlanningError(
            code="UNSUPPORTED_EXECUTION_STRATEGY",
            message="worker-pool execution is unsupported by the selected scheduler",
        )
    if (
        selected == WORKER_POOL
        and policy.worker_pool.requeue_limit > 0
        and not capabilities.scheduler_requeue_recovery
    ):
        raise PlanningError(
            code="UNSUPPORTED_SCHEDULER_RECOVERY",
            message=("selected scheduler worker pools require target requeue_limit 0"),
            details={"requeue_limit": policy.worker_pool.requeue_limit},
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
            resources=effective_resources,
            parameter_set=expanded[item.parameter_set_ordinal].parameter_set,
        )
        for item in preview
    )
    requested_workers: int | None = None
    requested_slots: int | None = None
    if selected == MULTI_ARRAY:
        scheduler_batches = ceil(task_space.task_count / policy.max_array_size)
        worker_count = None
        task_slots_per_worker = 1
    else:
        leases = ceil(task_space.task_count / policy.worker_pool.tasks_per_lease)
        requested_workers = (
            policy.worker_pool.default_worker_count if workers is None else workers
        )
        requested_slots = (
            policy.worker_pool.task_slots_per_worker
            if task_slots_per_worker is None
            else task_slots_per_worker
        )
        if requested_workers > policy.worker_pool.max_workers:
            raise PlanningError(
                code="WORKER_LIMIT_EXCEEDED",
                message="requested workers exceed the target policy",
                details={
                    "requested_workers": requested_workers,
                    "max_workers": policy.worker_pool.max_workers,
                },
            )
        if requested_slots > policy.worker_pool.max_slot_count:
            raise PlanningError(
                code="WORKER_SLOT_LIMIT_EXCEEDED",
                message="requested task slots per worker exceed the target policy",
                details={
                    "requested_task_slots_per_worker": requested_slots,
                    "max_task_slots_per_worker": policy.worker_pool.max_slot_count,
                },
            )
        if (
            version >= 6
            and requested_workers * requested_slots > policy.max_active_tasks
        ):
            raise PlanningError(
                code="ACTIVE_TASK_LIMIT_EXCEEDED",
                message="requested worker capacity exceeds the target policy",
                details={
                    "requested_capacity": requested_workers * requested_slots,
                    "max_active_tasks": policy.max_active_tasks,
                },
            )
        task_slots_per_worker = (
            min(requested_slots, task_space.task_count) if version >= 5 else 1
        )
        worker_limits = [
            requested_workers,
            policy.max_active_tasks // task_slots_per_worker,
            policy.max_concurrent_jobs,
            policy.max_array_size,
            ceil(task_space.task_count / task_slots_per_worker),
        ]
        if version < 6:
            worker_limits.append(leases)
        worker_count = min(worker_limits)
        scheduler_batches = 1
    concurrent_task_capacity = (worker_count or 1) * task_slots_per_worker
    max_lane_depth = ceil(task_space.task_count / concurrent_task_capacity)
    worker_resources = (
        route_scheduler_resources(
            _worker_resources(
                spec.resources,
                task_slots_per_worker,
                max_lane_depth,
            ),
            target,
        )[0]
        if selected == WORKER_POOL
        else effective_resources
    )
    if (
        version >= 7
        and policy.max_memory_per_worker is not None
        and worker_resources.memory_bytes is not None
        and worker_resources.memory_bytes > policy.max_memory_per_worker
    ):
        raise PlanningError(
            code="WORKER_MEMORY_LIMIT_EXCEEDED",
            message="aggregate worker memory exceeds the target policy",
            details={
                "logical_task_memory_bytes": spec.resources.memory_bytes or 0,
                "task_slots_per_worker": task_slots_per_worker,
                "worker_memory_bytes": worker_resources.memory_bytes,
                "max_memory_per_worker": policy.max_memory_per_worker,
            },
        )
    return ExecutionPlan(
        version=version,
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
        task_slots_per_worker=(task_slots_per_worker if version >= 5 else None),
        concurrent_task_capacity=(concurrent_task_capacity if version >= 5 else None),
        max_lane_depth=(max_lane_depth if version >= 5 else None),
        worker_resources=(worker_resources if version >= 5 else None),
        requested_workers=(
            requested_workers if version >= 6 and selected == WORKER_POOL else None
        ),
        requested_task_slots_per_worker=(
            requested_slots if version >= 6 and selected == WORKER_POOL else None
        ),
    )


def _worker_resources(
    resources: ResourceRequest,
    task_slots_per_worker: int,
    max_lane_depth: int,
) -> ResourceRequest:
    if task_slots_per_worker > 1:
        if resources.nodes != 1 or resources.tasks != 1:
            raise PlanningError(
                code="UNSUPPORTED_WORKER_RESOURCES",
                message=(
                    "intra-allocation concurrency requires one-node, one-task "
                    "logical resources"
                ),
            )
        if resources.gpus_per_task != 0:
            raise PlanningError(
                code="UNSUPPORTED_WORKER_RESOURCES",
                message=("intra-allocation concurrency does not support per-task GPUs"),
            )
    return ResourceRequest(
        nodes=1,
        tasks=task_slots_per_worker,
        cpus_per_task=resources.cpus_per_task,
        gpus_per_task=0,
        memory_bytes=(
            resources.memory_bytes * task_slots_per_worker
            if resources.memory_bytes is not None
            else None
        ),
        walltime=(
            resources.walltime * max_lane_depth
            if resources.walltime is not None
            else None
        ),
        native=resources.native,
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
    effective_resources, _ = route_scheduler_resources(spec.resources, target)
    units = tuple(
        ExecutionUnit(
            task_id=TaskId.from_ordinal(index),
            seed=seed,
            config=config,
            command=_render_command(spec.command, config, seed),
            resources=effective_resources,
        )
        for index, seed in enumerate(normalized_seeds)
    )
    capabilities = scheduler_capabilities(target.scheduler.kind)
    uses_array = capabilities.arrays and len(units) > 1
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
        strategy=(capabilities.array_strategy if uses_array else ONE_UNIT_PER_TASK),
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
    effective_resources, _ = route_scheduler_resources(spec.resources, target)
    units = tuple(
        ExecutionUnit(
            task_id=TaskId.from_ordinal(index),
            seed=seed,
            config=expanded_config.config,
            command=_render_command(spec.command, expanded_config.config, seed),
            resources=effective_resources,
            parameter_set=expanded_config.parameter_set,
        )
        for index, (expanded_config, seed) in enumerate(
            itertools.product(expanded, normalized_seeds)
        )
    )
    capabilities = scheduler_capabilities(target.scheduler.kind)
    uses_array = capabilities.arrays and len(units) > 1
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
        strategy=(capabilities.array_strategy if uses_array else ONE_UNIT_PER_TASK),
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
    if scheduler_capabilities(target.scheduler.kind).arrays and len(units) > 1:
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
