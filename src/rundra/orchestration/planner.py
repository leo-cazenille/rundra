from __future__ import annotations

import re
from collections.abc import Sequence
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
from rundra.orchestration.models import (
    ONE_UNIT_PER_TASK,
    SLURM_ARRAY,
    ExecutionGroup,
    ExecutionPlan,
    ExecutionUnit,
    PlanningError,
)

_PLACEHOLDER_PATTERN = re.compile(r"\{[^{}]+\}")
_REQUIRED_PLACEHOLDERS = frozenset({"{config}", "{seed}"})
_SEED_RANGE_PATTERN = re.compile(r"(-?[0-9]+):(-?[0-9]+)\Z")


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
