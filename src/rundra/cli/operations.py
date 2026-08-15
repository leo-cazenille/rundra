from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from rundra.config.errors import ConfigError
from rundra.config.experiments import load_config_snapshot, load_experiment
from rundra.config.targets import load_targets
from rundra.domain.models import ExperimentSpec, Target
from rundra.orchestration.models import ExecutionPlan, PlanningError
from rundra.orchestration.planner import create_plan, expand_seeds
from rundra.results import OperationError, OperationResult


@dataclass(frozen=True, slots=True)
class ValidationValue:
    source: Path
    experiment: ExperimentSpec


@dataclass(frozen=True, slots=True)
class TargetsValue:
    source: Path
    targets: Mapping[str, Target]


def validate_operation(source: Path) -> OperationResult[ValidationValue]:
    try:
        return OperationResult.success(
            "validate", ValidationValue(source, load_experiment(source))
        )
    except ConfigError as error:
        return OperationResult.failure("validate", _config_error(error))


def plan_operation(
    experiment_source: Path,
    config_source: Path,
    targets_source: Path,
    target_name: str,
    *,
    seed: object = None,
    seeds: object = None,
) -> OperationResult[ExecutionPlan]:
    try:
        experiment = load_experiment(experiment_source)
        config = load_config_snapshot(config_source)
        targets = load_targets(targets_source)
        if target_name not in targets:
            return OperationResult.failure(
                "plan",
                OperationError(
                    "TARGET_NOT_FOUND",
                    f"Target '{target_name}' is not defined",
                    {"source": str(targets_source), "target": target_name},
                ),
            )
        plan = create_plan(
            experiment,
            config,
            targets[target_name],
            seeds=expand_seeds(seed=seed, seeds=seeds),
        )
        return OperationResult.success("plan", plan)
    except ConfigError as error:
        return OperationResult.failure("plan", _config_error(error))
    except PlanningError as error:
        return OperationResult.failure(
            "plan", OperationError(error.code, error.message, error.details)
        )


def targets_operation(source: Path) -> OperationResult[TargetsValue]:
    try:
        return OperationResult.success(
            "targets", TargetsValue(source, load_targets(source))
        )
    except ConfigError as error:
        return OperationResult.failure("targets", _config_error(error))


def _config_error(error: ConfigError) -> OperationError:
    return OperationError(
        error.code,
        error.message,
        {"source": str(error.source), "path": error.path},
    )
