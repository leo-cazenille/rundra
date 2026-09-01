from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rundra.cli.campaign_operations import (
    CampaignLaunchPlanValue,
    CampaignPlanValue,
)
from rundra.cli.capability_doctor import _scheduler_inventory
from rundra.cli.doctor import doctor_operation as target_doctor_operation
from rundra.cli.operations import (
    PlanValue,
    ResolvedRunInputs,
    plan_operation,
    resolve_run_inputs_operation,
)
from rundra.config.campaigns import (
    CampaignDefinition,
    CampaignLaunchConfig,
    CampaignSeedSelector,
)
from rundra.config.errors import ConfigError
from rundra.config.launch import (
    LaunchResolutionError,
    LaunchValues,
    ProjectLaunchConfig,
    discover_project_launch,
    discover_user_launch,
    resolve_launch,
)
from rundra.config.placement import (
    PlacementPolicy,
    automatic_placement_policy,
)
from rundra.config.targets import load_targets_config
from rundra.domain.campaigns import CampaignFailurePolicy, valid_campaign_launch_name
from rundra.domain.placement import PlacementDecision, PlacementTargetDecision
from rundra.domain.scaling import SeedRange
from rundra.ports import SchedulerPartition
from rundra.results import OperationError, OperationResult
from rundra.scheduler_registry import scheduler_capabilities

type PlacementObserver = Callable[
    [Path, str], OperationResult[tuple[SchedulerPartition, ...]]
]


def placement_requested(
    experiment_source: Path,
    *,
    placement: str | None,
    project_file: Path | None,
    profile: str | None,
) -> bool:
    if placement is not None:
        return True
    try:
        project = discover_project_launch(experiment_source, project_file=project_file)
        if project is None:
            return False
        selected = profile or project.default_profile
        if selected is not None and selected in project.profiles:
            if project.profiles[selected].placement is not None:
                return True
        return project.defaults.placement is not None
    except ConfigError:
        return False


def placement_plan_operation(
    experiment_source: Path,
    *,
    placement: str | None = None,
    candidate_targets: Sequence[str] = (),
    config: Path | None = None,
    seed: int | None = None,
    seeds: str | None = None,
    target: str | None = None,
    targets_file: Path | None = None,
    source_root: Path | None = None,
    destination: Path | None = None,
    data_dir: Path | None = None,
    project_file: Path | None = None,
    profile: str | None = None,
    user_config_source: Path | None = None,
    random_seed: bool = False,
    prepare_location: str = "auto",
    rebuild: bool = False,
    rebuild_image: bool = False,
    offline: bool = False,
    workers: int | None = None,
    task_slots_per_worker: int | None = None,
    fetch_mode: str | None = None,
    execution_strategy: str = "auto",
    retrieval_policy: str = "manifest",
    observer: PlacementObserver | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> OperationResult[CampaignPlanValue]:
    """Resolve one read-only placement snapshot into an explicit campaign plan."""
    if target is not None:
        return _failure(
            "PLACEMENT_TARGET_CONFLICT",
            "--placement and --target are mutually exclusive",
        )
    candidates_override = tuple(candidate_targets)
    if len(set(candidates_override)) != len(candidates_override):
        return _failure(
            "DUPLICATE_PLACEMENT_TARGET",
            "--candidate-target values must be unique",
        )
    try:
        project = discover_project_launch(experiment_source, project_file=project_file)
        user = discover_user_launch(user_config_source)
        launch = resolve_launch(
            cli=LaunchValues(
                config=config,
                placement=placement,
                source_root=source_root,
                destination=destination,
                targets_file=targets_file,
                data_dir=data_dir,
                workers=workers,
                task_slots_per_worker=task_slots_per_worker,
                fetch_mode=fetch_mode,
            ),
            project=project,
            user=user,
            builtins=LaunchValues(
                config=experiment_source.expanduser().resolve().parent / "config.yaml",
                target="local",
                source_root=experiment_source.expanduser().resolve().parent,
                targets_file=Path("~/.config/rundra/targets.yaml").expanduser(),
                data_dir=Path("~/.local/share/rundra/runs").expanduser(),
            ),
            profile=profile,
        )
        policy_name = launch.values.placement
        if policy_name is None:
            return _failure(
                "PLACEMENT_NOT_REQUESTED", "No automatic placement policy resolved"
            )
        policy = _policy(policy_name, project)
        resolved_targets_file = launch.values.targets_file
        assert resolved_targets_file is not None
        target_config = load_targets_config(resolved_targets_file)
    except ConfigError as error:
        return OperationResult.failure(
            "plan",
            OperationError(
                error.code,
                error.message,
                {"source": str(error.source), "path": error.path},
            ),
        )
    except (LaunchResolutionError, ValueError) as error:
        code = error.code if isinstance(error, LaunchResolutionError) else "INVALID_PLACEMENT"
        message = error.message if isinstance(error, LaunchResolutionError) else str(error)
        return _failure(code, message)

    candidate_names = candidates_override or policy.candidates or tuple(
        name
        for name, configured in target_config.targets.items()
        if configured.transport.kind != "local"
    )
    if not candidate_names:
        return _failure(
            "PLACEMENT_NO_CANDIDATES", "No remote candidate targets are configured"
        )
    unknown = tuple(name for name in candidate_names if name not in target_config.targets)
    if unknown:
        return OperationResult.failure(
            "plan",
            OperationError(
                "TARGET_NOT_FOUND",
                "Placement candidate targets are not configured",
                {"targets": unknown},
            ),
        )

    first = resolve_run_inputs_operation(
        experiment_source,
        config=config,
        seed=seed,
        seeds=seeds,
        target=candidate_names[0],
        targets_file=resolved_targets_file,
        source_root=source_root,
        destination=destination,
        data_dir=data_dir,
        project_file=project_file,
        profile=profile,
        user_config_source=user_config_source,
        random_seed=random_seed,
        operation="plan",
        prepare_location=prepare_location,
        rebuild=rebuild,
        rebuild_image=rebuild_image,
        offline=offline,
        workers=workers,
        task_slots_per_worker=task_slots_per_worker,
        fetch_mode=fetch_mode,
    )
    if not first.ok:
        assert first.error is not None
        return OperationResult.failure("plan", first.error)
    assert first.value is not None
    bounds = _contiguous_seed_bounds(first.value)
    if bounds is None:
        return _failure(
            "PLACEMENT_SEED_RANGE_REQUIRED",
            "Automatic placement requires one contiguous seed range",
        )
    seed_start, seed_stop = bounds
    seed_count = seed_stop - seed_start + 1
    normalized_seeds = f"{seed_start}:{seed_stop}"
    active_observer = observer or _observe_target
    observed_at = clock()
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise TypeError("Placement clock must return a timezone-aware datetime")

    accepted: list[tuple[str, int, ResolvedRunInputs]] = []
    decisions: list[PlacementTargetDecision] = []
    task_multiplier: int | None = None
    for candidate in candidate_names:
        configured = target_config.targets[candidate]
        if configured.transport.kind == "local":
            decisions.append(
                PlacementTargetDecision(candidate, False, "local target is not remote")
            )
            continue
        resolved = resolve_run_inputs_operation(
            experiment_source,
            config=config,
            seeds=normalized_seeds,
            target=candidate,
            targets_file=resolved_targets_file,
            source_root=source_root,
            destination=destination,
            data_dir=data_dir,
            project_file=project_file,
            profile=profile,
            user_config_source=user_config_source,
            operation="plan",
            prepare_location=prepare_location,
            rebuild=rebuild,
            rebuild_image=rebuild_image,
            offline=offline,
            workers=workers,
            task_slots_per_worker=task_slots_per_worker,
            fetch_mode=fetch_mode,
        )
        if not resolved.ok:
            assert resolved.error is not None
            decisions.append(
                PlacementTargetDecision(candidate, False, resolved.error.code.lower())
            )
            continue
        assert resolved.value is not None
        planned = _plan_child(
            experiment_source,
            resolved.value,
            execution_strategy=execution_strategy,
            retrieval_policy=retrieval_policy,
        )
        if not planned.ok:
            assert planned.error is not None
            decisions.append(
                PlacementTargetDecision(candidate, False, planned.error.code.lower())
            )
            continue
        assert planned.value is not None
        execution_plan = planned.value.plan
        scheduler = execution_plan.target.scheduler.kind
        if not scheduler_capabilities(scheduler).detached_submission:
            decisions.append(
                PlacementTargetDecision(candidate, False, "scheduler is synchronous")
            )
            continue
        observation = active_observer(resolved_targets_file, candidate)
        if not observation.ok:
            reason = observation.error.code.lower() if observation.error else "unreachable"
            decisions.append(PlacementTargetDecision(candidate, False, reason))
            continue
        assert observation.value is not None
        partition = _selected_partition(execution_plan)
        inventory_item = _inventory_partition(observation.value, partition)
        if inventory_item is None:
            decisions.append(
                PlacementTargetDecision(
                    candidate,
                    False,
                    "scheduler partition was not discovered",
                    partition=partition,
                )
            )
            continue
        if inventory_item.availability.lower() not in {"up", "yes"}:
            decisions.append(
                PlacementTargetDecision(
                    candidate,
                    False,
                    f"partition is {inventory_item.availability}",
                    partition=inventory_item.name,
                    utilization_percent=inventory_item.utilization_percent,
                    idle_cpus=inventory_item.cpu_idle,
                )
            )
            continue
        utilization = inventory_item.utilization_percent
        idle_cpus = inventory_item.cpu_idle
        if utilization is None or idle_cpus is None:
            decisions.append(
                PlacementTargetDecision(
                    candidate,
                    False,
                    "scheduler capacity is unavailable",
                    partition=inventory_item.name,
                )
            )
            continue
        if utilization >= policy.max_utilization_percent:
            decisions.append(
                PlacementTargetDecision(
                    candidate,
                    False,
                    "utilization threshold reached",
                    partition=inventory_item.name,
                    utilization_percent=utilization,
                    idle_cpus=idle_cpus,
                )
            )
            continue
        if idle_cpus < policy.minimum_idle_cpus:
            decisions.append(
                PlacementTargetDecision(
                    candidate,
                    False,
                    "insufficient idle CPUs",
                    partition=inventory_item.name,
                    utilization_percent=utilization,
                    idle_cpus=idle_cpus,
                )
            )
            continue
        task_count = (
            len(execution_plan.units)
            if execution_plan.task_space is None
            else execution_plan.task_space.task_count
        )
        planned_capacity = execution_plan.concurrent_task_capacity or task_count
        logical_cpus = max(
            1,
            execution_plan.units[0].resources.tasks
            * execution_plan.units[0].resources.cpus_per_task,
        )
        usable_capacity = min(planned_capacity, idle_cpus // logical_cpus)
        if usable_capacity < 1:
            decisions.append(
                PlacementTargetDecision(
                    candidate,
                    False,
                    "insufficient idle CPUs for one Task",
                    partition=inventory_item.name,
                    utilization_percent=utilization,
                    idle_cpus=idle_cpus,
                    planned_capacity=planned_capacity,
                    usable_capacity=0,
                )
            )
            continue
        multiplier = task_count // seed_count
        task_multiplier = multiplier if task_multiplier is None else task_multiplier
        if multiplier != task_multiplier:
            return _failure(
                "PLACEMENT_TASK_SPACE_MISMATCH",
                "Candidate targets resolved different logical Task spaces",
            )
        accepted.append((candidate, usable_capacity, resolved.value))
        decisions.append(
            PlacementTargetDecision(
                candidate,
                True,
                "eligible",
                partition=inventory_item.name,
                utilization_percent=utilization,
                idle_cpus=idle_cpus,
                planned_capacity=planned_capacity,
                usable_capacity=usable_capacity,
            )
        )

    if not accepted:
        return OperationResult.failure(
            "plan",
            OperationError(
                "PLACEMENT_NO_ELIGIBLE_TARGETS",
                "No candidate target passed automatic placement checks",
                {"rejections": tuple((item.target, item.reason) for item in decisions)},
            ),
        )
    assert task_multiplier is not None
    total_tasks = seed_count * task_multiplier
    ranked = sorted(accepted, key=lambda item: (-item[1], item[0]))
    limit = min(policy.max_targets or len(ranked), seed_count)
    selected: list[tuple[str, int, ResolvedRunInputs]] = []
    for item in ranked[:limit]:
        selected.append(item)
        if sum(capacity for _, capacity, _ in selected) >= total_tasks:
            break
    assignments = _allocate_seeds(seed_count, tuple(item[1] for item in selected))
    campaign_name = _campaign_name(policy.name, experiment_source)
    destination_root = (
        destination.expanduser().resolve()
        if destination is not None
        else ((project.project_root if project is not None else experiment_source.parent) / "retrieved" / campaign_name).resolve()
    )
    explicit_launches: list[CampaignLaunchConfig] = []
    launch_plans: list[CampaignLaunchPlanValue] = []
    assigned_start = seed_start
    assigned_by_target: dict[str, tuple[int, int]] = {}
    for (candidate, _, template), count in zip(selected, assignments, strict=True):
        assigned_stop = assigned_start + count - 1
        assigned_by_target[candidate] = (assigned_start, assigned_stop)
        child_destination = destination_root / candidate
        child = resolve_run_inputs_operation(
            experiment_source,
            config=template.config,
            seeds=f"{assigned_start}:{assigned_stop}",
            target=candidate,
            targets_file=template.targets_file,
            source_root=template.source_root,
            destination=child_destination,
            data_dir=template.data_dir,
            project_file=project_file,
            profile=profile,
            user_config_source=user_config_source,
            operation="plan",
            prepare_location=prepare_location,
            rebuild=rebuild,
            rebuild_image=rebuild_image,
            offline=offline,
            workers=template.workers,
            task_slots_per_worker=template.task_slots_per_worker,
            fetch_mode=fetch_mode,
        )
        if not child.ok:
            assert child.error is not None
            return OperationResult.failure("plan", child.error)
        assert child.value is not None
        child_plan = _plan_child(
            experiment_source,
            child.value,
            execution_strategy=execution_strategy,
            retrieval_policy=retrieval_policy,
        )
        if not child_plan.ok:
            assert child_plan.error is not None
            return OperationResult.failure("plan", child_plan.error)
        assert child_plan.value is not None
        explicit_launches.append(
            CampaignLaunchConfig(
                name=candidate,
                seeds=CampaignSeedSelector(assigned_start, assigned_stop),
                target=candidate,
                config=child.value.config,
                source_root=child.value.source_root,
                destination=child.value.destination,
                workers=child.value.workers,
                task_slots_per_worker=child.value.task_slots_per_worker,
                fetch_mode=fetch_mode,
            )
        )
        launch_plans.append(
            CampaignLaunchPlanValue(candidate, child.value, child_plan.value)
        )
        assigned_start = assigned_stop + 1
    resolved_decisions = tuple(
        replace(
            item,
            assigned_seed_start=assigned_by_target[item.target][0],
            assigned_seed_stop=assigned_by_target[item.target][1],
            reason="selected",
        )
        if item.target in assigned_by_target
        else item
        for item in decisions
    )
    definition = CampaignDefinition(
        1,
        campaign_name,
        (project.source if project is not None else experiment_source.resolve()),
        experiment_source.resolve(),
        project.source if project is not None else None,
        CampaignFailurePolicy.CANCEL,
        False,
        tuple(explicit_launches),
    )
    return OperationResult.success(
        "plan",
        CampaignPlanValue(
            definition,
            experiment_source.resolve(),
            project.source if project is not None else None,
            tuple(launch_plans),
            (),
            placement=PlacementDecision(
                policy.name,
                policy.strategy,
                observed_at,
                resolved_decisions,
            ),
        ),
    )


def _policy(
    name: str, project: ProjectLaunchConfig | None
) -> PlacementPolicy:
    if name == "auto":
        return automatic_placement_policy()
    if project is None or name not in project.placements:
        raise ValueError(f"Placement policy '{name}' is not defined")
    return project.placements[name]


def _observe_target(
    targets_file: Path, target_name: str
) -> OperationResult[tuple[SchedulerPartition, ...]]:
    checked = target_doctor_operation(targets_file, target_name, connect=True)
    if not checked.ok or checked.value is None:
        return _failure("PLACEMENT_TARGET_UNREACHABLE", "Target capability check failed")
    if any(item.status == "fail" for item in checked.value.checks):
        return _failure("PLACEMENT_TARGET_UNREACHABLE", "Target capability check failed")
    inventory, checks = _scheduler_inventory(checked.value.target)
    if any(item.status == "fail" for item in checks):
        return _failure(
            "PLACEMENT_INVENTORY_FAILED", "Scheduler inventory query failed"
        )
    return OperationResult.success("plan", inventory)


def _plan_child(
    experiment_source: Path,
    inputs: ResolvedRunInputs,
    *,
    execution_strategy: str,
    retrieval_policy: str,
) -> OperationResult[PlanValue]:
    return plan_operation(
        experiment_source,
        inputs.config,
        inputs.targets_file,
        inputs.target,
        seed=inputs.seed,
        seeds=inputs.seeds if inputs.seed is None else None,
        launch=inputs.launch,
        preparation=inputs.preparation_plan,
        sweep=inputs.sweep,
        execution_strategy=execution_strategy,
        retrieval_policy=retrieval_policy,
        workers=inputs.workers,
        task_slots_per_worker=inputs.task_slots_per_worker,
        source_root=inputs.source_root,
    )


def _contiguous_seed_bounds(inputs: ResolvedRunInputs) -> tuple[int, int] | None:
    if isinstance(inputs.seeds, SeedRange):
        return inputs.seeds.start, inputs.seeds.stop
    values = inputs.seeds
    if tuple(values) != tuple(range(values[0], values[-1] + 1)):
        return None
    return values[0], values[-1]


def _selected_partition(plan: Any) -> str | None:
    worker = plan.worker_resources
    resources = worker if worker is not None else plan.units[0].resources
    slurm = resources.native.get("slurm", {})
    partition = slurm.get("partition")
    return partition if isinstance(partition, str) else None


def _inventory_partition(
    inventory: tuple[SchedulerPartition, ...], selected: str | None
) -> SchedulerPartition | None:
    if selected is not None:
        return next((item for item in inventory if item.name == selected), None)
    return next((item for item in inventory if item.default), None)


def _allocate_seeds(seed_count: int, capacities: tuple[int, ...]) -> tuple[int, ...]:
    if not capacities or len(capacities) > seed_count:
        raise ValueError("Seed allocation inputs are invalid")
    assignments = [1] * len(capacities)
    remaining = seed_count - len(capacities)
    total = sum(capacities)
    remainders: list[tuple[int, int]] = []
    for index, capacity in enumerate(capacities):
        numerator = remaining * capacity
        assignments[index] += numerator // total
        remainders.append((numerator % total, index))
    missing = seed_count - sum(assignments)
    for _, index in sorted(remainders, key=lambda item: (-item[0], item[1]))[:missing]:
        assignments[index] += 1
    return tuple(assignments)


def _campaign_name(policy: str, source: Path) -> str:
    raw = re.sub(r"[^a-z0-9_-]+", "-", f"auto-{policy}-{source.stem.lower()}")
    name = raw.strip("-_")[:64]
    if not valid_campaign_launch_name(name):
        return "automatic-placement"
    return name


def _failure(code: str, message: str) -> OperationResult[Any]:
    return OperationResult.failure("plan", OperationError(code, message))
