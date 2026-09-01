from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rundra.cli.capability_doctor import (
    DoctorValue,
    doctor_operation,
)
from rundra.cli.operations import (
    PlanValue,
    ResolvedRunInputs,
    _config_error,
    plan_operation,
    resolve_run_inputs_operation,
)
from rundra.config.campaigns import (
    CampaignDefinition,
    CampaignLaunchConfig,
    load_campaign,
)
from rundra.config.errors import ConfigError
from rundra.config.launch import discover_project_launch
from rundra.config.sweeps import load_sweep_config
from rundra.domain.campaigns import CampaignFailurePolicy
from rundra.domain.preparation import PreparationStorageConfig
from rundra.domain.scaling import SeedRange
from rundra.results import OperationError, OperationResult
from rundra.scheduler_registry import scheduler_capabilities


@dataclass(frozen=True, slots=True)
class CampaignLaunchPlanValue:
    name: str
    inputs: ResolvedRunInputs
    plan: PlanValue

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name:
            raise ValueError("Campaign launch plan name must be nonblank")
        if type(self.inputs) is not ResolvedRunInputs:
            raise TypeError("Campaign launch plan inputs are invalid")
        if type(self.plan) is not PlanValue:
            raise TypeError("Campaign launch plan is invalid")

    @property
    def target(self) -> str:
        return self.inputs.target

    @property
    def destination(self) -> Path:
        return self.inputs.destination

    @property
    def task_count(self) -> int:
        task_space = self.plan.plan.task_space
        return (
            task_space.task_count
            if task_space is not None
            else len(self.plan.plan.units)
        )

    @property
    def concurrent_task_capacity(self) -> int:
        capacity = self.plan.plan.concurrent_task_capacity
        return self.task_count if capacity is None else capacity


@dataclass(frozen=True, slots=True)
class CampaignPlanValue:
    definition: CampaignDefinition
    experiment_source: Path
    project_file: Path | None
    launches: tuple[CampaignLaunchPlanValue, ...]
    warnings: tuple[str, ...] = ()
    format_version: int = 1

    def __post_init__(self) -> None:
        if type(self.definition) is not CampaignDefinition:
            raise TypeError("Campaign plan definition is invalid")
        if not isinstance(self.experiment_source, Path):
            raise TypeError("Campaign experiment source must be a Path")
        if self.project_file is not None and not isinstance(self.project_file, Path):
            raise TypeError("Campaign project file must be a Path or None")
        launches = tuple(self.launches)
        if not launches or any(
            type(item) is not CampaignLaunchPlanValue for item in launches
        ):
            raise ValueError("Campaign plan launches are invalid")
        if tuple(item.name for item in launches) != tuple(
            item.name for item in self.definition.launches
        ):
            raise ValueError("Campaign plan launch order differs from its definition")
        if any(type(warning) is not str or not warning for warning in self.warnings):
            raise ValueError("Campaign plan warnings are invalid")
        if self.format_version != 1:
            raise ValueError("Campaign plan format version must be 1")
        object.__setattr__(self, "launches", launches)
        object.__setattr__(self, "warnings", tuple(self.warnings))

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def on_submit_failure(self) -> CampaignFailurePolicy:
        return self.definition.on_submit_failure

    @property
    def total_tasks(self) -> int:
        return sum(item.task_count for item in self.launches)

    @property
    def total_concurrent_task_capacity(self) -> int:
        return sum(item.concurrent_task_capacity for item in self.launches)


@dataclass(frozen=True, slots=True)
class CampaignLaunchDoctorValue:
    name: str
    doctor: DoctorValue


@dataclass(frozen=True, slots=True)
class CampaignDoctorValue:
    plan: CampaignPlanValue
    launches: tuple[CampaignLaunchDoctorValue, ...]
    format_version: int = 1

    @property
    def ready(self) -> bool:
        return all(item.doctor.ready for item in self.launches)

    @property
    def complete(self) -> bool:
        return all(item.doctor.complete for item in self.launches)


def campaign_plan_operation(
    source: Path,
    *,
    campaign_name: str | None = None,
    project_file: Path | None = None,
    targets_file: Path | None = None,
    data_dir: Path | None = None,
    destination: Path | None = None,
    source_root: Path | None = None,
    user_config_source: Path | None = None,
    prepare_location: str = "auto",
    rebuild: bool = False,
    rebuild_image: bool = False,
    offline: bool = False,
    execution_strategy: str = "auto",
    retrieval_policy: str = "manifest",
) -> OperationResult[CampaignPlanValue]:
    """Resolve and plan every launch in one static single-experiment campaign."""
    try:
        definition, experiment_source, resolved_project = _campaign_request(
            source, campaign_name, project_file
        )
    except ConfigError as error:
        return OperationResult.failure("plan", _config_error(error))
    except CampaignResolutionError as error:
        return OperationResult.failure("plan", error.operation_error)

    launches: list[CampaignLaunchPlanValue] = []
    default_root = (definition.source.parent / "retrieved" / definition.name).resolve()
    for launch in definition.launches:
        launch_destination = _campaign_destination(
            launch,
            campaign_root=destination,
            default_root=default_root,
        )
        resolved = resolve_run_inputs_operation(
            experiment_source,
            config=launch.config,
            seed=launch.seeds.start if launch.seeds.count == 1 else None,
            seeds=(
                None
                if launch.seeds.count == 1
                else f"{launch.seeds.start}:{launch.seeds.stop}"
            ),
            target=launch.target,
            targets_file=targets_file,
            source_root=source_root or launch.source_root,
            destination=launch_destination,
            data_dir=data_dir,
            project_file=resolved_project,
            profile=launch.profile,
            user_config_source=user_config_source,
            operation="plan",
            prepare_location=prepare_location,
            rebuild=rebuild,
            rebuild_image=rebuild_image,
            offline=offline,
            workers=launch.workers,
            task_slots_per_worker=launch.task_slots_per_worker,
            fetch_mode=launch.fetch_mode,
        )
        if not resolved.ok:
            assert resolved.error is not None
            return OperationResult.failure(
                "plan", _launch_error(launch.name, resolved.error)
            )
        assert resolved.value is not None
        inputs = resolved.value
        planned = plan_operation(
            experiment_source,
            inputs.config,
            inputs.targets_file,
            inputs.target,
            seed=inputs.seed,
            seeds=(
                None
                if inputs.seed is not None
                else f"{inputs.seeds.start}:{inputs.seeds.stop}"
                if isinstance(inputs.seeds, SeedRange)
                else f"{inputs.seeds[0]}:{inputs.seeds[-1]}"
            ),
            launch=inputs.launch,
            preparation=inputs.preparation_plan,
            sweep=inputs.sweep,
            execution_strategy=execution_strategy,
            retrieval_policy=retrieval_policy,
            workers=inputs.workers,
            task_slots_per_worker=inputs.task_slots_per_worker,
            source_root=inputs.source_root,
        )
        if not planned.ok:
            assert planned.error is not None
            return OperationResult.failure(
                "plan", _launch_error(launch.name, planned.error)
            )
        assert planned.value is not None
        scheduler = planned.value.plan.target.scheduler.kind
        if not scheduler_capabilities(scheduler).detached_submission:
            return OperationResult.failure(
                "plan",
                OperationError(
                    "CAMPAIGN_TARGET_NOT_DETACHED",
                    f"Campaign launch '{launch.name}' selects a synchronous target",
                    {
                        "launch": launch.name,
                        "target": inputs.target,
                        "scheduler": scheduler,
                    },
                ),
            )
        launches.append(CampaignLaunchPlanValue(launch.name, inputs, planned.value))

    duplicate_overlaps = _duplicate_overlaps(tuple(launches))
    if duplicate_overlaps and not definition.allow_duplicate_tasks:
        left, right, count = duplicate_overlaps[0]
        return OperationResult.failure(
            "plan",
            OperationError(
                "DUPLICATE_CAMPAIGN_TASKS",
                f"Campaign launches '{left}' and '{right}' overlap by {count} logical Tasks",
                {"launches": (left, right), "task_count": count},
            ),
        )
    warnings = tuple(
        f"duplicate logical Tasks allowed between {left} and {right}: {count}"
        for left, right, count in duplicate_overlaps
    )
    return OperationResult.success(
        "plan",
        CampaignPlanValue(
            definition,
            experiment_source,
            resolved_project,
            tuple(launches),
            warnings,
        ),
    )


def campaign_doctor_operation(
    source: Path,
    *,
    campaign_name: str | None = None,
    project_file: Path | None = None,
    targets_file: Path | None = None,
    data_dir: Path | None = None,
    destination: Path | None = None,
    source_root: Path | None = None,
    user_config_source: Path | None = None,
    prepare_location: str = "auto",
    rebuild: bool = False,
    rebuild_image: bool = False,
    offline: bool = False,
    execution_strategy: str = "auto",
    retrieval_policy: str = "manifest",
    connect: bool = False,
    scheduler_probe: bool = False,
    scheduler_inventory: bool = False,
    probe_timeout: int = 120,
    write_probe: bool = True,
    local_target_access: bool = False,
    agent: str = "generic",
) -> OperationResult[CampaignDoctorValue]:
    planned = campaign_plan_operation(
        source,
        campaign_name=campaign_name,
        project_file=project_file,
        targets_file=targets_file,
        data_dir=data_dir,
        destination=destination,
        source_root=source_root,
        user_config_source=user_config_source,
        prepare_location=prepare_location,
        rebuild=rebuild,
        rebuild_image=rebuild_image,
        offline=offline,
        execution_strategy=execution_strategy,
        retrieval_policy=retrieval_policy,
    )
    if not planned.ok:
        assert planned.error is not None
        return OperationResult.failure("doctor", planned.error)
    assert planned.value is not None
    campaign_plan = planned.value
    results: list[CampaignLaunchDoctorValue] = []
    probed_targets: set[str] = set()
    for launch in campaign_plan.launches:
        inputs = launch.inputs
        storage = inputs.preparation_storage
        result = doctor_operation(
            inputs.targets_file,
            inputs.target,
            connect=connect,
            scheduler_probe=scheduler_probe and inputs.target not in probed_targets,
            scheduler_inventory=(
                scheduler_inventory and inputs.target not in probed_targets
            ),
            probe_timeout=probe_timeout,
            write_probe=write_probe,
            data_dir=inputs.data_dir,
            destination=inputs.destination,
            source_root=inputs.source_root,
            experiment_source=campaign_plan.experiment_source,
            config_source=inputs.config,
            cache_root=_cache_root(storage),
            preparation=inputs.preparation_plan,
            preparation_storage=storage,
            offline=offline,
            local_target_access=local_target_access,
            agent=agent,
        )
        if not result.ok:
            assert result.error is not None
            return OperationResult.failure(
                "doctor", _launch_error(launch.name, result.error)
            )
        assert result.value is not None
        results.append(CampaignLaunchDoctorValue(launch.name, result.value))
        probed_targets.add(inputs.target)
    return OperationResult.success(
        "doctor", CampaignDoctorValue(campaign_plan, tuple(results))
    )


@dataclass(frozen=True, slots=True)
class CampaignResolutionError(Exception):
    operation_error: OperationError


def _campaign_request(
    source: Path,
    campaign_name: str | None,
    project_file: Path | None,
) -> tuple[CampaignDefinition, Path, Path | None]:
    if campaign_name is None:
        definition = load_campaign(source)
        assert definition.experiment is not None
        selected_project = project_file or definition.project_file
        return (
            definition,
            definition.experiment,
            selected_project.expanduser().resolve() if selected_project else None,
        )
    experiment = source.expanduser().resolve()
    project = discover_project_launch(experiment, project_file=project_file)
    if project is None:
        raise CampaignResolutionError(
            OperationError(
                "CAMPAIGN_PROJECT_REQUIRED",
                "A named campaign requires rundra.yaml or --project-file",
                {"campaign": campaign_name},
            )
        )
    if campaign_name not in project.campaigns:
        raise CampaignResolutionError(
            OperationError(
                "CAMPAIGN_NOT_FOUND",
                f"Campaign '{campaign_name}' is not defined",
                {"campaign": campaign_name, "source": str(project.source)},
            )
        )
    return project.campaigns[campaign_name], experiment, project.source


def _campaign_destination(
    launch: CampaignLaunchConfig,
    *,
    campaign_root: Path | None,
    default_root: Path,
) -> Path:
    root = campaign_root.expanduser().resolve() if campaign_root else default_root
    if campaign_root is not None or launch.destination is None:
        return (root / launch.name).resolve()
    return launch.destination


def _duplicate_overlaps(
    launches: tuple[CampaignLaunchPlanValue, ...],
) -> tuple[tuple[str, str, int], ...]:
    overlaps: list[tuple[str, str, int]] = []
    fingerprints = [
        (
            item,
            {
                config.sha256
                for config in (
                    item.inputs.sweep or load_sweep_config(item.inputs.config)
                ).configs
            },
            _seed_bounds(item.inputs),
        )
        for item in launches
    ]
    for index, (left, left_configs, left_seeds) in enumerate(fingerprints):
        for right, right_configs, right_seeds in fingerprints[index + 1 :]:
            config_count = len(left_configs & right_configs)
            seed_count = _range_overlap(left_seeds, right_seeds)
            if config_count and seed_count:
                overlaps.append((left.name, right.name, config_count * seed_count))
    return tuple(overlaps)


def _seed_bounds(inputs: ResolvedRunInputs) -> tuple[int, int]:
    seeds = inputs.seeds
    if isinstance(seeds, SeedRange):
        return seeds.start, seeds.stop
    return seeds[0], seeds[-1]


def _range_overlap(left: tuple[int, int], right: tuple[int, int]) -> int:
    start = max(left[0], right[0])
    stop = min(left[1], right[1])
    return max(0, stop - start + 1)


def _launch_error(name: str, error: OperationError) -> OperationError:
    return OperationError(
        error.code,
        f"Campaign launch '{name}': {error.message}",
        {**error.details, "launch": name},
    )


def _cache_root(storage: PreparationStorageConfig) -> Path | None:
    return None if storage.cache_root is None else Path(str(storage.cache_root))
