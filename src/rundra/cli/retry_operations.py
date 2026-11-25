from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, replace
from importlib.metadata import version
from pathlib import Path

from rundra.adapters._local_paths import resolve_write_destination
from rundra.adapters.rsync import RsyncStager
from rundra.cli.operations import (
    _create_effective_scaling_plan,
    _effective_execution_policy,
    _execution_adapters,
    _load_record,
    _record_stager,
    _unsupported_execution_target,
)
from rundra.cli.retained_inputs import RetainedInputError, RetainedInputs
from rundra.config.errors import ConfigError
from rundra.config.targets import load_targets_config
from rundra.domain.campaigns import CampaignId
from rundra.domain.models import Command, ConfigSnapshot, RunId, TaskId
from rundra.domain.parameters import ParameterSet
from rundra.domain.records import RunRecord
from rundra.domain.scaling import SeedRange
from rundra.domain.states import ExecutionState
from rundra.domain.sweeps import ExpandedConfig
from rundra.orchestration.models import PlanningError
from rundra.orchestration.planner import (
    create_task_subset_plan,
    validate_task_confirmation,
)
from rundra.orchestration.service import (
    OrchestrationError,
    OrchestrationService,
    RunExecutionRequest,
)
from rundra.persistence import JsonRunStore, SqliteTaskStore, SubmissionReceiptStore
from rundra.persistence.campaign_store import JsonCampaignStore
from rundra.persistence.errors import RunStoreError
from rundra.results import ErrorDetail, OperationError, OperationResult
from rundra.scheduler_registry import scheduler_capabilities

LINEAGE_DIRECTORY = "retry-lineage"
_TERMINAL = {ExecutionState.SUCCEEDED, ExecutionState.FAILED, ExecutionState.CANCELLED}


@dataclass(frozen=True, slots=True)
class RetryTask:
    parent_id: TaskId
    config: ConfigSnapshot
    seed: int
    parameter_set: ParameterSet | None


@dataclass(frozen=True, slots=True)
class RetryValue:
    parent_run_id: RunId
    target: str
    task_count: int
    preview: tuple[tuple[str, str, int, str | None], ...]
    run_id: RunId | None = None
    state: ExecutionState | None = None
    scheduler_job_ids: tuple[str, ...] = ()

    @property
    def exit_code(self) -> int:
        return (
            2 if self.state in {ExecutionState.FAILED, ExecutionState.CANCELLED} else 0
        )


def retry_operation(
    selector: str,
    data_dir: Path,
    targets_source: Path,
    *,
    launch: str | None = None,
    target_name: str | None = None,
    image: Path | None = None,
    confirm_tasks: int | None = None,
    destination: Path | None = None,
) -> OperationResult[RetryValue]:
    """Preview or submit a sparse exact retry using retained Task inputs."""
    store = JsonRunStore(data_dir)
    task_store = SqliteTaskStore(data_dir)
    try:
        if selector.startswith("campaign_"):
            campaign = JsonCampaignStore(data_dir).load(CampaignId(selector))
            child = next(
                (item for item in campaign.launches if item.name == launch), None
            )
            if child is None:
                return _failure(
                    "RETRY_LAUNCH_REQUIRED",
                    "Select a campaign child with --launch",
                    {"launches": tuple(item.name for item in campaign.launches)},
                )
            selector = str(child.run_id)
        elif launch is not None:
            return _failure("CLI_USAGE_ERROR", "--launch requires a campaign ID")
        parent, error = _load_record(selector, store)
        if error is not None:
            return OperationResult.failure("retry", error)
        assert parent is not None
        if parent.run.state not in _TERMINAL:
            return _failure("RETRY_RUN_ACTIVE", "Retry requires a terminal parent Run")
        inputs = RetainedInputs(parent)
        tasks = _failed_tasks(parent, inputs, task_store)
        if not tasks:
            return _failure(
                "RETRY_NO_FAILED_TASKS", "The parent Run has no failed Tasks"
            )
        if confirm_tasks is not None and confirm_tasks != len(tasks):
            return _failure(
                "TASK_CONFIRMATION_MISMATCH",
                "Confirm the current failed Task count before retrying",
                {"task_count": len(tasks)},
            )
        targets = load_targets_config(targets_source)
        name = target_name if target_name is not None else parent.run.target.name
        if name not in targets.targets:
            return _failure(
                "TARGET_NOT_FOUND",
                "The retry target is absent from the selected targets file",
                {"target": name},
            )
        target = targets.targets[name]
        asynchronous = scheduler_capabilities(target.scheduler.kind).detached_submission
        experiment = parent.experiment
        if image is not None:
            if (
                experiment.container is None
                or parent.container_digest is None
                or not image.is_absolute()
            ):
                return _failure(
                    "RETRY_IMAGE_UNVERIFIED",
                    "--image needs an absolute target path and a recorded parent image digest",
                )
            experiment = replace(
                experiment, container=replace(experiment.container, image=image)
            )
        unsupported = _unsupported_execution_target(
            target, experiment, asynchronous=asynchronous
        )
        if unsupported is not None:
            return OperationResult.failure("retry", unsupported)
        transport, stager, runtime, scheduler = _execution_adapters(target)
        if (
            experiment.container is not None
            and experiment.container.image.is_absolute()
        ):
            same_transport = target.transport == parent.run.target.transport
            if not same_transport and parent.container_digest is None:
                return _failure(
                    "RETRY_IMAGE_UNVERIFIED",
                    "Moving an absolute image to another host requires its recorded digest",
                )
            image_path = str(experiment.container.image)
            available = transport.run(
                Command(
                    (
                        "sh",
                        "-c",
                        '[ -f "$1" ] && [ -r "$1" ]',
                        "rundra-retry-image",
                        image_path,
                    )
                )
            )
            if available.exit_code != 0:
                raise RetainedInputError(
                    "The retained container image is unavailable on the retry target"
                )
            if parent.container_digest is not None and confirm_tasks is not None:
                checksum = transport.run(Command(("sha256sum", "--", image_path)))
                if checksum.exit_code != 0 or checksum.stdout.split()[:1] != [
                    parent.container_digest
                ]:
                    return _failure(
                        "RETRY_IMAGE_MISMATCH",
                        "The target image differs from the recorded image digest",
                    )
        source = inputs.require_source()
        if parent.preparation is not None and parent.preparation.builder_status not in {
            None,
            "SUCCEEDED",
        }:
            raise RetainedInputError("The original preparation did not complete")
        remote_copy = (
            not inputs.local
            and target.transport == parent.run.target.transport
            and isinstance(stager, RsyncStager)
        )
        origin_stager = (
            _record_stager(parent) if not inputs.local and not remote_copy else None
        )
        if (
            not inputs.local
            and not remote_copy
            and not isinstance(origin_stager, RsyncStager)
        ):
            raise RetainedInputError(
                "No client-visible or rsync source snapshot is available"
            )
        policy, policy_version = _effective_execution_policy(
            targets.execution.get(name),
            experiment,
            target,
            targets_source,
            targets.version,
        )
        if policy is not None:
            validate_task_confirmation(
                len(tasks),
                policy,
                len(tasks) if confirm_tasks is None else confirm_tasks,
            )
        plan = create_task_subset_plan(
            experiment,
            target,
            tuple((task.config, task.seed, task.parameter_set) for task in tasks),
        )
        sizing = (
            _create_effective_scaling_plan(
                experiment,
                (ExpandedConfig(tasks[0].config, None),),
                target,
                SeedRange(0, len(tasks) - 1),
                policy,
                policy_version,
            )
            if policy is not None
            else None
        )
        capabilities = scheduler_capabilities(target.scheduler.kind)
        if (
            len(tasks) > 1
            and not capabilities.arrays
            and (
                sizing is None
                or sizing.worker_count is None
                or not capabilities.materialized_worker_pool
            )
        ):
            return _failure(
                "UNSUPPORTED_TASK_COUNT",
                "The retry target needs array or worker-pool support for multiple Tasks",
            )
        values = RetryValue(
            parent.run.id,
            name,
            len(tasks),
            tuple(
                (
                    str(task.parent_id),
                    str(TaskId.from_ordinal(index)),
                    task.seed,
                    None if task.parameter_set is None else task.parameter_set.id,
                )
                for index, task in enumerate(tasks[:50])
            ),
        )
        if confirm_tasks is None:
            return OperationResult.success("retry", values)

        child_id = RunId.new()
        fetch_destination = resolve_write_destination(
            destination or data_dir / "retry-results" / str(child_id)
        )
        if fetch_destination.exists() or fetch_destination.is_symlink():
            return _failure(
                "RETRY_DESTINATION_EXISTS",
                "Choose a new result destination for the retry",
            )
        with tempfile.TemporaryDirectory(prefix="rundra-retry-source-") as scratch:
            source_root = source
            if isinstance(origin_stager, RsyncStager):
                source_root = Path(scratch)
                origin_stager.copy_sealed_source(inputs.workspace, source_root)
            if not remote_copy:
                _require_plain_tree(Path(source_root))
            _write_lineage(data_dir, parent.run.id, child_id, tasks)
            service = OrchestrationService(
                store=store,
                stager=stager,
                runtime=runtime,
                scheduler=scheduler,
                transport=transport,
                framework_version=version("rundra"),
                submission_receipts=SubmissionReceiptStore(data_dir),
                task_store=task_store,
                run_id_factory=lambda: child_id,
            )
            request = RunExecutionRequest(
                plan=plan,
                experiment=experiment,
                source_root=source_root,
                fetch_destination=fetch_destination,
                remote_source_root=source if remote_copy else None,
                max_concurrent_jobs=None
                if policy is None
                else min(policy.max_concurrent_jobs, policy.max_active_tasks),
                max_workers=None if sizing is None else sizing.worker_count,
                task_slots_per_worker=(
                    (sizing.task_slots_per_worker or 1) if sizing is not None else 1
                ),
                worker_resources=None if sizing is None else sizing.worker_resources,
                retry_parent=parent.run.id,
                retry_image_digest=parent.container_digest,
            )
            result = (
                service.submit_one(request)
                if asynchronous
                else service.execute_one(request)
            )
        return OperationResult.success(
            "retry",
            replace(
                values,
                run_id=child_id,
                state=result.record.run.state,
                scheduler_job_ids=result.record.scheduler_job_ids,
            ),
        )
    except OrchestrationError as error:
        return _failure(
            error.code,
            error.message,
            {} if error.run_id is None else {"run_id": str(error.run_id)},
        )
    except (ConfigError, PlanningError) as error:
        return _failure(error.code, error.message)
    except RetainedInputError as error:
        return _failure("RETRY_INPUTS_UNAVAILABLE", str(error))
    except RunStoreError as error:
        return _failure("RUN_STORE_ERROR", str(error))
    except (OSError, RuntimeError, ValueError) as error:
        return _failure("RETRY_FAILED", str(error))


def _failed_tasks(
    record: RunRecord, inputs: RetainedInputs, task_store: SqliteTaskStore
) -> tuple[RetryTask, ...]:
    if not record.is_compact:
        return tuple(
            RetryTask(task.id, task.config, task.seed, task.parameter_set)
            for task in record.run.tasks
            if task.state is ExecutionState.FAILED
        )
    tasks = []
    for state in task_store.all_states(record.run.id):
        if state.execution_state is not ExecutionState.FAILED:
            continue
        coordinate = state.coordinate
        config = inputs.compact_config(coordinate.parameter_set_ordinal)
        tasks.append(
            RetryTask(
                coordinate.task_id, config.config, coordinate.seed, config.parameter_set
            )
        )
    return tuple(tasks)


def _require_plain_tree(source: Path) -> None:
    if not source.is_dir() or source.is_symlink():
        raise RetainedInputError(
            "The client-visible sealed source directory is unavailable"
        )
    for directory, child_dirs, files in source.walk():
        if any(
            (directory / name).is_symlink() for name in (*child_dirs, *files)
        ) or any(not (directory / name).is_file() for name in files):
            raise RetainedInputError(
                "The sealed source contains a link or a special file"
            )


def _write_lineage(
    data_dir: Path, parent_id: RunId, child_id: RunId, tasks: tuple[RetryTask, ...]
) -> None:
    root = data_dir / LINEAGE_DIRECTORY
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink():
        raise RetainedInputError(
            "The retry lineage directory must not be a symbolic link"
        )
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=root, prefix=".lineage-", delete=False, mode="w", encoding="utf-8"
        ) as stream:
            temporary_path = Path(stream.name)
            json.dump(
                {
                    "format_version": 1,
                    "parent_run_id": str(parent_id),
                    "run_id": str(child_id),
                    "tasks": [
                        {
                            "parent_task_id": str(task.parent_id),
                            "task_id": str(TaskId.from_ordinal(index)),
                            "seed": task.seed,
                            "parameter_set": (
                                None
                                if task.parameter_set is None
                                else task.parameter_set.id
                            ),
                        }
                        for index, task in enumerate(tasks)
                    ],
                },
                stream,
                separators=(",", ":"),
            )
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary_path, root / f"{child_id}.json")
        directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _failure(
    code: str, message: str, details: dict[str, ErrorDetail] | None = None
) -> OperationResult[RetryValue]:
    return OperationResult.failure(
        "retry", OperationError(code, message, details or {})
    )
