from __future__ import annotations

import json
import shlex
from collections.abc import Mapping, Sequence
from datetime import timedelta
from hashlib import sha256
from typing import Any

from rundra.cli.agent_guide import AgentGuideValue
from rundra.cli.campaign_operations import (
    CampaignAndRunListValue,
    CampaignArtifactsValue,
    CampaignCancelValue,
    CampaignDoctorValue,
    CampaignFetchValue,
    CampaignInspectValue,
    CampaignListValue,
    CampaignLogsValue,
    CampaignPlanValue,
    CampaignPurgeValue,
    CampaignRunValue,
    CampaignStatusValue,
    CampaignSubmitValue,
    CampaignTasksValue,
    CampaignValidationValue,
    CampaignWaitValue,
)
from rundra.cli.capability_doctor import DoctorValue
from rundra.cli.operations import (
    ArtifactsValue,
    AwaitRunsValue,
    CancelValue,
    FetchValue,
    InspectValue,
    LaunchResolutionValue,
    ListRunsValue,
    LogsValue,
    PlanValue,
    PreparationLogsValue,
    PurgeValue,
    RunValue,
    StatusValue,
    SubmissionRecoveryValue,
    TargetsValue,
    TaskStatusValue,
    TasksValue,
    ValidationValue,
    WaitValue,
)
from rundra.domain.models import Artifact, Command, ResourceRequest, Target, Task
from rundra.domain.preparation import (
    PreparationImage,
    PreparationImageDefinition,
    PreparationPlan,
    PreparationSourceGit,
    source_recipe_identity,
)
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.models import ExecutionPlan, ExecutionUnit
from rundra.persistence import receipt_document, record_to_dict
from rundra.persistence.campaign_store import campaign_record_to_dict
from rundra.results import OperationResult
from rundra.scheduler_registry import scheduler_capabilities_document

_FORMAT_VERSION = 1


def render_json(result: OperationResult[Any]) -> str:
    """Render the versioned public JSON contract deterministically."""
    return json.dumps(
        result_document(result),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def result_document(result: OperationResult[Any]) -> dict[str, Any]:
    value = result.value
    document: dict[str, Any] = {
        "format_version": _result_format_version(value),
        "ok": result.ok,
        "operation": result.operation,
    }
    if result.error is not None:
        document["error"] = {
            "code": result.error.code,
            "message": result.error.message,
            "details": dict(result.error.details),
        }
        return document
    if isinstance(value, CampaignValidationValue):
        document["campaign"] = {
            "name": value.definition.name,
            "schema_version": value.definition.version,
            "source": str(value.definition.source),
            "experiment": str(value.experiment.source),
            "launches": [item.name for item in value.definition.launches],
        }
    elif isinstance(value, CampaignPlanValue):
        document["campaign"] = _campaign_plan_document(value)
    elif isinstance(value, CampaignDoctorValue):
        document["campaign_doctor"] = {
            "name": value.plan.name,
            "ready": value.ready,
            "complete": value.complete,
            "launches": [
                {
                    "name": item.name,
                    "target": (
                        None if item.doctor.target is None else item.doctor.target.name
                    ),
                    "ready": item.doctor.ready,
                    "complete": item.doctor.complete,
                    "connected": item.doctor.connected,
                    "checks": [
                        {
                            "name": check.name,
                            "status": check.status,
                            "message": check.message,
                        }
                        for check in item.doctor.checks
                    ],
                }
                for item in value.launches
            ],
        }
    elif isinstance(value, CampaignRunValue):
        document["campaign_run"] = {
            "campaign_id": str(value.submission.campaign_id),
            "state": value.wait.status.state,
            "timed_out": value.wait.timed_out,
            "fetch": _campaign_fetch_document(value.fetch),
        }
    elif isinstance(value, CampaignSubmitValue):
        document["campaign"] = campaign_record_to_dict(value.record)
    elif isinstance(value, CampaignStatusValue):
        document["campaign_status"] = _campaign_status_document(value)
    elif isinstance(value, CampaignWaitValue):
        document["campaign_wait"] = {
            "campaign_id": str(value.status.record.id),
            "state": value.status.state,
            "terminal": value.status.terminal,
            "timed_out": value.timed_out,
            "elapsed_seconds": value.elapsed_seconds,
            "status": _campaign_status_document(value.status),
        }
    elif isinstance(value, CampaignFetchValue):
        document["campaign_fetch"] = _campaign_fetch_document(value)
    elif isinstance(value, CampaignCancelValue):
        document["campaign_cancel"] = {
            "campaign_id": str(value.record.id),
            "cancelled_run_ids": [str(item) for item in value.cancelled_run_ids],
            "record": campaign_record_to_dict(value.record),
        }
    elif isinstance(value, CampaignInspectValue):
        document["campaign_record"] = campaign_record_to_dict(value.record)
    elif isinstance(value, CampaignTasksValue):
        document["campaign_tasks"] = {
            "campaign_id": str(value.campaign_id),
            "total": value.total,
            "offset": value.offset,
            "limit": value.limit,
            "returned": len(value.tasks),
            "items": [
                _campaign_task_document(
                    item.selector, item.launch, item.run_id, item.value
                )
                for item in value.tasks
            ],
        }
    elif isinstance(value, CampaignArtifactsValue):
        document["campaign_artifacts"] = {
            "campaign_id": str(value.campaign_id),
            "total": value.total,
            "offset": value.offset,
            "limit": value.limit,
            "returned": len(value.artifacts),
            "items": [
                {
                    "launch": item.launch,
                    "run_id": str(item.run_id),
                    "artifact": _artifact_document(item.artifact),
                }
                for item in value.artifacts
            ],
        }
    elif isinstance(value, CampaignLogsValue):
        logs = value.value
        document["campaign_logs"] = {
            "campaign_id": str(value.campaign_id),
            "launch": value.launch,
            "run_id": str(logs.run_id),
            "kind": "preparation" if isinstance(logs, PreparationLogsValue) else "task",
            "scheduler_id": (
                logs.scheduler_id if isinstance(logs, PreparationLogsValue) else None
            ),
            "task_id": str(logs.task_id) if isinstance(logs, LogsValue) else None,
            "stdout": logs.stdout,
            "stderr": logs.stderr,
            "stdout_path": str(logs.stdout_path),
            "stderr_path": str(logs.stderr_path),
        }
    elif isinstance(value, CampaignPurgeValue):
        document["campaign_purge"] = {
            "campaign_id": str(value.record.id),
            "dry_run": value.dry_run,
            "deleted": value.deleted,
            "children": [
                {
                    "launch": name,
                    "run_id": str(child.run_id),
                    "scope": child.scope.value,
                    "outcome": child.result.outcome.value,
                    "path": str(child.result.path),
                }
                for name, child in value.children
            ],
        }
    elif isinstance(value, CampaignListValue):
        document["campaigns"] = [
            campaign_record_to_dict(item) for item in value.campaigns
        ]
        document["page"] = {
            "offset": value.offset,
            "limit": value.limit,
            "returned": len(value.campaigns),
            "total": value.total,
        }
    elif isinstance(value, CampaignAndRunListValue):
        if not isinstance(value.runs, ListRunsValue):
            raise TypeError("Combined list has invalid Run values")
        document["runs"] = [_status_document(item) for item in value.runs.runs]
        document["campaigns"] = [
            campaign_record_to_dict(item) for item in value.campaigns.campaigns
        ]
    elif isinstance(value, ValidationValue):
        document["experiment"] = {
            "name": value.experiment.name,
            "schema_version": value.experiment.version,
            "source": str(value.source),
        }
        if value.project is not None and value.project.version >= 2:
            document["project"] = {
                "schema_version": value.project.version,
                "source": str(value.project.source),
                "preparation": "validated",
            }
    elif isinstance(value, PlanValue):
        document["plan"] = _plan_document(value.plan)
        document["source_snapshot"] = (
            None
            if value.source_snapshot is None
            else _source_snapshot_document(value.source_snapshot)
        )
        if value.launch is not None:
            document["launch"] = _launch_document(value.launch)
    elif isinstance(value, TargetsValue):
        document["source"] = str(value.source)
        document["targets"] = [
            _target_document(value.targets[name], include_capabilities=True)
            for name in sorted(value.targets)
        ]
    elif isinstance(value, DoctorValue):
        document["doctor"] = {
            "mode": value.mode,
            "source": str(value.source),
            "target": None if value.target is None else value.target.name,
            "connected": value.connected,
            "scheduler_probed": value.scheduler_probed,
            "scheduler_inventoried": bool(value.scheduler_inventory),
            "scheduler_inventory": [
                {
                    "name": item.name,
                    "default": item.default,
                    "availability": item.availability,
                    "max_walltime_seconds": item.max_walltime_seconds,
                    "max_walltime_raw": item.max_walltime_raw,
                    "gres": item.gres,
                }
                for item in value.scheduler_inventory
            ],
            "scheduler_capabilities": (
                None
                if value.target is None
                else scheduler_capabilities_document(value.target.scheduler.kind)
            ),
            "ready": value.ready,
            "complete": value.complete,
            "run_store_durability": (
                None
                if value.durability is None
                else {
                    "status": value.durability.status,
                    "data_dir": str(value.durability.data_dir),
                    "verification_argv": list(value.durability.verification_argv),
                }
            ),
            "checks": [
                {
                    "name": check.name,
                    "scope": "client",
                    "status": check.status,
                    "message": check.message,
                }
                for check in value.checks
            ],
            "requirements": [
                {
                    "kind": requirement.kind,
                    "value": requirement.value,
                    "access": requirement.access,
                    "purpose": requirement.purpose,
                    "location": requirement.location,
                    "status": requirement.status,
                }
                for requirement in value.requirements
            ],
            "remediation": {
                "agent": value.agent,
                "actions": [
                    {"code": action.code, "message": action.message}
                    for action in value.actions
                ],
                "config": value.agent_config,
            },
        }
    elif isinstance(value, RunValue):
        document["run"] = _run_value_document(value)
        if value.launch is not None:
            document["launch"] = _launch_document(value.launch)
    elif isinstance(value, SubmissionRecoveryValue):
        record = value.record
        document["submission"] = {
            "action": value.action,
            "run_id": str(record.run.id),
            "state": record.run.state.value,
            "scheduler_job_ids": list(record.scheduler_job_ids),
            **_scheduler_job_roles_document(record),
        }
    elif isinstance(value, StatusValue):
        document["status"] = _status_document(value)
    elif isinstance(value, WaitValue):
        document["wait"] = {
            "run_id": str(value.status.run_id),
            "terminal": value.terminal,
            "timed_out": value.timed_out,
            "elapsed_seconds": value.elapsed_seconds,
            "status": _status_document(value.status),
        }
    elif isinstance(value, AwaitRunsValue):
        statuses = []
        for status in value.statuses:
            item = _status_document(status)
            item.pop("task_details", None)
            statuses.append(item)
        terminal_states = {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
        document["await"] = {
            "condition_met": value.condition_met,
            "counts": {
                "active": sum(
                    item.state not in terminal_states for item in value.statuses
                ),
                "cancelled": sum(
                    item.state is ExecutionState.CANCELLED for item in value.statuses
                ),
                "failed": sum(
                    item.state is ExecutionState.FAILED for item in value.statuses
                ),
                "succeeded": sum(
                    item.state is ExecutionState.SUCCEEDED for item in value.statuses
                ),
                "terminal": sum(
                    item.state in terminal_states for item in value.statuses
                ),
                "total": len(value.statuses),
            },
            "elapsed_seconds": value.elapsed_seconds,
            "runs": statuses,
            "timed_out": value.timed_out,
            "until": value.until,
        }
    elif isinstance(value, CancelValue):
        document["cancel"] = _status_document(value.status)
    elif isinstance(value, ListRunsValue):
        runs = [_status_document(run) for run in value.runs]
        if not value.include_tasks:
            for run in runs:
                run.pop("task_details", None)
        document["runs"] = runs
        document["page"] = {
            "offset": value.offset,
            "limit": value.limit,
            "returned": len(value.runs),
            "total": value.total,
            "next_offset": value.next_offset,
            "task_details_included": value.include_tasks,
        }
    elif isinstance(value, LogsValue):
        document["logs"] = {
            "run_id": str(value.run_id),
            "task_id": str(value.task_id),
            "stdout": value.stdout,
            "stderr": value.stderr,
            "stdout_path": str(value.stdout_path),
            "stderr_path": str(value.stderr_path),
        }
    elif isinstance(value, PreparationLogsValue):
        document["preparation_logs"] = {
            "run_id": str(value.run_id),
            "scheduler_id": value.scheduler_id,
            "stdout": value.stdout,
            "stderr": value.stderr,
            "stdout_path": str(value.stdout_path),
            "stderr_path": str(value.stderr_path),
        }
    elif isinstance(value, FetchValue):
        document["fetch"] = {
            "run_id": str(value.run_id),
            "destination": str(value.destination),
            "retrieval_state": value.retrieval_state.value,
            "task_ids": [str(task_id) for task_id in value.task_ids],
            "artifact_total": value.artifact_total,
            "artifacts_included": value.artifacts_included,
            "artifacts": [_artifact_document(item) for item in value.artifacts],
        }
    elif isinstance(value, InspectValue):
        document["record"] = (
            _run_record_summary_document(value.record)
            if value.summary
            else record_to_dict(value.record)
        )
        if value.retention is not None:
            document["retention"] = receipt_document(value.retention)
    elif isinstance(value, PurgeValue):
        document["purge"] = {
            "run_id": str(value.run_id),
            "scope": value.scope.value,
            "dry_run": value.dry_run,
            "backend": value.result.backend,
            "path": str(value.result.path),
            "tombstone": str(value.result.tombstone),
            "outcome": value.result.outcome.value,
            "receipt_path": str(value.receipt_path),
            "receipt": (
                None if value.receipt is None else receipt_document(value.receipt)
            ),
        }
    elif isinstance(value, TasksValue):
        document["tasks"] = {
            "run_id": str(value.run_id),
            "total": value.total,
            "offset": value.offset,
            "limit": value.limit,
            "returned": len(value.tasks),
            "items": [
                {
                    "task_id": str(item.coordinate.task_id),
                    "ordinal": item.coordinate.ordinal,
                    "parameter_set_ordinal": item.coordinate.parameter_set_ordinal,
                    "seed_ordinal": item.coordinate.seed_ordinal,
                    "seed": item.coordinate.seed,
                    "state": item.execution_state.value,
                    "retrieval_state": item.retrieval_state.value,
                    "scheduler_id": item.scheduler_id,
                    "native_state": item.native_state,
                    "exit_code": item.exit_code,
                    "attempt": item.attempt,
                }
                for item in value.tasks
            ],
        }
    elif isinstance(value, ArtifactsValue):
        document["artifacts"] = {
            "run_id": str(value.run_id),
            "total": value.total,
            "offset": value.offset,
            "limit": value.limit,
            "returned": len(value.artifacts),
            "next_offset": value.next_offset,
            "items": [_artifact_document(item) for item in value.artifacts],
        }
    elif isinstance(value, AgentGuideValue):
        document["agent_guide"] = {
            "action": value.action,
            "path": None if value.path is None else str(value.path),
            "content": value.content,
        }
    else:
        raise TypeError(f"No public renderer for {type(value).__name__}")
    return document


def render_human(result: OperationResult[Any]) -> str:
    """Render the same operation value for a person."""
    if result.error is not None:
        location = result.error.details.get("source")
        prefix = f"{location}: " if location else ""
        rendered = f"Error [{result.error.code}]: {prefix}{result.error.message}"
        hint = _error_hint(result.error.code, result.error.details)
        return rendered if hint is None else f"{rendered}\nNext: {hint}"
    value = result.value
    if isinstance(value, CampaignValidationValue):
        return (
            f"Valid campaign: {value.definition.name} "
            f"({len(value.definition.launches)} launches, "
            f"experiment {value.experiment.experiment.name})"
        )
    if isinstance(value, CampaignPlanValue):
        lines = [
            f"Campaign plan: {value.name}",
            f"Launches: {len(value.launches)}",
            f"Tasks: {value.total_tasks}",
            f"Concurrent capacity: {value.total_concurrent_task_capacity}",
            f"On submit failure: {value.on_submit_failure.value}",
        ]
        lines.extend(
            f"  {item.name}: target={item.target} tasks={item.task_count} "
            f"capacity={item.concurrent_task_capacity} destination={item.destination}"
            for item in value.launches
        )
        lines.extend(f"WARNING: {warning}" for warning in value.warnings)
        return "\n".join(lines)
    if isinstance(value, CampaignDoctorValue):
        lines = [
            f"Doctor for campaign {value.plan.name}:",
            f"Ready: {'yes' if value.ready else 'no'}",
            f"Verification complete: {'yes' if value.complete else 'no'}",
        ]
        lines.extend(
            f"  {item.name}: target="
            f"{item.doctor.target.name if item.doctor.target else '-'} "
            f"ready={'yes' if item.doctor.ready else 'no'}"
            for item in value.launches
        )
        return "\n".join(lines)
    if isinstance(value, CampaignRunValue):
        return (
            f"Campaign: {value.submission.campaign_id}\n"
            f"State: {value.wait.status.state}\n"
            f"Fetched launches: {len(value.fetch.launches)}"
        )
    if isinstance(value, CampaignSubmitValue):
        lines = [
            f"Campaign: {value.campaign_id}",
            f"Name: {value.record.name}",
            f"Launches: {len(value.record.launches)}",
        ]
        lines.extend(
            f"  {item.name}: run={item.run_id} target={item.target} "
            f"submission={item.submission_state.value}"
            for item in value.record.launches
        )
        return "\n".join(lines)
    if isinstance(value, CampaignStatusValue):
        counts = ", ".join(
            f"{name}={count}" for name, count in sorted(value.task_counts.items())
        )
        lines = [
            f"Campaign: {value.record.id}",
            f"State: {value.state}",
            f"Tasks: {counts}",
        ]
        lines.extend(
            f"  {item.name}: run={item.run_id} "
            f"state={item.status.state.value if item.status else item.submission_state.value}"
            for item in value.launches
        )
        return "\n".join(lines)
    if isinstance(value, CampaignWaitValue):
        return (
            f"Campaign: {value.status.record.id}\n"
            f"State: {value.status.state}\n"
            f"Terminal: {'yes' if value.status.terminal else 'no'}\n"
            f"Timed out: {'yes' if value.timed_out else 'no'}\n"
            f"Waited: {value.elapsed_seconds:.1f}s"
        )
    if isinstance(value, CampaignFetchValue):
        return (
            f"Campaign: {value.record.id}\n"
            f"Fetched launches: {len(value.launches)}\n"
            f"Destination root: {value.destination or 'persisted per-launch destinations'}"
        )
    if isinstance(value, CampaignCancelValue):
        return (
            f"Campaign: {value.record.id}\n"
            f"Cancelled Runs: {len(value.cancelled_run_ids)}"
        )
    if isinstance(value, CampaignInspectValue):
        return (
            f"Campaign: {value.record.id}\n"
            f"Name: {value.record.name}\n"
            f"Launches: {len(value.record.launches)}\n"
            f"Policy: {value.record.on_submit_failure.value}"
        )
    if isinstance(value, CampaignTasksValue):
        lines = [
            f"Campaign: {value.campaign_id}",
            f"Tasks: offset={value.offset} returned={len(value.tasks)} total={value.total}",
        ]
        lines.extend(f"  {item.selector}" for item in value.tasks)
        return "\n".join(lines)
    if isinstance(value, CampaignArtifactsValue):
        return (
            f"Campaign: {value.campaign_id}\n"
            f"Artifacts: offset={value.offset} returned={len(value.artifacts)} "
            f"total={value.total}"
        )
    if isinstance(value, CampaignLogsValue):
        logs = value.value
        return (
            f"Campaign: {value.campaign_id}\nLaunch: {value.launch}\n"
            f"--- stdout ---\n{logs.stdout}"
            f"--- stderr ---\n{logs.stderr}"
        ).rstrip()
    if isinstance(value, CampaignPurgeValue):
        return (
            f"Campaign: {value.record.id}\n"
            f"Dry run: {'yes' if value.dry_run else 'no'}\n"
            f"Deleted: {'yes' if value.deleted else 'no'}\n"
            f"Child purges: {len(value.children)}"
        )
    if isinstance(value, CampaignListValue):
        lines = [
            f"Campaigns: offset={value.offset} returned={len(value.campaigns)} total={value.total}"
        ]
        lines.extend(f"  {item.id}: {item.name}" for item in value.campaigns)
        return "\n".join(lines)
    if isinstance(value, CampaignAndRunListValue):
        if not isinstance(value.runs, ListRunsValue):
            raise TypeError("Combined list has invalid Run values")
        return (
            f"Runs: {len(value.runs.runs)}\nCampaigns: {len(value.campaigns.campaigns)}"
        )
    if isinstance(value, ValidationValue):
        rendered = (
            f"Valid experiment: {value.experiment.name} "
            f"(schema v{value.experiment.version})"
        )
        if value.project is not None and value.project.version >= 2:
            rendered += f"; project preparation v{value.project.version} validated"
        return rendered
    if isinstance(value, PlanValue):
        plan = value.plan
        seeds = _human_sequence(tuple(unit.seed for unit in plan.units))
        resources = plan.units[0].resources
        task_count = (
            plan.task_space.task_count
            if plan.task_space is not None
            else len(plan.units)
        )
        rendered = (
            f"Plan for {plan.experiment_name} on {plan.target.name}: "
            f"{task_count} task(s)\n"
            f"{_human_task_dimensions(plan.units, seeds)}\n"
            f"Strategy: {plan.strategy}\n"
            f"Resources: {_human_resources(resources)}\n"
            f"Native options: {_human_native_options(resources)}\n"
            f"Staging: {_human_staging(plan)}\n"
            "Safety: validated offline; no target contact, workspace creation, "
            "Run creation, or submission"
        )
        if value.source_snapshot is None:
            rendered += "\nSource snapshot: unavailable for an acquired Git source"
        else:
            snapshot = value.source_snapshot
            rendered += (
                f"\nSource snapshot: {_human_bytes(snapshot.size_bytes)} in "
                f"{snapshot.file_count} file(s) after exclusions "
                f"({'exact' if snapshot.exact else 'estimated'})"
            )
            if snapshot.largest_entries:
                rendered += "\nLargest included roots: " + ", ".join(
                    f"{item.path}={_human_bytes(item.size_bytes)}"
                    for item in snapshot.largest_entries
                )
        if plan.version in {4, 5, 6, 7, 8}:
            assert plan.execution_policy is not None
            rendered += (
                f"\nScheduling: batches={plan.scheduler_batches}, "
                f"workers={plan.worker_count or 0}, "
                f"max_active={plan.execution_policy.max_active_tasks}, "
                f"retrieval={plan.retrieval_policy}, preview={len(plan.units)}"
            )
            if plan.version in {5, 6, 7, 8}:
                rendered += (
                    f", slots_per_worker={plan.task_slots_per_worker}, "
                    f"task_capacity={plan.concurrent_task_capacity}, "
                    f"lane_depth={plan.max_lane_depth}"
                )
                assert plan.worker_resources is not None
                rendered += (
                    "\nWorker allocation: "
                    f"nodes={plan.worker_resources.nodes}, "
                    f"tasks={plan.worker_resources.tasks}, "
                    f"cpus_per_task={plan.worker_resources.cpus_per_task}, "
                    f"memory_bytes={plan.worker_resources.memory_bytes}, "
                    f"sequential_tasks_per_slot={plan.max_lane_depth}; "
                    f"potential_nodes=up_to_{plan.worker_count or 0}; "
                    "placement=scheduler-controlled"
                )
                if plan.version >= 6:
                    policy = plan.execution_policy.worker_pool
                    rendered += (
                        "\nRequested scale: "
                        f"workers={plan.requested_workers}, "
                        "slots_per_worker="
                        f"{plan.requested_task_slots_per_worker}; "
                        "target ceilings: "
                        f"workers={policy.max_workers}, "
                        f"slots_per_worker={policy.max_slot_count}, "
                        f"active_tasks={plan.execution_policy.max_active_tasks}"
                    )
                    if plan.version >= 7:
                        rendered += (
                            ", memory_per_worker="
                            f"{plan.execution_policy.max_memory_per_worker}"
                        )
        if plan.array_mapping:
            mapping = _human_sequence(
                tuple(
                    f"{item.array_index}={item.task_id}/seed={item.seed}"
                    for item in plan.array_mapping
                )
            )
            rendered = f"{rendered}\nArray mapping: {mapping}"
        if plan.preparation is not None:
            preparation = plan.preparation
            image = preparation.recipe.image
            image_identity = (
                (
                    f"sha256:{image.sha256}"
                    if image.sha256 is not None
                    else f"UNPINNED:{image.name}"
                )
                if type(image) is PreparationImage
                else f"definition:{image.path}"
                if type(image) is PreparationImageDefinition
                else "unsupported"
            )
            rendered += (
                "\nPreparation: "
                f"{preparation.source_mode}, image {image_identity}, "
                f"requested_location={preparation.requested_location}, "
                f"selected_location={preparation.selected_location}, "
                f"offline={preparation.offline}, rebuild={preparation.rebuild}, "
                f"rebuild_image={preparation.rebuild_image}"
            )
            if type(image) is PreparationImage and image.sha256 is None:
                rendered += (
                    "\nWARNING: prebuilt image is unpinned; Rundra will trust an "
                    "existing file, measure its SHA-256, and record that digest. "
                    "Registry pulls are disabled for unpinned images."
                )
        if plan.target.partition_policy is not None:
            partition = plan.units[0].resources.native.get("slurm", {}).get("partition")
            route = next(
                (
                    item
                    for item in plan.target.partition_policy.routes
                    if item.partition == partition
                ),
                None,
            )
            if route is not None:
                rendered += (
                    "\nPartition route: "
                    f"{route.name} ({route.resource_class}, {route.partition}, "
                    f"limit={int(route.max_walltime.total_seconds())}s)"
                )
        if plan.target.execution_storage is not None:
            storage = plan.target.execution_storage
            rendered += (
                "\nExecution storage: allocation-local Slurm scratch; "
                f"cpu={storage.cpu_environment}, gpu={storage.gpu_environment}, "
                "source/config/image staged per allocation, "
                "outputs copied back after each task"
            )
        return _with_launch(rendered, value.launch)
    if isinstance(value, TargetsValue):
        lines = ["Configured targets:"]
        lines.extend(
            f"  {name}: {value.targets[name].transport.kind} / "
            f"{value.targets[name].scheduler.kind}"
            for name in sorted(value.targets)
        )
        return "\n".join(lines)
    if isinstance(value, DoctorValue):
        subject = (
            "installation" if value.target is None else f"target {value.target.name}"
        )
        lines = [f"Doctor for {subject} ({value.mode}):"]
        lines.extend(
            f"  [{check.status.upper()}] {check.name}: {check.message}"
            for check in value.checks
        )
        lines.append(f"Ready: {'yes' if value.ready else 'no'}")
        lines.append(f"Verification complete: {'yes' if value.complete else 'no'}")
        if value.actions:
            lines.append("Next actions:")
            lines.extend(
                f"  {action.code}: {action.message}" for action in value.actions
            )
        if value.agent_config is not None:
            lines.extend(
                (
                    "Codex permission profile (generated, not applied):",
                    value.agent_config.rstrip(),
                )
            )
        return "\n".join(lines)
    if isinstance(value, RunValue):
        seed_line = _human_run_dimensions(value)
        rendered = (
            f"Run: {value.run_id}\n{seed_line}\n"
            f"State: {value.record.run.state.value}\n"
            f"Retrieval: {value.record.run.retrieval_state.value}\n"
            f"Target: {value.record.run.target.name}"
        )
        preparation_record = value.record.preparation
        if preparation_record is not None:
            rendered += (
                "\nPreparation scheduler job: "
                f"{preparation_record.builder_scheduler_id or '-'}"
            )
        rendered += "\nScientific scheduler jobs: " + (
            ", ".join(value.record.scheduler_job_ids)
            if value.record.scheduler_job_ids
            else "-"
        )
        if preparation_record is not None and preparation_record.image_action in {
            "resolve_unpinned_in_preparation_job",
            "trust_unpinned_existing_image",
        }:
            rendered += (
                "\nWARNING: this Run trusts an unpinned existing image; "
                "the measured digest is preserved in Run provenance."
            )
        if value.record.run.state not in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }:
            rendered += f"\nNext: {_lifecycle_command('wait', value)}"
        elif value.record.run.retrieval_state is not RetrievalState.SUCCEEDED:
            rendered += f"\nNext: rundr fetch {shlex.quote(str(value.run_id))}"
        return _with_launch(rendered, value.launch)
    if isinstance(value, SubmissionRecoveryValue):
        record = value.record
        return (
            f"Run: {record.run.id}\n"
            f"Submission: {value.action}\n"
            f"State: {record.run.state.value}\n"
            "Scheduler jobs: " + ", ".join(record.scheduler_job_ids)
        )
    if isinstance(value, StatusValue):
        counts = ", ".join(
            f"{state.lower()}={count}"
            for state, count in sorted(value.task_counts.items())
        )
        summary = (
            f"Run: {value.run_id}\nState: {value.state.value}\n"
            f"Retrieval: {value.retrieval_state.value}\nTasks: {counts}"
        )
        if value.preparation is not None:
            preparation_status = value.preparation
            summary += (
                "\nPreparation: "
                f"{preparation_status.state or '-'} "
                f"native={preparation_status.native_state or '-'} "
                f"job={preparation_status.scheduler_id or '-'} "
                f"location={preparation_status.location}"
            )
        if value.worker_count is not None and value.task_slots_per_worker is not None:
            summary += (
                f"\nWorker pool: workers={value.worker_count}, "
                f"slots_per_worker={value.task_slots_per_worker}, "
                "concurrent_capacity="
                f"{value.worker_count * value.task_slots_per_worker}"
            )
        if not value.task_details:
            return summary
        preview = _bounded_preview(value.task_details)
        details = "\n".join(
            "  "
            f"{task.task_id} seed={task.seed} state={task.state.value} "
            f"{_human_status_parameter(task)}"
            f"retrieval={task.retrieval_state.value} "
            f"native={task.native_state or '-'} exit="
            f"{task.exit_code if task.exit_code is not None else '-'}"
            for task in preview
        )
        omitted = len(value.task_details) - len(preview)
        if omitted:
            details += f"\n  ... {omitted} additional Task(s); use --json or tasks"
        return f"{summary}\nTask details:\n{details}"
    if isinstance(value, WaitValue):
        status = value.status
        rendered = (
            f"Run: {status.run_id}\nState: {status.state.value}\n"
            f"Terminal: {'yes' if value.terminal else 'no'}\n"
            f"Waited: {value.elapsed_seconds:.1f}s"
        )
        if value.timed_out:
            rendered += f"\nNext: rundr wait {shlex.quote(str(status.run_id))}"
        elif status.retrieval_state is not RetrievalState.SUCCEEDED:
            rendered += f"\nNext: rundr fetch {shlex.quote(str(status.run_id))}"
        return rendered
    if isinstance(value, AwaitRunsValue):
        terminal_states = {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
        terminal = sum(item.state in terminal_states for item in value.statuses)
        lines = [
            f"Runs: {len(value.statuses)}",
            f"Condition: {value.until}",
            f"Terminal: {terminal}/{len(value.statuses)}",
            f"Condition met: {'yes' if value.condition_met else 'no'}",
            f"Timed out: {'yes' if value.timed_out else 'no'}",
            f"Waited: {value.elapsed_seconds:.1f}s",
        ]
        lines.extend(f"  {item.run_id}: {item.state.value}" for item in value.statuses)
        return "\n".join(lines)
    if isinstance(value, CancelValue):
        status = value.status
        return f"Run: {status.run_id}\nState after cancellation: {status.state.value}"
    if isinstance(value, ListRunsValue):
        page = (
            f"Runs: offset={value.offset} returned={len(value.runs)} "
            f"total={value.total}"
        )
        if not value.runs:
            return page
        return (
            page
            + "\n"
            + "\n".join(
                f"  {run.run_id}: {run.state.value} ({run.experiment} on {run.target})"
                for run in value.runs
            )
        )
    if isinstance(value, LogsValue):
        return (
            f"Run: {value.run_id}\nTask: {value.task_id}\n"
            f"--- stdout ---\n{value.stdout}"
            f"--- stderr ---\n{value.stderr}"
        ).rstrip()
    if isinstance(value, PreparationLogsValue):
        return (
            f"Run: {value.run_id}\nPreparation job: {value.scheduler_id or '-'}\n"
            f"--- stdout ---\n{value.stdout}"
            f"--- stderr ---\n{value.stderr}"
        ).rstrip()
    if isinstance(value, FetchValue):
        selected = _human_sequence(value.task_ids) if value.task_ids else "all"
        return (
            f"Fetched {value.artifact_total} artifact(s) for {value.run_id} "
            f"to {value.destination}\nTasks: {selected}\n"
            f"Retrieval: {value.retrieval_state.value}"
        )
    if isinstance(value, InspectValue):
        record = value.record
        rendered = (
            f"Run: {record.run.id}\nExperiment: {record.run.experiment_name}\n"
            f"State: {record.run.state.value}\n"
            f"Retrieval: {record.run.retrieval_state.value}\n"
            f"Artifacts: {len(record.artifacts)}"
        )
        if value.retention is not None:
            rendered += f"\nPurge attempts: {len(value.retention.attempts)}"
        return rendered
    if isinstance(value, PurgeValue):
        return (
            f"Run: {value.run_id}\nScope: {value.scope.value}\n"
            f"Backend: {value.result.backend}\nOutcome: {value.result.outcome.value}\n"
            f"Path: {value.result.path}\nReceipt: {value.receipt_path}"
        )
    if isinstance(value, TasksValue):
        header = (
            f"Run: {value.run_id}\nTasks: offset={value.offset} "
            f"returned={len(value.tasks)} total={value.total}"
        )
        details = "\n".join(
            f"  {item.coordinate.task_id} seed={item.coordinate.seed} "
            f"parameter_set={item.coordinate.parameter_set_ordinal} "
            f"state={item.execution_state.value} "
            f"retrieval={item.retrieval_state.value}"
            for item in value.tasks
        )
        return header if not details else f"{header}\n{details}"
    if isinstance(value, ArtifactsValue):
        return (
            f"Run: {value.run_id}\nArtifacts: offset={value.offset} "
            f"returned={len(value.artifacts)} total={value.total}\n"
            f"Next offset: {value.next_offset if value.next_offset is not None else '-'}"
        )
    if isinstance(value, AgentGuideValue):
        if value.action in {"printed", "topic", "topics"}:
            return value.content.rstrip("\n")
        return f"Agent guide {value.action}: {value.path}"
    raise TypeError(f"No human renderer for {type(value).__name__}")


def _plan_document(plan: ExecutionPlan) -> dict[str, Any]:
    resources = plan.units[0].resources
    document: dict[str, Any] = {
        "version": plan.version,
        "experiment": plan.experiment_name,
        "target": _target_document(plan.target, include_capabilities=plan.version >= 8),
        "strategy": plan.strategy,
        "staging_backend": plan.staging_backend,
        "resources": _resources_document(resources),
        "native_options": {
            backend: dict(options)
            for backend, options in sorted(resources.native.items())
        },
        "staging": _staging_document(plan),
        "validation": {
            "target_capabilities": "validated",
            "resources": "validated",
            "native_options": "validated",
        },
        "safety": {
            "contacts_target": False,
            "creates_workspace": False,
            "creates_run": False,
            "submits": False,
        },
        "groups": [
            {"task_ids": [str(task_id) for task_id in group.task_ids]}
            for group in plan.groups
        ],
        "array_mapping": [
            {
                "task_id": str(item.task_id),
                "seed": item.seed,
                "array_index": item.array_index,
            }
            for item in plan.array_mapping
        ],
        "units": [
            {
                "task_id": str(unit.task_id),
                "seed": unit.seed,
                "config": str(unit.config.source),
                "command": _command_document(unit.command),
                "resources": _resources_document(unit.resources),
                **(
                    {
                        "parameter_set": {
                            "id": unit.parameter_set.id,
                            "choices": dict(unit.parameter_set.choices),
                        },
                        "config_sha256": sha256(
                            unit.config.content.encode("utf-8")
                        ).hexdigest(),
                    }
                    if unit.parameter_set is not None
                    else {}
                ),
            }
            for unit in plan.units
        ],
    }
    if plan.preparation is not None:
        document["preparation"] = _preparation_document(plan.preparation)
    if plan.version in {4, 5, 6, 7, 8}:
        assert plan.task_space is not None
        assert plan.execution_policy is not None
        policy = plan.execution_policy
        document["task_space"] = {
            "parameter_set_count": plan.task_space.parameter_set_count,
            "seeds": {
                "start": plan.task_space.seeds.start,
                "stop": plan.task_space.seeds.stop,
                "step": plan.task_space.seeds.step,
            },
            "task_count": plan.task_space.task_count,
            "preview_count": len(plan.units),
        }
        scheduling: dict[str, Any] = {
            "scheduler_batches": plan.scheduler_batches,
            "worker_count": plan.worker_count,
            "max_active_tasks": policy.max_active_tasks,
            "max_concurrent_jobs": policy.max_concurrent_jobs,
            "max_array_size": policy.max_array_size,
        }
        document["scheduling"] = scheduling
        if plan.version in {5, 6, 7, 8}:
            assert plan.worker_resources is not None
            scheduling.update(
                {
                    "task_slots_per_worker": plan.task_slots_per_worker,
                    "concurrent_task_capacity": plan.concurrent_task_capacity,
                    "max_lane_depth": plan.max_lane_depth,
                    "worker_resources": _resources_document(plan.worker_resources),
                }
            )
            if plan.version >= 6:
                scheduling.update(
                    {
                        "requested_workers": plan.requested_workers,
                        "requested_task_slots_per_worker": (
                            plan.requested_task_slots_per_worker
                        ),
                        "max_workers": policy.worker_pool.max_workers,
                        "max_task_slots_per_worker": (
                            policy.worker_pool.max_slot_count
                        ),
                        "placement": "scheduler-controlled",
                    }
                )
                if plan.version >= 7:
                    scheduling["max_memory_per_worker"] = policy.max_memory_per_worker
        document["retrieval_policy"] = plan.retrieval_policy
        safety = document["safety"]
        assert isinstance(safety, dict)
        safety.update(
            {
                "hard_task_limit": policy.hard_task_limit,
                "confirmation_threshold": policy.confirmation_threshold,
            }
        )
    return document


def _source_snapshot_document(snapshot: object) -> dict[str, Any]:
    from rundra.sync import SourceSnapshotPreview

    if type(snapshot) is not SourceSnapshotPreview:
        raise TypeError("source snapshot must be SourceSnapshotPreview")
    return {
        "source_root": str(snapshot.source_root),
        "file_count": snapshot.file_count,
        "size_bytes": snapshot.size_bytes,
        "exact": snapshot.exact,
        "unreadable_entries": snapshot.unreadable_entries,
        "symlink_entries": snapshot.symlink_entries,
        "excluded_patterns": list(snapshot.excluded_patterns),
        "largest_entries": [
            {
                "path": str(item.path),
                "file_count": item.file_count,
                "size_bytes": item.size_bytes,
            }
            for item in snapshot.largest_entries
        ],
    }


def _human_bytes(size_bytes: int) -> str:
    value = float(size_bytes)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{int(value)} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def _preparation_document(plan: PreparationPlan) -> dict[str, Any]:
    recipe = plan.recipe
    build = recipe.build
    source = recipe.source
    image = recipe.image
    return {
        "source": {
            "mode": plan.source_mode,
            "root": None if plan.source_root is None else str(plan.source_root),
            "git": (
                {
                    "url": source.url,
                    "revision": source.revision,
                    "recipe_identity": source_recipe_identity(source),
                }
                if type(source) is PreparationSourceGit
                else None
            ),
        },
        "image": (
            {
                "kind": "prebuilt",
                "name": str(image.name),
                "uri": image.uri,
                "sha256": image.sha256,
            }
            if type(image) is PreparationImage
            else {
                "kind": "definition",
                "name": str(image.name),
                "path": str(image.path),
                "resources": _resources_document(image.resources),
            }
            if type(image) is PreparationImageDefinition
            else {"kind": "unsupported", "name": str(image.name)}
        ),
        "build": (
            None
            if build is None
            else {
                "argv": list(build.argv),
                "outputs": [
                    {"path": str(item.path), "executable": item.executable}
                    for item in build.outputs
                ],
                "cache_scope": build.cache_scope,
                "resources": _resources_document(build.resources),
            }
        ),
        "strategy": {
            "requested_location": plan.requested_location,
            "selected_location": plan.selected_location,
            "offline": plan.offline,
            "rebuild": plan.rebuild,
            "rebuild_image": plan.rebuild_image,
            "possible_actions": list(plan.possible_actions),
            "cache_hits_known": False,
        },
        "safety": {
            "contacts_target": False,
            "fetches_git": False,
            "pulls_image": False,
            "builds": False,
        },
    }


def _result_format_version(value: object) -> int:
    if isinstance(
        value,
        (
            CampaignValidationValue,
            CampaignPlanValue,
            CampaignDoctorValue,
            CampaignRunValue,
            CampaignSubmitValue,
            CampaignStatusValue,
            CampaignWaitValue,
            CampaignFetchValue,
            CampaignCancelValue,
            CampaignInspectValue,
            CampaignTasksValue,
            CampaignArtifactsValue,
            CampaignLogsValue,
            CampaignPurgeValue,
            CampaignListValue,
            CampaignAndRunListValue,
        ),
    ):
        return value.format_version
    if isinstance(value, PlanValue):
        return value.format_version
    if isinstance(value, RunValue):
        return 5 if len(value.record.run.tasks) >= 1000 else value.record.format_version
    if isinstance(value, InspectValue):
        return 5 if value.retention is not None else value.record.format_version
    if isinstance(value, StatusValue) and len(value.task_details) >= 1000:
        return 5
    if isinstance(value, StatusValue) and value.preparation is not None:
        return value.format_version
    if isinstance(value, SubmissionRecoveryValue):
        return value.record.format_version
    if isinstance(value, StatusValue):
        return value.format_version
    if isinstance(value, WaitValue):
        return (
            5 if len(value.status.task_details) >= 1000 else value.status.format_version
        )
    if isinstance(value, AwaitRunsValue):
        return 1
    if isinstance(value, CancelValue) and value.status.preparation is not None:
        return value.status.format_version
    if isinstance(value, CancelValue):
        return value.status.format_version
    if isinstance(value, ListRunsValue):
        return value.format_version
    if isinstance(value, PreparationLogsValue):
        return value.format_version
    if isinstance(value, (LogsValue, FetchValue, PurgeValue, ArtifactsValue)):
        return value.format_version
    if isinstance(value, TasksValue):
        return value.format_version
    if isinstance(value, DoctorValue):
        return value.format_version
    if isinstance(value, TargetsValue):
        return value.format_version
    if (
        isinstance(value, ValidationValue)
        and value.project is not None
        and value.project.version >= 2
    ):
        return 2
    return _FORMAT_VERSION


def _campaign_plan_document(value: CampaignPlanValue) -> dict[str, Any]:
    document = {
        "name": value.name,
        "source": str(value.definition.source),
        "experiment": str(value.experiment_source),
        "on_submit_failure": value.on_submit_failure.value,
        "allow_duplicate_tasks": value.definition.allow_duplicate_tasks,
        "total_tasks": value.total_tasks,
        "total_concurrent_task_capacity": value.total_concurrent_task_capacity,
        "warnings": list(value.warnings),
        "launches": [
            {
                "name": item.name,
                "target": item.target,
                "destination": str(item.destination),
                "task_count": item.task_count,
                "concurrent_task_capacity": item.concurrent_task_capacity,
                "plan": _plan_document(item.plan.plan),
                "source_snapshot": (
                    None
                    if item.plan.source_snapshot is None
                    else _source_snapshot_document(item.plan.source_snapshot)
                ),
                "launch": (
                    None
                    if item.plan.launch is None
                    else _launch_document(item.plan.launch)
                ),
            }
            for item in value.launches
        ],
    }
    if value.placement is not None:
        document["placement"] = {
            "policy": value.placement.policy,
            "strategy": value.placement.strategy,
            "observed_at": value.placement.observed_at.isoformat(),
            "selected_targets": list(value.placement.selected_targets),
            "targets": [
                {
                    "target": item.target,
                    "accepted": item.accepted,
                    "reason": item.reason,
                    "partition": item.partition,
                    "utilization_percent": item.utilization_percent,
                    "idle_cpus": item.idle_cpus,
                    "planned_capacity": item.planned_capacity,
                    "usable_capacity": item.usable_capacity,
                    "assigned_seed_start": item.assigned_seed_start,
                    "assigned_seed_stop": item.assigned_seed_stop,
                }
                for item in value.placement.targets
            ],
        }
    return document


def _campaign_status_document(value: CampaignStatusValue) -> dict[str, Any]:
    return {
        "campaign_id": str(value.record.id),
        "name": value.record.name,
        "state": value.state,
        "terminal": value.terminal,
        "task_counts": value.task_counts,
        "launches": [
            {
                "name": item.name,
                "run_id": str(item.run_id),
                "submission_state": item.submission_state.value,
                "status": (
                    None if item.status is None else _status_document(item.status)
                ),
            }
            for item in value.launches
        ],
    }


def _campaign_fetch_document(value: CampaignFetchValue) -> dict[str, Any]:
    launches = []
    for item in value.launches:
        fetch = item.value
        if not isinstance(fetch, FetchValue):
            raise TypeError("Campaign fetch launch has an invalid value")
        launches.append(
            {
                "name": item.name,
                "run_id": str(fetch.run_id),
                "destination": str(fetch.destination),
                "retrieval_state": fetch.retrieval_state.value,
                "artifact_total": fetch.artifact_total,
                "artifacts_included": fetch.artifacts_included,
                "artifacts": [_artifact_document(value) for value in fetch.artifacts],
            }
        )
    return {
        "campaign_id": str(value.record.id),
        "destination": None if value.destination is None else str(value.destination),
        "launches": launches,
    }


def _campaign_task_document(
    selector: str, launch: str, run_id: object, value: object
) -> dict[str, Any]:
    task: Any = value
    return {
        "selector": selector,
        "launch": launch,
        "run_id": str(run_id),
        "task_id": str(task.coordinate.task_id),
        "ordinal": task.coordinate.ordinal,
        "seed": task.coordinate.seed,
        "state": task.execution_state.value,
        "retrieval_state": task.retrieval_state.value,
        "scheduler_id": task.scheduler_id,
        "native_state": task.native_state,
        "exit_code": task.exit_code,
        "attempt": task.attempt,
    }


def _error_hint(code: str, details: Mapping[str, object]) -> str | None:
    run_id = details.get("run_id")
    hints = {
        "CONFIG_NOT_FOUND": "check the reported path, then run rundr validate EXPERIMENT",
        "CAPABILITY_CHECK_FAILED": "run rundr doctor EXPERIMENT --connect",
        "SCHEDULER_SUBMISSION_FAILED": "review rundr plan output and target resource limits",
        "SCHEDULER_QUERY_FAILED": (
            f"retry rundr status {run_id}" if run_id is not None else "retry status"
        ),
        "RESULT_RETRIEVAL_FAILED": (
            f"retry rundr fetch {run_id}"
            if run_id is not None
            else "retry fetch and inspect the reported destination"
        ),
        "RUN_STORE_CONFLICT": "retry the same lifecycle command",
        "TASK_CONFIRMATION_REQUIRED": (
            f"review the plan, then pass --confirm-tasks {details.get('task_count')}"
        ),
    }
    return hints.get(code)


def _lifecycle_command(command: str, value: RunValue) -> str:
    argv = ["rundr", command, str(value.run_id)]
    if value.launch is not None:
        data_dir = value.launch.values.get("data_dir")
        if type(data_dir) is str:
            argv.extend(("--data-dir", data_dir))
    return shlex.join(argv)


def _staging_document(plan: ExecutionPlan) -> dict[str, Any]:
    local = plan.staging_backend == "local"
    return {
        "backend": plan.staging_backend,
        "workspace_root": str(plan.target.workspace),
        "source": "local_copy" if local else "rsync_upload",
        "effective_config": "local_copy" if local else "rsync_upload",
        "inputs_sealed": True,
        "results": "local_copy" if local else "rsync_download",
    }


def _human_task_dimensions(
    units: tuple[ExecutionUnit, ...], fallback_seeds: str
) -> str:
    lines = _human_parameter_lines(units)
    return (
        f"Parameter sets:\n{lines}" if lines is not None else f"Seeds: {fallback_seeds}"
    )


def _human_run_dimensions(value: RunValue) -> str:
    lines = _human_parameter_lines(value.record.run.tasks)
    if lines is not None:
        return f"Parameter sets:\n{lines}"
    return (
        f"Seed: {value.seed}"
        if len(value.seeds) == 1
        else f"Seeds: {_human_sequence(value.seeds)}"
    )


def _human_parameter_lines(
    items: Sequence[ExecutionUnit | Task],
) -> str | None:
    if not items or any(item.parameter_set is None for item in items):
        return None
    grouped: dict[str, tuple[Mapping[str, object], list[int]]] = {}
    for item in items:
        assert item.parameter_set is not None
        entry = grouped.setdefault(
            item.parameter_set.id, (item.parameter_set.choices, [])
        )
        entry[1].append(item.seed)
    return "\n".join(
        f"  {identifier} ({_human_choices(choices)}): seeds="
        f"{_human_sequence(tuple(seeds), separator=',')}"
        for identifier, (choices, seeds) in grouped.items()
    )


def _human_sequence(
    values: Sequence[object], *, separator: str = ", ", limit: int = 12
) -> str:
    normalized = tuple(str(value) for value in values)
    if len(normalized) <= limit:
        return separator.join(normalized)
    shown = (*normalized[:8], *normalized[-2:])
    return (
        separator.join((*shown[:8], f"... {len(normalized) - 10} omitted", *shown[8:]))
        + f" ({len(normalized)} total)"
    )


def _bounded_preview(values: Sequence[TaskStatusValue]) -> tuple[TaskStatusValue, ...]:
    normalized = tuple(values)
    if len(normalized) <= 20:
        return normalized
    return (*normalized[:12], *normalized[-3:])


def _human_choices(choices: Mapping[str, object]) -> str:
    return ", ".join(f"{name}={value}" for name, value in sorted(choices.items()))


def _human_status_parameter(task: TaskStatusValue) -> str:
    if task.parameter_set is None:
        return ""
    return (
        f"parameter_set={task.parameter_set.id} "
        f"choices={_human_choices(task.parameter_set.choices)} "
    )


def _human_resources(resources: ResourceRequest) -> str:
    memory = "unset" if resources.memory_bytes is None else str(resources.memory_bytes)
    walltime = (
        "unset"
        if resources.walltime is None
        else str(int(resources.walltime.total_seconds()))
    )
    return (
        f"nodes={resources.nodes}, tasks={resources.tasks}, "
        f"cpus/task={resources.cpus_per_task}, gpus/task={resources.gpus_per_task}, "
        f"memory_bytes={memory}, walltime_seconds={walltime}"
    )


def _human_native_options(resources: ResourceRequest) -> str:
    rendered = [
        f"{backend}.{name}={value}"
        for backend, options in sorted(resources.native.items())
        for name, value in sorted(options.items())
    ]
    return ", ".join(rendered) if rendered else "none"


def _human_staging(plan: ExecutionPlan) -> str:
    staging = _staging_document(plan)
    return (
        f"{staging['backend']} ({staging['source']} source/config; "
        f"sealed inputs; {staging['results']} results; "
        f"workspace root {staging['workspace_root']})"
    )


def _launch_document(value: LaunchResolutionValue) -> dict[str, Any]:
    return {
        "profile": value.profile,
        "values": dict(value.values),
        "sources": dict(value.sources),
    }


def _with_launch(rendered: str, launch: LaunchResolutionValue | None) -> str:
    if launch is None:
        return rendered
    config = launch.values.get("config")
    config_source = launch.sources.get("config")
    source_label = (
        "adjacent default"
        if config_source == "built_in"
        else "CLI"
        if config_source == "cli"
        else "user defaults"
        if config_source == "user"
        else "project profile"
        if config_source is not None and config_source.startswith("project_profile:")
        else "project defaults"
        if config_source == "project"
        else config_source or "unknown"
    )
    lines = [rendered]
    if config is not None:
        lines.append(f"Config: {config} ({source_label})")
    lines.append("Launch resolution:")
    lines.extend(
        f"  {name}={launch.values[name]} ({launch.sources[name]})"
        for name in sorted(launch.values)
    )
    if launch.profile is not None:
        lines.append(f"  profile={launch.profile}")
    return "\n".join(lines)


def _target_document(
    target: Target, *, include_capabilities: bool = False
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "name": target.name,
        "transport": _backend_document(target.transport.kind, target.transport.options),
        "scheduler": (
            {
                **_backend_document(target.scheduler.kind, target.scheduler.options),
                "capabilities": scheduler_capabilities_document(target.scheduler.kind),
            }
            if include_capabilities
            else _backend_document(target.scheduler.kind, target.scheduler.options)
        ),
        "staging": _backend_document(target.staging.kind, target.staging.options),
        "container": _backend_document(target.container.kind, target.container.options),
        "workspace": str(target.workspace),
    }
    if target.execution_storage is not None:
        policy = target.execution_storage
        document["execution_storage"] = {
            "type": "slurm_scratch",
            "cpu_environment": policy.cpu_environment,
            "gpu_environment": policy.gpu_environment,
            "stage_image": policy.stage_image,
            "copy_back": policy.copy_back,
        }
    if target.partition_policy is not None:
        document["partition_routes"] = [
            {
                "name": route.name,
                "partition": route.partition,
                "resource_class": route.resource_class,
                "max_walltime_seconds": int(route.max_walltime.total_seconds()),
            }
            for route in target.partition_policy.routes
        ]
    return document


def _backend_document(kind: str, options: Mapping[str, object]) -> dict[str, Any]:
    return {"type": kind, "options": dict(options)}


def _command_document(command: Command) -> dict[str, Any]:
    return {
        "argv": list(command.argv),
        "environment": dict(command.environment),
        "working_directory": (
            str(command.working_directory)
            if command.working_directory is not None
            else None
        ),
    }


def _resources_document(resources: ResourceRequest) -> dict[str, Any]:
    return {
        "nodes": resources.nodes,
        "tasks": resources.tasks,
        "cpus_per_task": resources.cpus_per_task,
        "gpus_per_task": resources.gpus_per_task,
        "memory_bytes": resources.memory_bytes,
        "walltime_seconds": _duration_seconds(resources.walltime),
        "native": {
            backend: dict(options)
            for backend, options in sorted(resources.native.items())
        },
    }


def _duration_seconds(value: timedelta | None) -> int | None:
    return int(value.total_seconds()) if value is not None else None


def _run_value_document(value: RunValue) -> dict[str, Any]:
    record = value.record
    job_roles = _scheduler_job_roles_document(record)
    if record.task_space is not None:
        task_space = record.task_space
        return {
            "run_id": str(record.run.id),
            "experiment": record.run.experiment_name,
            "target": record.run.target.name,
            "state": record.run.state.value,
            "retrieval_state": record.run.retrieval_state.value,
            "task_space": {
                "task_count": task_space.task_count,
                "parameter_set_count": task_space.parameter_set_count,
                "seeds": {
                    "start": task_space.seeds.start,
                    "stop": task_space.seeds.stop,
                    "step": task_space.seeds.step,
                },
            },
            "tasks": {"total": task_space.task_count, "details_included": False},
            "scheduler_job_ids": list(record.scheduler_job_ids),
            **job_roles,
            "scheduler": record.run.target.scheduler.kind,
            "artifacts": [_artifact_document(item) for item in record.artifacts],
        }
    if len(record.run.tasks) >= 1000:
        seeds = tuple(dict.fromkeys(task.seed for task in record.run.tasks))
        parameters = tuple(
            dict.fromkeys(
                None if task.parameter_set is None else task.parameter_set.id
                for task in record.run.tasks
            )
        )
        contiguous = all(
            right == left + 1 for left, right in zip(seeds, seeds[1:], strict=False)
        )
        return {
            "run_id": str(record.run.id),
            "experiment": record.run.experiment_name,
            "target": record.run.target.name,
            "state": record.run.state.value,
            "retrieval_state": record.run.retrieval_state.value,
            "task_space": {
                "task_count": len(record.run.tasks),
                "parameter_set_count": len(parameters),
                "seeds": (
                    {"start": seeds[0], "stop": seeds[-1], "step": 1}
                    if contiguous
                    else {"count": len(seeds)}
                ),
            },
            "tasks": {"total": len(record.run.tasks), "details_included": False},
            "scheduler_job_ids": list(record.scheduler_job_ids),
            **job_roles,
            "scheduler": record.run.target.scheduler.kind,
            "artifacts": [_artifact_document(item) for item in record.artifacts],
        }
    return {
        "run_id": str(record.run.id),
        "experiment": record.run.experiment_name,
        "target": record.run.target.name,
        "seed": value.seeds[0] if len(value.seeds) == 1 else None,
        "seeds": list(value.seeds),
        "state": record.run.state.value,
        "retrieval_state": record.run.retrieval_state.value,
        "tasks": len(record.run.tasks),
        "scheduler_job_ids": list(record.scheduler_job_ids),
        **job_roles,
        "task_exit_codes": {
            str(task_id): exit_code
            for task_id, exit_code in sorted(
                record.task_exit_codes.items(), key=lambda item: item[0].value
            )
        },
        "scheduler": record.run.target.scheduler.kind,
        "artifacts": [_artifact_document(item) for item in record.artifacts],
    }


def _status_document(value: StatusValue) -> dict[str, Any]:
    compact = not value.task_details_included or len(value.task_details) >= 1000
    document: dict[str, Any] = {
        "run_id": str(value.run_id),
        "experiment": value.experiment,
        "target": value.target,
        "state": value.state.value,
        "retrieval_state": value.retrieval_state.value,
        "native_state": value.native_state,
        "scheduler_job_ids": list(value.scheduler_job_ids),
        "preparation_scheduler_job_id": (
            None if value.preparation is None else value.preparation.scheduler_id
        ),
        "scientific_scheduler_job_ids": list(value.scheduler_job_ids),
        "task_details": []
        if compact
        else [
            {
                "task_id": str(task.task_id),
                "seed": task.seed,
                "state": task.state.value,
                "retrieval_state": task.retrieval_state.value,
                "native_id": task.native_id,
                "native_state": task.native_state,
                "exit_code": task.exit_code,
                **(
                    {
                        "parameter_set": {
                            "id": task.parameter_set.id,
                            "choices": dict(task.parameter_set.choices),
                        }
                    }
                    if task.parameter_set is not None
                    else {}
                ),
            }
            for task in value.task_details
        ],
        "tasks": {
            "total": sum(value.task_counts.values()),
            **{
                state.lower(): count
                for state, count in sorted(value.task_counts.items())
            },
        },
    }
    if compact:
        document["task_details_included"] = False
    if value.worker_count is not None:
        slots = value.task_slots_per_worker or 1
        document["workers"] = {
            "requested": value.worker_count,
            "active": value.active_workers,
            "task_slots_per_worker": slots,
            "concurrent_capacity": value.worker_count * slots,
        }
        document["progress"] = {
            "throughput_tasks_per_second": value.throughput_tasks_per_second,
            "eta_seconds": value.eta_seconds,
        }
    if value.preparation is not None:
        document["preparation"] = {
            "scheduler_id": value.preparation.scheduler_id,
            "state": value.preparation.state,
            "native_state": value.preparation.native_state,
            "location": value.preparation.location,
        }
    return document


def _artifact_document(artifact: Artifact) -> dict[str, Any]:
    return {
        "kind": artifact.kind.value,
        "path": str(artifact.path),
        "task_id": None if artifact.task_id is None else str(artifact.task_id),
        "size_bytes": artifact.size_bytes,
    }


def _scheduler_job_roles_document(record: RunRecord) -> dict[str, Any]:
    return {
        "preparation_scheduler_job_id": (
            None
            if record.preparation is None
            else record.preparation.builder_scheduler_id
        ),
        "scientific_scheduler_job_ids": list(record.scheduler_job_ids),
    }


def _run_record_summary_document(record: RunRecord) -> dict[str, Any]:
    return {
        "run_id": str(record.run.id),
        "experiment": record.run.experiment_name,
        "target": record.run.target.name,
        "state": record.run.state.value,
        "retrieval_state": record.run.retrieval_state.value,
        "native_state": record.native_state,
        "tasks": {"total": len(record.run.tasks), "details_included": False},
        "artifacts": {"total": len(record.artifacts), "details_included": False},
        "scheduler_job_ids": list(record.scheduler_job_ids),
        **_scheduler_job_roles_document(record),
    }
