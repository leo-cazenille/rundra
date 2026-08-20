from __future__ import annotations

import json
import shlex
from collections.abc import Mapping, Sequence
from datetime import timedelta
from hashlib import sha256
from typing import Any

from rundra.cli.agent_guide import AgentGuideValue
from rundra.cli.capability_doctor import DoctorValue
from rundra.cli.operations import (
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
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.models import ExecutionPlan, ExecutionUnit
from rundra.persistence import receipt_document, record_to_dict
from rundra.results import OperationResult

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
    if isinstance(value, ValidationValue):
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
        if value.launch is not None:
            document["launch"] = _launch_document(value.launch)
    elif isinstance(value, TargetsValue):
        document["source"] = str(value.source)
        document["targets"] = [
            _target_document(value.targets[name]) for name in sorted(value.targets)
        ]
    elif isinstance(value, DoctorValue):
        document["doctor"] = {
            "mode": value.mode,
            "source": str(value.source),
            "target": None if value.target is None else value.target.name,
            "connected": value.connected,
            "scheduler_probed": value.scheduler_probed,
            "ready": value.ready,
            "complete": value.complete,
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
            "artifacts": [_artifact_document(item) for item in value.artifacts],
        }
    elif isinstance(value, InspectValue):
        document["record"] = record_to_dict(value.record)
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
        if plan.version in {4, 5, 6, 7}:
            assert plan.execution_policy is not None
            rendered += (
                f"\nScheduling: batches={plan.scheduler_batches}, "
                f"workers={plan.worker_count or 0}, "
                f"max_active={plan.execution_policy.max_active_tasks}, "
                f"retrieval={plan.retrieval_policy}, preview={len(plan.units)}"
            )
            if plan.version in {5, 6, 7}:
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
                f"sha256:{image.sha256}"
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
            f"Fetched {len(value.artifacts)} artifact(s) for {value.run_id} "
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
    if isinstance(value, AgentGuideValue):
        if value.action == "printed":
            return value.content.rstrip("\n")
        return f"Agent guide {value.action}: {value.path}"
    raise TypeError(f"No human renderer for {type(value).__name__}")


def _plan_document(plan: ExecutionPlan) -> dict[str, Any]:
    resources = plan.units[0].resources
    document = {
        "version": plan.version,
        "experiment": plan.experiment_name,
        "target": _target_document(plan.target),
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
    if plan.version in {4, 5, 6, 7}:
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
        if plan.version in {5, 6, 7}:
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
    if isinstance(value, PlanValue):
        return value.plan.version
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
    if isinstance(value, CancelValue) and value.status.preparation is not None:
        return value.status.format_version
    if isinstance(value, CancelValue):
        return value.status.format_version
    if isinstance(value, ListRunsValue):
        return value.format_version
    if isinstance(value, PreparationLogsValue):
        return value.format_version
    if isinstance(value, (LogsValue, FetchValue, PurgeValue)):
        return value.format_version
    if isinstance(value, TasksValue):
        return value.format_version
    if isinstance(value, DoctorValue):
        return value.format_version
    if (
        isinstance(value, ValidationValue)
        and value.project is not None
        and value.project.version >= 2
    ):
        return 2
    return _FORMAT_VERSION


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
    lines = [rendered, "Launch resolution:"]
    lines.extend(
        f"  {name}={launch.values[name]} ({launch.sources[name]})"
        for name in sorted(launch.values)
    )
    if launch.profile is not None:
        lines.append(f"  profile={launch.profile}")
    return "\n".join(lines)


def _target_document(target: Target) -> dict[str, Any]:
    return {
        "name": target.name,
        "transport": _backend_document(target.transport.kind, target.transport.options),
        "scheduler": _backend_document(target.scheduler.kind, target.scheduler.options),
        "staging": _backend_document(target.staging.kind, target.staging.options),
        "container": _backend_document(target.container.kind, target.container.options),
        "workspace": str(target.workspace),
    }


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
    compact = len(value.task_details) >= 1000
    document: dict[str, Any] = {
        "run_id": str(value.run_id),
        "experiment": value.experiment,
        "target": value.target,
        "state": value.state.value,
        "retrieval_state": value.retrieval_state.value,
        "native_state": value.native_state,
        "scheduler_job_ids": list(value.scheduler_job_ids),
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
