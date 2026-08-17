from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from hashlib import sha256
from typing import Any

from rundra.cli.doctor import DoctorValue
from rundra.cli.operations import (
    CancelValue,
    FetchValue,
    InspectValue,
    LaunchResolutionValue,
    ListRunsValue,
    LogsValue,
    PlanValue,
    PreparationLogsValue,
    RunValue,
    StatusValue,
    TargetsValue,
    TaskStatusValue,
    ValidationValue,
)
from rundra.domain.models import Artifact, Command, ResourceRequest, Target, Task
from rundra.domain.preparation import PreparationPlan, source_recipe_identity
from rundra.orchestration.models import ExecutionPlan, ExecutionUnit
from rundra.persistence import record_to_dict
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
        if value.project is not None and value.project.version == 2:
            document["project"] = {
                "schema_version": 2,
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
            "source": str(value.source),
            "target": value.target.name,
            "connected": value.connected,
            "ready": value.ready,
            "checks": [
                {
                    "name": check.name,
                    "status": check.status,
                    "message": check.message,
                }
                for check in value.checks
            ],
        }
    elif isinstance(value, RunValue):
        document["run"] = _run_value_document(value)
        if value.launch is not None:
            document["launch"] = _launch_document(value.launch)
    elif isinstance(value, StatusValue):
        document["status"] = _status_document(value)
    elif isinstance(value, CancelValue):
        document["cancel"] = _status_document(value.status)
    elif isinstance(value, ListRunsValue):
        document["runs"] = [_status_document(run) for run in value.runs]
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
    else:
        raise TypeError(f"No public renderer for {type(value).__name__}")
    return document


def render_human(result: OperationResult[Any]) -> str:
    """Render the same operation value for a person."""
    if result.error is not None:
        location = result.error.details.get("source")
        prefix = f"{location}: " if location else ""
        return f"Error [{result.error.code}]: {prefix}{result.error.message}"
    value = result.value
    if isinstance(value, ValidationValue):
        rendered = (
            f"Valid experiment: {value.experiment.name} "
            f"(schema v{value.experiment.version})"
        )
        if value.project is not None and value.project.version == 2:
            rendered += "; project preparation v2 validated"
        return rendered
    if isinstance(value, PlanValue):
        plan = value.plan
        seeds = ", ".join(str(unit.seed) for unit in plan.units)
        resources = plan.units[0].resources
        rendered = (
            f"Plan for {plan.experiment_name} on {plan.target.name}: "
            f"{len(plan.units)} task(s)\n"
            f"{_human_task_dimensions(plan.units, seeds)}\n"
            f"Strategy: {plan.strategy}\n"
            f"Resources: {_human_resources(resources)}\n"
            f"Native options: {_human_native_options(resources)}\n"
            f"Staging: {_human_staging(plan)}\n"
            "Safety: validated offline; no target contact, workspace creation, "
            "Run creation, or submission"
        )
        if plan.array_mapping:
            mapping = ", ".join(
                f"{item.array_index}={item.task_id}/seed={item.seed}"
                for item in plan.array_mapping
            )
            rendered = f"{rendered}\nArray mapping: {mapping}"
        if plan.preparation is not None:
            preparation = plan.preparation
            rendered += (
                "\nPreparation: "
                f"{preparation.source_mode}, image sha256:"
                f"{preparation.recipe.image.sha256}, "
                f"location={preparation.requested_location}, "
                f"offline={preparation.offline}, rebuild={preparation.rebuild}"
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
        lines = [f"Doctor for target {value.target.name}:"]
        lines.extend(
            f"  [{check.status.upper()}] {check.name}: {check.message}"
            for check in value.checks
        )
        lines.append(f"Ready: {'yes' if value.ready else 'no'}")
        return "\n".join(lines)
    if isinstance(value, RunValue):
        seed_line = _human_run_dimensions(value)
        rendered = (
            f"Run: {value.run_id}\n{seed_line}\n"
            f"State: {value.record.run.state.value}\n"
            f"Retrieval: {value.record.run.retrieval_state.value}\n"
            f"Target: {value.record.run.target.name}"
        )
        return _with_launch(rendered, value.launch)
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
        if not value.task_details:
            return summary
        details = "\n".join(
            "  "
            f"{task.task_id} seed={task.seed} state={task.state.value} "
            f"{_human_status_parameter(task)}"
            f"retrieval={task.retrieval_state.value} "
            f"native={task.native_state or '-'} exit="
            f"{task.exit_code if task.exit_code is not None else '-'}"
            for task in value.task_details
        )
        return f"{summary}\nTask details:\n{details}"
    if isinstance(value, CancelValue):
        status = value.status
        return f"Run: {status.run_id}\nState after cancellation: {status.state.value}"
    if isinstance(value, ListRunsValue):
        if not value.runs:
            return "No Runs found."
        return "Known Runs:\n" + "\n".join(
            f"  {run.run_id}: {run.state.value} ({run.experiment} on {run.target})"
            for run in value.runs
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
        selected = ", ".join(str(task_id) for task_id in value.task_ids) or "all"
        return (
            f"Fetched {len(value.artifacts)} artifact(s) for {value.run_id} "
            f"to {value.destination}\nTasks: {selected}\n"
            f"Retrieval: {value.retrieval_state.value}"
        )
    if isinstance(value, InspectValue):
        record = value.record
        return (
            f"Run: {record.run.id}\nExperiment: {record.run.experiment_name}\n"
            f"State: {record.run.state.value}\n"
            f"Retrieval: {record.run.retrieval_state.value}\n"
            f"Artifacts: {len(record.artifacts)}"
        )
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
    return document


def _preparation_document(plan: PreparationPlan) -> dict[str, Any]:
    recipe = plan.recipe
    build = recipe.build
    return {
        "source": {
            "mode": plan.source_mode,
            "root": None if plan.source_root is None else str(plan.source_root),
            "git": {
                "url": recipe.source.url,
                "revision": recipe.source.revision,
                "recipe_identity": source_recipe_identity(recipe.source),
            },
        },
        "image": {
            "name": str(recipe.image.name),
            "uri": recipe.image.uri,
            "sha256": recipe.image.sha256,
        },
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
            "offline": plan.offline,
            "rebuild": plan.rebuild,
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
        return value.record.format_version
    if isinstance(value, InspectValue):
        return value.record.format_version
    if isinstance(value, StatusValue) and value.preparation is not None:
        return value.format_version
    if isinstance(value, StatusValue):
        return value.format_version
    if isinstance(value, CancelValue) and value.status.preparation is not None:
        return value.status.format_version
    if isinstance(value, CancelValue):
        return value.status.format_version
    if isinstance(value, ListRunsValue) and any(
        run.preparation is not None for run in value.runs
    ):
        return max(run.format_version for run in value.runs)
    if isinstance(value, ListRunsValue) and value.runs:
        return max(run.format_version for run in value.runs)
    if isinstance(value, PreparationLogsValue):
        return value.format_version
    if isinstance(value, (LogsValue, FetchValue)):
        return value.format_version
    if (
        isinstance(value, ValidationValue)
        and value.project is not None
        and value.project.version == 2
    ):
        return 2
    return _FORMAT_VERSION


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
        else f"Seeds: {', '.join(str(seed) for seed in value.seeds)}"
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
        f"{','.join(str(seed) for seed in seeds)}"
        for identifier, (choices, seeds) in grouped.items()
    )


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
    document: dict[str, Any] = {
        "run_id": str(value.run_id),
        "experiment": value.experiment,
        "target": value.target,
        "state": value.state.value,
        "retrieval_state": value.retrieval_state.value,
        "native_state": value.native_state,
        "scheduler_job_ids": list(value.scheduler_job_ids),
        "task_details": [
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
