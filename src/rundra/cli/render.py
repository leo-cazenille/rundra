from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from rundra.cli.operations import (
    FetchValue,
    InspectValue,
    ListRunsValue,
    LogsValue,
    RunValue,
    StatusValue,
    TargetsValue,
    ValidationValue,
)
from rundra.domain.models import Artifact, Command, ResourceRequest, Target
from rundra.orchestration.models import ExecutionPlan
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
    document: dict[str, Any] = {
        "format_version": _FORMAT_VERSION,
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
    value = result.value
    if isinstance(value, ValidationValue):
        document["experiment"] = {
            "name": value.experiment.name,
            "schema_version": value.experiment.version,
            "source": str(value.source),
        }
    elif isinstance(value, ExecutionPlan):
        document["plan"] = _plan_document(value)
    elif isinstance(value, TargetsValue):
        document["source"] = str(value.source)
        document["targets"] = [
            _target_document(value.targets[name]) for name in sorted(value.targets)
        ]
    elif isinstance(value, RunValue):
        document["run"] = _run_value_document(value)
    elif isinstance(value, StatusValue):
        document["status"] = _status_document(value)
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
    elif isinstance(value, FetchValue):
        document["fetch"] = {
            "run_id": str(value.run_id),
            "destination": str(value.destination),
            "retrieval_state": value.retrieval_state.value,
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
        return (
            f"Valid experiment: {value.experiment.name} "
            f"(schema v{value.experiment.version})"
        )
    if isinstance(value, ExecutionPlan):
        seeds = ", ".join(str(unit.seed) for unit in value.units)
        return (
            f"Plan for {value.experiment_name} on {value.target.name}: "
            f"{len(value.units)} task(s)\nSeeds: {seeds}\n"
            f"Strategy: {value.strategy}\nStaging: {value.staging_backend}"
        )
    if isinstance(value, TargetsValue):
        lines = ["Configured targets:"]
        lines.extend(
            f"  {name}: {value.targets[name].transport.kind} / "
            f"{value.targets[name].scheduler.kind}"
            for name in sorted(value.targets)
        )
        return "\n".join(lines)
    if isinstance(value, RunValue):
        return (
            f"Run: {value.run_id}\n"
            f"Seed: {value.seed}\n"
            f"State: {value.record.run.state.value}\n"
            f"Retrieval: {value.record.run.retrieval_state.value}\n"
            f"Target: {value.record.run.target.name}"
        )
    if isinstance(value, StatusValue):
        counts = ", ".join(
            f"{state.lower()}={count}"
            for state, count in sorted(value.task_counts.items())
        )
        return (
            f"Run: {value.run_id}\nState: {value.state.value}\n"
            f"Retrieval: {value.retrieval_state.value}\nTasks: {counts}"
        )
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
    if isinstance(value, FetchValue):
        return (
            f"Fetched {len(value.artifacts)} artifact(s) for {value.run_id} "
            f"to {value.destination}"
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
    return {
        "version": plan.version,
        "experiment": plan.experiment_name,
        "target": _target_document(plan.target),
        "strategy": plan.strategy,
        "staging_backend": plan.staging_backend,
        "units": [
            {
                "task_id": str(unit.task_id),
                "seed": unit.seed,
                "config": str(unit.config.source),
                "command": _command_document(unit.command),
                "resources": _resources_document(unit.resources),
            }
            for unit in plan.units
        ],
    }


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
        "seed": value.seed,
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
        "artifacts": [_artifact_document(item) for item in record.artifacts],
    }


def _status_document(value: StatusValue) -> dict[str, Any]:
    return {
        "run_id": str(value.run_id),
        "experiment": value.experiment,
        "target": value.target,
        "state": value.state.value,
        "retrieval_state": value.retrieval_state.value,
        "tasks": {
            "total": sum(value.task_counts.values()),
            **{
                state.lower(): count
                for state, count in sorted(value.task_counts.items())
            },
        },
    }


def _artifact_document(artifact: Artifact) -> dict[str, Any]:
    return {
        "kind": artifact.kind.value,
        "path": str(artifact.path),
        "task_id": None if artifact.task_id is None else str(artifact.task_id),
        "size_bytes": artifact.size_bytes,
    }
