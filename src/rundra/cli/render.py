from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from rundra.cli.operations import TargetsValue, ValidationValue
from rundra.domain.models import Command, ResourceRequest, Target
from rundra.orchestration.models import ExecutionPlan
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
