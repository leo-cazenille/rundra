from __future__ import annotations

from datetime import timedelta

from rundra.adapters._remote_shell import serialize_remote_command
from rundra.domain.models import NativeValue, ResourceRequest
from rundra.ports import SchedulerGroup

_MIB = 1024**2
_VALUE_OPTIONS = ("account", "constraint", "partition", "qos")
_FLAG_OPTIONS = ("exclusive",)
_ALLOWED_OPTIONS = frozenset((*_VALUE_OPTIONS, *_FLAG_OPTIONS))


class SlurmScriptError(ValueError):
    """Raised when a normalized group cannot be represented as an sbatch script."""


def render_sbatch_script(group: SchedulerGroup) -> str:
    """Render one deterministic, inspectable single-Task sbatch script."""
    if type(group) is not SchedulerGroup:
        raise TypeError("render_sbatch_script requires a SchedulerGroup")
    if len(group.units) != 1:
        raise SlurmScriptError("M3 Slurm submission requires exactly one Task")
    unit = group.units[0]
    resources = unit.resources
    directives = [
        f"#SBATCH --job-name=rundra-{unit.task_id}",
        f"#SBATCH --nodes={resources.nodes}",
        f"#SBATCH --ntasks={resources.tasks}",
        f"#SBATCH --cpus-per-task={resources.cpus_per_task}",
    ]
    if resources.gpus_per_task:
        directives.append(f"#SBATCH --gpus-per-task={resources.gpus_per_task}")
    if resources.memory_bytes is not None:
        memory_mib = (resources.memory_bytes + _MIB - 1) // _MIB
        directives.append(f"#SBATCH --mem={memory_mib}M")
    if resources.walltime is not None:
        directives.append(f"#SBATCH --time={_slurm_duration(resources.walltime)}")
    directives.extend(_native_directives(resources))
    command = serialize_remote_command(unit.command)
    return "\n".join(("#!/bin/sh", *directives, "", "set -eu", command, ""))


def _native_directives(resources: ResourceRequest) -> tuple[str, ...]:
    options = resources.native.get("slurm", {})
    unsupported = sorted(set(options) - _ALLOWED_OPTIONS)
    if unsupported:
        names = ", ".join(unsupported)
        raise SlurmScriptError(f"Unsupported resources.native.slurm options: {names}")
    directives: list[str] = []
    for name in _VALUE_OPTIONS:
        if name in options:
            directives.append(f"#SBATCH --{name}={_native_value(name, options[name])}")
    for name in _FLAG_OPTIONS:
        if name in options:
            value = options[name]
            if type(value) is not bool:
                raise SlurmScriptError(
                    f"resources.native.slurm.{name} must be a boolean"
                )
            if value:
                directives.append(f"#SBATCH --{name}")
    return tuple(directives)


def _native_value(name: str, value: NativeValue) -> str:
    if type(value) not in (str, int) or type(value) is bool:
        raise SlurmScriptError(
            f"resources.native.slurm.{name} must be a string or integer"
        )
    rendered = str(value)
    if (
        not rendered.strip()
        or "\n" in rendered
        or "\r" in rendered
        or "\x00" in rendered
    ):
        raise SlurmScriptError(
            f"resources.native.slurm.{name} contains an unsafe directive value"
        )
    return rendered


def _slurm_duration(value: timedelta) -> str:
    total_microseconds = (
        value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
    )
    total_seconds = (total_microseconds + 999_999) // 1_000_000
    days, remainder = divmod(total_seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, seconds = divmod(remainder, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock
