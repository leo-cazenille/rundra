from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePath
from types import MappingProxyType
from typing import Final

from rundra.domain.models import Target
from rundra.ports import Scheduler, Transport


@dataclass(frozen=True, slots=True)
class SchedulerCapabilities:
    """Static portable behavior implemented by one scheduler adapter."""

    detached_submission: bool
    arrays: bool
    dependencies: bool
    compact_worker_pool: bool
    scheduler_requeue_recovery: bool
    scheduler_probe: bool
    array_strategy: str

    def __post_init__(self) -> None:
        if any(
            type(value) is not bool
            for value in (
                self.detached_submission,
                self.arrays,
                self.dependencies,
                self.compact_worker_pool,
                self.scheduler_requeue_recovery,
                self.scheduler_probe,
            )
        ):
            raise TypeError("Scheduler capability flags must be booleans")
        if self.array_strategy not in {
            "one_unit_per_task",
            "scheduler_array",
            "slurm_array",
        }:
            raise ValueError("Scheduler array strategy is unsupported")


_CAPABILITIES: Final = MappingProxyType(
    {
        "local": SchedulerCapabilities(
            False, False, False, False, False, False, "one_unit_per_task"
        ),
        "slurm": SchedulerCapabilities(
            True, True, True, True, True, True, "slurm_array"
        ),
        "pbs": SchedulerCapabilities(
            True, True, True, True, False, True, "scheduler_array"
        ),
    }
)


def scheduler_capabilities(kind: str) -> SchedulerCapabilities:
    """Return immutable built-in capabilities or reject an unknown backend."""
    try:
        return _CAPABILITIES[kind]
    except KeyError as error:
        raise ValueError(f"Unsupported scheduler backend: {kind}") from error


def scheduler_for_target(
    target: Target,
    transport: Transport,
    *,
    log_directory: PurePath | None = None,
) -> Scheduler:
    """Construct the scheduler adapter selected by a validated Target."""
    if target.scheduler.kind == "local":
        from rundra.adapters.local import LocalScheduler

        return LocalScheduler(transport)
    if target.scheduler.kind == "pbs":
        from rundra.adapters.pbs import OpenPBSScheduler

        return OpenPBSScheduler(transport, log_directory=log_directory)
    if target.scheduler.kind == "slurm":
        from rundra.adapters.slurm import SlurmScheduler

        return SlurmScheduler(transport, log_directory=log_directory)
    raise ValueError(f"Unsupported scheduler backend: {target.scheduler.kind}")


def scheduler_capabilities_document(kind: str) -> dict[str, bool]:
    """Return the stable public capability projection."""
    value = scheduler_capabilities(kind)
    return {
        "arrays": value.arrays,
        "compact_worker_pool": value.compact_worker_pool,
        "dependencies": value.dependencies,
        "detached_submission": value.detached_submission,
        "scheduler_probe": value.scheduler_probe,
        "scheduler_requeue_recovery": value.scheduler_requeue_recovery,
    }
