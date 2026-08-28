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
    materialized_worker_pool: bool
    bundled_worker_pool: bool
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
                self.materialized_worker_pool,
                self.bundled_worker_pool,
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


@dataclass(frozen=True, slots=True)
class SchedulerBackendDescriptor:
    capabilities: SchedulerCapabilities
    required_tools: tuple[str, ...]


_BACKENDS: Final = MappingProxyType(
    {
        "local": SchedulerBackendDescriptor(
            SchedulerCapabilities(
                detached_submission=False,
                arrays=False,
                dependencies=False,
                compact_worker_pool=False,
                materialized_worker_pool=True,
                bundled_worker_pool=False,
                scheduler_requeue_recovery=False,
                scheduler_probe=False,
                array_strategy="one_unit_per_task",
            ),
            (),
        ),
        "slurm": SchedulerBackendDescriptor(
            SchedulerCapabilities(
                detached_submission=True,
                arrays=True,
                dependencies=True,
                compact_worker_pool=True,
                materialized_worker_pool=True,
                bundled_worker_pool=True,
                scheduler_requeue_recovery=True,
                scheduler_probe=True,
                array_strategy="slurm_array",
            ),
            ("sbatch", "squeue", "scancel", "scontrol", "sinfo"),
        ),
        "pbs": SchedulerBackendDescriptor(
            SchedulerCapabilities(
                detached_submission=True,
                arrays=True,
                dependencies=True,
                compact_worker_pool=True,
                materialized_worker_pool=True,
                bundled_worker_pool=True,
                scheduler_requeue_recovery=False,
                scheduler_probe=True,
                array_strategy="scheduler_array",
            ),
            ("qsub", "qstat", "qdel"),
        ),
        "htcondor": SchedulerBackendDescriptor(
            SchedulerCapabilities(
                detached_submission=True,
                arrays=True,
                dependencies=False,
                compact_worker_pool=False,
                materialized_worker_pool=False,
                bundled_worker_pool=False,
                scheduler_requeue_recovery=False,
                scheduler_probe=True,
                array_strategy="scheduler_array",
            ),
            (
                "condor_submit",
                "condor_q",
                "condor_history",
                "condor_rm",
                "condor_version",
            ),
        ),
    }
)


def scheduler_capabilities(kind: str) -> SchedulerCapabilities:
    """Return immutable built-in capabilities or reject an unknown backend."""
    try:
        return _BACKENDS[kind].capabilities
    except KeyError as error:
        raise ValueError(f"Unsupported scheduler backend: {kind}") from error


def scheduler_kinds() -> frozenset[str]:
    return frozenset(_BACKENDS)


def remote_scheduler_kinds() -> frozenset[str]:
    return scheduler_kinds() - {"local"}


def scheduler_required_tools(kind: str) -> tuple[str, ...]:
    try:
        return _BACKENDS[kind].required_tools
    except KeyError as error:
        raise ValueError(f"Unsupported scheduler backend: {kind}") from error


def validate_scheduler_resources(kind: str, resources: object) -> None:
    if kind == "local":
        return
    if kind == "pbs":
        from rundra.adapters.pbs import validate_pbs_resources

        validate_pbs_resources(resources)  # type: ignore[arg-type]
        return
    if kind == "slurm":
        from rundra.adapters.slurm import validate_slurm_resources

        validate_slurm_resources(resources)  # type: ignore[arg-type]
        return
    if kind == "htcondor":
        from rundra.adapters.htcondor import validate_htcondor_resources

        validate_htcondor_resources(resources)  # type: ignore[arg-type]
        return
    raise ValueError(f"Unsupported scheduler backend: {kind}")


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
    if target.scheduler.kind == "htcondor":
        from rundra.adapters.htcondor import HTCondorScheduler

        return HTCondorScheduler(transport, log_directory=log_directory)
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
