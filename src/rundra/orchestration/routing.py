from __future__ import annotations

from dataclasses import replace

from rundra.domain.models import ResourceRequest, Target
from rundra.domain.scheduling import SlurmPartitionRoute
from rundra.orchestration.models import PlanningError


def route_scheduler_resources(
    resources: ResourceRequest,
    target: Target,
) -> tuple[ResourceRequest, SlurmPartitionRoute | None]:
    """Apply a pure target-owned scheduler route to one resource request."""

    policy = target.partition_policy
    if policy is None:
        return resources, None
    if resources.walltime is None:
        raise PlanningError(
            code="PARTITION_ROUTE_REQUIRES_WALLTIME",
            message="Slurm partition routing requires an explicit walltime",
        )
    resource_class = "gpu" if resources.gpus_per_task > 0 else "cpu"
    explicit = resources.native.get("slurm", {}).get("partition")
    compatible = tuple(
        route
        for route in policy.routes
        if route.resource_class == resource_class
        and resources.walltime <= route.max_walltime
    )
    if explicit is not None:
        compatible = tuple(route for route in compatible if route.partition == explicit)
        if not compatible:
            raise PlanningError(
                code="PARTITION_ROUTE_POLICY_VIOLATION",
                message="Explicit Slurm partition is not compatible with target policy",
                details={
                    "partition": str(explicit),
                    "resource_class": resource_class,
                    "walltime_seconds": int(resources.walltime.total_seconds()),
                },
            )
    if not compatible:
        raise PlanningError(
            code="NO_COMPATIBLE_PARTITION_ROUTE",
            message="No target partition route fits the requested resources",
            details={
                "resource_class": resource_class,
                "walltime_seconds": int(resources.walltime.total_seconds()),
            },
        )
    selected = min(
        enumerate(compatible),
        key=lambda item: (item[1].max_walltime, item[0]),
    )[1]
    native = {backend: dict(options) for backend, options in resources.native.items()}
    native.setdefault("slurm", {})["partition"] = selected.partition
    return replace(resources, native=native), selected
