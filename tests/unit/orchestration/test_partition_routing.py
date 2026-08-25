from datetime import timedelta
from pathlib import PurePosixPath

import pytest

from rundra.domain.models import BackendConfig, ResourceRequest, Target
from rundra.domain.scheduling import SlurmPartitionPolicy, SlurmPartitionRoute
from rundra.orchestration.models import PlanningError
from rundra.orchestration.routing import route_scheduler_resources


def _target() -> Target:
    return Target(
        "cluster",
        BackendConfig("ssh", {"host": "cluster"}),
        BackendConfig("slurm"),
        BackendConfig("rsync"),
        BackendConfig("apptainer"),
        PurePosixPath("/shared/rundra"),
        partition_policy=SlurmPartitionPolicy(
            (
                SlurmPartitionRoute(
                    "cpu_day", "cpu-day", "cpu", timedelta(days=1)
                ),
                SlurmPartitionRoute(
                    "cpu_short", "cpu-short", "cpu", timedelta(hours=1)
                ),
                SlurmPartitionRoute(
                    "gpu_short", "gpu-short", "gpu", timedelta(hours=1)
                ),
            )
        ),
    )


def test_routing_selects_shortest_compatible_partition() -> None:
    resources, route = route_scheduler_resources(
        ResourceRequest(walltime=timedelta(minutes=30)), _target()
    )

    assert route is not None and route.name == "cpu_short"
    assert resources.native["slurm"]["partition"] == "cpu-short"


def test_routing_selects_gpu_class() -> None:
    resources, route = route_scheduler_resources(
        ResourceRequest(gpus_per_task=1, walltime=timedelta(minutes=30)), _target()
    )

    assert route is not None and route.name == "gpu_short"
    assert resources.native["slurm"]["partition"] == "gpu-short"


def test_routing_rejects_missing_walltime_and_policy_bypass() -> None:
    with pytest.raises(PlanningError) as missing:
        route_scheduler_resources(ResourceRequest(), _target())
    assert missing.value.code == "PARTITION_ROUTE_REQUIRES_WALLTIME"

    request = ResourceRequest(
        walltime=timedelta(minutes=30),
        native={"slurm": {"partition": "undeclared"}},
    )
    with pytest.raises(PlanningError) as bypass:
        route_scheduler_resources(request, _target())
    assert bypass.value.code == "PARTITION_ROUTE_POLICY_VIOLATION"
