from datetime import timedelta
from pathlib import PurePosixPath

import pytest

from rundra.domain.models import (
    BackendConfig,
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    ResourceRequest,
    Target,
)
from rundra.domain.parameters import ParameterSet
from rundra.domain.scheduling import SlurmPartitionPolicy, SlurmPartitionRoute
from rundra.domain.sweeps import ExpandedConfig
from rundra.orchestration.models import PlanningError
from rundra.orchestration.planner import create_sweep_plan
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
                SlurmPartitionRoute("cpu_day", "cpu-day", "cpu", timedelta(days=1)),
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


def test_worker_allocation_is_routed_after_aggregate_walltime() -> None:
    from rundra.orchestration.planner import _worker_resources

    logical = ResourceRequest(walltime=timedelta(minutes=20))
    aggregate = _worker_resources(logical, 1, 4)
    routed, route = route_scheduler_resources(aggregate, _target())

    assert routed.walltime == timedelta(minutes=80)
    assert route is not None and route.name == "cpu_day"
    assert routed.native["slurm"]["partition"] == "cpu-day"


def test_sweep_units_use_the_selected_partition_route() -> None:
    experiment = ExperimentSpec(
        version=1,
        name="routed-sweep",
        command=Command(("simulate", "--config", "{config}", "--seed", "{seed}")),
        resources=ResourceRequest(walltime=timedelta(minutes=30)),
        outputs=("result.json",),
    )
    config = ExpandedConfig(
        ConfigSnapshot(PurePosixPath("config.yaml"), "regime: test\n"),
        ParameterSet("parameter_set_000000", {"regime": "test"}),
    )

    plan = create_sweep_plan(experiment, (config,), _target(), seeds=(0,))

    assert plan.units[0].resources.native == {"slurm": {"partition": "cpu-short"}}
