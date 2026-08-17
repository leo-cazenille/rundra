from pathlib import PurePosixPath

from rundra.domain.models import (
    Command,
    ConfigSnapshot,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    TaskId,
)
from rundra.domain.parameters import ParameterSet
from rundra.orchestration.models import ExecutionUnit
from rundra.orchestration.service import _container_request
from rundra.ports import StagedWorkspace


def test_parameterized_task_uses_its_container_visible_effective_config() -> None:
    task_id = TaskId.from_ordinal(20)
    unit = ExecutionUnit(
        task_id,
        0,
        ConfigSnapshot(PurePosixPath("sweep.yaml"), "regime: long_tumble\n"),
        Command(("simulation", "--config", "{config}", "--seed", "{seed}")),
        ResourceRequest(),
        ParameterSet("parameter_set_000001", {"regime": "long_tumble"}),
    )
    experiment = ExperimentSpec(
        1,
        "sweep",
        unit.command,
        unit.resources,
        ContainerSpec(PurePosixPath("image.sif")),
    )
    root = PurePosixPath("/remote/runs/run_abc")
    workspace = StagedWorkspace(
        root,
        root / "source",
        root / "input",
        root / "input/config.yaml",
        root / "runtime",
        root / "output",
        root / "logs",
        root / "metadata",
        task_configs={task_id: root / f"input/{task_id}.yaml"},
    )

    request = _container_request(experiment, unit, workspace, isolate_task=True)

    assert request.command.argv == (
        "simulation",
        "--config",
        f"/workspace/input/{task_id}.yaml",
        "--seed",
        "0",
    )
    assert request.binds[1].source == workspace.inputs
    assert request.binds[1].destination == PurePosixPath("/workspace/input")


def test_unparameterized_task_retains_shared_config_path() -> None:
    task_id = TaskId.from_ordinal(0)
    unit = ExecutionUnit(
        task_id,
        7,
        ConfigSnapshot(PurePosixPath("config.yaml"), "value: 1\n"),
        Command(("simulation", "{config}", "{seed}")),
        ResourceRequest(),
    )
    experiment = ExperimentSpec(
        1,
        "ordinary",
        unit.command,
        unit.resources,
        ContainerSpec(PurePosixPath("image.sif")),
    )
    root = PurePosixPath("/remote/runs/run_abc")
    workspace = StagedWorkspace(
        root,
        root / "source",
        root / "input",
        root / "input/config.yaml",
        root / "runtime",
        root / "output",
        root / "logs",
        root / "metadata",
    )

    request = _container_request(experiment, unit, workspace)

    assert request.command.argv == ("simulation", "/workspace/input/config.yaml", "7")
