from collections import deque
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import pytest

from rundra.config.experiments import load_config_snapshot, load_experiment
from rundra.config.targets import load_targets
from rundra.domain.models import Command, RunId
from rundra.domain.states import ExecutionState
from rundra.orchestration.models import ExecutionUnit
from rundra.orchestration.planner import create_plan
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    ContainerRequest,
    ContainerRuntime,
    FetchRequest,
    FetchResult,
    Scheduler,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmission,
    StagedWorkspace,
    Stager,
    StageRequest,
    Transport,
)
from tests.fakes import (
    FakeScheduler,
    FakeStager,
    FakeTransport,
    RecordingContainerRuntime,
)


def _units() -> tuple[ExecutionUnit, ...]:
    root = Path(__file__).parents[2]
    spec = load_experiment(root / "examples/minimal/experiment.yaml")
    config = load_config_snapshot(root / "examples/minimal/config.yaml")
    target = load_targets(root / "examples/minimal/targets.yaml")["local"]
    return create_plan(spec, config, target, seeds=(7,)).units


def test_fake_transport_scripts_results_failures_and_call_history() -> None:
    command = Command(("python", "main.py"))
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = CommandResult(command, 0, "done\n", "", now, now)
    transport = FakeTransport(
        check_script=deque([CapabilityCheck("local")]),
        run_script=deque([result, RuntimeError("command failed")]),
    )

    assert isinstance(transport, Transport)
    assert transport.check() == CapabilityCheck("local")
    assert transport.run(command) == result
    with pytest.raises(RuntimeError, match="command failed"):
        transport.run(command)
    assert transport.check_calls == 1
    assert transport.run_calls == [command, command]


def test_fake_scheduler_scripts_observation_failure_and_cancellation() -> None:
    reference = SchedulerReference("native-17")
    units = _units()
    submission = SchedulerSubmission(reference, {units[0].task_id: "native-17_0"})
    queued = SchedulerObservation(reference, ExecutionState.QUEUED, "PENDING")
    cancelled = SchedulerObservation(reference, ExecutionState.CANCELLED, "CANCELLED")
    scheduler = FakeScheduler(
        submit_script=deque([submission, RuntimeError("submission failed")]),
        query_script=deque([(queued,)]),
        cancel_script=deque([(cancelled,)]),
    )

    assert isinstance(scheduler, Scheduler)
    assert scheduler.submit(units) == submission
    with pytest.raises(RuntimeError, match="submission failed"):
        scheduler.submit(units)
    assert scheduler.query((reference,)) == (queued,)
    assert scheduler.cancel((reference,)) == (cancelled,)
    assert scheduler.submit_calls == [units, units]
    assert scheduler.query_calls == [(reference,)]
    assert scheduler.cancel_calls == [(reference,)]


def test_fake_stager_and_recording_runtime_are_structural_ports() -> None:
    root = Path(__file__).parents[2]
    spec = load_experiment(root / "examples/minimal/experiment.yaml")
    config = load_config_snapshot(root / "examples/minimal/config.yaml")
    target = load_targets(root / "examples/minimal/targets.yaml")["local"]
    stage_request = StageRequest(
        RunId.new(), spec, config, target, PurePosixPath("project")
    )
    workspace = StagedWorkspace(
        PurePosixPath("run"),
        PurePosixPath("run/source"),
        PurePosixPath("run/config.yaml"),
        PurePosixPath("run/outputs"),
    )
    fetch_request = FetchRequest(workspace, ("results/**",), PurePosixPath("retrieved"))
    fetch_result = FetchResult(())
    stager = FakeStager(
        deque([workspace, RuntimeError("stage failed")]), deque([fetch_result])
    )
    wrapped = Command(("apptainer", "exec", "image.sif", "python"))
    runtime = RecordingContainerRuntime(CapabilityCheck("apptainer"), wrapped)
    request = ContainerRequest(
        command=Command(("python", "main.py")),
        image=PurePosixPath("image.sif"),
        gpu=False,
    )

    assert isinstance(stager, Stager)
    assert isinstance(runtime, ContainerRuntime)
    assert runtime.check() == CapabilityCheck("apptainer")
    assert runtime.build_command(request) == wrapped
    assert runtime.build_calls == [request]
    assert stager.stage(stage_request) == workspace
    with pytest.raises(RuntimeError, match="stage failed"):
        stager.stage(stage_request)
    assert stager.fetch(fetch_request) == fetch_result
    assert stager.stage_calls == [stage_request, stage_request]
    assert stager.fetch_calls == [fetch_request]
