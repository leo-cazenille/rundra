from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import pytest

from rundra.config.experiments import load_config_snapshot, load_experiment
from rundra.config.targets import load_targets
from rundra.domain.models import Command, ResourceRequest, RunId, TaskId
from rundra.domain.states import ExecutionState
from rundra.orchestration.planner import create_plan
from rundra.ports import (
    BindMount,
    CapabilityCheck,
    CommandResult,
    ContainerRequest,
    ContainerRuntime,
    FetchRequest,
    FetchResult,
    Scheduler,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmission,
    SchedulerUnit,
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


def _group() -> SchedulerGroup:
    root = Path(__file__).parents[2]
    spec = load_experiment(root / "examples/minimal/experiment.yaml")
    config = load_config_snapshot(root / "examples/minimal/config.yaml")
    target = load_targets(root / "examples/minimal/targets.yaml")["local"]
    planned = create_plan(spec, config, target, seeds=(7,)).units
    return SchedulerGroup(
        tuple(
            SchedulerUnit(unit.task_id, unit.command, unit.resources)
            for unit in planned
        )
    )


def test_scheduler_group_is_minimal_immutable_and_task_explicit() -> None:
    first = SchedulerUnit(
        TaskId.from_ordinal(0), Command(("program", "one")), ResourceRequest()
    )
    second = SchedulerUnit(
        TaskId.from_ordinal(1), Command(("program", "two")), ResourceRequest()
    )
    supplied = [first, second]

    group = SchedulerGroup(supplied)  # type: ignore[arg-type]
    supplied.clear()

    assert group.units == (first, second)
    assert not hasattr(first, "seed")
    assert not hasattr(first, "config")
    with pytest.raises(ValueError, match="at least one"):
        SchedulerGroup(())
    with pytest.raises(ValueError, match="unique"):
        SchedulerGroup((first, first))
    with pytest.raises(TypeError, match="SchedulerUnits"):
        SchedulerGroup((object(),))  # type: ignore[arg-type]


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
    group = _group()
    submission = SchedulerSubmission(reference, {group.units[0].task_id: "native-17_0"})
    queued = SchedulerObservation(reference, ExecutionState.QUEUED, "PENDING")
    cancelled = SchedulerObservation(reference, ExecutionState.CANCELLED, "CANCELLED")
    scheduler = FakeScheduler(
        submit_script=deque([submission, RuntimeError("submission failed")]),
        query_script=deque([(queued,)]),
        cancel_script=deque([(cancelled,)]),
    )

    assert isinstance(scheduler, Scheduler)
    assert scheduler.submit(group) == submission
    with pytest.raises(RuntimeError, match="submission failed"):
        scheduler.submit(group)
    assert scheduler.query((reference,)) == (queued,)
    assert scheduler.cancel((reference,)) == (cancelled,)
    assert scheduler.submit_calls == [group, group]
    assert scheduler.query_calls == [(reference,)]
    assert scheduler.cancel_calls == [(reference,)]


def test_scheduler_observation_optionally_carries_a_consistent_command_result() -> None:
    reference = SchedulerReference("local-17")
    command = Command(("python", "main.py"))
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = CommandResult(command, 0, "done\n", "", now, now)

    observation = SchedulerObservation(
        reference,
        ExecutionState.SUCCEEDED,
        "EXITED",
        exit_code=0,
        result=result,
    )

    assert observation.result == result
    assert observation.started_at == result.started_at
    assert observation.finished_at == result.finished_at
    with pytest.raises(ValueError, match="same exit"):
        SchedulerObservation(
            reference,
            ExecutionState.FAILED,
            "EXITED",
            exit_code=1,
            result=result,
        )
    with pytest.raises(TypeError, match="CommandResult"):
        SchedulerObservation(
            reference,
            ExecutionState.SUCCEEDED,
            "EXITED",
            result=object(),  # type: ignore[arg-type]
        )


def test_remote_scheduler_observation_normalizes_available_accounting_data() -> None:
    reference = SchedulerReference("12345")
    started = datetime(2026, 1, 1, 12, tzinfo=UTC)
    finished = started + timedelta(minutes=2)
    supplied_metadata = {"partition": "cpu", "elapsed_seconds": 120}

    observation = SchedulerObservation(
        reference,
        ExecutionState.FAILED,
        "OUT_OF_MEMORY",
        exit_code=137,
        metadata=supplied_metadata,
        started_at=started,
        finished_at=finished,
    )
    supplied_metadata.clear()

    assert observation.reference == reference
    assert observation.state is ExecutionState.FAILED
    assert observation.native_state == "OUT_OF_MEMORY"
    assert observation.exit_code == 137
    assert observation.metadata == {"partition": "cpu", "elapsed_seconds": 120}
    assert observation.started_at == started
    assert observation.finished_at == finished


@pytest.mark.parametrize(
    "observation",
    [
        lambda ref, now: SchedulerObservation(ref, ExecutionState.CREATED, "CREATED"),
        lambda ref, now: SchedulerObservation(
            ref, ExecutionState.QUEUED, "PENDING", exit_code=0
        ),
        lambda ref, now: SchedulerObservation(
            ref, ExecutionState.RUNNING, "RUNNING", finished_at=now
        ),
        lambda ref, now: SchedulerObservation(
            ref, ExecutionState.SUCCEEDED, "COMPLETED", exit_code=9
        ),
        lambda ref, now: SchedulerObservation(
            ref,
            ExecutionState.FAILED,
            "FAILED",
            started_at=now,
            finished_at=now - timedelta(seconds=1),
        ),
    ],
)
def test_scheduler_observation_rejects_inconsistent_states(
    observation: object,
) -> None:
    reference = SchedulerReference("12345")
    now = datetime(2026, 1, 1, tzinfo=UTC)

    with pytest.raises(ValueError):
        observation(reference, now)  # type: ignore[operator]


def test_scheduler_native_identifiers_and_metadata_are_safe_and_nonempty() -> None:
    task_id = TaskId.from_ordinal(0)
    reference = SchedulerReference("12345")

    for value in ("", "   ", "12\x003"):
        with pytest.raises(ValueError, match="native_id"):
            SchedulerReference(value)
    with pytest.raises(ValueError, match="must not be empty"):
        SchedulerSubmission(reference, {})
    for value in ("", "   ", "12\x003"):
        with pytest.raises(ValueError, match="native IDs"):
            SchedulerSubmission(reference, {task_id: value})
    with pytest.raises(ValueError, match="native_state"):
        SchedulerObservation(reference, ExecutionState.UNKNOWN, "   ")
    with pytest.raises(TypeError, match="metadata"):
        SchedulerObservation(
            reference,
            ExecutionState.UNKNOWN,
            "UNKNOWN",
            metadata={"": "value"},
        )


def test_fake_stager_and_recording_runtime_are_structural_ports() -> None:
    root = Path(__file__).parents[2]
    spec = load_experiment(root / "examples/minimal/experiment.yaml")
    config = load_config_snapshot(root / "examples/minimal/config.yaml")
    target = load_targets(root / "examples/minimal/targets.yaml")["local"]
    stage_request = StageRequest(
        RunId.new(), spec, config, target, PurePosixPath("project")
    )
    workspace = StagedWorkspace(
        root=PurePosixPath("run"),
        source=PurePosixPath("run/source"),
        inputs=PurePosixPath("run/input"),
        config=PurePosixPath("run/input/config.yaml"),
        runtime=PurePosixPath("run/runtime"),
        outputs=PurePosixPath("run/output"),
        logs=PurePosixPath("run/logs"),
        metadata=PurePosixPath("run/metadata"),
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
        binds=(
            BindMount(
                PurePosixPath("/host/source"),
                PurePosixPath("/workspace/source"),
            ),
        ),
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


def test_staged_workspace_derives_stable_isolated_task_locations() -> None:
    from rundra.domain.models import TaskId

    workspace = StagedWorkspace(
        root=PurePosixPath("/work/runs/run_0123456789abcdef0123456789abcdef"),
        source=PurePosixPath("/work/runs/run_0123456789abcdef0123456789abcdef/source"),
        inputs=PurePosixPath("/work/runs/run_0123456789abcdef0123456789abcdef/input"),
        config=PurePosixPath(
            "/work/runs/run_0123456789abcdef0123456789abcdef/input/config.yaml"
        ),
        runtime=PurePosixPath(
            "/work/runs/run_0123456789abcdef0123456789abcdef/runtime"
        ),
        outputs=PurePosixPath("/work/runs/run_0123456789abcdef0123456789abcdef/output"),
        logs=PurePosixPath("/work/runs/run_0123456789abcdef0123456789abcdef/logs"),
        metadata=PurePosixPath(
            "/work/runs/run_0123456789abcdef0123456789abcdef/metadata"
        ),
    )

    first = workspace.for_task(TaskId.from_ordinal(0))
    second = workspace.for_task(TaskId.from_ordinal(1))

    assert first.source == second.source == workspace.source
    assert first.config == second.config == workspace.config
    assert first.runtime == workspace.runtime / "task_000000"
    assert second.runtime == workspace.runtime / "task_000001"
    assert first.outputs == workspace.outputs / "task_000000"
    assert second.outputs == workspace.outputs / "task_000001"
    assert first.stdout == workspace.logs / "task_000000.stdout"
    assert first.stderr == workspace.logs / "task_000000.stderr"
    assert first.metadata == workspace.metadata / "task_000000"
    assert first != second


def test_staged_workspace_rejects_non_task_identity_for_task_locations() -> None:
    workspace = StagedWorkspace(*(PurePosixPath(f"root/{name}") for name in range(8)))

    with pytest.raises(TypeError, match="task_id"):
        workspace.for_task("task_000000")
