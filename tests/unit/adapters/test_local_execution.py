from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rundra.adapters.local import (
    LocalScheduler,
    LocalSchedulerError,
    LocalTransport,
    LocalTransportError,
)
from rundra.domain.models import Command, ResourceRequest, TaskId
from rundra.domain.states import ExecutionState
from rundra.ports import Scheduler, SchedulerGroup, SchedulerUnit, Transport


def _unit(command: Command, ordinal: int = 0) -> SchedulerUnit:
    return SchedulerUnit(
        task_id=TaskId.from_ordinal(ordinal),
        command=command,
        resources=ResourceRequest(),
    )


def test_local_transport_runs_argv_with_explicit_cwd_environment_and_output(
    tmp_path: Path,
) -> None:
    command = Command(
        (
            sys.executable,
            "-c",
            "import os, pathlib, sys; "
            "print(pathlib.Path.cwd().name); "
            "print(os.environ['RUNDRA_TEST_VALUE']); "
            "print(sys.argv[1], file=sys.stderr); "
            "raise SystemExit(3)",
            "; touch should-not-exist",
        ),
        environment={"RUNDRA_TEST_VALUE": "literal value"},
        working_directory=tmp_path,
    )

    transport = LocalTransport()
    result = transport.run(command)

    assert isinstance(transport, Transport)
    assert transport.check().name == "local"
    assert result.command == command
    assert result.exit_code == 3
    assert result.stdout == f"{tmp_path.name}\nliteral value\n"
    assert result.stderr == "; touch should-not-exist\n"
    assert result.started_at.tzinfo is UTC
    assert result.finished_at >= result.started_at
    assert not (tmp_path / "should-not-exist").exists()


def test_local_transport_reports_process_start_failures() -> None:
    with pytest.raises(LocalTransportError, match="Could not execute local command"):
        LocalTransport().run(Command(("/definitely/missing/rundra-command",)))


@pytest.mark.parametrize("value", ["not-a-command", object()])
def test_local_transport_rejects_non_commands(value: object) -> None:
    with pytest.raises(TypeError, match="Command"):
        LocalTransport().run(value)  # type: ignore[arg-type]


def test_local_scheduler_executes_one_unit_and_reports_terminal_observation() -> None:
    scheduler = LocalScheduler(
        LocalTransport(), reference_factory=lambda: "local-test-reference"
    )
    unit = _unit(Command((sys.executable, "-c", "print('done')")))

    submission = scheduler.submit(SchedulerGroup((unit,)))
    observations = scheduler.query((submission.reference,))

    assert isinstance(scheduler, Scheduler)
    assert submission.reference.native_id == "local-test-reference"
    assert submission.task_native_ids == {unit.task_id: "local-test-reference"}
    assert len(observations) == 1
    assert observations[0].state is ExecutionState.SUCCEEDED
    assert observations[0].native_state == "EXITED"
    assert observations[0].exit_code == 0
    assert observations[0].result is not None
    assert observations[0].result.stdout == "done\n"
    assert observations[0].started_at == observations[0].result.started_at
    assert observations[0].finished_at == observations[0].result.finished_at
    assert scheduler.cancel((submission.reference,)) == observations


def test_local_scheduler_maps_nonzero_exit_and_rejects_unsupported_requests() -> None:
    scheduler = LocalScheduler(
        LocalTransport(), reference_factory=lambda: "local-failed-reference"
    )
    failed = _unit(Command((sys.executable, "-c", "raise SystemExit(9)")))

    submission = scheduler.submit(SchedulerGroup((failed,)))

    observation = scheduler.query((submission.reference,))[0]
    assert observation.state is ExecutionState.FAILED
    assert observation.exit_code == 9
    with pytest.raises(LocalSchedulerError, match="exactly one"):
        scheduler.submit(SchedulerGroup((failed, _unit(failed.command, ordinal=1))))
    with pytest.raises(LocalSchedulerError, match="Unknown local scheduler"):
        scheduler.query((type(submission.reference)("missing"),))


def test_local_scheduler_rejects_duplicate_native_references() -> None:
    scheduler = LocalScheduler(LocalTransport(), reference_factory=lambda: "same")
    unit = _unit(Command((sys.executable, "-c", "pass")))
    group = SchedulerGroup((unit,))
    scheduler.submit(group)

    with pytest.raises(LocalSchedulerError, match="already exists"):
        scheduler.submit(group)


def test_local_scheduler_timestamps_come_from_the_transport_result() -> None:
    scheduler = LocalScheduler(
        LocalTransport(), reference_factory=lambda: "local-time-reference"
    )
    before = datetime.now(UTC)

    submission = scheduler.submit(
        SchedulerGroup((_unit(Command((sys.executable, "-c", "pass"))),))
    )
    result = scheduler.query((submission.reference,))[0].result

    assert result is not None
    assert before <= result.started_at <= result.finished_at <= datetime.now(UTC)
