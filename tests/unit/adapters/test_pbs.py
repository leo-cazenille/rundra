from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pytest

from rundra.adapters.pbs import (
    OpenPBSScheduler,
    PBSSubmissionError,
    render_qsub_array_script,
    render_qsub_script,
)
from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import Command, ResourceRequest, TaskId
from rundra.domain.scaling import SeedRange, TaskSpace
from rundra.domain.states import ExecutionState
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    CompactArrayScheduler,
    CompactDependencyScheduler,
    CompactSchedulerArrayRequest,
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerReference,
    SchedulerSubmissionOutcome,
    SchedulerUnit,
)


class FakeTransport:
    def __init__(self, outputs: list[tuple[int, str]]) -> None:
        self.outputs = outputs
        self.commands: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("fake")

    def run(self, command: Command) -> CommandResult:
        self.commands.append(command)
        exit_code, stdout = self.outputs.pop(0)
        now = datetime.now(UTC)
        return CommandResult(command, exit_code, stdout, "", now, now)


def _unit(ordinal: int = 0) -> SchedulerUnit:
    return SchedulerUnit(
        TaskId.from_ordinal(ordinal),
        Command(("/bin/echo", f"seed={ordinal}")),
        ResourceRequest(
            cpus_per_task=2,
            memory_bytes=33 * 1024 * 1024,
            walltime=timedelta(seconds=61),
        ),
    )


def test_render_qsub_script_owns_portable_resources_and_logs() -> None:
    script = render_qsub_script(
        SchedulerGroup((_unit(),)),
        log_directory=PurePosixPath("/work/logs"),
    )

    assert "#PBS -l select=1:ncpus=2:mpiprocs=1:mem=33mb" in script
    assert "#PBS -l mem=33mb" not in script
    assert "#PBS -l walltime=00:01:01" in script
    assert "${PBS_JOBID}.stdout" in script
    assert "/bin/echo seed=0" in script


def test_submit_array_maps_openpbs_subjob_ids() -> None:
    transport = FakeTransport([(0, "42[].server\n")])
    scheduler = OpenPBSScheduler(transport, log_directory=PurePosixPath("/work/logs"))
    units = (_unit(0), _unit(1))
    request = SchedulerArrayRequest(
        SchedulerGroup(units),
        (
            ArrayTaskMapping(units[0].task_id, 0, 0),
            ArrayTaskMapping(units[1].task_id, 1, 1),
        ),
        PurePosixPath("/work/array.sh"),
    )

    submission = scheduler.submit_array(request)

    assert submission.reference.native_id == "42[].server"
    assert dict(submission.task_native_ids) == {
        units[0].task_id: "42[0].server",
        units[1].task_id: "42[1].server",
    }
    assert "#PBS -J 0-1%2" in transport.commands[0].argv[5]


def test_unbundled_qsub_array_does_not_finalize_bundle_metadata() -> None:
    units = (_unit(0), _unit(1))
    request = SchedulerArrayRequest(
        SchedulerGroup(units),
        (
            ArrayTaskMapping(units[0].task_id, 0, 0),
            ArrayTaskMapping(units[1].task_id, 1, 1),
        ),
        PurePosixPath("/work/array.sh"),
    )

    script = render_qsub_array_script(request)
    completed = subprocess.run(
        ("bash", "-c", script),
        env={**os.environ, "PBS_ARRAY_INDEX": "0", "PBS_JOBID": "42[].server"},
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "bundle-status" not in script


def test_submit_compact_array_maps_workers_and_runs_concurrent_lanes() -> None:
    transport = FakeTransport([(0, "77[].server\n")])
    scheduler = OpenPBSScheduler(transport, log_directory=PurePosixPath("/work/logs"))
    assert isinstance(scheduler, CompactArrayScheduler)
    assert isinstance(scheduler, CompactDependencyScheduler)
    request = CompactSchedulerArrayRequest(
        TaskSpace(1, SeedRange(0, 7)),
        (Command(("simulate", "--seed", "{seed}")),),
        ResourceRequest(memory_bytes=1024, walltime=timedelta(minutes=1)),
        ResourceRequest(
            tasks=2,
            memory_bytes=2048,
            walltime=timedelta(minutes=4),
        ),
        PurePosixPath("/work/metadata/tasks.sh"),
        worker_count=4,
        task_slots_per_worker=2,
    )

    submission = scheduler.submit_compact_array(request)

    assert submission.reference.native_id == "77[].server"
    assert submission.worker_native_ids == (
        "77[0].server",
        "77[1].server",
        "77[2].server",
        "77[3].server",
    )
    command = transport.commands[0]
    assert "#PBS -J 0-3%4" in command.argv[6]
    assert "export SLURM_PROCID=1" in command.argv[6]
    assert "RUNDRA_TASK_EVENTS" in command.argv[5]


def test_openpbs_compact_array_rejects_scheduler_requeue_policy() -> None:
    transport = FakeTransport([])
    scheduler = OpenPBSScheduler(transport, log_directory=PurePosixPath("/work/logs"))
    request = CompactSchedulerArrayRequest(
        TaskSpace(1, SeedRange(0, 1)),
        (Command(("true",)),),
        ResourceRequest(),
        ResourceRequest(),
        PurePosixPath("/work/metadata/tasks.sh"),
        worker_count=1,
        requeue_limit=1,
    )

    with pytest.raises(PBSSubmissionError, match="requeue_limit 0") as caught:
        scheduler.submit_compact_array(request)

    assert caught.value.outcome is SchedulerSubmissionOutcome.REJECTED
    assert not transport.commands


@pytest.mark.parametrize(
    ("exit_code", "stdout", "outcome"),
    [
        (1, "", SchedulerSubmissionOutcome.REJECTED),
        (0, "not-a-job", SchedulerSubmissionOutcome.UNCERTAIN),
    ],
)
def test_openpbs_classifies_submission_outcomes(
    exit_code: int,
    stdout: str,
    outcome: SchedulerSubmissionOutcome,
) -> None:
    scheduler = OpenPBSScheduler(FakeTransport([(exit_code, stdout)]))

    with pytest.raises(PBSSubmissionError) as caught:
        scheduler.submit(SchedulerGroup((_unit(),)))

    assert caught.value.outcome is outcome


def test_query_maps_json_history_and_log_metadata() -> None:
    payload = {
        "Jobs": {
            "42[0].server": {
                "job_state": "F",
                "Exit_status": 0,
                "exec_host": "compute1/0",
            },
            "42[1].server": {"job_state": "F", "Exit_status": 7},
        }
    }
    scheduler = OpenPBSScheduler(
        FakeTransport([(0, json.dumps(payload))]),
        log_directory=PurePosixPath("/work/logs"),
    )
    references = (
        SchedulerReference("42[0].server"),
        SchedulerReference("42[1].server"),
    )

    first, second = scheduler.query(references)

    assert first.state is ExecutionState.SUCCEEDED
    assert first.metadata["node_list"] == "compute1/0"
    assert first.metadata["stdout_path"] == "/work/logs/42[0].server.stdout"
    assert second.state is ExecutionState.FAILED
    assert second.exit_code == 7


def test_cancel_returns_durable_deletion_request() -> None:
    scheduler = OpenPBSScheduler(FakeTransport([(0, "")]))
    reference = SchedulerReference("42.server")

    observation = scheduler.cancel((reference,))[0]

    assert observation.state is ExecutionState.CANCELLED
    assert observation.native_state == "DELETION_REQUESTED"
