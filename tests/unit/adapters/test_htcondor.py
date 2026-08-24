from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pytest

from rundra.adapters.htcondor import (
    HTCondorScheduler,
    HTCondorScriptError,
    render_condor_array_submit,
    validate_htcondor_resources,
)
from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import Command, ResourceRequest, TaskId
from rundra.domain.states import ExecutionState
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerReference,
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


def _request() -> SchedulerArrayRequest:
    resources = ResourceRequest(
        cpus_per_task=2,
        memory_bytes=33 * 1024 * 1024,
        walltime=timedelta(seconds=61),
    )
    units = tuple(
        SchedulerUnit(
            TaskId.from_ordinal(index),
            Command(("python3", "simulate.py", "--seed", str(index))),
            resources,
        )
        for index in range(3)
    )
    return SchedulerArrayRequest(
        SchedulerGroup(units),
        tuple(
            ArrayTaskMapping(unit.task_id, index, index)
            for index, unit in enumerate(units)
        ),
        PurePosixPath("/shared/run/metadata/tasks.sh"),
        max_concurrent_jobs=2,
    )


def test_render_array_owns_resources_shared_io_and_materialization_limit() -> None:
    rendered = render_condor_array_submit(
        _request(), log_directory=PurePosixPath("/shared/run/logs")
    )

    assert "executable = /shared/run/metadata/tasks.sh" in rendered
    assert "should_transfer_files = NO" in rendered
    assert "transfer_executable = False" in rendered
    assert "request_cpus = 2" in rendered
    assert "request_memory = 33" in rendered
    assert "max_materialize = 2" in rendered
    assert rendered.endswith("queue 3\n")


def test_submit_array_maps_cluster_and_process_ids() -> None:
    transport = FakeTransport([(0, "91.0 - 91.2\n")])
    scheduler = HTCondorScheduler(
        transport, log_directory=PurePosixPath("/shared/run/logs")
    )

    submission = scheduler.submit_array(_request())

    assert submission.reference.native_id == "91"
    assert tuple(submission.task_native_ids.values()) == ("91.0", "91.1", "91.2")
    assert transport.commands[0].argv[-1] == "condor_submit"
    assert 'case "$1" in' in transport.commands[0].argv[5]


def test_query_uses_history_for_missing_active_job() -> None:
    completed = json.dumps(
        [
            {
                "ClusterId": 91,
                "ProcId": 2,
                "JobStatus": 4,
                "ExitCode": 0,
                "JobStartDate": 10,
                "CompletionDate": 20,
                "LastRemoteHost": "slot1@worker-2",
            }
        ]
    )
    transport = FakeTransport([(0, "[]"), (0, completed)])
    scheduler = HTCondorScheduler(transport)

    observation = scheduler.query((SchedulerReference("91.2"),))[0]

    assert observation.state is ExecutionState.SUCCEEDED
    assert observation.exit_code == 0
    assert observation.metadata["last_node"] == "slot1@worker-2"
    assert transport.commands[1].argv[0] == "condor_history"


def test_cancel_is_exact_and_returns_portable_state() -> None:
    transport = FakeTransport([(0, "Job 91.2 marked for removal")])
    scheduler = HTCondorScheduler(transport)

    observation = scheduler.cancel((SchedulerReference("91.2"),))[0]

    assert observation.state is ExecutionState.CANCELLED
    assert transport.commands[0].argv == ("condor_rm", "91.2")


@pytest.mark.parametrize(
    "native",
    [
        {"unknown": "value"},
        {"requirements": "True\nqueue 99"},
        {"requirements": "$(malicious)"},
        {"request_disk": "0GiB"},
        {"concurrency_limits": "name;rm"},
    ],
)
def test_native_options_reject_unowned_or_unsafe_submit_content(
    native: dict[str, object],
) -> None:
    with pytest.raises(HTCondorScriptError):
        validate_htcondor_resources(
            ResourceRequest(native={"htcondor": native})  # type: ignore[arg-type]
        )
