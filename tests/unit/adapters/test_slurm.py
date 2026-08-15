from __future__ import annotations

import subprocess
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pytest

from rundra.adapters import (
    SlurmCancellationError,
    SlurmQueryError,
    SlurmScheduler,
    SlurmScriptError,
    SlurmSubmissionError,
    render_sbatch_script,
)
from rundra.adapters.slurm import _portable_state
from rundra.domain.models import Command, ResourceRequest, TaskId
from rundra.domain.states import ExecutionState
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    Scheduler,
    SchedulerGroup,
    SchedulerReference,
    SchedulerUnit,
)


class ScriptedTransport:
    def __init__(self, results: deque[CommandResult | Exception]) -> None:
        self.results = results
        self.run_calls: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("scripted")

    def run(self, command: Command) -> CommandResult:
        self.run_calls.append(command)
        outcome = self.results.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _group(
    *,
    command: Command | None = None,
    resources: ResourceRequest | None = None,
    ordinal: int = 0,
) -> SchedulerGroup:
    return SchedulerGroup(
        (
            SchedulerUnit(
                TaskId.from_ordinal(ordinal),
                command or Command(("python3", "experiment.py")),
                resources or ResourceRequest(),
            ),
        )
    )


def test_render_sbatch_script_translates_portable_and_allowed_native_resources() -> (
    None
):
    script = render_sbatch_script(
        _group(
            resources=ResourceRequest(
                nodes=2,
                tasks=4,
                cpus_per_task=8,
                gpus_per_task=1,
                memory_bytes=16 * 1024**3,
                walltime=timedelta(days=1, hours=2, minutes=3, seconds=4),
                native={
                    "slurm": {
                        "partition": "gpu",
                        "account": "science",
                        "qos": "normal",
                        "constraint": "a100",
                        "exclusive": True,
                    }
                },
            )
        )
    )

    assert (
        script
        == """\
#!/bin/sh
#SBATCH --job-name=rundra-task_000000
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=8
#SBATCH --gpus-per-task=1
#SBATCH --mem=16384M
#SBATCH --time=1-02:03:04
#SBATCH --account=science
#SBATCH --constraint=a100
#SBATCH --partition=gpu
#SBATCH --qos=normal
#SBATCH --exclusive

set -eu
exec env -- python3 experiment.py
"""
    )


def test_rendered_script_preserves_hostile_command_literals_without_execution(
    tmp_path: PurePosixPath,
) -> None:
    marker = tmp_path / "not-created"
    literal = f"spaces; $(touch {marker}); 'quotes' and *"
    script = render_sbatch_script(
        _group(
            command=Command(
                ("printf", "%s\\n", literal),
                environment={"LITERAL": literal},
                working_directory=tmp_path,
            )
        )
    )

    completed = subprocess.run(
        ("/bin/sh", "-c", script),
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == f"{literal}\n"
    assert not marker.exists()


def test_resource_rounding_never_requests_less_than_the_portable_value() -> None:
    script = render_sbatch_script(
        _group(
            resources=ResourceRequest(
                memory_bytes=1024**2 + 1,
                walltime=timedelta(seconds=1, microseconds=1),
                native={"slurm": {"exclusive": False}},
            )
        )
    )

    assert "#SBATCH --mem=2M" in script
    assert "#SBATCH --time=00:00:02" in script
    assert "--exclusive" not in script


def test_sbatch_script_uses_explicit_normalized_log_paths() -> None:
    script = render_sbatch_script(
        _group(),
        stdout_path=PurePosixPath("/remote/logs/%j.stdout"),
        stderr_path=PurePosixPath("/remote/logs/%j.stderr"),
    )

    assert "#SBATCH --output=/remote/logs/%j.stdout" in script
    assert "#SBATCH --error=/remote/logs/%j.stderr" in script


@pytest.mark.parametrize(
    ("stdout", "stderr"),
    [
        (PurePosixPath("relative.out"), PurePosixPath("/logs/error")),
        (PurePosixPath("/logs/with space"), PurePosixPath("/logs/error")),
        (PurePosixPath("/logs/output"), None),
    ],
)
def test_sbatch_log_paths_must_be_complete_absolute_and_directive_safe(
    stdout: PurePosixPath, stderr: PurePosixPath | None
) -> None:
    with pytest.raises(SlurmScriptError, match="path|provided together"):
        render_sbatch_script(_group(), stdout_path=stdout, stderr_path=stderr)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"nodes": 4}, "Unsupported"),
        ({"output": "stolen"}, "Unsupported"),
        ({"reservation": "special"}, "Unsupported"),
        ({"partition": "gpu\n#SBATCH --nodes=99"}, "unsafe"),
        ({"qos": True}, "string or integer"),
        ({"exclusive": "yes"}, "boolean"),
    ],
)
def test_native_slurm_options_are_explicitly_allowlisted(
    options: dict[str, object], message: str
) -> None:
    resources = ResourceRequest(native={"slurm": options})  # type: ignore[arg-type]

    with pytest.raises(SlurmScriptError, match=message):
        render_sbatch_script(_group(resources=resources))


def test_m3_renderer_rejects_multi_task_groups_and_non_groups() -> None:
    first = _group().units[0]
    second = _group(ordinal=1).units[0]

    with pytest.raises(SlurmScriptError, match="exactly one Task"):
        render_sbatch_script(SchedulerGroup((first, second)))
    with pytest.raises(TypeError, match="SchedulerGroup"):
        render_sbatch_script(object())  # type: ignore[arg-type]


def _command_result(
    command: Command, exit_code: int, stdout: str, stderr: str = ""
) -> CommandResult:
    now = datetime(2026, 8, 15, tzinfo=UTC)
    return CommandResult(command, exit_code, stdout, stderr, now, now)


def test_slurm_scheduler_submits_generated_script_with_parsable_output() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(transport, sbatch="/opt/slurm/bin/sbatch")
    group = _group()
    expected_script = render_sbatch_script(group)
    expected_command = Command(
        (
            "/bin/sh",
            "-c",
            "set -eu\n"
            'script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")\n'
            "trap 'rm -f \"$script\"' EXIT HUP INT TERM\n"
            'printf \'%s\' "$1" > "$script"\n'
            '"$2" --parsable "$script"\n',
            "rundra-slurm-submit",
            expected_script,
            "/opt/slurm/bin/sbatch",
        )
    )
    transport.results.append(_command_result(expected_command, 0, "12345;alpha\n"))

    submission = scheduler.submit(group)

    assert isinstance(scheduler, Scheduler)
    assert submission.reference.native_id == "12345"
    assert submission.task_native_ids == {group.units[0].task_id: "12345"}
    assert transport.run_calls == [expected_command]


def test_slurm_submission_creates_and_uses_configured_log_directory() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        timezone=UTC,
        log_directory=PurePosixPath("/remote/work/.scheduler-logs"),
    )
    transport.results.append(_command_result(Command(("unused",)), 0, "12345\n"))

    scheduler.submit(_group())

    command = transport.run_calls[0]
    assert "mkdir -p" in command.argv[2]
    assert command.argv[-1] == "/remote/work/.scheduler-logs"
    assert "#SBATCH --output=/remote/work/.scheduler-logs/%j.stdout" in command.argv[4]
    assert "#SBATCH --error=/remote/work/.scheduler-logs/%j.stderr" in command.argv[4]


@pytest.mark.parametrize(
    ("exit_code", "stdout", "stderr", "message"),
    [
        (1, "", "invalid account", "exit code 1: invalid account"),
        (0, "Submitted batch job 123", "", "invalid parsable"),
        (0, "123\nextra", "", "invalid parsable"),
        (0, "123;cluster;extra", "", "invalid parsable"),
    ],
)
def test_slurm_submission_rejects_failures_and_nonparsable_output(
    exit_code: int, stdout: str, stderr: str, message: str
) -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(transport)
    group = _group()
    expected = Command(
        (
            "/bin/sh",
            "-c",
            "set -eu\n"
            'script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")\n'
            "trap 'rm -f \"$script\"' EXIT HUP INT TERM\n"
            'printf \'%s\' "$1" > "$script"\n'
            '"$2" --parsable "$script"\n',
            "rundra-slurm-submit",
            render_sbatch_script(group),
            "sbatch",
        )
    )
    transport.results.append(_command_result(expected, exit_code, stdout, stderr))

    with pytest.raises(SlurmSubmissionError, match=message):
        scheduler.submit(group)


def test_slurm_submission_normalizes_transport_start_failure() -> None:
    transport = ScriptedTransport(deque([RuntimeError("connection lost")]))

    with pytest.raises(SlurmSubmissionError, match="Could not start"):
        SlurmScheduler(transport).submit(_group())


def test_slurm_query_combines_queue_and_accounting_in_request_order() -> None:
    first = SchedulerReference("123")
    second = SchedulerReference("456")
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        timezone=UTC,
        log_directory=PurePosixPath("/remote/work/.scheduler-logs"),
    )
    squeue_command = Command(
        (
            "squeue",
            "--noheader",
            "--jobs",
            "123,456",
            "--format",
            "%i|%T|%S|%N",
        )
    )
    sacct_command = Command(
        (
            "sacct",
            "--noheader",
            "--parsable2",
            "--jobs",
            "456",
            "--format",
            "JobIDRaw,State%32,ExitCode,Start,End,NodeList",
        )
    )
    transport.results.extend(
        (
            _command_result(
                squeue_command,
                0,
                "123|RUNNING|2026-08-15T10:01:02|node[01-02]\n",
            ),
            _command_result(
                sacct_command,
                0,
                "456|OUT_OF_MEMORY|137:0|2026-08-15T09:00:00|"
                "2026-08-15T09:01:00|node03|\n"
                "456.batch|FAILED|137:0|2026-08-15T09:00:00|"
                "2026-08-15T09:01:00|node03|\n",
            ),
        )
    )

    observations = scheduler.query((first, second))

    assert [item.reference for item in observations] == [first, second]
    assert observations[0].state is ExecutionState.RUNNING
    assert observations[0].native_state == "RUNNING"
    assert observations[0].started_at == datetime(2026, 8, 15, 10, 1, 2, tzinfo=UTC)
    assert observations[0].metadata == {
        "source": "squeue",
        "allocated_nodes": "node[01-02]",
        "native_start": "2026-08-15T10:01:02",
        "stdout_path": "/remote/work/.scheduler-logs/123.stdout",
        "stderr_path": "/remote/work/.scheduler-logs/123.stderr",
    }
    assert observations[1].state is ExecutionState.FAILED
    assert observations[1].native_state == "OUT_OF_MEMORY"
    assert observations[1].exit_code == 137
    assert observations[1].finished_at == datetime(2026, 8, 15, 9, 1, tzinfo=UTC)
    assert transport.run_calls == [squeue_command, sacct_command]


def test_naive_slurm_timestamps_are_not_fabricated_without_site_timezone() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(
                    Command(("unused",)),
                    0,
                    "123|RUNNING|2026-08-15T10:01:02|node01\n",
                )
            ]
        )
    )

    observation = SlurmScheduler(transport).query((reference,))[0]

    assert observation.started_at is None
    assert observation.metadata["native_start"] == "2026-08-15T10:01:02"


def test_slurm_query_marks_accounting_lag_explicitly() -> None:
    reference = SchedulerReference("789")
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(transport)
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, ""),
            _command_result(Command(("unused",)), 0, ""),
        )
    )

    observation = scheduler.query((reference,))[0]

    assert observation.state is ExecutionState.UNKNOWN
    assert observation.native_state == "ACCOUNTING_PENDING"
    assert observation.metadata == {"accounting_pending": True}


@pytest.mark.parametrize(
    ("native", "exit_code", "expected"),
    [
        ("PENDING", None, ExecutionState.QUEUED),
        ("CONFIGURING", None, ExecutionState.QUEUED),
        ("RUNNING", None, ExecutionState.RUNNING),
        ("COMPLETING", None, ExecutionState.RUNNING),
        ("COMPLETED", 0, ExecutionState.SUCCEEDED),
        ("COMPLETED", 1, ExecutionState.FAILED),
        ("FAILED", 1, ExecutionState.FAILED),
        ("OUT_OF_MEMORY", 137, ExecutionState.FAILED),
        ("TIMEOUT", 0, ExecutionState.FAILED),
        ("PREEMPTED", 0, ExecutionState.FAILED),
        ("CANCELLED by 42", 0, ExecutionState.CANCELLED),
        ("FUTURE_STATE", None, ExecutionState.UNKNOWN),
    ],
)
def test_slurm_native_states_have_comprehensive_portable_mapping(
    native: str, exit_code: int | None, expected: ExecutionState
) -> None:
    assert _portable_state(native, exit_code) is expected


@pytest.mark.parametrize(
    ("results", "message"),
    [
        ((1, "", "permission denied"), "squeue failed"),
        ((0, "malformed", ""), "malformed row"),
    ],
)
def test_slurm_query_rejects_command_and_parse_failures(
    results: tuple[int, str, str], message: str
) -> None:
    exit_code, stdout, stderr = results
    transport = ScriptedTransport(
        deque([_command_result(Command(("unused",)), exit_code, stdout, stderr)])
    )

    with pytest.raises(SlurmQueryError, match=message):
        SlurmScheduler(transport).query((SchedulerReference("123"),))


def test_slurm_query_validates_reference_collection() -> None:
    scheduler = SlurmScheduler(ScriptedTransport(deque([])))
    reference = SchedulerReference("123")

    assert scheduler.query(()) == ()
    with pytest.raises(ValueError, match="unique"):
        scheduler.query((reference, reference))
    with pytest.raises(ValueError, match="numeric"):
        scheduler.query((SchedulerReference("local-1"),))


def test_slurm_cancel_requests_then_reconciles_state() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(Command(("unused",)), 0, ""),
                _command_result(Command(("unused",)), 0, "123|CANCELLED|N/A|(null)\n"),
            ]
        )
    )

    observation = SlurmScheduler(transport).cancel((reference,))[0]

    assert observation.state is ExecutionState.CANCELLED
    assert transport.run_calls[0] == Command(("scancel", "--", "123"))


def test_slurm_cancel_treats_terminal_invalid_job_race_as_success() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(Command(("unused",)), 1, "", "invalid job id"),
                _command_result(Command(("unused",)), 0, ""),
                _command_result(
                    Command(("unused",)),
                    0,
                    "123|COMPLETED|0:0|2026-08-15T10:00:00|"
                    "2026-08-15T10:01:00|node01|\n",
                ),
            ]
        )
    )

    observation = SlurmScheduler(transport).cancel((reference,))[0]

    assert observation.state is ExecutionState.SUCCEEDED


def test_slurm_cancel_reports_nonzero_when_job_remains_active() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(Command(("unused",)), 1, "", "permission denied"),
                _command_result(
                    Command(("unused",)),
                    0,
                    "123|RUNNING|2026-08-15T10:00:00|node01\n",
                ),
            ]
        )
    )

    with pytest.raises(SlurmCancellationError, match="permission denied"):
        SlurmScheduler(transport).cancel((reference,))
