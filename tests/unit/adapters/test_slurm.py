from __future__ import annotations

import subprocess
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pytest

from rundra.adapters import (
    SlurmScheduler,
    SlurmScriptError,
    SlurmSubmissionError,
    render_sbatch_script,
)
from rundra.domain.models import Command, ResourceRequest, TaskId
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    Scheduler,
    SchedulerGroup,
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
