from __future__ import annotations

import pytest

from rundra.cli.main import CLIUsageError, build_parser, main
from rundra.cli.operations import LAST_RUN_SELECTOR


def test_help_lists_common_workflow_and_registered_commands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(("help",)) == 0

    output = capsys.readouterr().out
    assert "Common workflow:" in output
    assert "rundr submit EXPERIMENT" in output
    assert "cancel active scheduler work for a Run" in output
    assert "fetch" in output
    assert "version" in output
    assert "await" in output
    assert "Run 'rundr help COMMAND'" in output


def test_help_topic_uses_command_parser_details(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(("help", "fetch")) == 0

    output = capsys.readouterr().out
    assert "usage: rundr fetch" in output
    assert "--destination" in output
    assert "--progress" in output


def test_lifecycle_command_accepts_last_run_selector() -> None:
    arguments = build_parser().parse_args(("wait", "--last"))

    assert arguments.run_id == LAST_RUN_SELECTOR


def test_lifecycle_command_accepts_explicit_run_id() -> None:
    run_id = "run_0123456789abcdef0123456789abcdef"
    arguments = build_parser().parse_args(("wait", run_id))

    assert arguments.run_id == run_id


def test_await_accepts_aggregate_wait_options() -> None:
    arguments = build_parser().parse_args(
        (
            "await",
            "run_11111111111111111111111111111111",
            "run_22222222222222222222222222222222",
            "--until",
            "any",
            "--timeout",
            "30",
            "--poll-interval",
            "5",
            "--fail-on-run-failure",
            "--notify-file",
            "/tmp/rundra-await.json",
        )
    )

    assert len(arguments.run_ids) == 2
    assert arguments.until == "any"
    assert arguments.timeout == 30
    assert arguments.poll_interval == 5
    assert arguments.fail_on_run_failure is True


def test_wait_accepts_agent_efficient_feedback_options() -> None:
    run_id = "run_0123456789abcdef0123456789abcdef"
    arguments = build_parser().parse_args(
        ("wait", run_id, "--notify", "--progress-interval", "30")
    )

    assert arguments.notify is True
    assert arguments.progress_interval == 30

    with pytest.raises(CLIUsageError):
        build_parser().parse_args(("wait", run_id, "--progress-interval", "0"))


def test_resume_accepts_last_run_selector() -> None:
    arguments = build_parser().parse_args(("resume", "--last"))

    assert arguments.run_id == LAST_RUN_SELECTOR


def test_resolve_submission_requires_explicit_confirmation() -> None:
    run_id = "run_0123456789abcdef0123456789abcdef"
    arguments = build_parser().parse_args(
        (
            "resolve-submission",
            run_id,
            "--not-submitted",
            "--confirm",
            run_id,
        )
    )

    assert arguments.run_id == run_id
    assert arguments.not_submitted is True
    assert arguments.confirm == run_id


def test_list_accepts_pagination_and_explicit_task_expansion() -> None:
    arguments = build_parser().parse_args(
        ("list", "--offset", "20", "--limit", "10", "--include-tasks")
    )

    assert arguments.offset == 20
    assert arguments.limit == 10
    assert arguments.include_tasks is True
