from __future__ import annotations

import pytest

from rundra.cli.main import build_parser, main
from rundra.cli.operations import LAST_RUN_SELECTOR


def test_help_lists_common_workflow_and_registered_commands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(("help",)) == 0

    output = capsys.readouterr().out
    assert "Common workflow:" in output
    assert "rundr submit EXPERIMENT" in output
    assert "fetch" in output
    assert "version" in output
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


def test_resume_accepts_last_run_selector() -> None:
    arguments = build_parser().parse_args(("resume", "--last"))

    assert arguments.run_id == LAST_RUN_SELECTOR
