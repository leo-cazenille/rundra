from __future__ import annotations

import pytest

from rundra.cli.main import main


def test_help_lists_common_workflow_and_registered_commands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(("help",)) == 0

    output = capsys.readouterr().out
    assert "Common workflow:" in output
    assert "rundr submit EXPERIMENT" in output
    assert "fetch" in output
    assert "Run 'rundr help COMMAND'" in output


def test_help_topic_uses_command_parser_details(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(("help", "fetch")) == 0

    output = capsys.readouterr().out
    assert "usage: rundr fetch" in output
    assert "--destination" in output
    assert "--progress" in output
