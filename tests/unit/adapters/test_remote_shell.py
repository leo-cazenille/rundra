from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from rundra.adapters._remote_shell import (
    RemoteShellSerializationError,
    serialize_remote_command,
)
from rundra.domain.models import Command


def test_serialize_remote_command_builds_one_shell_boundary() -> None:
    command = Command(
        ("python", "script.py", "value with spaces"),
        environment={"MODE": "test value"},
        working_directory=PurePosixPath("/remote/work tree"),
    )

    serialized = serialize_remote_command(command)

    assert serialized == (
        "cd -- '/remote/work tree' && exec env -- "
        "'MODE=test value' python script.py 'value with spaces'"
    )


def test_serialize_remote_command_rejects_non_commands() -> None:
    with pytest.raises(TypeError, match="requires a Command"):
        serialize_remote_command(object())  # type: ignore[arg-type]


def test_ssh_specific_error_is_kept_out_of_the_reusable_boundary() -> None:
    command = Command(("program", "bad\x00argument"))

    with pytest.raises(RemoteShellSerializationError, match="command argument"):
        serialize_remote_command(command)
