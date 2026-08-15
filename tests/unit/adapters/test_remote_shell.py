from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path, PurePosixPath

import pytest
import yaml

from rundra.adapters._remote_shell import (
    RemoteShellSerializationError,
    serialize_remote_command,
)
from rundra.config.experiments import load_experiment
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


def test_remote_shell_round_trips_hostile_literals_without_interpretation(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "should-not-exist"
    working_directory = tmp_path / "work 'quoted'; $(literal)\nline"
    working_directory.mkdir()
    hostile_arguments = (
        "value with spaces",
        "single'quote and \"double quote",
        f"$(touch {marker})",
        f"`touch {marker}`",
        f"; touch {marker}",
        "line one\nline two",
        "wildcards * ? [abc]",
        r"backslash\and\\more",
        "--looks-like-an-option",
    )
    environment = {
        "EMPTY": "",
        "MULTILINE": "first\nsecond",
        "ODD-NAME": f"$HOME; $(touch {marker}) 'quoted'",
    }
    probe = (
        "import json, os, pathlib, sys; "
        "print(json.dumps({'argv': sys.argv[1:], "
        "'cwd': str(pathlib.Path.cwd()), "
        "'environment': {name: os.environ[name] for name in "
        "('EMPTY', 'MULTILINE', 'ODD-NAME')}}, sort_keys=True))"
    )
    command = Command(
        (sys.executable, "-c", probe, *hostile_arguments),
        environment=environment,
        working_directory=PurePosixPath(working_directory),
    )

    completed = subprocess.run(
        ("/bin/sh", "-c", serialize_remote_command(command)),
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="strict",
        shell=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "argv": list(hostile_arguments),
        "cwd": str(working_directory),
        "environment": environment,
    }
    assert not marker.exists()


def test_remote_shell_quotes_hostile_values_loaded_from_yaml(tmp_path: Path) -> None:
    marker = tmp_path / "yaml-should-not-exist"
    experiment_path = tmp_path / "experiment.yaml"
    literal = f"$(touch {marker}); 'quoted'\nnext line"
    experiment_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "experiment": {"name": "hostile-literals"},
                "command": {
                    "argv": ["printf", "%s\\n", literal],
                    "environment": {"LITERAL_VALUE": literal},
                    "working_directory": str(tmp_path),
                },
                "resources": {},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    command = load_experiment(experiment_path).command

    completed = subprocess.run(
        ("/bin/sh", "-c", serialize_remote_command(command)),
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="strict",
        shell=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == f"{literal}\n"
    assert not marker.exists()


def test_remote_shell_serialization_orders_environment_deterministically() -> None:
    command = Command(("program",), environment={"Z_LAST": "z", "A_FIRST": "a"})

    assert serialize_remote_command(command) == (
        "exec env -- A_FIRST=a Z_LAST=z program"
    )


@pytest.mark.parametrize(
    ("command", "message"),
    [
        (Command(("program", "bad\x00argument")), "command argument"),
        (Command(("program",), environment={"": "value"}), "variable name"),
        (Command(("program",), environment={"BAD=NAME": "value"}), "variable name"),
        (Command(("program",), environment={"BAD\x00NAME": "value"}), "variable name"),
        (Command(("program",), environment={"NAME": "bad\x00value"}), "value"),
        (
            Command(
                ("program",),
                working_directory=PurePosixPath("/bad\x00directory"),
            ),
            "working directory",
        ),
    ],
)
def test_remote_shell_rejects_only_unrepresentable_literals(
    command: Command,
    message: str,
) -> None:
    with pytest.raises(RemoteShellSerializationError, match=message):
        serialize_remote_command(command)
