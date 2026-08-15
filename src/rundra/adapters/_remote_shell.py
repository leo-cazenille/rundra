from __future__ import annotations

import shlex

from rundra.domain.models import Command


class RemoteShellSerializationError(ValueError):
    """Raised when a command cannot cross a POSIX remote-shell boundary."""


def serialize_remote_command(command: Command) -> str:
    """Serialize a Command once for an OpenSSH-style remote login shell."""
    if type(command) is not Command:
        raise TypeError("serialize_remote_command requires a Command")
    words: list[str] = []
    if command.working_directory is not None:
        working_directory = str(command.working_directory)
        _validate_literal(working_directory, kind="working directory")
        words.extend(("cd", "--", shlex.quote(working_directory), "&&"))
    words.extend(("exec", "env", "--"))
    for name, value in sorted(command.environment.items()):
        if not name or "=" in name or "\x00" in name:
            raise RemoteShellSerializationError(
                "Remote environment variable name is invalid"
            )
        _validate_literal(value, kind="environment value")
        words.append(shlex.quote(f"{name}={value}"))
    for argument in command.argv:
        _validate_literal(argument, kind="command argument")
        words.append(shlex.quote(argument))
    return " ".join(words)


def redacted_remote_command_summary(command: Command) -> str:
    """Describe command shape for diagnostics without exposing literal values."""
    if type(command) is not Command:
        raise TypeError("redacted_remote_command_summary requires a Command")
    working_directory = (
        "<redacted>" if command.working_directory is not None else "unset"
    )
    return (
        "remote command "
        f"(argv=<redacted:{len(command.argv)}>, "
        f"environment=<redacted:{len(command.environment)}>, "
        f"working_directory={working_directory})"
    )


def _validate_literal(value: str, *, kind: str) -> None:
    if "\x00" in value:
        raise RemoteShellSerializationError(f"Remote {kind} must not contain NUL")
