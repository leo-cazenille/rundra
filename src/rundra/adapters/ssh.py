from __future__ import annotations

import shlex
import shutil
import subprocess
from datetime import UTC, datetime

from rundra.domain.models import Command
from rundra.ports import CapabilityCheck, CommandResult


class SSHTransportError(RuntimeError):
    """Base class for actionable OpenSSH transport failures."""


class SSHUnavailableError(SSHTransportError):
    """Raised when the configured OpenSSH client cannot be discovered."""


class SSHCommandError(SSHTransportError):
    """Raised when a remote command cannot be represented safely."""


class SSHExecutionError(SSHTransportError):
    """Raised when the local OpenSSH process cannot be started."""


class SSHTransport:
    """Use the user's OpenSSH client configuration to reach one host alias."""

    def __init__(self, host: str, *, executable: str = "ssh") -> None:
        if type(host) is not str:
            raise TypeError("SSH host must be a string")
        if not host.strip():
            raise ValueError("SSH host must not be blank")
        if "\x00" in host:
            raise ValueError("SSH host must not contain NUL")
        if type(executable) is not str:
            raise TypeError("SSH executable must be a string")
        if not executable.strip():
            raise ValueError("SSH executable must not be blank")
        if "\x00" in executable:
            raise ValueError("SSH executable must not contain NUL")
        self._host = host
        self._executable = executable

    def check(self) -> CapabilityCheck:
        """Confirm that the configured OpenSSH client is discoverable."""
        try:
            resolved = shutil.which(self._executable)
        except OSError as error:
            raise SSHUnavailableError(
                f"Could not search for SSH executable {self._executable!r}: {error}"
            ) from error
        if resolved is None:
            raise SSHUnavailableError(
                f"SSH executable {self._executable!r} was not found on PATH"
            )
        return CapabilityCheck("ssh")

    def run(self, command: Command) -> CommandResult:
        """Run one command through OpenSSH and capture its textual output."""
        if type(command) is not Command:
            raise TypeError("SSHTransport.run requires a Command")
        remote_command = _remote_command(command)
        ssh_argv = (self._executable, "-T", "--", self._host, remote_command)
        started_at = datetime.now(UTC)
        try:
            completed = subprocess.run(
                ssh_argv,
                capture_output=True,
                check=False,
                encoding="utf-8",
                errors="replace",
                shell=False,
            )
        except (OSError, ValueError) as error:
            raise SSHExecutionError(
                "Could not start SSH executable "
                f"{self._executable!r} for host {self._host!r}: {error}"
            ) from error
        finished_at = datetime.now(UTC)
        return CommandResult(
            command=command,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            started_at=started_at,
            finished_at=finished_at,
        )


def _remote_command(command: Command) -> str:
    """Serialize one Command at OpenSSH's unavoidable remote-shell boundary."""
    arguments: list[str] = []
    if command.working_directory is not None:
        working_directory = str(command.working_directory)
        _validate_literal(working_directory, name="working directory")
        arguments.extend(("cd", "--", shlex.quote(working_directory), "&&"))
    arguments.extend(("exec", "env", "--"))
    for name, value in sorted(command.environment.items()):
        if not name or "=" in name or "\x00" in name:
            raise SSHCommandError("Remote environment variable name is invalid")
        _validate_literal(value, name="environment value")
        arguments.append(shlex.quote(f"{name}={value}"))
    for argument in command.argv:
        _validate_literal(argument, name="command argument")
        arguments.append(shlex.quote(argument))
    return " ".join(arguments)


def _validate_literal(value: str, *, name: str) -> None:
    if "\x00" in value:
        raise SSHCommandError(f"Remote {name} must not contain NUL")
