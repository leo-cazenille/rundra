from __future__ import annotations

import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import PurePath

from rundra.adapters._remote_shell import (
    RemoteShellSerializationError,
    redacted_remote_command_summary,
    serialize_remote_command,
)
from rundra.domain.models import Command
from rundra.ports import CapabilityCheck, CommandResult
from rundra.security import is_safe_ssh_destination


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

    def __init__(
        self, host: str, *, executable: str = "ssh", config_file: PurePath | None = None
    ) -> None:
        if type(host) is not str:
            raise TypeError("SSH host must be a string")
        if not host.strip():
            raise ValueError("SSH host must not be blank")
        if not is_safe_ssh_destination(host):
            raise ValueError("SSH host must be a safe host alias or user@host")
        if type(executable) is not str:
            raise TypeError("SSH executable must be a string")
        if not executable.strip():
            raise ValueError("SSH executable must not be blank")
        if "\x00" in executable:
            raise ValueError("SSH executable must not contain NUL")
        if config_file is not None and not isinstance(config_file, PurePath):
            raise TypeError("SSH config_file must be a path or None")
        if config_file is not None and (
            not config_file.is_absolute()
            or config_file == PurePath("/")
            or "\x00" in str(config_file)
        ):
            raise ValueError("SSH config_file must be an absolute non-root path")
        self._host = host
        self._executable = executable
        self._config_file = config_file

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
        try:
            remote_command = serialize_remote_command(command)
        except RemoteShellSerializationError as error:
            raise SSHCommandError(str(error)) from error
        ssh_argv = (
            self._executable,
            *(("-F", str(self._config_file)) if self._config_file is not None else ()),
            "-T",
            "--",
            self._host,
            remote_command,
        )
        started_at = datetime.now(UTC)
        completed: subprocess.CompletedProcess[str] | None = None
        failure_detail: str | None = None
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
            failure_detail = _safe_failure_detail(error)
        if failure_detail is not None:
            summary = redacted_remote_command_summary(command)
            raise SSHExecutionError(
                "Could not start SSH executable "
                f"{self._executable!r} for host {self._host!r} while executing "
                f"{summary}: {failure_detail}"
            ) from None
        if completed is None:  # pragma: no cover - defensive subprocess boundary
            raise SSHExecutionError("SSH subprocess returned no result")
        finished_at = datetime.now(UTC)
        return CommandResult(
            command=command,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            started_at=started_at,
            finished_at=finished_at,
        )


def _safe_failure_detail(error: OSError | ValueError) -> str:
    detail = type(error).__name__
    if isinstance(error, OSError) and error.errno is not None:
        detail = f"{detail} (errno {error.errno})"
    return detail
