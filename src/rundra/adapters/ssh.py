from __future__ import annotations

import shutil

from rundra.ports import CapabilityCheck


class SSHTransportError(RuntimeError):
    """Base class for actionable OpenSSH transport failures."""


class SSHUnavailableError(SSHTransportError):
    """Raised when the configured OpenSSH client cannot be discovered."""


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
