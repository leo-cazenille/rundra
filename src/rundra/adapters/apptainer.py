from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import PurePath

from rundra.domain.models import Command
from rundra.ports import BindMount, CapabilityCheck, ContainerRequest, Transport

_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_RESERVED_ENVIRONMENT_NAMES = frozenset(
    {
        "APPEND_LD_LIBRARY_PATH",
        "APPEND_PATH",
        "PREPEND_LD_LIBRARY_PATH",
        "PREPEND_PATH",
    }
)
_MAX_VERSION_LENGTH = 256


class ApptainerRuntimeError(RuntimeError):
    """Base class for actionable Apptainer runtime failures."""


class ApptainerUnavailableError(ApptainerRuntimeError):
    """Raised when the configured Apptainer-compatible executable is unavailable."""


class ApptainerConfigurationError(ApptainerRuntimeError):
    """Raised when a request cannot be represented safely as Apptainer argv."""


class ApptainerRuntime:
    """Validate availability and construct shell-free Apptainer exec commands."""

    def __init__(self, executable: str = "apptainer") -> None:
        if type(executable) is not str:
            raise TypeError("Apptainer executable must be a string")
        if not executable.strip():
            raise ValueError("Apptainer executable must not be blank")
        if "\x00" in executable:
            raise ValueError("Apptainer executable must not contain NUL")
        self._executable = executable

    def check(self) -> CapabilityCheck:
        """Confirm the configured executable is discoverable without running it."""
        try:
            resolved = shutil.which(self._executable)
        except OSError as error:
            raise ApptainerUnavailableError(
                f"Could not search for Apptainer executable {self._executable!r}: {error}"
            ) from error
        if resolved is None:
            raise ApptainerUnavailableError(
                f"Apptainer executable {self._executable!r} was not found on PATH"
            )
        return CapabilityCheck("apptainer")

    def identity(self) -> CapabilityCheck:
        """Execute one bounded version probe for an actual Run or submission."""
        try:
            resolved = shutil.which(self._executable)
            if resolved is None:
                raise ApptainerUnavailableError(
                    f"Apptainer executable {self._executable!r} was not found on PATH"
                )
            completed = subprocess.run(
                (resolved, "version"),
                capture_output=True,
                check=False,
                encoding="utf-8",
                errors="replace",
                shell=False,
                timeout=10.0,
            )
        except ApptainerUnavailableError:
            raise
        except (OSError, subprocess.SubprocessError, ValueError) as error:
            raise ApptainerUnavailableError(
                "Could not determine the local Apptainer runtime version"
            ) from error
        if completed.returncode != 0:
            raise ApptainerUnavailableError(
                "Could not determine the local Apptainer runtime version"
            )
        return CapabilityCheck("apptainer", _version_line(completed.stdout))

    def build_command(self, request: ContainerRequest) -> Command:
        """Construct an Apptainer exec argument vector without executing it."""
        if type(request) is not ContainerRequest:
            raise TypeError(
                "ApptainerRuntime.build_command requires a ContainerRequest"
            )
        if request.image is None:
            raise ApptainerConfigurationError(
                "Apptainer execution requires a container image"
            )
        image = str(request.image)
        _validate_argument(image, name="container image")
        if image.startswith("-"):
            raise ApptainerConfigurationError(
                "Container image must not look like an Apptainer option"
            )
        _validate_command(request.command)
        for bind in request.binds:
            _validate_bind(bind)

        argv: list[str] = [
            self._executable,
            "exec",
            "--cleanenv",
            "--no-eval",
        ]
        if request.gpu:
            argv.append("--nv")
        for bind in request.binds:
            mode = "ro" if bind.read_only else "rw"
            argv.extend(
                (
                    "--bind",
                    f"{bind.source}:{bind.destination}:{mode}",
                )
            )
        if request.command.working_directory is not None:
            argv.extend(("--cwd", str(request.command.working_directory)))
        argv.append(image)
        argv.extend(request.command.argv)
        environment_prefix = (
            "SINGULARITYENV_"
            if PurePath(self._executable).name == "singularity"
            else "APPTAINERENV_"
        )
        environment = {
            f"{environment_prefix}{name}": value
            for name, value in sorted(request.command.environment.items())
        }
        return Command(tuple(argv), environment=environment)


class RemoteApptainerRuntime:
    """Check Apptainer through a Transport while sharing pure command construction."""

    def __init__(self, transport: Transport, executable: str = "apptainer") -> None:
        if not isinstance(transport, Transport):
            raise TypeError("RemoteApptainerRuntime requires a Transport")
        self._transport = transport
        self._runtime = ApptainerRuntime(executable)
        self._executable = executable

    def check(self) -> CapabilityCheck:
        command = Command(
            (
                "/bin/sh",
                "-c",
                'command -v -- "$1" >/dev/null 2>&1',
                "rundra-apptainer-check",
                self._executable,
            )
        )
        try:
            result = self._transport.run(command)
        except Exception as error:
            raise ApptainerUnavailableError(
                "Could not check the remote Apptainer executable"
            ) from error
        if result.exit_code != 0:
            raise ApptainerUnavailableError(
                f"Apptainer executable {self._executable!r} was not found remotely"
            )
        return CapabilityCheck("apptainer")

    def identity(self) -> CapabilityCheck:
        """Read the target runtime version without exposing transport diagnostics."""
        try:
            result = self._transport.run(Command((self._executable, "version")))
        except Exception as error:
            raise ApptainerUnavailableError(
                "Could not determine the remote Apptainer runtime version"
            ) from error
        if result.exit_code != 0:
            raise ApptainerUnavailableError(
                "Could not determine the remote Apptainer runtime version"
            )
        return CapabilityCheck("apptainer", _version_line(result.stdout))

    def build_command(self, request: ContainerRequest) -> Command:
        return self._runtime.build_command(request)


def _version_line(stdout: str) -> str:
    lines = tuple(line.strip() for line in stdout.splitlines() if line.strip())
    if len(lines) != 1 or len(lines[0]) > _MAX_VERSION_LENGTH or "\x00" in lines[0]:
        raise ApptainerUnavailableError(
            "Apptainer runtime returned an invalid version identifier"
        )
    return lines[0]


def _validate_command(command: Command) -> None:
    for index, argument in enumerate(command.argv):
        _validate_argument(argument, name=f"payload argument {index}")
    if command.working_directory is not None:
        working_directory = str(command.working_directory)
        _validate_argument(working_directory, name="container working directory")
        if not command.working_directory.is_absolute():
            raise ApptainerConfigurationError(
                "Container working directory must be absolute"
            )
    for name, value in command.environment.items():
        if _ENVIRONMENT_NAME.fullmatch(name) is None:
            raise ApptainerConfigurationError(
                f"Container environment variable name is invalid: {name!r}"
            )
        if name in _RESERVED_ENVIRONMENT_NAMES:
            raise ApptainerConfigurationError(
                "Container environment variable conflicts with an "
                f"APPTAINERENV_ path transformer: {name!r}"
            )
        _validate_argument(value, name=f"environment variable {name}")


def _validate_bind(bind: BindMount) -> None:
    source = str(bind.source)
    destination = str(bind.destination)
    for value, name in (
        (source, "bind source"),
        (destination, "bind destination"),
    ):
        _validate_argument(value, name=name)
        if ":" in value or "," in value:
            raise ApptainerConfigurationError(
                f"Apptainer {name} cannot contain ':' or ',': {value!r}"
            )
    if not bind.source.is_absolute() or not bind.destination.is_absolute():
        raise ApptainerConfigurationError(
            "Apptainer bind source and destination must both be absolute"
        )


def _validate_argument(value: str, *, name: str) -> None:
    if "\x00" in value:
        raise ApptainerConfigurationError(f"{name} must not contain NUL")
