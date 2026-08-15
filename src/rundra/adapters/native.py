from __future__ import annotations

import re
from pathlib import PurePath

from rundra.domain.models import Command
from rundra.ports import CapabilityCheck, ContainerRequest


class NativeRuntimeError(RuntimeError):
    """Raised when container-only semantics are requested from native execution."""


class NativeRuntime:
    """Map semantic staged paths to the host without starting a container."""

    def check(self) -> CapabilityCheck:
        """Report the explicit no-container runtime capability."""
        return CapabilityCheck("native")

    def build_command(self, request: ContainerRequest) -> Command:
        """Construct a host command from the same staged semantic request."""
        if type(request) is not ContainerRequest:
            raise TypeError("NativeRuntime.build_command requires a ContainerRequest")
        if request.image is not None:
            raise NativeRuntimeError("NativeRuntime cannot use a container image")
        if request.gpu:
            raise NativeRuntimeError(
                "NativeRuntime cannot promise container GPU passthrough"
            )
        replacements = tuple(
            sorted(
                ((str(bind.destination), str(bind.source)) for bind in request.binds),
                key=lambda item: len(item[0]),
                reverse=True,
            )
        )
        working_directory = request.command.working_directory
        if working_directory is None:
            raise NativeRuntimeError(
                "Native execution requires a staged semantic working directory"
            )
        mapped = _map_value(str(working_directory), replacements)
        if mapped == str(working_directory):
            raise NativeRuntimeError(
                "Native working directory must be inside a staged semantic path"
            )
        mapped_working_directory = PurePath(mapped)
        return Command(
            tuple(
                _map_value(argument, replacements) for argument in request.command.argv
            ),
            environment={
                name: _map_value(value, replacements)
                for name, value in request.command.environment.items()
            },
            working_directory=mapped_working_directory,
        )


def _map_value(value: str, replacements: tuple[tuple[str, str], ...]) -> str:
    for semantic, host in replacements:
        value = _replace_semantic_path(value, semantic, host)
    return value


def _replace_semantic_path(value: str, semantic: str, host: str) -> str:
    return re.sub(re.escape(semantic) + r"(?=/|$)", lambda _: host, value)
