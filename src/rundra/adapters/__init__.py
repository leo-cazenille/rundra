"""Concrete infrastructure adapters."""

from rundra.adapters.apptainer import (
    ApptainerConfigurationError,
    ApptainerRuntime,
    ApptainerRuntimeError,
    ApptainerUnavailableError,
)
from rundra.adapters.local import (
    LocalScheduler,
    LocalSchedulerError,
    LocalStager,
    LocalStagerError,
    LocalTransport,
    LocalTransportError,
    WorkspaceCollisionError,
)
from rundra.adapters.native import NativeRuntime, NativeRuntimeError
from rundra.adapters.remote import (
    RemoteWorkspaceAllocator,
    RemoteWorkspaceCollisionError,
    RemoteWorkspaceError,
)
from rundra.adapters.ssh import (
    SSHCommandError,
    SSHExecutionError,
    SSHTransport,
    SSHTransportError,
    SSHUnavailableError,
)

__all__ = [
    "ApptainerConfigurationError",
    "ApptainerRuntime",
    "ApptainerRuntimeError",
    "ApptainerUnavailableError",
    "LocalScheduler",
    "LocalSchedulerError",
    "LocalStager",
    "LocalStagerError",
    "LocalTransport",
    "LocalTransportError",
    "NativeRuntime",
    "NativeRuntimeError",
    "RemoteWorkspaceAllocator",
    "RemoteWorkspaceCollisionError",
    "RemoteWorkspaceError",
    "SSHCommandError",
    "SSHExecutionError",
    "SSHTransport",
    "SSHTransportError",
    "SSHUnavailableError",
    "WorkspaceCollisionError",
]
