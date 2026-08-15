from __future__ import annotations

from pathlib import PurePath, PurePosixPath

from rundra.domain.models import Command, RunId
from rundra.ports import CommandResult, StagedWorkspace, Transport


class RemoteWorkspaceError(RuntimeError):
    """Raised when an isolated remote Run workspace cannot be allocated."""


class RemoteWorkspaceCollisionError(RemoteWorkspaceError):
    """Raised when a remote Run workspace has already been allocated."""


class RemoteWorkspaceAllocator:
    """Allocate semantic Run directories through a transport."""

    def __init__(self, transport: Transport) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("RemoteWorkspaceAllocator requires a Transport")
        self._transport = transport

    def create(self, run_id: RunId, workspace_root: PurePath) -> StagedWorkspace:
        """Create one new remote workspace without uploading any content."""
        if type(run_id) is not RunId:
            raise TypeError("Remote workspace run_id must be a RunId")
        root = _safe_remote_root(workspace_root)
        runs = root / "runs"
        run_root = runs / str(run_id)
        workspace = _workspace(run_root)
        validate_remote_workspace(workspace, configured_root=root)

        self._checked_run(
            Command(("mkdir", "-p", "--", str(runs))),
            operation="create remote Runs directory",
        )
        allocation = self._run(
            Command(("mkdir", "--", str(run_root))),
            operation="allocate remote Run workspace",
        )
        if allocation.exit_code != 0:
            collision = self._run(
                Command(("test", "-e", str(run_root))),
                operation="check remote Run collision",
            )
            if collision.exit_code == 0:
                raise RemoteWorkspaceCollisionError(
                    f"Remote workspace for Run {run_id} already exists"
                )
            raise RemoteWorkspaceError(
                f"Could not allocate remote workspace for Run {run_id}"
            )
        self._checked_run(
            Command(
                (
                    "mkdir",
                    "--",
                    str(workspace.source),
                    str(workspace.inputs),
                    str(workspace.runtime),
                    str(workspace.outputs),
                    str(workspace.logs),
                    str(workspace.metadata),
                )
            ),
            operation="create remote Run directories",
        )
        return workspace

    def _checked_run(self, command: Command, *, operation: str) -> None:
        result = self._run(command, operation=operation)
        if result.exit_code != 0:
            raise RemoteWorkspaceError(f"Could not {operation}")

    def _run(self, command: Command, *, operation: str) -> CommandResult:
        try:
            return self._transport.run(command)
        except Exception as error:
            raise RemoteWorkspaceError(f"Could not {operation}") from error


def _safe_remote_root(value: PurePath) -> PurePosixPath:
    if not isinstance(value, PurePath):
        raise TypeError("Remote workspace root must be a PurePath")
    root = PurePosixPath(str(value))
    if not root.is_absolute():
        raise RemoteWorkspaceError("Remote workspace root must be absolute")
    if not root.name:
        raise RemoteWorkspaceError("Remote workspace root must not be filesystem root")
    if ".." in root.parts:
        raise RemoteWorkspaceError("Remote workspace root must not contain traversal")
    if "\x00" in str(root):
        raise RemoteWorkspaceError("Remote workspace root must not contain NUL")
    return root


def _workspace(root: PurePosixPath) -> StagedWorkspace:
    inputs = root / "input"
    return StagedWorkspace(
        root=root,
        source=root / "source",
        inputs=inputs,
        config=inputs / "config.yaml",
        runtime=root / "runtime",
        outputs=root / "output",
        logs=root / "logs",
        metadata=root / "metadata",
    )


def validate_remote_workspace(
    workspace: StagedWorkspace,
    *,
    configured_root: PurePath | None = None,
) -> None:
    """Reject malformed or escaping semantic remote workspace paths."""
    if type(workspace) is not StagedWorkspace:
        raise TypeError("Remote workspace must be a StagedWorkspace")
    run_root = _safe_remote_root(workspace.root)
    try:
        RunId(run_root.name)
    except (TypeError, ValueError) as error:
        raise RemoteWorkspaceError(
            "Remote workspace root must end with a validated Run ID"
        ) from error
    if run_root.parent.name != "runs":
        raise RemoteWorkspaceError(
            "Remote workspace root must be contained by a Runs directory"
        )
    root = (
        _safe_remote_root(configured_root) if configured_root is not None else run_root
    )
    expected = _workspace(run_root)
    for name in (
        "root",
        "source",
        "inputs",
        "config",
        "runtime",
        "outputs",
        "logs",
        "metadata",
    ):
        path = PurePosixPath(str(getattr(workspace, name)))
        if not path.is_relative_to(root) or ".." in path.parts:
            raise RemoteWorkspaceError(
                f"Derived remote workspace path escapes its root: {name}"
            )
        if getattr(expected, name) != path:
            raise RemoteWorkspaceError(
                f"Remote workspace has an unexpected semantic path: {name}"
            )
