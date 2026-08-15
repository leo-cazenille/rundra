from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath

import pytest

from rundra.adapters.remote import (
    RemoteWorkspaceAllocator,
    RemoteWorkspaceCollisionError,
    RemoteWorkspaceError,
)
from rundra.domain.models import Command, RunId
from rundra.ports import CapabilityCheck, CommandResult


@dataclass
class RecordingTransport:
    run_script: deque[CommandResult | Exception]
    run_calls: list[Command] = field(default_factory=list)

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("ssh")

    def run(self, command: Command) -> CommandResult:
        self.run_calls.append(command)
        result = self.run_script.popleft()
        if isinstance(result, Exception):
            raise result
        return result


def _result(command: Command, exit_code: int = 0) -> CommandResult:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return CommandResult(command, exit_code, "", "", now, now)


def _transport(*exit_codes: int) -> RecordingTransport:
    placeholder = Command(("true",))
    return RecordingTransport(deque(_result(placeholder, code) for code in exit_codes))


def test_remote_workspace_allocator_creates_exact_isolated_paths() -> None:
    transport = _transport(0, 0, 0)
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    root = PurePosixPath("/shoal home/user/.rundra 'literal'\nline")

    workspace = RemoteWorkspaceAllocator(transport).create(run_id, root)

    run_root = root / "runs" / str(run_id)
    assert workspace.root == run_root
    assert workspace.source == run_root / "source"
    assert workspace.inputs == run_root / "input"
    assert workspace.config == run_root / "input/config.yaml"
    assert workspace.runtime == run_root / "runtime"
    assert workspace.outputs == run_root / "output"
    assert workspace.logs == run_root / "logs"
    assert workspace.metadata == run_root / "metadata"
    assert workspace.artifacts == ()
    assert transport.run_calls == [
        Command(("mkdir", "-p", "--", str(root / "runs"))),
        Command(("mkdir", "--", str(run_root))),
        Command(
            (
                "mkdir",
                "--",
                str(run_root / "source"),
                str(run_root / "input"),
                str(run_root / "runtime"),
                str(run_root / "output"),
                str(run_root / "logs"),
                str(run_root / "metadata"),
            )
        ),
    ]


def test_remote_workspace_allocator_reports_collisions_without_reusing_path() -> None:
    transport = _transport(0, 1, 0)
    run_id = RunId.new()

    with pytest.raises(RemoteWorkspaceCollisionError, match=str(run_id)):
        RemoteWorkspaceAllocator(transport).create(
            run_id, PurePosixPath("/remote/workspace")
        )

    assert transport.run_calls[-1] == Command(
        ("test", "-e", f"/remote/workspace/runs/{run_id}")
    )


@pytest.mark.parametrize(
    "root",
    [
        PurePosixPath("relative/workspace"),
        PurePosixPath("/"),
        PurePosixPath("/safe/../escape"),
        PurePosixPath("/remote/bad\x00root"),
    ],
)
def test_remote_workspace_allocator_rejects_unsafe_roots_without_transport_calls(
    root: PurePosixPath,
) -> None:
    transport = _transport()

    with pytest.raises(RemoteWorkspaceError, match="workspace root"):
        RemoteWorkspaceAllocator(transport).create(RunId.new(), root)

    assert transport.run_calls == []


def test_remote_workspace_allocator_distinguishes_allocation_and_transport_failures() -> (
    None
):
    run_id = RunId.new()
    unavailable = _transport(1)
    with pytest.raises(RemoteWorkspaceError, match="Runs directory"):
        RemoteWorkspaceAllocator(unavailable).create(
            run_id, PurePosixPath("/remote/workspace")
        )

    permission_denied = _transport(0, 1, 1)
    with pytest.raises(RemoteWorkspaceError, match="allocate"):
        RemoteWorkspaceAllocator(permission_denied).create(
            run_id, PurePosixPath("/remote/workspace")
        )

    failed_transport = RecordingTransport(deque([RuntimeError("connection lost")]))
    with pytest.raises(RemoteWorkspaceError, match="Runs directory") as captured:
        RemoteWorkspaceAllocator(failed_transport).create(
            run_id, PurePosixPath("/remote/workspace")
        )
    assert isinstance(captured.value.__cause__, RuntimeError)


@pytest.mark.parametrize("value", ["run_bad", object()])
def test_remote_workspace_allocator_rejects_non_run_ids(value: object) -> None:
    with pytest.raises(TypeError, match="run_id"):
        RemoteWorkspaceAllocator(_transport()).create(  # type: ignore[arg-type]
            value, PurePosixPath("/remote/workspace")
        )


def test_remote_workspace_allocator_requires_a_transport() -> None:
    with pytest.raises(TypeError, match="Transport"):
        RemoteWorkspaceAllocator(object())  # type: ignore[arg-type]
