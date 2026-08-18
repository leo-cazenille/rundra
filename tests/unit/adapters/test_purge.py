from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from rundra.adapters.purge import LocalPurger, SSHPurger
from rundra.domain.models import Command, RunId
from rundra.domain.purge import PurgeOutcome, PurgeRequest, PurgeScope
from rundra.ports import CapabilityCheck, CommandResult


def _request(root: Path, scope: PurgeScope = PurgeScope.OUTPUTS) -> PurgeRequest:
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    return PurgeRequest(run_id, root / "runs" / str(run_id), root, scope)


def test_local_purge_is_dry_runnable_idempotent_and_resumable(tmp_path: Path) -> None:
    request = _request(tmp_path / "workspace")
    output = Path(request.run_root) / "output"
    output.mkdir(parents=True)
    (output / "result.bin").write_bytes(b"result")
    purger = LocalPurger()

    planned = purger.purge(request, dry_run=True)
    first = purger.purge(request)
    second = purger.purge(request)

    assert planned.outcome is PurgeOutcome.PLANNED
    assert first.outcome is PurgeOutcome.PURGED
    assert second.outcome is PurgeOutcome.ALREADY_ABSENT
    assert not output.exists()
    assert Path(request.run_root).is_dir()

    tombstone = Path(first.tombstone)
    tombstone.mkdir()
    (tombstone / "partial").write_text("x", encoding="utf-8")
    assert purger.purge(request).outcome is PurgeOutcome.RESUMED
    assert not tombstone.exists()


def test_local_workspace_purge_handles_sealed_directories(tmp_path: Path) -> None:
    request = _request(tmp_path / "workspace", PurgeScope.WORKSPACE)
    source = Path(request.run_root) / "source/nested"
    source.mkdir(parents=True)
    (source / "code.py").write_text("pass\n", encoding="utf-8")
    source.chmod(0o500)

    result = LocalPurger().purge(request)

    assert result.outcome is PurgeOutcome.PURGED
    assert not Path(request.run_root).exists()


class RecordingTransport:
    def __init__(self) -> None:
        self.commands: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("recording")

    def run(self, command: Command) -> CommandResult:
        self.commands.append(command)
        now = datetime.now(UTC)
        return CommandResult(command, 0, "planned\n", "", now, now)


def test_ssh_purge_passes_paths_as_shell_arguments() -> None:
    transport = RecordingTransport()
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    root = PurePosixPath("/remote/work with spaces")
    request = PurgeRequest(
        run_id, root / "runs" / str(run_id), root, PurgeScope.OUTPUTS
    )

    result = SSHPurger(transport).purge(request, dry_run=True)

    assert result.outcome is PurgeOutcome.PLANNED
    assert transport.commands[0].argv[4:] == (
        str(root),
        str(run_id),
        "outputs",
        "dry-run",
    )
