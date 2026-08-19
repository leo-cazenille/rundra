from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rundra.adapters.rsync import RsyncRetrievalError, RsyncStager
from rundra.domain.models import Command
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    FetchRequest,
    StagedWorkspace,
)


class SharedProbeTransport:
    def check(self) -> CapabilityCheck:
        return CapabilityCheck("shared-probe")

    def run(self, command: Command) -> CommandResult:
        argv = command.argv
        if argv[0] == "/bin/sh":
            Path(argv[-2]).write_text(argv[-1], encoding="utf-8")
        elif argv[:3] == ("rm", "-f", "--"):
            Path(argv[3]).unlink(missing_ok=True)
        now = datetime.now(UTC)
        return CommandResult(command, 0, "", "", now, now)


def _workspace(root: Path) -> StagedWorkspace:
    for name in ("source", "input", "runtime", "output", "logs", "metadata"):
        (root / name).mkdir(parents=True, exist_ok=True)
    return StagedWorkspace(
        root=root,
        source=root / "source",
        inputs=root / "input",
        config=root / "input/config.yaml",
        runtime=root / "runtime",
        outputs=root / "output",
        logs=root / "logs",
        metadata=root / "metadata",
    )


def test_rsync_auto_fetch_uses_verified_shared_reference(tmp_path: Path) -> None:
    workspace = _workspace(
        tmp_path / "shared/runs/run_0123456789abcdef0123456789abcdef"
    )
    (Path(workspace.outputs) / "result.txt").write_text("large\n", encoding="utf-8")
    destination = tmp_path / "retrieved"

    result = RsyncStager(SharedProbeTransport(), host="cluster").fetch(
        FetchRequest(workspace, ("result.txt",), destination, mode="auto")
    )

    manifest = Path(result.artifacts[0].path)
    document = json.loads(manifest.read_text(encoding="utf-8"))
    assert document["kind"] == "rundra-shared-reference"
    assert document["output_root"] == str(workspace.outputs)
    assert not (destination / "output").exists()
    assert not tuple(Path(workspace.metadata).glob(".rundra-fetch-visibility-*"))


def test_rsync_explicit_reference_rejects_nonvisible_workspace(tmp_path: Path) -> None:
    root = tmp_path / "missing/runs/run_0123456789abcdef0123456789abcdef"
    workspace = StagedWorkspace(
        root=root,
        source=root / "source",
        inputs=root / "input",
        config=root / "input/config.yaml",
        runtime=root / "runtime",
        outputs=root / "output",
        logs=root / "logs",
        metadata=root / "metadata",
    )

    with pytest.raises(RsyncRetrievalError, match="not jointly visible"):
        RsyncStager(SharedProbeTransport(), host="cluster").fetch(
            FetchRequest(
                workspace,
                ("result.txt",),
                tmp_path / "retrieved",
                mode="reference",
            )
        )


def test_rsync_auto_fetch_falls_back_to_transfer_on_visibility_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "remote-only/runs/run_0123456789abcdef0123456789abcdef"
    workspace = StagedWorkspace(
        root=root,
        source=root / "source",
        inputs=root / "input",
        config=root / "input/config.yaml",
        runtime=root / "runtime",
        outputs=root / "output",
        logs=root / "logs",
        metadata=root / "metadata",
    )
    stager = RsyncStager(SharedProbeTransport(), host="cluster")
    transfers: list[str] = []
    monkeypatch.setattr(stager, "_workspace_is_locally_visible", lambda _: False)
    monkeypatch.setattr(stager, "check", lambda: CapabilityCheck("rsync"))
    monkeypatch.setattr(
        stager,
        "_retrieve",
        lambda _argv, *, tree: transfers.append(tree),
    )

    result = stager.fetch(
        FetchRequest(
            workspace,
            ("result.txt",),
            tmp_path / "retrieved-copy",
            mode="auto",
        )
    )

    assert transfers == ["output", "logs", "metadata"]
    assert result.artifacts == ()
