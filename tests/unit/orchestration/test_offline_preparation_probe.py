from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from rundra.domain.models import BackendConfig, Command, Target
from rundra.domain.preparation import (
    PreparationConfig,
    PreparationImage,
    PreparationPlan,
    PreparationSourceGit,
)
from rundra.orchestration.preparation import (
    _remote_image_digest_command,
    probe_remote_offline_preparation,
)
from rundra.ports import CapabilityCheck, CommandResult


class ProbeTransport:
    def __init__(self, outputs: list[tuple[int, str]]) -> None:
        self.outputs = outputs
        self.commands: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("probe")

    def run(self, command: Command) -> CommandResult:
        self.commands.append(command)
        exit_code, stdout = self.outputs.pop(0)
        now = datetime.now(UTC)
        return CommandResult(command, exit_code, stdout, "", now, now)


def _target() -> Target:
    return Target(
        "cluster",
        BackendConfig("ssh", {"host": "cluster"}),
        BackendConfig("slurm"),
        BackendConfig("rsync"),
        BackendConfig("apptainer"),
        PurePosixPath("/work"),
    )


def _plan() -> PreparationPlan:
    source = PreparationSourceGit("https://example.invalid/source.git", "a" * 40)
    image = PreparationImage(
        PurePosixPath("image.sif"), "library://example/image:1", "b" * 64
    )
    return PreparationPlan(
        PreparationConfig(source, image, None), "git", None, offline=True
    )


def test_remote_offline_probe_verifies_commit_and_image_digest() -> None:
    transport = ProbeTransport([(0, ""), (0, f"{'b' * 64}  image.sif\n")])

    probe = probe_remote_offline_preparation(_plan(), _target(), transport)

    assert probe.source_ready and probe.image_ready
    assert transport.commands[0].argv[-2:] == ("-e", f"{'a' * 40}^{{commit}}")
    assert transport.commands[1].argv[0] == "sh"
    assert 'test ! -L "$image"' in transport.commands[1].argv[2]
    assert "receipt=$image.receipt" in transport.commands[1].argv[2]


def test_remote_offline_probe_requires_regular_non_symlink_image() -> None:
    transport = ProbeTransport([(0, ""), (1, "")])

    probe = probe_remote_offline_preparation(_plan(), _target(), transport)

    assert probe.source_ready
    assert not probe.image_ready
    assert 'test -f "$image"' in transport.commands[1].argv[2]
    assert 'test ! -L "$image"' in transport.commands[1].argv[2]


def test_remote_offline_probe_reports_cold_source_without_image_probe() -> None:
    transport = ProbeTransport([(1, "")])

    probe = probe_remote_offline_preparation(_plan(), _target(), transport)

    assert not probe.source_ready
    assert not probe.image_ready
    assert len(transport.commands) == 1


def test_remote_image_digest_command_uses_tab_delimited_receipt(
    tmp_path: Path,
) -> None:
    image = tmp_path / "image.sif"
    image.write_bytes(b"immutable image")
    image.chmod(0o444)
    digest = "b" * 64
    receipt = tmp_path / "image.sif.receipt"
    receipt.write_text(f"1\t{digest}\t{image.stat().st_size}\n", encoding="ascii")
    receipt.chmod(0o444)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sha256sum = fake_bin / "sha256sum"
    fake_sha256sum.write_text("#!/bin/sh\nexit 91\n", encoding="ascii")
    fake_sha256sum.chmod(0o755)

    completed = subprocess.run(
        _remote_image_digest_command(image).argv,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
    )

    assert completed.returncode == 0
    assert completed.stdout.split(maxsplit=1)[0] == digest
