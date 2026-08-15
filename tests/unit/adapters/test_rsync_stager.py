from __future__ import annotations

import shutil
import subprocess
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import pytest

from rundra.adapters.rsync import (
    RsyncStager,
    RsyncStagerError,
    RsyncUnavailableError,
    RsyncUploadError,
)
from rundra.domain.models import (
    ArtifactKind,
    BackendConfig,
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    ResourceRequest,
    RunId,
    Target,
)
from rundra.ports import CapabilityCheck, CommandResult, StageRequest


@dataclass
class RecordingTransport:
    exits: deque[int]
    calls: list[Command] = field(default_factory=list)

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("ssh")

    def run(self, command: Command) -> CommandResult:
        self.calls.append(command)
        now = datetime(2026, 1, 1, tzinfo=UTC)
        return CommandResult(command, self.exits.popleft(), "", "", now, now)


def _request(source: Path, *, excludes: tuple[str, ...] = ()) -> StageRequest:
    target = Target(
        name="remote",
        transport=BackendConfig("ssh", {"host": "cluster-alias"}),
        scheduler=BackendConfig("slurm"),
        staging=BackendConfig("rsync"),
        container=BackendConfig("apptainer"),
        workspace=PurePosixPath("/remote/work tree"),
    )
    experiment = ExperimentSpec(
        version=1,
        name="experiment",
        command=Command(("python", "main.py")),
        resources=ResourceRequest(),
        sync_excludes=excludes,
    )
    return StageRequest(
        RunId("run_0123456789abcdef0123456789abcdef"),
        experiment,
        ConfigSnapshot(PurePosixPath("config.yaml"), "value: exact\r\n"),
        target,
        source,
    )


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "non-git-source"
    source.mkdir()
    (source / "tracked.py").write_text("dirty working tree\n", encoding="utf-8")
    (source / "untracked.txt").write_text("included\n", encoding="utf-8")
    (source / ".git").mkdir()
    (source / ".git/config").write_text("excluded\n", encoding="utf-8")
    (source / "generated").mkdir()
    (source / "generated/result.bin").write_bytes(b"excluded")
    return source


def test_rsync_stager_uploads_live_tree_exact_config_and_seals_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path)
    request = _request(source, excludes=("generated/",))
    transport = RecordingTransport(deque([0, 0, 0, 0]))
    calls: list[tuple[str, ...]] = []
    config_bytes: list[bytes] = []
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")

    def run(
        argv: tuple[str, ...], **options: object
    ) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if len(calls) == 2:
            config_bytes.append(Path(argv[-2]).read_bytes())
        assert options == {
            "capture_output": True,
            "check": False,
            "encoding": "utf-8",
            "errors": "replace",
            "shell": False,
        }
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)

    workspace = RsyncStager(transport).stage(request)

    run_root = request.target.workspace / "runs" / str(request.run_id)
    expected_prefix = (
        "rsync",
        "--archive",
        "--copy-links",
        "--delete",
        "--protect-args",
    )
    assert calls[0][:5] == expected_prefix
    assert calls[0][-3:] == (
        "--",
        f"{source.resolve()}/",
        f"cluster-alias:{run_root}/source/",
    )
    assert calls[0][5:] == (
        "--exclude",
        ".git",
        "--exclude",
        ".hg",
        "--exclude",
        ".svn",
        "--exclude",
        ".venv",
        "--exclude",
        "venv",
        "--exclude",
        "__pycache__",
        "--exclude",
        ".pytest_cache",
        "--exclude",
        ".mypy_cache",
        "--exclude",
        ".ruff_cache",
        "--exclude",
        ".tox",
        "--exclude",
        ".nox",
        "--exclude",
        ".rundra",
        "--exclude",
        "*.py[cod]",
        "--exclude",
        "generated",
        "--",
        f"{source.resolve()}/",
        f"cluster-alias:{run_root}/source/",
    )
    assert calls[1][0:4] == ("rsync", "--archive", "--protect-args", "--")
    assert calls[1][-1] == f"cluster-alias:{run_root}/input/config.yaml"
    assert config_bytes == [b"value: exact\r\n"]
    assert transport.calls[-1] == Command(
        (
            "chmod",
            "-R",
            "a-w",
            "--",
            str(run_root / "source"),
            str(run_root / "input"),
        )
    )
    assert workspace.artifacts[0].kind is ArtifactKind.SOURCE_SNAPSHOT
    assert workspace.artifacts[1].kind is ArtifactKind.EFFECTIVE_CONFIG
    assert workspace.artifacts[1].size_bytes == len(b"value: exact\r\n")
    assert (source / "untracked.txt").exists()


def test_rsync_stager_reports_capability_and_upload_failures_without_data_leaks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(_source(tmp_path))
    monkeypatch.setattr(shutil, "which", lambda executable: None)
    with pytest.raises(RsyncUnavailableError, match="not found"):
        RsyncStager(RecordingTransport(deque())).stage(request)

    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")

    def fail(argv: tuple[str, ...], **options: object) -> None:
        raise OSError("SECRET_VALUE_FROM_ARGV")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(RsyncUploadError) as captured:
        RsyncStager(RecordingTransport(deque([0, 0, 0]))).stage(request)
    assert "SECRET_VALUE_FROM_ARGV" not in str(captured.value)


def test_rsync_stager_does_not_seal_after_interrupted_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(_source(tmp_path))
    transport = RecordingTransport(deque([0, 0, 0]))
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **options: subprocess.CompletedProcess(
            argv, 23, "", "partial transfer SECRET"
        ),
    )

    with pytest.raises(RsyncUploadError, match="exit code 23") as captured:
        RsyncStager(transport).stage(request)

    assert "SECRET" not in str(captured.value)
    assert all(call.argv[0] != "chmod" for call in transport.calls)


def test_rsync_stager_reports_config_and_sealing_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(_source(tmp_path))
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")
    outcomes = iter((0, 12))
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **options: subprocess.CompletedProcess(
            argv, next(outcomes), "", ""
        ),
    )
    with pytest.raises(RsyncUploadError, match="effective config"):
        RsyncStager(RecordingTransport(deque([0, 0, 0]))).stage(request)

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **options: subprocess.CompletedProcess(argv, 0, "", ""),
    )
    with pytest.raises(RsyncUploadError, match="seal"):
        RsyncStager(RecordingTransport(deque([0, 0, 0, 1]))).stage(request)


def test_rsync_stager_rejects_invalid_targets_exclusions_and_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path)
    request = _request(source)
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")
    stager = RsyncStager(RecordingTransport(deque()))

    with pytest.raises(RsyncStagerError, match="not rsync"):
        stager.stage(
            replace(
                request, target=replace(request.target, staging=BackendConfig("local"))
            )
        )
    with pytest.raises(RsyncStagerError, match="not SSH"):
        stager.stage(
            replace(
                request,
                target=replace(request.target, transport=BackendConfig("local")),
            )
        )
    with pytest.raises(RsyncStagerError, match="relative exclusion"):
        stager.stage(_request(source, excludes=("../outside",)))
    with pytest.raises(RsyncStagerError, match="does not exist"):
        stager.stage(replace(request, source_root=tmp_path / "missing"))
    bad_host = replace(
        request.target,
        transport=BackendConfig("ssh", {"host": "bad:host"}),
    )
    with pytest.raises(RsyncStagerError, match="host"):
        stager.stage(replace(request, target=bad_host))


@pytest.mark.parametrize("value", ["not-a-request", object()])
def test_rsync_stager_rejects_non_requests(value: object) -> None:
    with pytest.raises(TypeError, match="StageRequest"):
        RsyncStager(RecordingTransport(deque())).stage(value)  # type: ignore[arg-type]
