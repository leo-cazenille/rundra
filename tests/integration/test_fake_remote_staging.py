from __future__ import annotations

import stat
import sys
from pathlib import Path, PurePosixPath
from textwrap import dedent

import pytest

from rundra.adapters.rsync import RsyncRetrievalError, RsyncStager
from rundra.adapters.ssh import SSHTransport
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
from rundra.ports import FetchRequest, StageRequest


def _write_executable(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _fake_ssh(tmp_path: Path) -> Path:
    return _write_executable(
        tmp_path / "fake-ssh",
        f"#!{sys.executable}\n"
        + dedent(
            """
            import os
            import sys

            if len(sys.argv) != 5 or sys.argv[1:4] != ["-T", "--", "fake-host"]:
                raise SystemExit(97)
            os.execv("/bin/sh", ("sh", "-c", sys.argv[4]))
            """
        ),
    )


def _fake_rsync(tmp_path: Path) -> Path:
    return _write_executable(
        tmp_path / "fake-rsync",
        f"#!{sys.executable}\n"
        + dedent(
            r"""
            import fnmatch
            import os
            import shutil
            import sys
            from pathlib import Path

            arguments = sys.argv[1:]
            separator = arguments.index("--")
            options = arguments[:separator]
            source_value, destination_value = arguments[separator + 1:]

            def local_path(value):
                return Path(value.split(":", 1)[1] if value.startswith("fake-host:") else value)

            source = local_path(source_value)
            destination = local_path(destination_value)
            failure = os.environ.get("RUNDRA_FAKE_RSYNC_FAIL")
            if failure and failure in source_value:
                print("sensitive interrupted payload", file=sys.stderr)
                raise SystemExit(23)

            excludes = []
            includes = []
            index = 0
            while index < len(options):
                if options[index] == "--exclude":
                    excludes.append(options[index + 1])
                    index += 2
                elif options[index] == "--include":
                    includes.append(options[index + 1])
                    index += 2
                else:
                    index += 1

            def excluded(relative):
                if includes and included(relative):
                    return False
                return any(
                    fnmatch.fnmatch(relative.as_posix(), pattern)
                    or any(fnmatch.fnmatch(part, pattern) for part in relative.parts)
                    for pattern in excludes
                )

            def included(relative):
                file_patterns = [pattern for pattern in includes if pattern != "*/"]
                return not file_patterns or any(
                    fnmatch.fnmatch(relative.as_posix(), pattern)
                    for pattern in file_patterns
                )

            if source.is_file():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
                raise SystemExit(0)

            destination.mkdir(parents=True, exist_ok=True)
            for item in source.rglob("*"):
                relative = item.relative_to(source)
                if excluded(relative):
                    continue
                target = destination / relative
                if item.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                elif item.is_symlink() and "--no-links" in options:
                    continue
                elif included(relative):
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(item.resolve() if item.is_symlink() else item, target)
            """
        ),
    )


def _request(
    source: Path,
    remote_root: Path,
    run_id: RunId,
    *,
    config: str,
) -> StageRequest:
    target = Target(
        name="fake-remote",
        transport=BackendConfig("ssh", {"host": "fake-host"}),
        scheduler=BackendConfig("slurm"),
        staging=BackendConfig("rsync"),
        container=BackendConfig("apptainer"),
        workspace=PurePosixPath(remote_root),
    )
    experiment = ExperimentSpec(
        version=1,
        name="fake-remote",
        command=Command(("python", "main.py")),
        resources=ResourceRequest(),
        outputs=("results/**",),
        sync_excludes=("ignored/",),
    )
    return StageRequest(
        run_id,
        experiment,
        ConfigSnapshot(PurePosixPath("config.yaml"), config),
        target,
        source,
    )


def _restore_writes(root: Path) -> None:
    if not root.exists():
        return
    for path in (root, *root.rglob("*")):
        if not path.is_symlink():
            path.chmod(path.stat().st_mode | stat.S_IWUSR)


def test_fake_ssh_rsync_round_trip_isolated_live_snapshots_and_repeat_fetch(
    tmp_path: Path,
) -> None:
    fake_ssh = _fake_ssh(tmp_path)
    fake_rsync = _fake_rsync(tmp_path)
    source = tmp_path / "live non-git source"
    source.mkdir()
    (source / "main.py").write_text("version one\n", encoding="utf-8")
    (source / "untracked.txt").write_text("included\n", encoding="utf-8")
    (source / ".git").mkdir()
    (source / ".git/config").write_text("excluded\n", encoding="utf-8")
    (source / "ignored").mkdir()
    (source / "ignored/cache.bin").write_bytes(b"excluded")
    remote_root = tmp_path / "remote workspace 'quoted'"
    transport = SSHTransport("fake-host", executable=str(fake_ssh))
    stager = RsyncStager(
        transport,
        host="fake-host",
        executable=str(fake_rsync),
    )
    run_one = RunId("run_11111111111111111111111111111111")
    run_two = RunId("run_22222222222222222222222222222222")

    try:
        assert transport.check().name == "ssh"
        assert stager.check().name == "rsync"
        first = stager.stage(
            _request(source, remote_root, run_one, config="value: one\r\n")
        )
        (source / "main.py").write_text("version two\n", encoding="utf-8")
        second = stager.stage(
            _request(source, remote_root, run_two, config="value: two\n")
        )

        assert Path(first.source, "main.py").read_text(encoding="utf-8") == (
            "version one\n"
        )
        assert Path(second.source, "main.py").read_text(encoding="utf-8") == (
            "version two\n"
        )
        assert Path(first.source, "untracked.txt").is_file()
        assert not Path(first.source, ".git").exists()
        assert not Path(first.source, "ignored").exists()
        assert Path(first.config).read_bytes() == b"value: one\r\n"
        assert Path(second.config).read_bytes() == b"value: two\n"
        assert Path(first.source).stat().st_mode & stat.S_IWUSR == 0
        assert Path(first.config).stat().st_mode & stat.S_IWUSR == 0

        (Path(first.outputs) / "results").mkdir()
        result = Path(first.outputs) / "results/result.txt"
        result.write_text("first retrieval\n", encoding="utf-8")
        (Path(first.outputs) / "ignored.txt").write_text(
            "not selected\n", encoding="utf-8"
        )
        Path(first.logs, "task_000000.stdout").write_text("stdout\n", encoding="utf-8")
        Path(first.logs, "task_000000.stderr").write_text("stderr\n", encoding="utf-8")
        Path(first.metadata, "job.json").write_text("{}\n", encoding="utf-8")
        destination = tmp_path / "retrieved"
        fetch_request = FetchRequest(first, ("results/**",), destination)

        initial = stager.fetch(fetch_request)
        result.write_text("second retrieval\n", encoding="utf-8")
        repeated = stager.fetch(fetch_request)

        assert (destination / "output/results/result.txt").read_text(
            encoding="utf-8"
        ) == "second retrieval\n"
        assert not (destination / "output/ignored.txt").exists()
        assert (destination / "logs/task_000000.stdout").read_text(
            encoding="utf-8"
        ) == "stdout\n"
        assert (destination / "metadata/job.json").is_file()
        assert [
            (artifact.kind, artifact.path, artifact.task_id)
            for artifact in initial.artifacts
        ] == [
            (artifact.kind, artifact.path, artifact.task_id)
            for artifact in repeated.artifacts
        ]
        assert repeated.artifacts[-1].size_bytes == len(b"second retrieval\n")
        assert {artifact.kind for artifact in repeated.artifacts} == {
            ArtifactKind.RAW_RESULT,
            ArtifactKind.STDOUT,
            ArtifactKind.STDERR,
            ArtifactKind.SCHEDULER_METADATA,
        }
    finally:
        _restore_writes(remote_root)


def test_fake_rsync_interruption_fails_then_explicit_retry_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ssh = _fake_ssh(tmp_path)
    fake_rsync = _fake_rsync(tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    (source / "main.py").write_text("source\n", encoding="utf-8")
    remote_root = tmp_path / "remote"
    transport = SSHTransport("fake-host", executable=str(fake_ssh))
    stager = RsyncStager(
        transport,
        host="fake-host",
        executable=str(fake_rsync),
    )
    request = _request(
        source,
        remote_root,
        RunId("run_33333333333333333333333333333333"),
        config="value: one\n",
    )

    try:
        workspace = stager.stage(request)
        Path(workspace.outputs, "results").mkdir()
        Path(workspace.outputs, "results/result.txt").write_text(
            "complete\n", encoding="utf-8"
        )
        fetch_request = FetchRequest(workspace, ("results/**",), tmp_path / "retrieved")
        monkeypatch.setenv("RUNDRA_FAKE_RSYNC_FAIL", "/output/")

        with pytest.raises(RsyncRetrievalError, match="exit code 23") as captured:
            stager.fetch(fetch_request)

        assert "sensitive interrupted payload" not in str(captured.value)
        monkeypatch.delenv("RUNDRA_FAKE_RSYNC_FAIL")
        fetched = stager.fetch(fetch_request)
        assert [artifact.kind for artifact in fetched.artifacts] == [
            ArtifactKind.RAW_RESULT
        ]
        assert (tmp_path / "retrieved/output/results/result.txt").read_text(
            encoding="utf-8"
        ) == "complete\n"
    finally:
        _restore_writes(remote_root)
