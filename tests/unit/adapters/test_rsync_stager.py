from __future__ import annotations

import hashlib
import shutil
import subprocess
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import pytest

from rundra.adapters.rsync import (
    RsyncRetrievalError,
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
    TaskId,
)
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    FetchRequest,
    StagedWorkspace,
    StageRequest,
)


@dataclass
class RecordingTransport:
    exits: deque[int | Exception]
    calls: list[Command] = field(default_factory=list)

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("ssh")

    def run(self, command: Command) -> CommandResult:
        self.calls.append(command)
        now = datetime(2026, 1, 1, tzinfo=UTC)
        outcome = self.exits.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return CommandResult(command, outcome, "", "", now, now)


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
            config_bytes.append((Path(argv[-2]) / "input/config.yaml").read_bytes())
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
        ".agents",
        "--exclude",
        "results",
        "--exclude",
        "outputs",
        "--exclude",
        "retrieved",
        "--exclude",
        "tmp",
        "--exclude",
        "downloads",
        "--exclude",
        "*.py[cod]",
        "--exclude",
        "*.sif",
        "--exclude",
        "*.simg",
        "--exclude",
        "generated",
        "--",
        f"{source.resolve()}/",
        f"cluster-alias:{run_root}/source/",
    )
    assert calls[1][0:4] == ("rsync", "--archive", "--protect-args", "--")
    assert calls[1][-1] == f"cluster-alias:{run_root}/"
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


def test_rsync_stager_batches_all_task_configs_and_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path)
    task_ids = tuple(TaskId.from_ordinal(index) for index in range(3))
    request = replace(
        _request(source),
        task_ids=task_ids,
        task_configs={
            task_id: ConfigSnapshot(
                PurePosixPath(f"config-{index}.yaml"), f"value: {index}\n"
            )
            for index, task_id in enumerate(task_ids)
        },
        task_manifest='{"schema_version":1,"tasks":[]}',
    )
    transport = RecordingTransport(deque([0] * 10))
    calls: list[tuple[str, ...]] = []
    staged_files: dict[str, str] = {}
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")

    def run(
        argv: tuple[str, ...], **options: object
    ) -> subprocess.CompletedProcess[str]:
        del options
        calls.append(argv)
        if len(calls) == 2:
            root = Path(argv[-2])
            staged_files.update(
                {
                    path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
                    for path in root.rglob("*")
                    if path.is_file()
                }
            )
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)

    workspace = RsyncStager(transport).stage(request)

    assert len(calls) == 2
    assert staged_files == {
        "input/config.yaml": "value: exact\n",
        "input/task_000000.yaml": "value: 0\n",
        "input/task_000001.yaml": "value: 1\n",
        "input/task_000002.yaml": "value: 2\n",
        "metadata/tasks.json": '{"schema_version":1,"tasks":[]}\n',
    }
    assert workspace.task_configs == {
        task_id: workspace.inputs / f"{task_id}.yaml" for task_id in task_ids
    }


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


def test_rsync_stager_copies_an_existing_remote_source_without_uploading_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote_source = PurePosixPath("/remote/cache/builds/key/source")
    request = replace(
        _request(tmp_path / "missing-local-source"),
        remote_source_root=remote_source,
    )
    transport = RecordingTransport(deque([0] * 10))
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")

    def run(
        argv: tuple[str, ...], **options: object
    ) -> subprocess.CompletedProcess[str]:
        del options
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)

    workspace = RsyncStager(transport).stage(request)

    assert len(calls) == 1
    assert calls[0][-1] == f"cluster-alias:{workspace.root}/"
    assert Command(("test", "-d", str(remote_source))) in transport.calls
    assert Command(("test", "!", "-L", str(remote_source))) in transport.calls
    assert (
        Command(("cp", "-a", "--", f"{remote_source}/.", str(workspace.source)))
        in transport.calls
    )


def test_verified_file_publication_is_atomic_and_reuses_target_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "application.sif"
    source.write_bytes(b"immutable image")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    destination = PurePosixPath(f"/remote/cache/images/{digest}.sif")

    class CacheTransport:
        def __init__(self) -> None:
            self.calls: list[Command] = []
            self.files: set[str] = set()

        def check(self) -> CapabilityCheck:
            return CapabilityCheck("ssh")

        def run(self, command: Command) -> CommandResult:
            self.calls.append(command)
            now = datetime(2026, 1, 1, tzinfo=UTC)
            argv = command.argv
            exit_code = 0
            stdout = ""
            if argv[:2] == ("test", "-f"):
                exit_code = 0 if argv[-1] in self.files else 1
            elif argv[:2] == ("test", "-L"):
                exit_code = 1
            elif argv[0] == "sha256sum":
                stdout = f"{digest}  {argv[-1]}\n"
            elif argv[0] == "mv":
                self.files.discard(argv[-2])
                self.files.add(argv[-1])
            elif argv[0] == "rm":
                self.files.discard(argv[-1])
            return CommandResult(command, exit_code, stdout, "", now, now)

    transport = CacheTransport()
    transfers: list[tuple[str, ...]] = []
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")

    def transfer(
        argv: tuple[str, ...], **options: object
    ) -> subprocess.CompletedProcess[str]:
        del options
        transfers.append(argv)
        transport.files.add(argv[-1].split(":", 1)[1])
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", transfer)
    stager = RsyncStager(transport, host="cluster-alias")

    cold = stager.publish_verified_file(source, destination, digest)
    warm = stager.publish_verified_file(source, destination, digest)

    assert cold == "transfer_image_to_target"
    assert warm == "reuse_target_image_cache"
    assert len(transfers) == 1
    assert transfers[0][0:4] == ("rsync", "--archive", "--protect-args", "--")
    assert str(destination) in transport.files
    assert any(call.argv[0] == "chmod" for call in transport.calls)
    assert any(call.argv[0] == "mv" for call in transport.calls)


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
    with pytest.raises(RsyncUploadError, match="effective inputs"):
        RsyncStager(RecordingTransport(deque([0, 0, 0]))).stage(request)

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **options: subprocess.CompletedProcess(argv, 0, "", ""),
    )
    with pytest.raises(RsyncUploadError, match="seal"):
        RsyncStager(RecordingTransport(deque([0, 0, 0, 1]))).stage(request)
    with pytest.raises(RsyncUploadError, match="seal") as captured:
        RsyncStager(
            RecordingTransport(deque([0, 0, 0, RuntimeError("SECRET_REMOTE")]))
        ).stage(request)
    assert "SECRET_REMOTE" not in str(captured.value)


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


def _workspace() -> StagedWorkspace:
    root = PurePosixPath("/remote/work tree/runs/run_0123456789abcdef0123456789abcdef")
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


def test_rsync_fetch_is_idempotent_and_returns_result_log_metadata_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")
    calls: list[tuple[str, ...]] = []
    filter_contents: list[str] = []
    generation = 0

    def run(
        argv: tuple[str, ...], **options: object
    ) -> subprocess.CompletedProcess[str]:
        nonlocal generation
        calls.append(argv)
        if "--filter" in argv:
            filter_argument = argv[argv.index("--filter") + 1]
            filter_contents.append(
                Path(filter_argument.removeprefix("merge ")).read_text(encoding="utf-8")
            )
        destination = Path(argv[-1])
        destination.mkdir(parents=True, exist_ok=True)
        tree = len(calls) % 3
        if tree == 1:
            generation += 1
            result = destination / "results/result.txt"
            result.parent.mkdir(parents=True, exist_ok=True)
            result.write_text(f"generation {generation}\n", encoding="utf-8")
        elif tree == 2:
            (destination / "task_000000.stdout").write_text("out\n", encoding="utf-8")
            (destination / "task_000000.stderr").write_text("err\n", encoding="utf-8")
        else:
            (destination / "job.json").write_text("{}\n", encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    destination = tmp_path / "retrieved"
    request = FetchRequest(
        _workspace(),
        (
            "results/**",
            ".rundra-shards/*.tar",
            ".rundra-shards/*.sha256",
        ),
        destination,
    )
    stager = RsyncStager(RecordingTransport(deque()), host="cluster-alias")

    first = stager.fetch(request)
    second = stager.fetch(request)

    workspace = _workspace()
    assert calls[0][:7] == (
        "rsync",
        "--archive",
        "--no-links",
        "--protect-args",
        "--delay-updates",
        "--prune-empty-dirs",
        "--filter",
    )
    assert calls[0][-3:] == (
        "--",
        f"cluster-alias:{workspace.outputs}/",
        f"{destination.resolve()}/output/",
    )
    assert (
        filter_contents
        == [
            "+ results/\n"
            "+ results/**\n"
            "+ .rundra-shards/\n"
            "+ .rundra-shards/*.tar\n"
            "+ .rundra-shards/*.sha256\n"
            "- *\n"
        ]
        * 2
    )
    assert all("+ */\n" not in content for content in filter_contents)
    assert calls[1:3] == [
        (
            "rsync",
            "--archive",
            "--no-links",
            "--protect-args",
            "--delay-updates",
            "--",
            f"cluster-alias:{workspace.logs}/",
            f"{destination.resolve()}/logs/",
        ),
        (
            "rsync",
            "--archive",
            "--no-links",
            "--protect-args",
            "--delay-updates",
            "--",
            f"cluster-alias:{workspace.metadata}/",
            f"{destination.resolve()}/metadata/",
        ),
    ]
    assert (destination / "output/results/result.txt").read_text(
        encoding="utf-8"
    ) == "generation 2\n"
    assert first.artifacts == second.artifacts
    assert [artifact.kind for artifact in second.artifacts] == [
        ArtifactKind.STDERR,
        ArtifactKind.STDOUT,
        ArtifactKind.SCHEDULER_METADATA,
        ArtifactKind.RAW_RESULT,
    ]
    assert all(artifact.size_bytes is not None for artifact in second.artifacts)
    assert not list(destination.rglob("*.tmp"))


def test_rsync_fetch_rejects_unsafe_inputs_and_redacts_transfer_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")
    stager = RsyncStager(RecordingTransport(deque()), host="cluster-alias")
    with pytest.raises(RsyncRetrievalError, match="filesystem root"):
        stager.fetch(FetchRequest(_workspace(), ("results/**",), PurePosixPath("/")))
    with pytest.raises(RsyncRetrievalError, match="NUL"):
        stager.fetch(
            FetchRequest(_workspace(), ("results/\x00*",), tmp_path / "retrieved")
        )

    def fail(
        argv: tuple[str, ...], **options: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 12, "", "SECRET_REMOTE_DATA")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(RsyncRetrievalError, match="exit code 12") as captured:
        stager.fetch(
            FetchRequest(_workspace(), ("results/**",), tmp_path / "retrieved")
        )
    assert "SECRET_REMOTE_DATA" not in str(captured.value)


def test_rsync_fetch_rejects_symlinked_destination_before_transfer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shutil, "which", lambda executable: "/usr/bin/rsync")
    outside = tmp_path / "outside"
    outside.mkdir()
    destination = tmp_path / "retrieved"
    destination.mkdir()
    (destination / "output").symlink_to(outside, target_is_directory=True)

    def forbidden(*args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("rsync must not run for an unsafe destination")

    monkeypatch.setattr(subprocess, "run", forbidden)
    stager = RsyncStager(RecordingTransport(deque()), host="cluster")

    with pytest.raises(RsyncRetrievalError, match="destination tree.*symbolic links"):
        stager.fetch(FetchRequest(_workspace(), ("results/**",), destination))

    assert list(outside.iterdir()) == []


def test_rsync_fetch_requires_host_and_valid_semantic_workspace(tmp_path: Path) -> None:
    with pytest.raises(RsyncRetrievalError, match="requires a host"):
        RsyncStager(RecordingTransport(deque())).fetch(
            FetchRequest(_workspace(), ("results/**",), tmp_path / "retrieved")
        )
    workspace = _workspace()
    malformed = replace(workspace, outputs=workspace.root.parent / "escaped")
    with pytest.raises(RsyncRetrievalError, match="workspace"):
        RsyncStager(RecordingTransport(deque()), host="cluster").fetch(
            FetchRequest(malformed, ("results/**",), tmp_path / "retrieved")
        )
    invalid_run_root = PurePosixPath("/remote/work tree/runs/not-a-run")
    invalid_run = replace(
        workspace,
        root=invalid_run_root,
        source=invalid_run_root / "source",
        inputs=invalid_run_root / "input",
        config=invalid_run_root / "input/config.yaml",
        runtime=invalid_run_root / "runtime",
        outputs=invalid_run_root / "output",
        logs=invalid_run_root / "logs",
        metadata=invalid_run_root / "metadata",
    )
    with pytest.raises(RsyncRetrievalError, match="Run ID"):
        RsyncStager(RecordingTransport(deque()), host="cluster").fetch(
            FetchRequest(invalid_run, ("results/**",), tmp_path / "retrieved")
        )
