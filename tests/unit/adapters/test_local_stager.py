from __future__ import annotations

import os
import stat
from dataclasses import replace
from pathlib import Path

import pytest

from rundra.adapters.local import (
    LocalStager,
    LocalStagerError,
    WorkspaceCollisionError,
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
from rundra.ports import FetchRequest, Stager, StageRequest


def _request(
    source: Path,
    workspace: Path,
    *,
    run_id: str = "run_0123456789abcdef0123456789abcdef",
    excludes: tuple[str, ...] = (),
) -> StageRequest:
    experiment = ExperimentSpec(
        version=1,
        name="example",
        command=Command(("python", "main.py")),
        resources=ResourceRequest(),
        outputs=("results/**",),
        sync_excludes=excludes,
    )
    target = Target(
        name="local",
        transport=BackendConfig("local"),
        scheduler=BackendConfig("local"),
        staging=BackendConfig("local"),
        container=BackendConfig("apptainer"),
        workspace=workspace,
    )
    return StageRequest(
        RunId(run_id),
        experiment,
        ConfigSnapshot(source / "config.yaml", "alpha: 1\r\nno_eof: true"),
        target,
        source,
    )


def _make_source(root: Path) -> Path:
    source = root / "project"
    (source / ".git").mkdir(parents=True)
    (source / ".venv").mkdir()
    (source / "__pycache__").mkdir()
    (source / "build").mkdir()
    (source / "nested").mkdir()
    (source / "main.py").write_text("print('snapshot')\n", encoding="utf-8")
    (source / "nested" / "keep.txt").write_text("keep\n", encoding="utf-8")
    (source / "build" / "drop.txt").write_text("drop\n", encoding="utf-8")
    (source / "notes.tmp").write_text("drop\n", encoding="utf-8")
    (source / ".git" / "config").write_text("git\n", encoding="utf-8")
    (source / ".venv" / "python").write_text("venv\n", encoding="utf-8")
    (source / "__pycache__" / "main.pyc").write_bytes(b"cache")
    os.chmod(source / "main.py", 0o755)
    return source


def _restore_writes(*roots: Path) -> None:
    for root in roots:
        if not root.exists():
            continue
        for path in (root, *root.rglob("*")):
            if not path.is_symlink():
                os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)


def test_local_stage_copies_isolates_excludes_and_seals_inputs(tmp_path: Path) -> None:
    source = _make_source(tmp_path)
    request = _request(
        source,
        tmp_path / "workspace",
        excludes=("build/", "*.tmp"),
    )
    stager = LocalStager()

    workspace = stager.stage(request)

    assert isinstance(stager, Stager)
    assert workspace.root == tmp_path / "workspace/runs" / str(request.run_id)
    assert workspace.source == workspace.root / "source"
    assert workspace.inputs == workspace.root / "input"
    assert workspace.config == workspace.inputs / "config.yaml"
    assert workspace.runtime == workspace.root / "runtime"
    assert workspace.outputs == workspace.root / "output"
    assert workspace.logs == workspace.root / "logs"
    assert workspace.metadata == workspace.root / "metadata"
    assert (workspace.source / "main.py").read_text(encoding="utf-8") == (
        "print('snapshot')\n"
    )
    assert (workspace.source / "nested/keep.txt").is_file()
    assert not (workspace.source / ".git").exists()
    assert not (workspace.source / ".venv").exists()
    assert not (workspace.source / "__pycache__").exists()
    assert not (workspace.source / "build").exists()
    assert not (workspace.source / "notes.tmp").exists()
    assert workspace.config.read_bytes() == b"alpha: 1\r\nno_eof: true"
    assert stat.S_IMODE((workspace.source / "main.py").stat().st_mode) & 0o111
    assert [artifact.kind for artifact in workspace.artifacts] == [
        ArtifactKind.SOURCE_SNAPSHOT,
        ArtifactKind.EFFECTIVE_CONFIG,
    ]


def test_local_stage_creates_isolated_task_mutation_directories(
    tmp_path: Path,
) -> None:
    source = _make_source(tmp_path)
    request = replace(
        _request(source, tmp_path / "workspace"),
        task_ids=(TaskId.from_ordinal(0), TaskId.from_ordinal(1)),
    )

    workspace = LocalStager().stage(request)

    for task_id in request.task_ids:
        assert (workspace.runtime / str(task_id)).is_dir()
        assert (workspace.outputs / str(task_id)).is_dir()
    assert workspace.artifacts[1].size_bytes == len(request.config.content.encode())

    (source / "main.py").write_text("print('edited')\n", encoding="utf-8")
    assert (workspace.source / "main.py").read_text(encoding="utf-8") == (
        "print('snapshot')\n"
    )
    for sealed in (workspace.source, workspace.inputs):
        for path in (sealed, *sealed.rglob("*")):
            assert stat.S_IMODE(path.stat().st_mode) & 0o222 == 0
    for mutable in (
        workspace.root,
        workspace.runtime,
        workspace.outputs,
        workspace.logs,
        workspace.metadata,
    ):
        assert stat.S_IMODE(mutable.stat().st_mode) & stat.S_IWUSR
    try:
        with pytest.raises(PermissionError):
            workspace.config.write_text("changed\n", encoding="utf-8")
        with pytest.raises(PermissionError):
            (workspace.source / "main.py").write_text("changed\n", encoding="utf-8")
    finally:
        _restore_writes(workspace.source, workspace.inputs)


def test_local_stage_is_collision_safe_and_run_snapshots_are_independent(
    tmp_path: Path,
) -> None:
    source = _make_source(tmp_path)
    stager = LocalStager()
    first_request = _request(source, tmp_path / "workspace")
    first = stager.stage(first_request)
    second_request = _request(
        source,
        tmp_path / "workspace",
        run_id="run_fedcba9876543210fedcba9876543210",
    )
    second = stager.stage(second_request)

    try:
        assert first.root != second.root
        assert (first.source / "main.py").read_bytes() == (
            second.source / "main.py"
        ).read_bytes()
        with pytest.raises(WorkspaceCollisionError, match=str(first_request.run_id)):
            stager.stage(first_request)
        assert (first.source / "main.py").is_file()
    finally:
        _restore_writes(first.source, first.inputs, second.source, second.inputs)


def test_local_stage_avoids_recursing_into_workspace_inside_source(
    tmp_path: Path,
) -> None:
    source = _make_source(tmp_path)
    request = _request(source, source / ".rundra")

    workspace = LocalStager().stage(request)

    try:
        assert (workspace.source / "main.py").is_file()
        assert not (workspace.source / ".rundra").exists()
    finally:
        _restore_writes(workspace.source, workspace.inputs)


def test_local_stage_dereferences_source_symlinks_into_the_snapshot(
    tmp_path: Path,
) -> None:
    source = _make_source(tmp_path)
    external = tmp_path / "external.txt"
    external.write_text("original\n", encoding="utf-8")
    (source / "linked.txt").symlink_to(external)

    workspace = LocalStager().stage(_request(source, tmp_path / "workspace"))
    external.write_text("edited\n", encoding="utf-8")

    try:
        staged = workspace.source / "linked.txt"
        assert not staged.is_symlink()
        assert staged.read_text(encoding="utf-8") == "original\n"
    finally:
        _restore_writes(workspace.source, workspace.inputs)


def test_local_stage_rejects_unsafe_requests_before_allocating_a_run(
    tmp_path: Path,
) -> None:
    source = _make_source(tmp_path)
    stager = LocalStager()

    with pytest.raises(LocalStagerError, match="relative exclusion"):
        stager.stage(_request(source, tmp_path / "workspace", excludes=("../secret",)))
    assert not (tmp_path / "workspace/runs").exists()

    remote = replace(
        _request(source, tmp_path / "workspace"),
        target=replace(
            _request(source, tmp_path / "workspace").target,
            staging=BackendConfig("rsync"),
        ),
    )
    with pytest.raises(LocalStagerError, match="staging backend is not local"):
        stager.stage(remote)

    with pytest.raises(LocalStagerError, match="must differ from source root"):
        stager.stage(_request(source, source))


def test_local_stage_cleans_partial_workspace_after_copy_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rundra.adapters.local as local

    source = _make_source(tmp_path)
    request = _request(source, tmp_path / "workspace")

    def fail_copy(*args: object, **kwargs: object) -> None:
        raise OSError("simulated copy failure")

    monkeypatch.setattr(local.shutil, "copytree", fail_copy)
    with pytest.raises(LocalStagerError, match="simulated copy failure"):
        LocalStager().stage(request)

    assert not (tmp_path / "workspace/runs" / str(request.run_id)).exists()


def test_local_fetch_is_idempotent_atomic_and_returns_raw_artifacts(
    tmp_path: Path,
) -> None:
    source = _make_source(tmp_path)
    workspace = LocalStager().stage(_request(source, tmp_path / "workspace"))
    (workspace.outputs / "results/nested").mkdir(parents=True)
    result_file = workspace.outputs / "results/result.txt"
    nested_file = workspace.outputs / "results/nested/value.bin"
    result_file.write_text("first\n", encoding="utf-8")
    nested_file.write_bytes(b"\x00\x01")
    destination = tmp_path / "retrieved"
    request = FetchRequest(workspace, ("results/**",), destination)

    first = LocalStager().fetch(request)
    result_file.write_text("second\n", encoding="utf-8")
    second = LocalStager().fetch(request)

    try:
        assert (destination / "results/result.txt").read_text(encoding="utf-8") == (
            "second\n"
        )
        assert (destination / "results/nested/value.bin").read_bytes() == b"\x00\x01"
        assert first.artifacts[0].kind is ArtifactKind.RAW_RESULT
        assert [artifact.path for artifact in second.artifacts] == [
            destination / "results/nested/value.bin",
            destination / "results/result.txt",
        ]
        assert [artifact.size_bytes for artifact in second.artifacts] == [2, 7]
        assert not list(destination.rglob("*.tmp"))
    finally:
        _restore_writes(workspace.source, workspace.inputs)


def test_fetch_rejects_path_traversal_symlinks_and_run_destinations(
    tmp_path: Path,
) -> None:
    source = _make_source(tmp_path)
    workspace = LocalStager().stage(_request(source, tmp_path / "workspace"))
    (workspace.outputs / "result.txt").write_text("result\n", encoding="utf-8")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside\n", encoding="utf-8")
    (workspace.outputs / "link.txt").symlink_to(outside)
    stager = LocalStager()

    try:
        with pytest.raises(ValueError, match="relative patterns"):
            FetchRequest(workspace, ("../outside.txt",), tmp_path / "retrieved")
        with pytest.raises(LocalStagerError, match="symbolic link"):
            stager.fetch(FetchRequest(workspace, ("*.txt",), tmp_path / "retrieved"))
        with pytest.raises(LocalStagerError, match="outside the Run workspace"):
            stager.fetch(
                FetchRequest(
                    workspace,
                    ("result.txt",),
                    workspace.metadata / "retrieved",
                )
            )
    finally:
        _restore_writes(workspace.source, workspace.inputs)
