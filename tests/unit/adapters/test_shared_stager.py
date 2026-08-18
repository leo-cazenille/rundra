from __future__ import annotations

from pathlib import Path

import pytest

from rundra.adapters.shared import SharedStager, SharedStagerError
from rundra.domain.models import (
    BackendConfig,
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    ResourceRequest,
    RunId,
    Target,
)
from rundra.ports import FetchRequest, StageRequest


def _request(source: Path, root: Path) -> StageRequest:
    return StageRequest(
        RunId("run_0123456789abcdef0123456789abcdef"),
        ExperimentSpec(
            1,
            "shared",
            Command(("true",)),
            ResourceRequest(),
            outputs=("result.txt",),
        ),
        ConfigSnapshot(source / "config.yaml", "value: 1\n"),
        Target(
            "shoal",
            BackendConfig("ssh", {"host": "fishvision"}),
            BackendConfig("slurm"),
            BackendConfig("shared", {"root": str(root)}),
            BackendConfig("apptainer"),
            root / "workspace",
        ),
        source,
    )


def test_shared_stager_stages_and_fetches_without_transport(tmp_path: Path) -> None:
    root = tmp_path / "shared"
    source = root / "project"
    source.mkdir(parents=True)
    (source / "main.py").write_text("pass\n", encoding="utf-8")
    stager = SharedStager(root)

    workspace = stager.stage(_request(source, root))
    (Path(workspace.outputs) / "result.txt").write_text("ok\n", encoding="utf-8")
    destination = root / "retrieved"
    result = stager.fetch(FetchRequest(workspace, ("result.txt",), destination))

    assert (destination / "result.txt").read_text(encoding="utf-8") == "ok\n"
    assert result.artifacts[0].path == destination / "result.txt"


def test_shared_stager_rejects_paths_outside_declared_root(tmp_path: Path) -> None:
    root = tmp_path / "shared"
    root.mkdir()
    source = tmp_path / "outside"
    source.mkdir()

    with pytest.raises(SharedStagerError, match="source root"):
        SharedStager(root).stage(_request(source, root))
