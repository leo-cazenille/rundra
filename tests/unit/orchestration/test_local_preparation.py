from __future__ import annotations

import hashlib
import os
from datetime import timedelta
from pathlib import Path, PurePath

import pytest

from rundra.domain.models import (
    BackendConfig,
    Command,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    Target,
)
from rundra.domain.preparation import (
    PreparationBuild,
    PreparationConfig,
    PreparationImage,
    PreparationOutput,
    PreparationPlan,
    PreparationSourceGit,
)
from rundra.orchestration.preparation import PreparationError, prepare_local


def _target(tmp_path: Path) -> Target:
    return Target(
        name="local",
        transport=BackendConfig("local"),
        scheduler=BackendConfig("local"),
        staging=BackendConfig("local"),
        container=BackendConfig("apptainer"),
        workspace=tmp_path / "workspace",
    )


def _recipe(image: Path, *, build: bool) -> PreparationConfig:
    digest = hashlib.sha256(image.read_bytes()).hexdigest()
    return PreparationConfig(
        source=PreparationSourceGit(
            "https://example.test/project.git",
            "01" * 20,
        ),
        image=PreparationImage(PurePath("application.sif"), "library://unused", digest),
        build=(
            PreparationBuild(
                argv=(
                    "python3",
                    "-c",
                    "from pathlib import Path; "
                    "p=Path('bin/model'); p.parent.mkdir(); "
                    "p.write_text('built'); p.chmod(0o755)",
                ),
                outputs=(PreparationOutput(PurePath("bin/model"), True),),
                cache_scope="target",
                resources=ResourceRequest(
                    memory_bytes=1024**2,
                    walltime=timedelta(minutes=1),
                ),
            )
            if build
            else None
        ),
    )


def _experiment(recipe: PreparationConfig) -> ExperimentSpec:
    return ExperimentSpec(
        version=1,
        name="prepared",
        command=Command(("bin/model", "{config}", "{seed}")),
        resources=ResourceRequest(),
        container=ContainerSpec(recipe.image.name),
    )


def _fake_apptainer(tmp_path: Path) -> Path:
    executable = tmp_path / "fake-apptainer"
    executable.write_text(
        """#!/usr/bin/env python3
import os
import subprocess
import sys

args = sys.argv[1:]
if args[0] != "exec":
    raise SystemExit(64)
bind = args[args.index("--bind") + 1]
work = bind.split(":", 1)[0]
image_index = args.index("/workspace") + 1
command = args[image_index + 1:]
raise SystemExit(subprocess.run(command, cwd=work, env=os.environ).returncode)
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable


def test_local_preparation_publishes_and_reuses_verified_build_cache(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "input.txt").write_text("one", encoding="utf-8")
    image = tmp_path / "application.sif"
    image.write_bytes(b"immutable-sif")
    recipe = _recipe(image, build=True)
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=source,
        offline=True,
    )
    executable = _fake_apptainer(tmp_path)
    cache = tmp_path / "cache"

    cold = prepare_local(
        plan,
        _experiment(recipe),
        _target(tmp_path),
        project_root=tmp_path,
        source_root=source,
        cache_root=cache,
        apptainer_executable=str(executable),
    )
    warm = prepare_local(
        plan,
        _experiment(recipe),
        _target(tmp_path),
        project_root=tmp_path,
        source_root=source,
        cache_root=cache,
        apptainer_executable=str(executable),
    )

    assert cold.record.image_sha256 == recipe.image.sha256
    assert cold.record.image_path.is_absolute()
    assert cold.record.build_cache_key == warm.record.build_cache_key
    assert warm.record.source_action == "reuse_source_cache"
    assert (cold.source_root / "bin/model").read_text(encoding="utf-8") == "built"
    assert cold.record.build_outputs[0].executable is True
    assert os.access(cold.source_root / "bin/model", os.X_OK)
    assert stat_mode(cold.source_root) & 0o222 == 0


def test_working_tree_content_change_invalidates_source_and_build_keys(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    input_file = source / "input.txt"
    input_file.write_text("one", encoding="utf-8")
    image = tmp_path / "application.sif"
    image.write_bytes(b"immutable-sif")
    recipe = _recipe(image, build=True)
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=source,
        offline=True,
    )
    executable = _fake_apptainer(tmp_path)
    cache = tmp_path / "cache"

    first = prepare_local(
        plan,
        _experiment(recipe),
        _target(tmp_path),
        project_root=tmp_path,
        source_root=source,
        cache_root=cache,
        apptainer_executable=str(executable),
    )
    input_file.write_text("two", encoding="utf-8")
    second = prepare_local(
        plan,
        _experiment(recipe),
        _target(tmp_path),
        project_root=tmp_path,
        source_root=source,
        cache_root=cache,
        apptainer_executable=str(executable),
    )

    assert first.record.source_digest != second.record.source_digest
    assert first.record.build_cache_key != second.record.build_cache_key


def test_offline_preparation_rejects_an_unverified_image_candidate(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    image = tmp_path / "application.sif"
    image.write_bytes(b"expected")
    recipe = _recipe(image, build=False)
    image.write_bytes(b"tampered")
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=source,
        offline=True,
    )

    with pytest.raises(PreparationError, match="unavailable in offline mode"):
        prepare_local(
            plan,
            _experiment(recipe),
            _target(tmp_path),
            project_root=tmp_path,
            source_root=source,
            cache_root=tmp_path / "cache",
        )


def stat_mode(path: Path) -> int:
    return path.stat().st_mode
