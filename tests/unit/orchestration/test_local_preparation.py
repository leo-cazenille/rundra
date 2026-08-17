from __future__ import annotations

import hashlib
import os
import subprocess
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
from rundra.orchestration.preparation import (
    PreparationError,
    PreparedSource,
    build_remote_preparation_command,
    create_remote_preparation_spec,
    prepare_local,
)
from rundra.ports import StagedWorkspace


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


def test_local_preparation_checks_requested_name_in_explicit_search_path(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    search = tmp_path / "shared-images"
    search.mkdir()
    image = search / "application.sif"
    image.write_bytes(b"verified-shared-image")
    recipe = _recipe(image, build=False)
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=source,
        offline=True,
    )

    prepared = prepare_local(
        plan,
        _experiment(recipe),
        _target(tmp_path),
        project_root=tmp_path / "project-without-image",
        source_root=source,
        cache_root=tmp_path / "cache",
        image_search_paths=(search,),
    )

    assert prepared.record.image_action == "cache_verified_candidate"
    assert prepared.record.image_path.read_bytes() == b"verified-shared-image"


def test_remote_preparation_script_builds_and_reuses_target_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "remote" / "runs" / "run_0" / "source"
    source.mkdir(parents=True)
    (source / "input.txt").write_text("source", encoding="utf-8")
    image_candidate = tmp_path / "application.sif"
    image_candidate.write_bytes(b"immutable-sif")
    recipe = _recipe(image_candidate, build=True)
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=tmp_path,
        offline=True,
    )
    target = _target(tmp_path)
    target = Target(
        name=target.name,
        transport=target.transport,
        scheduler=target.scheduler,
        staging=target.staging,
        container=target.container,
        workspace=tmp_path / "remote",
    )
    prepared_source = PreparedSource(source, "34" * 32, "snapshot", "working-tree")
    target_cache = tmp_path / "shared-cache"
    image_search = tmp_path / "shared-images"
    image_search.mkdir()
    (image_search / recipe.image.name).write_bytes(image_candidate.read_bytes())
    spec = create_remote_preparation_spec(
        plan,
        prepared_source,
        target,
        "56" * 32,
        cache_root=target_cache,
        image_search_paths=(image_search,),
    )
    run_root = source.parent
    workspace = StagedWorkspace(
        root=run_root,
        source=source,
        inputs=run_root / "input",
        config=run_root / "input/config.yaml",
        runtime=run_root / "runtime",
        outputs=run_root / "output",
        logs=run_root / "logs",
        metadata=run_root / "metadata",
    )
    workspace.metadata.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _fake_apptainer(fake_bin).rename(fake_bin / "apptainer")
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")
    command = build_remote_preparation_command(spec, workspace)

    cold = subprocess.run(
        command.argv, check=False, capture_output=True, text=True, timeout=10
    )
    warm = subprocess.run(
        command.argv, check=False, capture_output=True, text=True, timeout=10
    )

    assert (cold.returncode, cold.stderr) == (0, "")
    assert (warm.returncode, warm.stderr) == (0, "")
    assert (workspace.source / "bin/model").read_text(encoding="utf-8") == "built"
    assert spec.build_key is not None
    assert (target_cache / "images" / f"{recipe.image.sha256}.sif").is_file()
    entry = target_cache / "builds" / spec.build_key
    assert (entry / ".complete").is_file()
    assert (entry / "source/bin/model").is_file()
    assert stat_mode(entry) & 0o222 == 0


def stat_mode(path: Path) -> int:
    return path.stat().st_mode
