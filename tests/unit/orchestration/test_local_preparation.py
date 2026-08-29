from __future__ import annotations

import hashlib
import os
import subprocess
from datetime import timedelta
from pathlib import Path, PurePath

import pytest

from rundra.adapters import LocalTransport
from rundra.domain.models import (
    BackendConfig,
    Command,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    Target,
)
from rundra.domain.preparation import (
    DefinitionBuildPolicy,
    PreparationBuild,
    PreparationConfig,
    PreparationImage,
    PreparationImageDefinition,
    PreparationOutput,
    PreparationPlan,
    PreparationSourceGit,
    PreparationSourceWorkingTree,
)
from rundra.domain.storage import SlurmScratchPolicy
from rundra.orchestration.preparation import (
    PreparationError,
    PreparedSource,
    build_remote_preparation_command,
    create_remote_preparation_spec,
    prepare_local,
    prepare_source_snapshot,
    probe_remote_preparation_cache,
    read_remote_preparation_result,
    remote_platform_fingerprint,
    select_remote_preparation_location,
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


@pytest.mark.parametrize(
    ("requested", "allowed", "expected"),
    (
        ("auto", ("target",), "target"),
        ("auto", ("local",), "local"),
        ("auto", ("local", "target"), "local"),
        ("local", ("target",), "local"),
        ("target", ("local",), "target"),
    ),
)
def test_remote_definition_build_location_respects_target_policy(
    tmp_path: Path,
    requested: str,
    allowed: tuple[str, ...],
    expected: str,
) -> None:
    image = PreparationImageDefinition(
        PurePath("python.sif"),
        PurePath("python.def"),
        ResourceRequest(
            cpus_per_task=1,
            memory_bytes=1024**3,
            walltime=timedelta(minutes=5),
        ),
    )
    plan = PreparationPlan(
        PreparationConfig(PreparationSourceWorkingTree(), image, None),
        source_mode="working_tree",
        source_root=tmp_path,
        requested_location=requested,
    )
    policy = DefinitionBuildPolicy(
        allowed,
        "unprivileged",
        ResourceRequest(
            cpus_per_task=2,
            memory_bytes=2 * 1024**3,
            walltime=timedelta(minutes=15),
        ),
    )

    assert select_remote_preparation_location(plan, policy) == expected


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
if args[0] == "version":
    print("apptainer version 1.4.0")
    raise SystemExit(0)
if args[0] == "build":
    output = args[-2]
    definition = args[-1]
    with open(definition, "rb") as source, open(output, "wb") as target:
        target.write(b"fake-sif:")
        target.write(source.read())
    raise SystemExit(0)
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
    assert cold.record.build_action == "build_and_publish"
    assert warm.record.build_action == "reuse_build_cache"
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


def test_definition_image_build_is_content_addressed_and_reused(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "python.def").write_text(
        "Bootstrap: docker\nFrom: python:3.12-slim\n", encoding="utf-8"
    )
    recipe = PreparationConfig(
        source=PreparationSourceWorkingTree(),
        image=PreparationImageDefinition(
            PurePath("python.sif"),
            PurePath("python.def"),
            ResourceRequest(
                cpus_per_task=1,
                memory_bytes=1024**3,
                walltime=timedelta(minutes=5),
            ),
        ),
        build=None,
    )
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=source,
    )
    policy = DefinitionBuildPolicy(
        ("local",),
        "fakeroot",
        ResourceRequest(
            cpus_per_task=2,
            memory_bytes=2 * 1024**3,
            walltime=timedelta(minutes=15),
        ),
    )
    executable = _fake_apptainer(tmp_path)
    cache = tmp_path / "cache"

    cold = prepare_local(
        plan,
        _experiment(recipe),
        _target(tmp_path),
        project_root=source,
        source_root=source,
        cache_root=cache,
        definition_build=policy,
        apptainer_executable=str(executable),
    )
    warm = prepare_local(
        plan,
        _experiment(recipe),
        _target(tmp_path),
        project_root=source,
        source_root=source,
        cache_root=cache,
        definition_build=policy,
        apptainer_executable=str(executable),
    )

    assert cold.record.image_action == "build_definition_image"
    assert warm.record.image_action == "reuse_definition_image_cache"
    assert cold.record.image_sha256 == warm.record.image_sha256
    assert cold.record.image_path.name == f"{cold.record.image_sha256}.sif"
    assert cold.record.image_uri == "definition:python.def"


def test_working_tree_preparation_applies_default_sync_exclusions(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "keep.txt").write_text("included", encoding="utf-8")
    for transient in (".agents", "retrieved", "tmp", "downloads"):
        (source / transient).mkdir()
        (source / transient / "large.bin").write_bytes(b"excluded")
    (source / "image.sif").write_bytes(b"excluded")
    (source / "legacy.simg").write_bytes(b"excluded")
    recipe_image = tmp_path / "expected.sif"
    recipe_image.write_bytes(b"recipe-image")
    plan = PreparationPlan(
        _recipe(recipe_image, build=False),
        source_mode="working_tree",
        source_root=source,
        offline=True,
    )

    prepared = prepare_source_snapshot(
        plan,
        source_root=source,
        excludes=(),
        cache_root=tmp_path / "cache",
    )

    assert (prepared.root / "keep.txt").is_file()
    for transient in (".agents", "retrieved", "tmp", "downloads"):
        assert not (prepared.root / transient).exists()
    assert not (prepared.root / "image.sif").exists()
    assert not (prepared.root / "legacy.simg").exists()


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
        source_mode="git",
        source_root=None,
        offline=True,
    )
    target = _target(tmp_path)
    target = Target(
        name=target.name,
        transport=target.transport,
        scheduler=target.scheduler,
        staging=target.staging,
        container=BackendConfig("apptainer", {"executable": "singularity"}),
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
        remote_platform_fingerprint(LocalTransport()),
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
    _fake_apptainer(fake_bin).rename(fake_bin / "singularity")
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")
    command = build_remote_preparation_command(spec, workspace)
    scratch_command = build_remote_preparation_command(
        spec, workspace, scratch_policy=SlurmScratchPolicy()
    )

    assert "SLURM_TMPDIR is required by target policy" in scratch_command.argv[2]
    assert "singularity exec" in command.argv[-1]
    assert "apptainer exec" not in command.argv[-1]
    assert "--no-eval" not in command.argv[-1]
    assert "--pwd /workspace" in command.argv[-1]
    assert "$rundra_run_root/source" in scratch_command.argv[4]
    assert 'mktemp -d "$rundra_scratch/build.XXXXXX"' in scratch_command.argv[4]

    cold = subprocess.run(
        command.argv, check=False, capture_output=True, text=True, timeout=10
    )
    cold_result = read_remote_preparation_result(LocalTransport(), workspace)
    warm = subprocess.run(
        command.argv, check=False, capture_output=True, text=True, timeout=10
    )
    warm_result = read_remote_preparation_result(LocalTransport(), workspace)

    assert (cold.returncode, cold.stderr) == (0, "")
    assert (warm.returncode, warm.stderr) == (0, "")
    assert cold_result is not None
    assert cold_result.image_action == "cache_verified_candidate"
    assert cold_result.build_action == "build_and_publish"
    assert warm_result is not None
    assert warm_result.image_action == "reuse_image_cache"
    assert warm_result.build_action == "reuse_build_cache"
    assert (workspace.source / "bin/model").read_text(encoding="utf-8") == "built"
    assert spec.build_key is not None
    assert (target_cache / "images" / f"{recipe.image.sha256}.sif").is_file()
    assert (target_cache / "images" / f"{recipe.image.sha256}.sif.receipt").is_file()
    entry = target_cache / "builds" / spec.build_key
    assert (entry / ".complete").is_file()
    assert (entry / "source/bin/model").is_file()
    assert stat_mode(entry) & 0o222 == 0

    cached = probe_remote_preparation_cache(
        plan,
        _experiment(recipe),
        target,
        LocalTransport(),
        cache_root=target_cache,
    )
    assert cached is not None
    assert cached.source_root == entry / "source"
    assert cached.experiment_image == (
        target_cache / "images" / f"{recipe.image.sha256}.sif"
    )
    assert cached.record.source_action == "reuse_target_source_cache"


def test_remote_definition_build_runs_in_bounded_preparation_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "remote/runs/run_0/source"
    source.mkdir(parents=True)
    (source / "python.def").write_text(
        "Bootstrap: docker\nFrom: python:3.12-slim\n", encoding="utf-8"
    )
    image = PreparationImageDefinition(
        PurePath("python.sif"),
        PurePath("python.def"),
        ResourceRequest(
            cpus_per_task=2,
            memory_bytes=2 * 1024**3,
            walltime=timedelta(minutes=15),
        ),
    )
    application_build = PreparationBuild(
        argv=(
            "python3",
            "-c",
            "from pathlib import Path; p=Path('bin/model'); p.parent.mkdir(); "
            "p.write_text('built'); p.chmod(0o755)",
        ),
        outputs=(PreparationOutput(PurePath("bin/model"), True),),
        cache_scope="target",
        resources=ResourceRequest(
            cpus_per_task=1,
            memory_bytes=1024**3,
            walltime=timedelta(minutes=5),
        ),
    )
    recipe = PreparationConfig(PreparationSourceWorkingTree(), image, application_build)
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=tmp_path,
        requested_location="target",
    )
    policy = DefinitionBuildPolicy(
        ("target",),
        "fakeroot",
        ResourceRequest(
            cpus_per_task=2,
            memory_bytes=2 * 1024**3,
            walltime=timedelta(minutes=15),
        ),
    )
    target = _target(tmp_path)
    prepared_source = PreparedSource(source, "34" * 32, "snapshot", "working-tree")
    cache = tmp_path / "cache"
    spec = create_remote_preparation_spec(
        plan,
        prepared_source,
        target,
        "56" * 32,
        cache_root=cache,
        definition_build=policy,
        builder_version="apptainer version 1.4.0",
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
    cold_result = read_remote_preparation_result(LocalTransport(), workspace)
    warm = subprocess.run(
        command.argv, check=False, capture_output=True, text=True, timeout=10
    )
    warm_result = read_remote_preparation_result(LocalTransport(), workspace)

    assert (cold.returncode, cold.stderr) == (0, "")
    assert (warm.returncode, warm.stderr) == (0, "")
    assert "apptainer build --disable-cache --fakeroot" in command.argv[-1]
    assert "apptainer pull" not in command.argv[-1]
    assert cold_result is not None and cold_result.image_sha256 is not None
    assert cold_result.image_action == "build_definition_image"
    assert cold_result.build_action == "build_and_publish"
    assert cold_result.build_key is not None
    assert (workspace.source / "bin/model").read_text(encoding="utf-8") == "built"
    assert warm_result is not None
    assert warm_result.image_action == "reuse_definition_image_cache"
    assert warm_result.build_action == "reuse_build_cache"
    assert warm_result.build_key == cold_result.build_key
    assert warm_result.image_sha256 == cold_result.image_sha256
    assert warm_result.image_path is not None and Path(warm_result.image_path).is_file()

    unprivileged = DefinitionBuildPolicy(
        ("target",), "unprivileged", policy.max_resources
    )
    unprivileged_spec = create_remote_preparation_spec(
        plan,
        prepared_source,
        target,
        "56" * 32,
        cache_root=cache,
        definition_build=unprivileged,
        builder_version="apptainer version 1.4.0",
    )
    unprivileged_command = build_remote_preparation_command(
        unprivileged_spec, workspace
    )
    assert (
        "apptainer build --disable-cache --ignore-subuid"
        in (unprivileged_command.argv[-1])
    )


def test_remote_preparation_pull_uses_a_nonexistent_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "remote/runs/run_0/source"
    source.mkdir(parents=True)
    image_contents = b"pulled-immutable-sif"
    recipe_image = tmp_path / "expected.sif"
    recipe_image.write_bytes(image_contents)
    recipe = _recipe(recipe_image, build=False)
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=tmp_path,
        offline=False,
    )
    target = _target(tmp_path)
    prepared_source = PreparedSource(source, "34" * 32, "snapshot", "working-tree")
    target_cache = tmp_path / "cache"
    spec = create_remote_preparation_spec(
        plan,
        prepared_source,
        target,
        "56" * 32,
        cache_root=target_cache,
    )
    workspace = StagedWorkspace(
        root=source.parent,
        source=source,
        inputs=source.parent / "input",
        config=source.parent / "input/config.yaml",
        runtime=source.parent / "runtime",
        outputs=source.parent / "output",
        logs=source.parent / "logs",
        metadata=source.parent / "metadata",
    )
    workspace.metadata.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_apptainer = fake_bin / "apptainer"
    fake_apptainer.write_text(
        """#!/usr/bin/env python3
import pathlib
import sys

destination = pathlib.Path(sys.argv[-2])
if destination.exists():
    print(f"destination already exists: {destination}", file=sys.stderr)
    raise SystemExit(17)
destination.write_bytes(b"pulled-immutable-sif")
""",
        encoding="utf-8",
    )
    fake_apptainer.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")

    completed = subprocess.run(
        build_remote_preparation_command(spec, workspace).argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    result = read_remote_preparation_result(LocalTransport(), workspace)

    assert (completed.returncode, completed.stderr) == (0, "")
    assert result is not None
    assert result.image_action == "pull_image"
    assert (
        target_cache / "images" / f"{recipe.image.sha256}.sif"
    ).read_bytes() == image_contents
    assert (target_cache / "images" / f"{recipe.image.sha256}.sif.receipt").is_file()


def test_remote_preparation_reuses_receipt_without_hashing_image(
    tmp_path: Path,
) -> None:
    source = tmp_path / "remote/runs/run_0/source"
    source.mkdir(parents=True)
    recipe_image = tmp_path / "expected.sif"
    recipe_image.write_bytes(b"immutable-sif")
    recipe = _recipe(recipe_image, build=False)
    plan = PreparationPlan(
        recipe,
        source_mode="working_tree",
        source_root=tmp_path,
        offline=True,
    )
    target_cache = tmp_path / "cache"
    images = target_cache / "images"
    images.mkdir(parents=True)
    cached = images / f"{recipe.image.sha256}.sif"
    cached.write_bytes(recipe_image.read_bytes())
    cached.chmod(0o444)
    receipt = images / f"{recipe.image.sha256}.sif.receipt"
    receipt.write_text(
        f"1\t{recipe.image.sha256}\t{cached.stat().st_size}\n",
        encoding="ascii",
    )
    receipt.chmod(0o444)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sha256sum = fake_bin / "sha256sum"
    fake_sha256sum.write_text("#!/bin/sh\nexit 91\n", encoding="ascii")
    fake_sha256sum.chmod(0o755)
    spec = create_remote_preparation_spec(
        plan,
        PreparedSource(source, "34" * 32, "snapshot", "working-tree"),
        _target(tmp_path),
        "56" * 32,
        cache_root=target_cache,
    )
    workspace = StagedWorkspace(
        root=source.parent,
        source=source,
        inputs=source.parent / "input",
        config=source.parent / "input/config.yaml",
        runtime=source.parent / "runtime",
        outputs=source.parent / "output",
        logs=source.parent / "logs",
        metadata=source.parent / "metadata",
    )
    workspace.metadata.mkdir()

    completed = subprocess.run(
        build_remote_preparation_command(spec, workspace).argv,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
    )

    assert (completed.returncode, completed.stderr) == (0, "")


def stat_mode(path: Path) -> int:
    return path.stat().st_mode
