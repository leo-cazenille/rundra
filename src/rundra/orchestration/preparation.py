from __future__ import annotations

import fcntl
import fnmatch
import hashlib
import os
import platform
import shlex
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import BinaryIO

from rundra.domain.models import Command, ContainerSpec, ExperimentSpec, Target
from rundra.domain.preparation import (
    PreparationOutput,
    PreparationPlan,
    PreparationRecord,
    PreparedOutput,
    build_cache_key,
    source_recipe_identity,
)
from rundra.ports import StagedWorkspace, Transport


class PreparationError(RuntimeError):
    """An actionable local preparation failure."""


@dataclass(frozen=True, slots=True)
class PreparedApplication:
    """Effective immutable inputs produced by preparation."""

    source_root: Path
    experiment: ExperimentSpec
    record: PreparationRecord


@dataclass(frozen=True, slots=True)
class PreparedSource:
    """One immutable source snapshot ready for local or remote staging."""

    root: Path
    digest: str
    action: str
    identity: str


@dataclass(frozen=True, slots=True)
class RemotePreparationSpec:
    """Resolved inputs for one scheduled target preparation job."""

    plan: PreparationPlan
    source_digest: str
    source_action: str
    source_identity: str
    platform_fingerprint: str
    build_key: str | None
    target_cache_root: PurePath | None = None
    image_search_paths: tuple[PurePath, ...] = ()


def prepare_source_snapshot(
    plan: PreparationPlan,
    *,
    source_root: Path,
    excludes: tuple[str, ...],
    cache_root: Path | None = None,
) -> PreparedSource:
    """Acquire and cache only source, without resolving an image or building."""
    root = (
        Path("~/.cache/rundra").expanduser().resolve()
        if cache_root is None
        else cache_root.expanduser().resolve()
    )
    root.mkdir(parents=True, exist_ok=True)
    snapshot, digest, action = _resolve_source(
        plan,
        source_root=source_root.expanduser().resolve(),
        cache_root=root,
        excludes=excludes,
    )
    identity = (
        source_recipe_identity(plan.recipe.source)
        if plan.source_mode == "git"
        else f"working-tree:{digest}"
    )
    return PreparedSource(snapshot, digest, action, identity)


def remote_platform_fingerprint(transport: Transport) -> str:
    """Read a target platform identity without running application compilation."""
    if not isinstance(transport, Transport):
        raise TypeError("remote_platform_fingerprint requires a Transport")
    result = transport.run(Command(("uname", "-srm")))
    if result.exit_code != 0 or not result.stdout.strip():
        raise PreparationError("Could not fingerprint target preparation platform")
    return hashlib.sha256(result.stdout.strip().encode("utf-8")).hexdigest()


def create_remote_preparation_spec(
    plan: PreparationPlan,
    source: PreparedSource,
    target: Target,
    platform_fingerprint: str,
    *,
    cache_root: PurePath | None = None,
    image_search_paths: tuple[PurePath, ...] = (),
) -> RemotePreparationSpec:
    """Create deterministic target preparation identities before submission."""
    build = plan.recipe.build
    key = None
    if build is not None:
        scope = target.name if build.cache_scope == "target" else platform_fingerprint
        key = build_cache_key(
            source_digest=source.digest,
            image_digest=plan.recipe.image.sha256,
            build=build,
            builder_scope=scope,
            platform_fingerprint=platform_fingerprint,
        )
    return RemotePreparationSpec(
        plan=plan,
        source_digest=source.digest,
        source_action=source.action,
        source_identity=source.identity,
        platform_fingerprint=platform_fingerprint,
        build_key=key,
        target_cache_root=(
            PurePath(str(target.workspace)) / "cache"
            if cache_root is None
            else cache_root
        ),
        image_search_paths=image_search_paths,
    )


def remote_preparation_record(
    spec: RemotePreparationSpec,
    target: Target,
) -> PreparationRecord:
    """Create pre-submission target preparation provenance."""
    image = (
        (spec.target_cache_root or PurePath(str(target.workspace)) / "cache")
        / "images"
        / (f"{spec.plan.recipe.image.sha256}.sif")
    )
    return PreparationRecord(
        source_identity=spec.source_identity,
        source_digest=spec.source_digest,
        source_action=spec.source_action,
        image_uri=spec.plan.recipe.image.uri,
        image_sha256=spec.plan.recipe.image.sha256,
        image_path=image,
        image_action="resolve_in_preparation_job",
        resolution_location="target",
        build_cache_key=spec.build_key,
        builder_location="target",
    )


def build_remote_preparation_command(
    spec: RemotePreparationSpec,
    workspace: StagedWorkspace,
) -> Command:
    """Render one bounded target job that verifies, builds, and publishes caches."""
    plan = spec.plan
    recipe = plan.recipe
    target_cache = spec.target_cache_root or workspace.root.parent.parent / "cache"
    image = target_cache / "images" / f"{recipe.image.sha256}.sif"
    image_lock = target_cache / "locks" / f"image-{recipe.image.sha256}.lock"
    lines = [
        "set -eu",
        "umask 077",
        f"cache={shlex.quote(str(target_cache))}",
        f"image={shlex.quote(str(image))}",
        f"image_lock={shlex.quote(str(image_lock))}",
        'mkdir -p -- "$cache/images" "$cache/builds" "$cache/locks"',
        "attempt=0",
        'while ! mkdir -- "$image_lock" 2>/dev/null; do',
        "  attempt=$((attempt + 1))",
        "  [ \"$attempt\" -lt 900 ] || { printf '%s\\n' 'image cache lock timeout' >&2; exit 75; }",
        "  sleep 1",
        "done",
        "trap 'rmdir -- \"$image_lock\" 2>/dev/null || :' EXIT HUP INT TERM",
        'if [ -f "$image" ]; then',
        "  actual=$(sha256sum -- \"$image\" | cut -d' ' -f1)",
        f"  [ \"$actual\" = {shlex.quote(recipe.image.sha256)} ] || {{ printf '%s\\n' 'cached image digest mismatch' >&2; exit 65; }}",
        "else",
    ]
    for search_root in spec.image_search_paths:
        candidate = search_root / recipe.image.name
        lines.extend(
            (
                f"  candidate={shlex.quote(str(candidate))}",
                '  if [ -f "$candidate" ] && [ ! -L "$candidate" ]; then',
                "    actual=$(sha256sum -- \"$candidate\" | cut -d' ' -f1)",
                f'    if [ "$actual" = {shlex.quote(recipe.image.sha256)} ]; then',
                '      image_tmp=$(mktemp "$cache/images/.candidate.XXXXXX")',
                '      cp -- "$candidate" "$image_tmp"',
                '      chmod a-w -- "$image_tmp"',
                '      mv -- "$image_tmp" "$image"',
                "    fi",
                "  fi",
            )
        )
    lines.append('  if [ ! -f "$image" ]; then')
    if plan.offline:
        lines.append(
            "  printf '%s\\n' 'verified image unavailable in offline mode' >&2; exit 69"
        )
    else:
        lines.extend(
            (
                '  image_tmp=$(mktemp "$cache/images/.pull.XXXXXX")',
                '  trap \'rm -f -- "$image_tmp"; rmdir -- "$image_lock" 2>/dev/null || :\' EXIT HUP INT TERM',
                f'  apptainer pull --disable-cache "$image_tmp" {shlex.quote(recipe.image.uri)}',
                "  actual=$(sha256sum -- \"$image_tmp\" | cut -d' ' -f1)",
                f"  [ \"$actual\" = {shlex.quote(recipe.image.sha256)} ] || {{ printf '%s\\n' 'pulled image digest mismatch' >&2; exit 65; }}",
                '  chmod a-w -- "$image_tmp"',
                '  mv -- "$image_tmp" "$image"',
            )
        )
    lines.extend(("  fi", "fi", 'rmdir -- "$image_lock"', "trap - EXIT HUP INT TERM"))
    build = recipe.build
    if build is not None:
        assert spec.build_key is not None
        entry = target_cache / "builds" / spec.build_key
        lock = target_cache / "locks" / f"build-{spec.build_key}.lock"
        lines.extend(
            (
                f"entry={shlex.quote(str(entry))}",
                f"build_lock={shlex.quote(str(lock))}",
                "attempt=0",
                'while ! mkdir -- "$build_lock" 2>/dev/null; do',
                "  attempt=$((attempt + 1))",
                "  [ \"$attempt\" -lt 900 ] || { printf '%s\\n' 'build cache lock timeout' >&2; exit 75; }",
                "  sleep 1",
                "done",
                "trap 'rmdir -- \"$build_lock\" 2>/dev/null || :' EXIT HUP INT TERM",
                f'if [ ! -f "$entry/.complete" ] || {"true" if plan.rebuild else "false"}; then',
                '  work=$(mktemp -d "$cache/builds/.work.XXXXXX")',
                '  publish=$(mktemp -d "$cache/builds/.publish.XXXXXX")',
                '  trap \'rm -rf -- "$work" "$publish"; rmdir -- "$build_lock" 2>/dev/null || :\' EXIT HUP INT TERM',
                f'  cp -a -- {shlex.quote(str(workspace.source))}/. "$work"/',
                '  chmod -R u+w -- "$work"',
                "  "
                + shlex.join(
                    (
                        "apptainer",
                        "exec",
                        "--cleanenv",
                        "--no-eval",
                        "--bind",
                        "$work:/workspace:rw",
                        "--cwd",
                        "/workspace",
                        "$image",
                        *build.argv,
                    )
                )
                .replace("'$work:/workspace:rw'", '"$work:/workspace:rw"')
                .replace("'$image'", '"$image"'),
            )
        )
        for output in build.outputs:
            rendered = shlex.quote(str(output.path))
            lines.append(f'  output="$work"/{rendered}')
            lines.append(
                "  [ -f \"$output\" ] || { printf '%s\\n' 'declared output missing' >&2; exit 66; }"
            )
            if output.executable:
                lines.append(
                    "  [ -x \"$output\" ] || { printf '%s\\n' 'declared output not executable' >&2; exit 66; }"
                )
        lines.extend(
            (
                '  mkdir -p -- "$publish/source"',
                '  cp -a -- "$work"/. "$publish"/source/',
                '  : > "$publish/.complete"',
                '  chmod -R a-w -- "$publish"',
                '  chmod a-w -- "$publish/.complete"',
                '  rm -rf -- "$entry"',
                '  mv -- "$publish" "$entry"',
                '  rm -rf -- "$work"',
                "fi",
                f"run_source_tmp={shlex.quote(str(workspace.root / '.prepared-source'))}",
                'if [ -e "$run_source_tmp" ]; then chmod -R u+w -- "$run_source_tmp"; fi',
                'rm -rf -- "$run_source_tmp"',
                'cp -a -- "$entry/source" "$run_source_tmp"',
                'chmod -R a-w -- "$run_source_tmp"',
                f"chmod -R u+w -- {shlex.quote(str(workspace.source))}",
                f"rm -rf -- {shlex.quote(str(workspace.source))}",
                f'mv -- "$run_source_tmp" {shlex.quote(str(workspace.source))}',
                'rmdir -- "$build_lock"',
                "trap - EXIT HUP INT TERM",
            )
        )
        lines.extend(
            (
                f"manifest_tmp={shlex.quote(str(workspace.metadata / '.preparation-outputs.tmp'))}",
                f"manifest={shlex.quote(str(workspace.metadata / 'preparation-outputs.tsv'))}",
                'rm -f -- "$manifest_tmp"',
            )
        )
        for output in build.outputs:
            rendered = shlex.quote(str(output.path))
            executable = "1" if output.executable else "0"
            lines.extend(
                (
                    f"output={shlex.quote(str(workspace.source))}/{rendered}",
                    "digest=$(sha256sum -- \"$output\" | cut -d' ' -f1)",
                    f'printf \'%s\\t%s\\t%s\\n\' "$digest" {executable} {rendered} >> "$manifest_tmp"',
                )
            )
        lines.extend(
            (
                'chmod a-w -- "$manifest_tmp"',
                'mv -- "$manifest_tmp" "$manifest"',
            )
        )
    return Command(("/bin/sh", "-c", "\n".join(lines)))


def read_remote_prepared_outputs(
    transport: Transport,
    workspace: StagedWorkspace,
) -> tuple[PreparedOutput, ...] | None:
    """Read a completed target preparation manifest without scheduler parsing."""
    result = transport.run(
        Command(("cat", "--", str(workspace.metadata / "preparation-outputs.tsv")))
    )
    if result.exit_code != 0:
        return None
    outputs: list[PreparedOutput] = []
    for index, line in enumerate(result.stdout.splitlines()):
        fields = line.split("\t")
        if len(fields) != 3 or fields[1] not in {"0", "1"}:
            raise PreparationError(
                f"Target preparation output manifest line {index + 1} is invalid"
            )
        outputs.append(
            PreparedOutput(
                path=PurePath(fields[2]),
                sha256=fields[0],
                executable=fields[1] == "1",
            )
        )
    return tuple(outputs)


def prepare_local(
    plan: PreparationPlan,
    experiment: ExperimentSpec,
    target: Target,
    *,
    project_root: Path,
    source_root: Path,
    cache_root: Path | None = None,
    image_search_paths: tuple[Path, ...] = (),
    apptainer_executable: str = "apptainer",
) -> PreparedApplication:
    """Resolve and build one project recipe in local immutable caches."""
    if type(plan) is not PreparationPlan:
        raise TypeError("prepare_local plan must be PreparationPlan")
    if type(experiment) is not ExperimentSpec or type(target) is not Target:
        raise TypeError("prepare_local requires experiment and target domain values")
    root = (
        Path("~/.cache/rundra").expanduser().resolve()
        if cache_root is None
        else cache_root.expanduser().resolve()
    )
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise PreparationError(
            f"Could not create preparation cache {root}: {error}"
        ) from error

    snapshot, source_digest, source_action = _resolve_source(
        plan,
        source_root=source_root.expanduser().resolve(),
        cache_root=root,
        excludes=experiment.sync_excludes,
    )
    image, image_action = _resolve_image(
        plan,
        project_root=project_root.expanduser().resolve(),
        cache_root=root,
        target=target,
        apptainer_executable=apptainer_executable,
        image_search_paths=image_search_paths,
    )
    prepared_source = snapshot
    key: str | None = None
    outputs: tuple[PreparedOutput, ...] = ()
    logs: tuple[PurePath, ...] = ()
    if plan.recipe.build is not None:
        prepared_source, key, outputs, logs = _resolve_build(
            plan,
            snapshot=snapshot,
            source_digest=source_digest,
            image=image,
            cache_root=root,
            target=target,
            apptainer_executable=apptainer_executable,
        )
    if experiment.container is None:
        raise PreparationError("Prepared execution requires a container")
    effective_experiment = ExperimentSpec(
        version=experiment.version,
        name=experiment.name,
        command=Command(
            experiment.command.argv,
            environment=experiment.command.environment,
            working_directory=experiment.command.working_directory,
        ),
        resources=experiment.resources,
        container=ContainerSpec(image=image, gpu=experiment.container.gpu),
        outputs=experiment.outputs,
        sync_excludes=experiment.sync_excludes,
    )
    record = PreparationRecord(
        source_identity=(
            source_recipe_identity(plan.recipe.source)
            if plan.source_mode == "git"
            else f"working-tree:{source_digest}"
        ),
        source_digest=source_digest,
        source_action=source_action,
        image_uri=plan.recipe.image.uri,
        image_sha256=plan.recipe.image.sha256,
        image_path=image,
        image_action=image_action,
        resolution_location="local",
        build_cache_key=key,
        builder_location="local" if key is not None else None,
        build_outputs=outputs,
        logs=logs,
    )
    return PreparedApplication(prepared_source, effective_experiment, record)


def _resolve_source(
    plan: PreparationPlan,
    *,
    source_root: Path,
    cache_root: Path,
    excludes: tuple[str, ...],
) -> tuple[Path, str, str]:
    sources = cache_root / "sources"
    sources.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="rundra-source-", dir=cache_root) as raw:
        temporary = Path(raw) / "snapshot"
        if plan.source_mode == "working_tree":
            _copy_snapshot(source_root, temporary, excludes)
            action = "snapshot_working_tree"
        else:
            _checkout_git(plan, temporary, cache_root)
            action = "checkout_git_cache"
        digest = _tree_digest(temporary)
        destination = sources / digest
        with _lock(cache_root / "locks" / f"source-{digest}.lock"):
            if destination.is_dir():
                return destination, digest, "reuse_source_cache"
            _publish_directory(temporary, destination)
        return destination, digest, action


def _checkout_git(plan: PreparationPlan, destination: Path, cache_root: Path) -> None:
    recipe = plan.recipe.source
    repositories = cache_root / "git"
    repositories.mkdir(parents=True, exist_ok=True)
    repository = repositories / source_recipe_identity(recipe)
    with _lock(cache_root / "locks" / f"git-{repository.name}.lock"):
        if not repository.exists():
            if plan.offline:
                raise PreparationError(
                    "Pinned Git source is absent from the offline cache"
                )
            _run(("git", "init", "--bare", str(repository)), cwd=cache_root)
        if not _git_has_commit(repository, recipe.revision):
            if plan.offline:
                raise PreparationError(
                    "Pinned Git commit is absent from the offline cache"
                )
            _run(
                (
                    "git",
                    "--git-dir",
                    str(repository),
                    "fetch",
                    "--no-tags",
                    "--depth=1",
                    recipe.url,
                    recipe.revision,
                ),
                cwd=cache_root,
            )
        checkout = destination.parent / "checkout"
        _run(
            (
                "git",
                "--git-dir",
                str(repository),
                "worktree",
                "add",
                "--detach",
                str(checkout),
                recipe.revision,
            ),
            cwd=cache_root,
        )
        try:
            _copy_snapshot(checkout, destination, (".git",))
        finally:
            _run(
                (
                    "git",
                    "--git-dir",
                    str(repository),
                    "worktree",
                    "remove",
                    "--force",
                    str(checkout),
                ),
                cwd=cache_root,
            )


def _git_has_commit(repository: Path, revision: str) -> bool:
    completed = subprocess.run(
        (
            "git",
            "--git-dir",
            str(repository),
            "cat-file",
            "-e",
            f"{revision}^{{commit}}",
        ),
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        shell=False,
    )
    return completed.returncode == 0


def _resolve_image(
    plan: PreparationPlan,
    *,
    project_root: Path,
    cache_root: Path,
    target: Target,
    apptainer_executable: str,
    image_search_paths: tuple[Path, ...],
) -> tuple[Path, str]:
    recipe = plan.recipe.image
    images = cache_root / "images"
    images.mkdir(parents=True, exist_ok=True)
    cached = images / f"{recipe.sha256}.sif"
    requested = str(recipe.name)
    candidates = (
        cached,
        project_root / requested,
        *(path.expanduser().resolve() / requested for path in image_search_paths),
    )
    for candidate in candidates:
        if candidate.is_symlink() or not candidate.is_file():
            continue
        if candidate == cached or _file_digest(candidate) == recipe.sha256:
            if candidate != cached:
                with _lock(cache_root / "locks" / f"image-{recipe.sha256}.lock"):
                    if not cached.exists():
                        _publish_file(candidate, cached)
            return (
                cached,
                "reuse_image_cache"
                if candidate == cached
                else "cache_verified_candidate",
            )
    if plan.offline:
        raise PreparationError(
            f"Verified image sha256:{recipe.sha256} is unavailable in offline mode"
        )
    with _lock(cache_root / "locks" / f"image-{recipe.sha256}.lock"):
        if cached.is_file():
            if _file_digest(cached) != recipe.sha256:
                raise PreparationError(f"Image cache entry has wrong digest: {cached}")
            return cached, "reuse_image_cache"
        with tempfile.TemporaryDirectory(prefix="rundra-image-", dir=cache_root) as raw:
            pulled = Path(raw) / requested
            _run(
                (
                    apptainer_executable,
                    "pull",
                    "--disable-cache",
                    str(pulled),
                    recipe.uri,
                ),
                cwd=Path(raw),
            )
            actual = _file_digest(pulled)
            if actual != recipe.sha256:
                raise PreparationError(
                    f"Pulled image digest mismatch: expected {recipe.sha256}, got {actual}"
                )
            _publish_file(pulled, cached)
    return cached, "pull_image"


def _resolve_build(
    plan: PreparationPlan,
    *,
    snapshot: Path,
    source_digest: str,
    image: Path,
    cache_root: Path,
    target: Target,
    apptainer_executable: str,
) -> tuple[Path, str, tuple[PreparedOutput, ...], tuple[PurePath, ...]]:
    build = plan.recipe.build
    assert build is not None
    fingerprint = _platform_fingerprint()
    scope = target.name if build.cache_scope == "target" else fingerprint
    key = build_cache_key(
        source_digest=source_digest,
        image_digest=plan.recipe.image.sha256,
        build=build,
        builder_scope=scope,
        platform_fingerprint=fingerprint,
    )
    entry = cache_root / "builds" / key
    prepared = entry / "source"
    stdout_path = entry / "build.stdout"
    stderr_path = entry / "build.stderr"
    with _lock(cache_root / "locks" / f"build-{key}.lock"):
        if prepared.is_dir() and not plan.rebuild:
            outputs = _verify_outputs(prepared, build.outputs)
            return prepared, key, outputs, (stdout_path, stderr_path)
        with tempfile.TemporaryDirectory(prefix="rundra-build-", dir=cache_root) as raw:
            temporary_entry = Path(raw) / "entry"
            work = temporary_entry / "source"
            shutil.copytree(snapshot, work, symlinks=False)
            _make_writable(work)
            completed = subprocess.run(
                (
                    apptainer_executable,
                    "exec",
                    "--cleanenv",
                    "--no-eval",
                    "--bind",
                    f"{work}:/workspace:rw",
                    "--cwd",
                    "/workspace",
                    str(image),
                    *build.argv,
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                shell=False,
            )
            temporary_entry.mkdir(exist_ok=True)
            (temporary_entry / "build.stdout").write_text(
                completed.stdout, encoding="utf-8"
            )
            (temporary_entry / "build.stderr").write_text(
                completed.stderr, encoding="utf-8"
            )
            if completed.returncode != 0:
                raise PreparationError(
                    f"Application build failed with exit code {completed.returncode}"
                )
            outputs = _verify_outputs(work, build.outputs)
            if entry.exists():
                _make_writable(entry)
                shutil.rmtree(entry)
            _publish_directory(temporary_entry, entry)
    return prepared, key, outputs, (stdout_path, stderr_path)


def _verify_outputs(
    source: Path, declarations: tuple[PreparationOutput, ...]
) -> tuple[PreparedOutput, ...]:
    verified: list[PreparedOutput] = []
    for declaration in declarations:
        path = source / str(declaration.path)
        if path.is_symlink() or not path.is_file():
            raise PreparationError(
                f"Declared build output is missing: {declaration.path}"
            )
        executable = os.access(path, os.X_OK)
        if declaration.executable and not executable:
            raise PreparationError(
                f"Declared build output is not executable: {declaration.path}"
            )
        verified.append(
            PreparedOutput(
                PurePath(str(declaration.path)),
                _file_digest(path),
                executable,
            )
        )
    return tuple(verified)


def _copy_snapshot(source: Path, destination: Path, excludes: tuple[str, ...]) -> None:
    if not source.is_dir():
        raise PreparationError(f"Preparation source root is not a directory: {source}")

    def ignored(directory: str, names: list[str]) -> set[str]:
        current = Path(directory)
        ignored_names: set[str] = set()
        for name in names:
            relative = (current / name).relative_to(source).as_posix()
            if any(
                fnmatch.fnmatch(relative, pattern) or fnmatch.fnmatch(name, pattern)
                for pattern in excludes
            ):
                ignored_names.add(name)
        return ignored_names

    try:
        shutil.copytree(source, destination, ignore=ignored, symlinks=False)
    except OSError as error:
        raise PreparationError(
            f"Could not snapshot preparation source: {error}"
        ) from error


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(
        root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()
    ):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        if path.is_symlink():
            raise PreparationError(f"Snapshot contains unresolved symlink: {path}")
        if path.is_dir():
            digest.update(b"d\0" + relative + b"\0")
            continue
        if not path.is_file():
            raise PreparationError(f"Snapshot contains unsupported file type: {path}")
        mode = b"x" if os.access(path, os.X_OK) else b"f"
        digest.update(mode + b"\0" + relative + b"\0")
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _platform_fingerprint() -> str:
    value = f"{platform.system()}|{platform.machine()}|{platform.release()}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _publish_directory(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.tmp-{os.getpid()}"
    if temporary.exists():
        _make_writable(temporary)
        shutil.rmtree(temporary)
    shutil.copytree(source, temporary, symlinks=False)
    _seal(temporary)
    os.replace(temporary, destination)


def _publish_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.tmp-{os.getpid()}"
    shutil.copy2(source, temporary)
    os.chmod(temporary, stat.S_IMODE(temporary.stat().st_mode) & ~0o222)
    os.replace(temporary, destination)


def _seal(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        if not path.is_symlink():
            os.chmod(path, stat.S_IMODE(path.stat().st_mode) & ~0o222)
    os.chmod(root, stat.S_IMODE(root.stat().st_mode) & ~0o222)


def _make_writable(root: Path) -> None:
    for path in (root, *root.rglob("*")):
        if not path.is_symlink():
            os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)


@contextmanager
def _lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        _lock_stream(stream)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _lock_stream(stream: BinaryIO) -> None:
    fcntl.flock(stream.fileno(), fcntl.LOCK_EX)


def _run(argv: tuple[str, ...], *, cwd: Path) -> None:
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            shell=False,
        )
    except OSError as error:
        raise PreparationError(
            f"Could not start preparation command {argv[0]!r}: {error}"
        ) from error
    if completed.returncode != 0:
        detail = completed.stderr.strip().splitlines()
        suffix = f": {detail[-1]}" if detail else ""
        raise PreparationError(
            f"Preparation command {argv[0]!r} failed with exit code {completed.returncode}{suffix}"
        )
