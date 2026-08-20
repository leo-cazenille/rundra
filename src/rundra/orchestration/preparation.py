from __future__ import annotations

import fcntl
import fnmatch
import hashlib
import json
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
    DefinitionBuildPolicy,
    PreparationImage,
    PreparationImageDefinition,
    PreparationOutput,
    PreparationPlan,
    PreparationRecord,
    PreparedOutput,
    build_cache_key,
    definition_image_recipe_key,
    source_recipe_identity,
)
from rundra.ports import StagedWorkspace, Transport
from rundra.sync import with_default_sync_excludes


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
class ResolvedImage:
    """One verified immutable image and its measured identity."""

    path: Path
    sha256: str
    uri: str
    action: str
    logs: tuple[PurePath, ...] = ()


@dataclass(frozen=True, slots=True)
class RemotePreparationSpec:
    """Resolved inputs for one scheduled target preparation job."""

    plan: PreparationPlan
    source_digest: str
    source_action: str
    source_identity: str
    platform_fingerprint: str
    build_key: str | None
    index_key: str | None = None
    target_cache_root: PurePath | None = None
    image_search_paths: tuple[PurePath, ...] = ()
    definition_build: DefinitionBuildPolicy | None = None
    image_recipe_key: str | None = None
    build_key_prefix: str | None = None
    build_key_suffix: str | None = None


@dataclass(frozen=True, slots=True)
class RemotePreparationResult:
    outputs: tuple[PreparedOutput, ...]
    image_action: str
    build_action: str
    image_sha256: str | None = None
    image_path: PurePath | None = None
    build_key: str | None = None


@dataclass(frozen=True, slots=True)
class RemotePreparationCacheHit:
    source_root: PurePath
    experiment_image: PurePath
    record: PreparationRecord


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


def remote_builder_version(transport: Transport) -> str:
    """Read the target Apptainer version without performing a build."""
    result = transport.run(Command(("apptainer", "version")))
    value = result.stdout.strip() or result.stderr.strip()
    if result.exit_code != 0 or not value:
        raise PreparationError("Could not determine target Apptainer builder version")
    return value


def remote_preparation_index_key(
    plan: PreparationPlan, target: Target, platform_fingerprint: str
) -> str:
    image = _prebuilt_image(plan)
    build = plan.recipe.build
    if build is None:
        raise PreparationError("Remote preparation index requires a build recipe")
    identity_digest = hashlib.sha256(
        json.dumps(
            {
                "source": source_recipe_identity(plan.recipe.source),
                "version": 1,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    scope = target.name if build.cache_scope == "target" else platform_fingerprint
    return build_cache_key(
        source_digest=identity_digest,
        image_digest=image.sha256,
        build=build,
        builder_scope=scope,
        platform_fingerprint=platform_fingerprint,
    )


def probe_remote_preparation_cache(
    plan: PreparationPlan,
    experiment: ExperimentSpec,
    target: Target,
    transport: Transport,
    *,
    cache_root: PurePath | None = None,
) -> RemotePreparationCacheHit | None:
    """Validate one exact pinned-source target cache entry without local source."""
    if plan.source_mode != "git" or plan.rebuild or plan.recipe.build is None:
        return None
    image_recipe = _prebuilt_image(plan)
    fingerprint = remote_platform_fingerprint(transport)
    root = target.workspace / "cache" if cache_root is None else cache_root
    index_key = remote_preparation_index_key(plan, target, fingerprint)
    index = root / "indexes" / f"{index_key}.tsv"
    result = transport.run(Command(("cat", "--", str(index))))
    if result.exit_code != 0:
        return None
    fields: dict[str, str] = {}
    outputs: list[PreparedOutput] = []
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 2 and parts[0] not in fields:
            fields[parts[0]] = parts[1]
        elif len(parts) == 4 and parts[0] == "output" and parts[2] in {"0", "1"}:
            outputs.append(
                PreparedOutput(
                    path=PurePath(parts[3]),
                    sha256=parts[1],
                    executable=parts[2] == "1",
                )
            )
        else:
            raise PreparationError("Target preparation index is invalid")
    if set(fields) != {"version", "source_digest", "platform", "build_key"}:
        raise PreparationError("Target preparation index is incomplete")
    if fields["version"] != "1" or fields["platform"] != fingerprint:
        raise PreparationError("Target preparation index identity mismatch")
    source_digest = fields["source_digest"]
    build_key = fields["build_key"]
    if not _is_sha256(source_digest) or not _is_sha256(build_key):
        raise PreparationError("Target preparation index digest is invalid")
    build = plan.recipe.build
    scope = target.name if build.cache_scope == "target" else fingerprint
    expected_key = build_cache_key(
        source_digest=source_digest,
        image_digest=image_recipe.sha256,
        build=build,
        builder_scope=scope,
        platform_fingerprint=fingerprint,
    )
    if build_key != expected_key:
        raise PreparationError("Target preparation index build key mismatch")
    entry = root / "builds" / build_key
    prepared_source = entry / "source"
    image = root / "images" / f"{image_recipe.sha256}.sif"
    for command, message in (
        (Command(("test", "-f", str(entry / ".complete"))), "build cache incomplete"),
        (Command(("test", "-d", str(prepared_source))), "prepared source missing"),
        (
            Command(("test", "!", "-L", str(prepared_source))),
            "prepared source is a symlink",
        ),
    ):
        if transport.run(command).exit_code != 0:
            raise PreparationError(f"Target {message}")
    indexed_outputs = {output.path: output for output in outputs}
    declared_outputs = {output.path: output for output in build.outputs}
    if set(indexed_outputs) != set(declared_outputs):
        raise PreparationError("Target preparation index outputs do not match recipe")
    for path, declared in declared_outputs.items():
        indexed = indexed_outputs[path]
        if indexed.executable != declared.executable:
            raise PreparationError(
                "Target preparation output mode does not match recipe"
            )
        cached_output = prepared_source / path
        digest_result = transport.run(Command(("sha256sum", "--", str(cached_output))))
        digest_fields = digest_result.stdout.split(maxsplit=1)
        if (
            digest_result.exit_code != 0
            or not digest_fields
            or digest_fields[0] != indexed.sha256
        ):
            raise PreparationError(f"Target prepared output digest mismatch: {path}")
        if (
            declared.executable
            and transport.run(Command(("test", "-x", str(cached_output)))).exit_code
            != 0
        ):
            raise PreparationError(f"Target prepared output is not executable: {path}")
    image_result = transport.run(Command(("sha256sum", "--", str(image))))
    image_fields = image_result.stdout.split(maxsplit=1)
    if (
        image_result.exit_code != 0
        or not image_fields
        or image_fields[0] != image_recipe.sha256
    ):
        raise PreparationError("Target cached image digest mismatch")
    if experiment.container is None:
        raise PreparationError("Prepared execution requires a container")
    record = PreparationRecord(
        source_identity=source_recipe_identity(plan.recipe.source),
        source_digest=source_digest,
        source_action="reuse_target_source_cache",
        image_uri=image_recipe.uri,
        image_sha256=image_recipe.sha256,
        image_path=image,
        image_action="reuse_image_cache",
        resolution_location="target",
        build_cache_key=build_key,
        builder_location="target",
        build_action="reuse_build_cache",
        build_outputs=tuple(outputs),
    )
    return RemotePreparationCacheHit(prepared_source, image, record)


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _prebuilt_image(plan: PreparationPlan) -> PreparationImage:
    image = plan.recipe.image
    if type(image) is not PreparationImage:
        raise PreparationError(
            "Legacy remote preparation requires a pinned prebuilt image"
        )
    return image


def create_remote_preparation_spec(
    plan: PreparationPlan,
    source: PreparedSource,
    target: Target,
    platform_fingerprint: str,
    *,
    cache_root: PurePath | None = None,
    image_search_paths: tuple[PurePath, ...] = (),
    definition_build: DefinitionBuildPolicy | None = None,
    builder_version: str | None = None,
) -> RemotePreparationSpec:
    """Create deterministic target preparation identities before submission."""
    build = plan.recipe.build
    image = plan.recipe.image
    image_recipe_key = None
    if type(image) is PreparationImageDefinition:
        if (
            definition_build is None
            or "target" not in definition_build.allowed_locations
        ):
            raise PreparationError(
                "Target policy does not allow target definition builds"
            )
        _validate_definition_resources(image, definition_build)
        if builder_version is None:
            raise PreparationError("Target definition build requires a builder version")
        image_recipe_key = definition_image_recipe_key(
            source_digest=(
                source.digest
                if image.context is None
                else _definition_context_digest(source.root, image)
            ),
            image=image,
            target_name=target.name,
            mode=definition_build.mode,
            platform_fingerprint=platform_fingerprint,
            builder_version=builder_version,
        )
    elif type(image) is not PreparationImage:
        raise PreparationError("Unsupported remote image recipe")
    key = None
    build_key_prefix = None
    build_key_suffix = None
    if build is not None:
        scope = target.name if build.cache_scope == "target" else platform_fingerprint
        if type(image) is PreparationImage:
            key = build_cache_key(
                source_digest=source.digest,
                image_digest=image.sha256,
                build=build,
                builder_scope=scope,
                platform_fingerprint=platform_fingerprint,
            )
        else:
            marker = "RUNDRA_IMAGE_DIGEST"
            canonical = json.dumps(
                {
                    "build": {
                        "argv": list(build.argv),
                        "cache_scope": build.cache_scope,
                        "outputs": [
                            {"executable": item.executable, "path": str(item.path)}
                            for item in build.outputs
                        ],
                    },
                    "builder_scope": scope,
                    "image_digest": marker,
                    "platform_fingerprint": platform_fingerprint,
                    "source_digest": source.digest,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            build_key_prefix, build_key_suffix = canonical.split(marker)
    return RemotePreparationSpec(
        plan=plan,
        source_digest=source.digest,
        source_action=source.action,
        source_identity=source.identity,
        platform_fingerprint=platform_fingerprint,
        build_key=key,
        index_key=(
            None
            if build is None or type(image) is PreparationImageDefinition
            else remote_preparation_index_key(plan, target, platform_fingerprint)
        ),
        target_cache_root=(
            PurePath(str(target.workspace)) / "cache"
            if cache_root is None
            else cache_root
        ),
        image_search_paths=image_search_paths,
        definition_build=definition_build,
        image_recipe_key=image_recipe_key,
        build_key_prefix=build_key_prefix,
        build_key_suffix=build_key_suffix,
    )


def remote_preparation_record(
    spec: RemotePreparationSpec,
    target: Target,
) -> PreparationRecord:
    """Create pre-submission target preparation provenance."""
    recipe = spec.plan.recipe.image
    if type(recipe) is PreparationImageDefinition:
        assert spec.image_recipe_key is not None
        image = (
            (spec.target_cache_root or PurePath(str(target.workspace)) / "cache")
            / "image-definitions"
            / f"{spec.image_recipe_key}.sif"
        )
        return PreparationRecord(
            source_identity=spec.source_identity,
            source_digest=spec.source_digest,
            source_action=spec.source_action,
            image_uri=f"definition:{recipe.path}",
            image_sha256=spec.image_recipe_key,
            image_path=image,
            image_action="build_definition_on_target",
            resolution_location="target",
            builder_location="target",
        )
    if type(recipe) is not PreparationImage:
        raise PreparationError("Unsupported remote image recipe")
    image = (
        (spec.target_cache_root or PurePath(str(target.workspace)) / "cache")
        / "images"
        / (f"{recipe.sha256}.sif")
    )
    return PreparationRecord(
        source_identity=spec.source_identity,
        source_digest=spec.source_digest,
        source_action=spec.source_action,
        image_uri=recipe.uri,
        image_sha256=recipe.sha256,
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
    if type(recipe.image) is PreparationImageDefinition:
        return _build_remote_definition_command(spec, workspace)
    image_recipe = _prebuilt_image(plan)
    target_cache = spec.target_cache_root or workspace.root.parent.parent / "cache"
    image = target_cache / "images" / f"{image_recipe.sha256}.sif"
    image_lock = target_cache / "locks" / f"image-{image_recipe.sha256}.lock"
    lines = [
        "set -eu",
        "umask 077",
        f"cache={shlex.quote(str(target_cache))}",
        f"image={shlex.quote(str(image))}",
        f"image_lock={shlex.quote(str(image_lock))}",
        "image_action=reuse_image_cache",
        "build_action=not_requested",
        'mkdir -p -- "$cache/images" "$cache/builds" "$cache/indexes" "$cache/locks"',
        "attempt=0",
        'while ! mkdir -- "$image_lock" 2>/dev/null; do',
        "  attempt=$((attempt + 1))",
        "  [ \"$attempt\" -lt 900 ] || { printf '%s\\n' 'image cache lock timeout' >&2; exit 75; }",
        "  sleep 1",
        "done",
        "trap 'rmdir -- \"$image_lock\" 2>/dev/null || :' EXIT HUP INT TERM",
        'if [ -f "$image" ]; then',
        "  actual=$(sha256sum -- \"$image\" | cut -d' ' -f1)",
        f"  [ \"$actual\" = {shlex.quote(image_recipe.sha256)} ] || {{ printf '%s\\n' 'cached image digest mismatch' >&2; exit 65; }}",
        "else",
    ]
    for search_root in spec.image_search_paths:
        candidate = search_root / image_recipe.name
        lines.extend(
            (
                f"  candidate={shlex.quote(str(candidate))}",
                '  if [ -f "$candidate" ] && [ ! -L "$candidate" ]; then',
                "    actual=$(sha256sum -- \"$candidate\" | cut -d' ' -f1)",
                f'    if [ "$actual" = {shlex.quote(image_recipe.sha256)} ]; then',
                '      image_tmp=$(mktemp "$cache/images/.candidate.XXXXXX")',
                '      cp -- "$candidate" "$image_tmp"',
                '      chmod a-w -- "$image_tmp"',
                '      mv -- "$image_tmp" "$image"',
                "      image_action=cache_verified_candidate",
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
                '  image_tmp_dir=$(mktemp -d "$cache/images/.pull.XXXXXX")',
                '  image_tmp="$image_tmp_dir/image.sif"',
                '  trap \'rm -rf -- "$image_tmp_dir"; rmdir -- "$image_lock" 2>/dev/null || :\' EXIT HUP INT TERM',
                f'  apptainer pull --disable-cache "$image_tmp" {shlex.quote(image_recipe.uri)}',
                "  actual=$(sha256sum -- \"$image_tmp\" | cut -d' ' -f1)",
                f"  [ \"$actual\" = {shlex.quote(image_recipe.sha256)} ] || {{ printf '%s\\n' 'pulled image digest mismatch' >&2; exit 65; }}",
                '  chmod a-w -- "$image_tmp"',
                '  mv -- "$image_tmp" "$image"',
                '  rmdir -- "$image_tmp_dir"',
                "  image_action=pull_image",
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
                "  build_action=build_and_publish",
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
                "else",
                "  build_action=reuse_build_cache",
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
        if plan.source_mode == "git":
            assert spec.index_key is not None
            lines.extend(
                (
                    f"index={shlex.quote(str(target_cache / 'indexes' / f'{spec.index_key}.tsv'))}",
                    'index_tmp=$(mktemp "$cache/indexes/.index.XXXXXX")',
                    f"printf 'version\\t1\\nsource_digest\\t%s\\nplatform\\t%s\\nbuild_key\\t%s\\n' {shlex.quote(spec.source_digest)} {shlex.quote(spec.platform_fingerprint)} {shlex.quote(spec.build_key)} > \"$index_tmp\"",
                )
            )
            for output in build.outputs:
                rendered = shlex.quote(str(output.path))
                executable = "1" if output.executable else "0"
                lines.extend(
                    (
                        f"output={shlex.quote(str(workspace.source))}/{rendered}",
                        "digest=$(sha256sum -- \"$output\" | cut -d' ' -f1)",
                        f'printf \'output\\t%s\\t{executable}\\t%s\\n\' "$digest" {rendered} >> "$index_tmp"',
                    )
                )
            lines.extend(
                (
                    'chmod a-w -- "$index_tmp"',
                    'mv -f -- "$index_tmp" "$index"',
                )
            )
    lines.extend(
        (
            f"actions_tmp={shlex.quote(str(workspace.metadata / '.preparation-actions.tmp'))}",
            f"actions={shlex.quote(str(workspace.metadata / 'preparation-actions.tsv'))}",
            'printf \'image_action\\t%s\\nbuild_action\\t%s\\n\' "$image_action" "$build_action" > "$actions_tmp"',
            'chmod a-w -- "$actions_tmp"',
            'mv -- "$actions_tmp" "$actions"',
        )
    )
    return Command(("/bin/sh", "-c", "\n".join(lines)))


def _build_remote_definition_command(
    spec: RemotePreparationSpec,
    workspace: StagedWorkspace,
) -> Command:
    recipe = spec.plan.recipe.image
    policy = spec.definition_build
    if type(recipe) is not PreparationImageDefinition or policy is None:
        raise PreparationError("Remote definition preparation is incomplete")
    assert spec.image_recipe_key is not None
    cache = spec.target_cache_root or workspace.root.parent.parent / "cache"
    index_root = cache / "image-definitions"
    index = index_root / f"{spec.image_recipe_key}.sha256"
    stable_image = index_root / f"{spec.image_recipe_key}.sif"
    lock = cache / "locks" / f"definition-{spec.image_recipe_key}.lock"
    definition = workspace.source / recipe.path
    lines = [
        "set -eu",
        "umask 077",
        f"cache={shlex.quote(str(cache))}",
        f"index_root={shlex.quote(str(index_root))}",
        f"index={shlex.quote(str(index))}",
        f"image={shlex.quote(str(stable_image))}",
        f"lock={shlex.quote(str(lock))}",
        f"definition={shlex.quote(str(definition))}",
        'mkdir -p -- "$cache/images" "$index_root" "$cache/locks"',
        "test -f \"$definition\" && test ! -L \"$definition\" || { printf '%s\\n' 'definition missing or symlinked' >&2; exit 66; }",
        "attempt=0",
        'while ! mkdir -- "$lock" 2>/dev/null; do',
        "  attempt=$((attempt + 1))",
        "  [ \"$attempt\" -lt 900 ] || { printf '%s\\n' 'definition cache lock timeout' >&2; exit 75; }",
        "  sleep 1",
        "done",
        "trap 'rmdir -- \"$lock\" 2>/dev/null || :' EXIT HUP INT TERM",
        "image_action=reuse_definition_image_cache",
        "build_action=not_requested",
        f'if [ -f "$index" ] && [ -f "$image" ] && [ {"false" if spec.plan.rebuild_image else "true"} = true ]; then',
        '  digest=$(cat -- "$index")',
        "  actual=$(sha256sum -- \"$image\" | cut -d' ' -f1)",
        "  [ \"$actual\" = \"$digest\" ] || { printf '%s\\n' 'definition image cache mismatch' >&2; exit 65; }",
        "else",
    ]
    if spec.plan.offline:
        lines.append(
            "  printf '%s\\n' 'definition image unavailable in offline mode' >&2; exit 69"
        )
    else:
        build_args = ["apptainer", "build", "--disable-cache"]
        if policy.mode == "fakeroot":
            build_args.append("--fakeroot")
        else:
            build_args.append("--ignore-subuid")
        lines.extend(
            (
                '  work=$(mktemp -d "$index_root/.build.XXXXXX")',
                '  built="$work/image.sif"',
                "  "
                + shlex.join((*build_args, "$built", "$definition"))
                .replace("'$built'", '"$built"')
                .replace("'$definition'", '"$definition"'),
                "  digest=$(sha256sum -- \"$built\" | cut -d' ' -f1)",
                '  content="$cache/images/$digest.sif"',
                '  if [ ! -f "$content" ]; then chmod a-w -- "$built"; mv -- "$built" "$content"; else rm -f -- "$built"; fi',
                '  image_tmp="$work/stable.sif"',
                '  cp -- "$content" "$image_tmp"',
                '  chmod a-w -- "$image_tmp"',
                '  mv -f -- "$image_tmp" "$image"',
                '  index_tmp="$work/index"',
                '  printf \'%s\\n\' "$digest" > "$index_tmp"',
                '  chmod a-w -- "$index_tmp"',
                '  mv -f -- "$index_tmp" "$index"',
                '  rmdir -- "$work"',
                "  image_action=build_definition_image",
            )
        )
    lines.extend(
        (
            "  actual=$(sha256sum -- \"$image\" | cut -d' ' -f1)",
            "  [ \"$actual\" = \"$digest\" ] || { printf '%s\\n' 'published image digest mismatch' >&2; exit 65; }",
            "fi",
            'rmdir -- "$lock"',
            "trap - EXIT HUP INT TERM",
        )
    )
    if spec.plan.recipe.build is not None:
        lines.extend(_remote_definition_application_lines(spec, workspace))
    lines.extend(
        (
            f"actions_tmp={shlex.quote(str(workspace.metadata / '.preparation-actions.tmp'))}",
            f"actions={shlex.quote(str(workspace.metadata / 'preparation-actions.tsv'))}",
            'printf \'image_action\\t%s\\nbuild_action\\t%s\\n\' "$image_action" "$build_action" > "$actions_tmp"',
            'chmod a-w -- "$actions_tmp"',
            'mv -- "$actions_tmp" "$actions"',
            f"image_manifest_tmp={shlex.quote(str(workspace.metadata / '.preparation-image.tmp'))}",
            f"image_manifest={shlex.quote(str(workspace.metadata / 'preparation-image.tsv'))}",
            'printf \'%s\\t%s\\n\' "$digest" "$image" > "$image_manifest_tmp"',
            'chmod a-w -- "$image_manifest_tmp"',
            'mv -- "$image_manifest_tmp" "$image_manifest"',
        )
    )
    return Command(("/bin/sh", "-c", "\n".join(lines)))


def _remote_definition_application_lines(
    spec: RemotePreparationSpec,
    workspace: StagedWorkspace,
) -> tuple[str, ...]:
    build = spec.plan.recipe.build
    if build is None or spec.build_key_prefix is None or spec.build_key_suffix is None:
        raise PreparationError("Remote definition application key is incomplete")
    lines = [
        f"build_prefix={shlex.quote(spec.build_key_prefix)}",
        f"build_suffix={shlex.quote(spec.build_key_suffix)}",
        'build_key=$(printf \'%s%s%s\' "$build_prefix" "$digest" "$build_suffix" | sha256sum | cut -d\' \' -f1)',
        'entry="$cache/builds/$build_key"',
        'build_lock="$cache/locks/build-$build_key.lock"',
        'mkdir -p -- "$cache/builds"',
        "attempt=0",
        'while ! mkdir -- "$build_lock" 2>/dev/null; do',
        "  attempt=$((attempt + 1))",
        "  [ \"$attempt\" -lt 900 ] || { printf '%s\\n' 'build cache lock timeout' >&2; exit 75; }",
        "  sleep 1",
        "done",
        "trap 'rmdir -- \"$build_lock\" 2>/dev/null || :' EXIT HUP INT TERM",
        f'if [ ! -f "$entry/.complete" ] || {"true" if spec.plan.rebuild else "false"}; then',
        "  build_action=build_and_publish",
        '  work=$(mktemp -d "$cache/builds/.work.XXXXXX")',
        '  publish=$(mktemp -d "$cache/builds/.publish.XXXXXX")',
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
    ]
    for output in build.outputs:
        rendered = shlex.quote(str(output.path))
        lines.extend(
            (
                f'  output="$work"/{rendered}',
                "  [ -f \"$output\" ] || { printf '%s\\n' 'declared output missing' >&2; exit 66; }",
            )
        )
        if output.executable:
            lines.append(
                "  [ -x \"$output\" ] || { printf '%s\\n' 'declared output not executable' >&2; exit 66; }"
            )
    lines.extend(
        (
            '  mkdir -p -- "$publish/source"',
            '  cp -a -- "$work"/. "$publish/source"/',
            '  : > "$publish/.complete"',
            '  chmod -R a-w -- "$publish"',
            '  rm -rf -- "$entry"',
            '  mv -- "$publish" "$entry"',
            '  rm -rf -- "$work"',
            "else",
            "  build_action=reuse_build_cache",
            "fi",
            f"run_source_tmp={shlex.quote(str(workspace.root / '.prepared-source'))}",
            'rm -rf -- "$run_source_tmp"',
            'cp -a -- "$entry/source" "$run_source_tmp"',
            'chmod -R a-w -- "$run_source_tmp"',
            f"chmod -R u+w -- {shlex.quote(str(workspace.source))}",
            f"rm -rf -- {shlex.quote(str(workspace.source))}",
            f'mv -- "$run_source_tmp" {shlex.quote(str(workspace.source))}',
            'rmdir -- "$build_lock"',
            "trap - EXIT HUP INT TERM",
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
                "output_digest=$(sha256sum -- \"$output\" | cut -d' ' -f1)",
                f'printf \'%s\\t%s\\t%s\\n\' "$output_digest" {executable} {rendered} >> "$manifest_tmp"',
            )
        )
    lines.extend(
        (
            'chmod a-w -- "$manifest_tmp"',
            'mv -- "$manifest_tmp" "$manifest"',
            f"build_manifest_tmp={shlex.quote(str(workspace.metadata / '.preparation-build.tmp'))}",
            f"build_manifest={shlex.quote(str(workspace.metadata / 'preparation-build.txt'))}",
            'printf \'%s\\n\' "$build_key" > "$build_manifest_tmp"',
            'chmod a-w -- "$build_manifest_tmp"',
            'mv -- "$build_manifest_tmp" "$build_manifest"',
        )
    )
    return tuple(lines)


def read_remote_preparation_result(
    transport: Transport,
    workspace: StagedWorkspace,
) -> RemotePreparationResult | None:
    """Read verified preparation outcomes produced by the target job."""
    action_result = transport.run(
        Command(("cat", "--", str(workspace.metadata / "preparation-actions.tsv")))
    )
    if action_result.exit_code != 0:
        return None
    actions: dict[str, str] = {}
    for line in action_result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) != 2 or fields[0] in actions or not fields[1]:
            raise PreparationError("Target preparation action manifest is invalid")
        actions[fields[0]] = fields[1]
    if set(actions) != {"image_action", "build_action"}:
        raise PreparationError("Target preparation action manifest is incomplete")
    outputs = read_remote_prepared_outputs(transport, workspace) or ()
    image_sha256 = None
    image_path = None
    image_result = transport.run(
        Command(("cat", "--", str(workspace.metadata / "preparation-image.tsv")))
    )
    if image_result.exit_code == 0:
        fields = image_result.stdout.strip().split("\t")
        if len(fields) == 2 and _is_sha256(fields[0]):
            image_sha256 = fields[0]
            image_path = PurePath(fields[1])
    build_key = None
    build_result = transport.run(
        Command(("cat", "--", str(workspace.metadata / "preparation-build.txt")))
    )
    if build_result.exit_code == 0 and _is_sha256(build_result.stdout.strip()):
        build_key = build_result.stdout.strip()
    return RemotePreparationResult(
        outputs,
        actions["image_action"],
        actions["build_action"],
        image_sha256,
        image_path,
        build_key,
    )


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
    definition_build: DefinitionBuildPolicy | None = None,
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
    image = _resolve_image(
        plan,
        snapshot=snapshot,
        source_digest=source_digest,
        project_root=project_root.expanduser().resolve(),
        cache_root=root,
        target=target,
        apptainer_executable=apptainer_executable,
        image_search_paths=image_search_paths,
        definition_build=definition_build,
    )
    prepared_source = snapshot
    key: str | None = None
    outputs: tuple[PreparedOutput, ...] = ()
    logs: tuple[PurePath, ...] = ()
    build_action: str | None = None
    if plan.recipe.build is not None:
        prepared_source, key, outputs, logs, build_action = _resolve_build(
            plan,
            snapshot=snapshot,
            source_digest=source_digest,
            image=image.path,
            image_digest=image.sha256,
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
        container=ContainerSpec(image=image.path, gpu=experiment.container.gpu),
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
        image_uri=image.uri,
        image_sha256=image.sha256,
        image_path=image.path,
        image_action=image.action,
        resolution_location="local",
        build_cache_key=key,
        builder_location="local" if key is not None else None,
        build_action=build_action,
        build_outputs=outputs,
        logs=(*image.logs, *logs),
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
            _copy_snapshot(
                source_root,
                temporary,
                with_default_sync_excludes(excludes),
            )
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
    if not hasattr(recipe, "revision") or not hasattr(recipe, "url"):
        raise PreparationError("Working-tree source cannot be acquired through Git")
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
    snapshot: Path,
    source_digest: str,
    project_root: Path,
    cache_root: Path,
    target: Target,
    apptainer_executable: str,
    image_search_paths: tuple[Path, ...],
    definition_build: DefinitionBuildPolicy | None,
) -> ResolvedImage:
    recipe = plan.recipe.image
    if type(recipe) is PreparationImageDefinition:
        return _resolve_definition_image(
            plan,
            recipe=recipe,
            snapshot=snapshot,
            source_digest=source_digest,
            cache_root=cache_root,
            target=target,
            apptainer_executable=apptainer_executable,
            policy=definition_build,
        )
    if type(recipe) is not PreparationImage:
        raise PreparationError("Unsupported preparation image recipe")
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
            return ResolvedImage(
                cached,
                recipe.sha256,
                recipe.uri,
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
            return ResolvedImage(cached, recipe.sha256, recipe.uri, "reuse_image_cache")
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
    return ResolvedImage(cached, recipe.sha256, recipe.uri, "pull_image")


def _resolve_definition_image(
    plan: PreparationPlan,
    *,
    recipe: PreparationImageDefinition,
    snapshot: Path,
    source_digest: str,
    cache_root: Path,
    target: Target,
    apptainer_executable: str,
    policy: DefinitionBuildPolicy | None,
) -> ResolvedImage:
    if policy is None:
        raise PreparationError(
            "Definition image builds require target preparation.definition_build policy"
        )
    if "local" not in policy.allowed_locations:
        raise PreparationError("Target policy does not allow local definition builds")
    _validate_definition_resources(recipe, policy)
    definition = snapshot / recipe.path
    if definition.is_symlink() or not definition.is_file():
        raise PreparationError(f"Definition file is missing: {recipe.path}")
    builder_version = _builder_version(apptainer_executable)
    fingerprint = _platform_fingerprint()
    key = definition_image_recipe_key(
        source_digest=(
            source_digest
            if recipe.context is None
            else _definition_context_digest(snapshot, recipe)
        ),
        image=recipe,
        target_name=target.name,
        mode=policy.mode,
        platform_fingerprint=fingerprint,
        builder_version=builder_version,
    )
    indexes = cache_root / "image-definitions"
    images = cache_root / "images"
    indexes.mkdir(parents=True, exist_ok=True)
    images.mkdir(parents=True, exist_ok=True)
    index = indexes / f"{key}.json"
    stdout_path = indexes / f"{key}.stdout"
    stderr_path = indexes / f"{key}.stderr"
    with _lock(cache_root / "locks" / f"definition-{key}.lock"):
        if index.is_file() and not plan.rebuild_image:
            try:
                document = json.loads(index.read_text(encoding="utf-8"))
                digest = document["sha256"]
            except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
                raise PreparationError(
                    f"Definition image cache index is invalid: {index}"
                ) from error
            cached = images / f"{digest}.sif"
            if (
                cached.is_file()
                and not cached.is_symlink()
                and _file_digest(cached) == digest
            ):
                return ResolvedImage(
                    cached,
                    digest,
                    f"definition:{recipe.path}",
                    "reuse_definition_image_cache",
                    (stdout_path, stderr_path),
                )
        if plan.offline:
            raise PreparationError(
                "Definition image is absent from the cache in offline mode"
            )
        with tempfile.TemporaryDirectory(
            prefix="rundra-definition-", dir=cache_root
        ) as raw:
            built = Path(raw) / str(recipe.name)
            argv = [apptainer_executable, "build", "--disable-cache"]
            if policy.mode == "fakeroot":
                argv.append("--fakeroot")
            else:
                argv.append("--ignore-subuid")
            argv.extend((str(built), str(definition)))
            completed = subprocess.run(
                tuple(argv),
                cwd=snapshot,
                check=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                shell=False,
            )
            stdout_path.write_text(completed.stdout, encoding="utf-8")
            stderr_path.write_text(completed.stderr, encoding="utf-8")
            if completed.returncode != 0:
                raise PreparationError(
                    "Definition image build failed with exit code "
                    f"{completed.returncode}; logs: {stdout_path}, {stderr_path}"
                )
            if built.is_symlink() or not built.is_file():
                raise PreparationError("Definition image build produced no SIF file")
            digest = _file_digest(built)
            cached = images / f"{digest}.sif"
            if not cached.exists():
                _publish_file(built, cached)
            temporary_index = indexes / f".{key}.{os.getpid()}.tmp"
            temporary_index.write_text(
                json.dumps(
                    {
                        "builder_version": builder_version,
                        "recipe_key": key,
                        "sha256": digest,
                        "version": 1,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            temporary_index.chmod(0o444)
            os.replace(temporary_index, index)
    return ResolvedImage(
        cached,
        digest,
        f"definition:{recipe.path}",
        "build_definition_image",
        (stdout_path, stderr_path),
    )


def _definition_context_digest(
    snapshot: Path, recipe: PreparationImageDefinition
) -> str:
    if recipe.context is None:
        raise PreparationError("Definition context digest requires project version 4")
    selected_paths: list[PurePath] = [recipe.path, *recipe.context]
    expanded: list[PurePath] = []
    for relative in selected_paths:
        candidate = snapshot / relative
        if candidate.is_symlink() or not candidate.exists():
            raise PreparationError(f"Definition context path is missing: {relative}")
        if candidate.is_file():
            expanded.append(relative)
        elif candidate.is_dir():
            expanded.extend(
                path.relative_to(snapshot)
                for path in sorted(candidate.rglob("*"))
                if path.is_file()
            )
        else:
            raise PreparationError(
                f"Definition context path is unsupported: {relative}"
            )
    selected = tuple(dict.fromkeys(expanded))
    digest = hashlib.sha256()
    for relative in sorted(selected, key=str):
        path = snapshot / relative
        encoded = str(relative).encode("utf-8")
        content = path.read_bytes()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _validate_definition_resources(
    recipe: PreparationImageDefinition, policy: DefinitionBuildPolicy
) -> None:
    requested = recipe.resources
    maximum = policy.max_resources
    if requested.cpus_per_task > maximum.cpus_per_task:
        raise PreparationError("Definition build CPU request exceeds target policy")
    assert requested.memory_bytes is not None and maximum.memory_bytes is not None
    if requested.memory_bytes > maximum.memory_bytes:
        raise PreparationError("Definition build memory request exceeds target policy")
    assert requested.walltime is not None and maximum.walltime is not None
    if requested.walltime > maximum.walltime:
        raise PreparationError("Definition build walltime exceeds target policy")


def _builder_version(executable: str) -> str:
    completed = subprocess.run(
        (executable, "version"),
        check=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        shell=False,
    )
    value = completed.stdout.strip() or completed.stderr.strip()
    if completed.returncode != 0 or not value:
        raise PreparationError(f"Could not determine {executable} builder version")
    return value


def _resolve_build(
    plan: PreparationPlan,
    *,
    snapshot: Path,
    source_digest: str,
    image: Path,
    image_digest: str,
    cache_root: Path,
    target: Target,
    apptainer_executable: str,
) -> tuple[
    Path,
    str,
    tuple[PreparedOutput, ...],
    tuple[PurePath, ...],
    str,
]:
    build = plan.recipe.build
    assert build is not None
    fingerprint = _platform_fingerprint()
    scope = target.name if build.cache_scope == "target" else fingerprint
    key = build_cache_key(
        source_digest=source_digest,
        image_digest=image_digest,
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
            return (
                prepared,
                key,
                outputs,
                (stdout_path, stderr_path),
                "reuse_build_cache",
            )
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
    return (
        prepared,
        key,
        outputs,
        (stdout_path, stderr_path),
        "build_and_publish",
    )


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
