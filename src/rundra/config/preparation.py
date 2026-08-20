from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path, PurePath
from urllib.parse import urlsplit

from rundra.config._schema import (
    ConfigPath,
    check_fields,
    expect_boolean,
    expect_integer,
    expect_mapping,
    expect_string,
    expect_string_list,
    fail,
)
from rundra.domain.models import ResourceRequest
from rundra.domain.preparation import (
    CACHE_SCOPES,
    PreparationBuild,
    PreparationConfig,
    PreparationImage,
    PreparationImageDefinition,
    PreparationOutput,
    PreparationSourceGit,
    PreparationSourceWorkingTree,
)

_SHA256_PATTERN = re.compile(r"[0-9a-fA-F]{64}\Z")
_GIT_SHA1_PATTERN = re.compile(r"[0-9a-fA-F]{40}\Z")
_MEMORY_PATTERN = re.compile(r"([1-9][0-9]*)(B|KiB|MiB|GiB|TiB)\Z")
_MEMORY_FACTORS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}
_WALLTIME_PATTERN = re.compile(r"([0-9]{2,}):([0-5][0-9]):([0-5][0-9])\Z")


def parse_preparation(
    value: object,
    *,
    source: Path,
    path: ConfigPath = ("preparation",),
    version: int = 2,
) -> PreparationConfig:
    """Parse one strict project-v2 preparation section."""
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"source", "image", "build"}),
        required=frozenset({"source", "image"}),
        source=source,
        path=path,
    )
    return PreparationConfig(
        source=_source(section["source"], source, (*path, "source"), version=version),
        image=_image(section["image"], source, (*path, "image"), version=version),
        build=(
            _build(section["build"], source, (*path, "build"))
            if "build" in section
            else None
        ),
    )


def _source(
    value: object, source: Path, path: ConfigPath, *, version: int
) -> PreparationSourceGit | PreparationSourceWorkingTree:
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"git"})
        if version == 2
        else frozenset({"git", "working_tree"}),
        required=frozenset(),
        source=source,
        path=path,
    )
    if len(section) != 1:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Source must select exactly one acquisition mode",
        )
    if "working_tree" in section:
        working = expect_mapping(
            section["working_tree"], source=source, path=(*path, "working_tree")
        )
        check_fields(
            working,
            allowed=frozenset(),
            required=frozenset(),
            source=source,
            path=(*path, "working_tree"),
        )
        return PreparationSourceWorkingTree()
    git_path = (*path, "git")
    git = expect_mapping(section["git"], source=source, path=git_path)
    check_fields(
        git,
        allowed=frozenset({"url", "revision"}),
        required=frozenset({"url", "revision"}),
        source=source,
        path=git_path,
    )
    url = expect_string(
        git["url"], source=source, path=(*git_path, "url"), nonblank=True
    )
    _validate_git_url(url, source=source, path=(*git_path, "url"))
    revision = expect_string(
        git["revision"],
        source=source,
        path=(*git_path, "revision"),
        nonblank=True,
    )
    if _GIT_SHA1_PATTERN.fullmatch(revision) is None:
        fail(
            source=source,
            path=(*git_path, "revision"),
            code="INVALID_VALUE",
            message="Git revision must be a full 40-character hexadecimal commit",
        )
    return PreparationSourceGit(url=url, revision=revision.lower())


def _image(
    value: object, source: Path, path: ConfigPath, *, version: int
) -> PreparationImage | PreparationImageDefinition:
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=(
            frozenset({"name", "uri", "sha256"})
            if version == 2
            else frozenset({"name", "prebuilt", "definition"})
        ),
        required=(
            frozenset({"name", "uri", "sha256"})
            if version == 2
            else frozenset({"name"})
        ),
        source=source,
        path=path,
    )
    name_path = (*path, "name")
    name = _safe_relative_path(section["name"], source, name_path)
    if len(name.parts) != 1:
        fail(
            source=source,
            path=name_path,
            code="INVALID_VALUE",
            message="Image name must be one logical filename",
        )
    if version >= 3:
        variants = [name for name in ("prebuilt", "definition") if name in section]
        if len(variants) != 1:
            fail(
                source=source,
                path=path,
                code="INVALID_VALUE",
                message="Image must select exactly one source",
            )
        if variants[0] == "definition":
            definition_path = (*path, "definition")
            definition = expect_mapping(
                section["definition"], source=source, path=definition_path
            )
            check_fields(
                definition,
                allowed=frozenset({"path", "resources", "context"}),
                required=(
                    frozenset({"path", "resources", "context"})
                    if version >= 4
                    else frozenset({"path", "resources"})
                ),
                source=source,
                path=definition_path,
            )
            context = None
            if "context" in definition:
                context_path = (*definition_path, "context")
                context_section = expect_mapping(
                    definition["context"], source=source, path=context_path
                )
                check_fields(
                    context_section,
                    allowed=frozenset({"include"}),
                    required=frozenset({"include"}),
                    source=source,
                    path=context_path,
                )
                raw_include = context_section["include"]
                if type(raw_include) is not list:
                    fail(
                        source=source,
                        path=(*context_path, "include"),
                        code="INVALID_TYPE",
                        message="Definition context include must be a list",
                    )
                context = tuple(
                    _safe_relative_path(
                        item, source, (*context_path, "include", index)
                    )
                    for index, item in enumerate(raw_include)
                )
            return PreparationImageDefinition(
                name=name,
                path=_safe_relative_path(
                    definition["path"], source, (*definition_path, "path")
                ),
                resources=_build_resources(
                    definition["resources"],
                    source,
                    (*definition_path, "resources"),
                ),
                context=context,
            )
        prebuilt_path = (*path, "prebuilt")
        prebuilt = expect_mapping(
            section["prebuilt"], source=source, path=prebuilt_path
        )
        check_fields(
            prebuilt,
            allowed=frozenset({"uri", "sha256"}),
            required=frozenset({"uri", "sha256"}),
            source=source,
            path=prebuilt_path,
        )
        section = {"name": str(name), **prebuilt}
        path = prebuilt_path
    uri = expect_string(
        section["uri"], source=source, path=(*path, "uri"), nonblank=True
    )
    if "\x00" in uri:
        fail(
            source=source,
            path=(*path, "uri"),
            code="INVALID_VALUE",
            message="Image URI must not contain NUL",
        )
    sha256 = expect_string(
        section["sha256"], source=source, path=(*path, "sha256"), nonblank=True
    )
    if _SHA256_PATTERN.fullmatch(sha256) is None:
        fail(
            source=source,
            path=(*path, "sha256"),
            code="INVALID_VALUE",
            message="Image SHA-256 must be 64 hexadecimal characters",
        )
    return PreparationImage(name=name, uri=uri, sha256=sha256.lower())


def _build(value: object, source: Path, path: ConfigPath) -> PreparationBuild:
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"argv", "outputs", "cache_scope", "resources"}),
        required=frozenset({"argv", "outputs", "resources"}),
        source=source,
        path=path,
    )
    argv = expect_string_list(
        section["argv"], source=source, path=(*path, "argv"), nonempty=True
    )
    for index, argument in enumerate(argv):
        if "\x00" in argument:
            fail(
                source=source,
                path=(*path, "argv", index),
                code="INVALID_VALUE",
                message="Build arguments must not contain NUL",
            )
    raw_outputs = section["outputs"]
    if type(raw_outputs) is not list or not raw_outputs:
        fail(
            source=source,
            path=(*path, "outputs"),
            code="INVALID_TYPE" if type(raw_outputs) is not list else "INVALID_VALUE",
            message="Build outputs must be a nonempty list",
        )
    outputs = tuple(
        _output(item, source, (*path, "outputs", index))
        for index, item in enumerate(raw_outputs)
    )
    if len({str(output.path) for output in outputs}) != len(outputs):
        fail(
            source=source,
            path=(*path, "outputs"),
            code="INVALID_VALUE",
            message="Build output paths must be unique",
        )
    cache_scope = expect_string(
        section.get("cache_scope", "target"),
        source=source,
        path=(*path, "cache_scope"),
        nonblank=True,
    )
    if cache_scope not in CACHE_SCOPES:
        fail(
            source=source,
            path=(*path, "cache_scope"),
            code="INVALID_VALUE",
            message="cache_scope must be target or architecture",
        )
    return PreparationBuild(
        argv=argv,
        outputs=outputs,
        cache_scope=cache_scope,
        resources=_build_resources(section["resources"], source, (*path, "resources")),
    )


def _output(value: object, source: Path, path: ConfigPath) -> PreparationOutput:
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"path", "executable"}),
        required=frozenset({"path"}),
        source=source,
        path=path,
    )
    return PreparationOutput(
        path=_safe_relative_path(section["path"], source, (*path, "path")),
        executable=(
            expect_boolean(
                section["executable"], source=source, path=(*path, "executable")
            )
            if "executable" in section
            else False
        ),
    )


def _build_resources(value: object, source: Path, path: ConfigPath) -> ResourceRequest:
    section = expect_mapping(value, source=source, path=path)
    allowed = frozenset({"cpus_per_task", "memory", "walltime"})
    check_fields(
        section,
        allowed=allowed,
        required=allowed,
        source=source,
        path=path,
    )
    return ResourceRequest(
        cpus_per_task=expect_integer(
            section["cpus_per_task"],
            source=source,
            path=(*path, "cpus_per_task"),
            minimum=1,
        ),
        memory_bytes=_parse_memory(section["memory"], source, (*path, "memory")),
        walltime=_parse_walltime(section["walltime"], source, (*path, "walltime")),
    )


def _safe_relative_path(value: object, source: Path, path: ConfigPath) -> PurePath:
    raw = expect_string(value, source=source, path=path, nonblank=True)
    candidate = PurePath(raw)
    if (
        candidate.is_absolute()
        or candidate == PurePath(".")
        or ".." in candidate.parts
        or "\x00" in raw
    ):
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Path must be safe and relative without parent traversal",
        )
    return candidate


def _validate_git_url(url: str, *, source: Path, path: ConfigPath) -> None:
    parsed = urlsplit(url)
    embedded_secret = parsed.password is not None or (
        parsed.username is not None and parsed.scheme in {"http", "https"}
    )
    if embedded_secret or parsed.query or parsed.fragment or "\x00" in url:
        fail(
            source=source,
            path=path,
            code="FORBIDDEN_VALUE",
            message="Git URL must not contain embedded credentials, query, or fragment",
        )


def _parse_memory(value: object, source: Path, path: ConfigPath) -> int:
    text = expect_string(value, source=source, path=path, nonblank=True)
    match = _MEMORY_PATTERN.fullmatch(text)
    if match is None:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Memory must be a positive integer followed by B, KiB, MiB, GiB, or TiB",
        )
    amount, unit = match.groups()
    return int(amount) * _MEMORY_FACTORS[unit]


def _parse_walltime(value: object, source: Path, path: ConfigPath) -> timedelta:
    text = expect_string(value, source=source, path=path, nonblank=True)
    match = _WALLTIME_PATTERN.fullmatch(text)
    if match is None:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Walltime must use HH:MM:SS with two-digit minutes and seconds",
        )
    hours, minutes, seconds = (int(part) for part in match.groups())
    duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    if duration <= timedelta(0):
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Walltime must be positive",
        )
    return duration
