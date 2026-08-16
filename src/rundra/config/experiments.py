from __future__ import annotations

import re
from datetime import timedelta
from math import isfinite
from pathlib import Path, PurePath

from rundra.config._schema import (
    check_fields,
    expect_boolean,
    expect_integer,
    expect_mapping,
    expect_string,
    expect_string_list,
    fail,
    require_version_one,
)
from rundra.config._yaml import (
    parse_yaml_document,
    read_yaml_document,
    read_yaml_text,
)
from rundra.domain.models import (
    Command,
    ConfigSnapshot,
    ContainerSpec,
    ExperimentSpec,
    NativeValue,
    ResourceRequest,
)
from rundra.security import is_credential_field

_ROOT_FIELDS = frozenset(
    {"version", "experiment", "command", "container", "resources", "outputs", "sync"}
)
_MEMORY_PATTERN = re.compile(r"([1-9][0-9]*)(B|KiB|MiB|GiB|TiB)\Z")
_MEMORY_FACTORS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}
_WALLTIME_PATTERN = re.compile(r"([0-9]{2,}):([0-5][0-9]):([0-5][0-9])\Z")


def load_experiment(source: Path) -> ExperimentSpec:
    """Load a version-1 experiment document into portable domain values."""
    document = expect_mapping(read_yaml_document(source), source=source, path=())
    check_fields(
        document,
        allowed=_ROOT_FIELDS,
        required=frozenset({"version", "experiment", "command", "resources"}),
        source=source,
        path=(),
    )
    version = require_version_one(document["version"], source=source)
    experiment = _experiment_section(document["experiment"], source)
    command = _command_section(document["command"], source)
    resources = _resource_section(document["resources"], source)
    container = (
        _container_section(document["container"], source)
        if "container" in document
        else None
    )
    outputs = _nested_string_list(document, "outputs", "include", source)
    sync_excludes = _nested_string_list(document, "sync", "exclude", source)
    return ExperimentSpec(
        version=version,
        name=experiment,
        command=command,
        resources=resources,
        container=container,
        outputs=outputs,
        sync_excludes=sync_excludes,
    )


def load_config_snapshot(source: Path) -> ConfigSnapshot:
    """Validate YAML syntax while preserving exact scientific config text."""
    content = read_yaml_text(source)
    parse_yaml_document(content, source=source)
    return ConfigSnapshot(source=source, content=content)


def _experiment_section(value: object, source: Path) -> str:
    path = ("experiment",)
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"name"}),
        required=frozenset({"name"}),
        source=source,
        path=path,
    )
    return expect_string(
        section["name"], source=source, path=(*path, "name"), nonblank=True
    )


def _command_section(value: object, source: Path) -> Command:
    path = ("command",)
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"argv", "environment", "working_directory"}),
        required=frozenset({"argv"}),
        source=source,
        path=path,
    )
    argv = expect_string_list(
        section["argv"], source=source, path=(*path, "argv"), nonempty=True
    )
    environment: dict[str, str] = {}
    if "environment" in section:
        raw_environment = expect_mapping(
            section["environment"], source=source, path=(*path, "environment")
        )
        for key, item in raw_environment.items():
            if is_credential_field(key):
                fail(
                    source=source,
                    path=(*path, "environment", key),
                    code="FORBIDDEN_FIELD",
                    message="Credentials must not be stored in experiment configuration",
                )
            environment[key] = expect_string(
                item,
                source=source,
                path=(*path, "environment", key),
            )
    working_directory = None
    if "working_directory" in section:
        working_directory = PurePath(
            expect_string(
                section["working_directory"],
                source=source,
                path=(*path, "working_directory"),
                nonblank=True,
            )
        )
    return Command(
        argv=argv,
        environment=environment,
        working_directory=working_directory,
    )


def _container_section(value: object, source: Path) -> ContainerSpec:
    path = ("container",)
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"image", "gpu"}),
        required=frozenset({"image"}),
        source=source,
        path=path,
    )
    image = PurePath(
        expect_string(
            section["image"], source=source, path=(*path, "image"), nonblank=True
        )
    )
    gpu = (
        expect_boolean(section["gpu"], source=source, path=(*path, "gpu"))
        if "gpu" in section
        else False
    )
    return ContainerSpec(image=image, gpu=gpu)


def _resource_section(value: object, source: Path) -> ResourceRequest:
    path = ("resources",)
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset(
            {
                "nodes",
                "tasks",
                "cpus_per_task",
                "gpus_per_task",
                "memory",
                "walltime",
                "native",
            }
        ),
        required=frozenset(),
        source=source,
        path=path,
    )
    return ResourceRequest(
        nodes=_optional_integer(section, "nodes", 1, 1, source, path),
        tasks=_optional_integer(section, "tasks", 1, 1, source, path),
        cpus_per_task=_optional_integer(section, "cpus_per_task", 1, 1, source, path),
        gpus_per_task=_optional_integer(section, "gpus_per_task", 0, 0, source, path),
        memory_bytes=(
            _parse_memory(section["memory"], source, (*path, "memory"))
            if "memory" in section
            else None
        ),
        walltime=(
            _parse_walltime(section["walltime"], source, (*path, "walltime"))
            if "walltime" in section
            else None
        ),
        native=(
            _native_options(section["native"], source, (*path, "native"))
            if "native" in section
            else {}
        ),
    )


def _optional_integer(
    section: dict[str, object],
    field: str,
    default: int,
    minimum: int,
    source: Path,
    path: tuple[str | int, ...],
) -> int:
    if field not in section:
        return default
    return expect_integer(
        section[field], source=source, path=(*path, field), minimum=minimum
    )


def _parse_memory(
    value: object,
    source: Path,
    path: tuple[str | int, ...],
) -> int:
    text = expect_string(value, source=source, path=path)
    match = _MEMORY_PATTERN.fullmatch(text)
    if match is None:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Memory must be a positive integer followed by B, KiB, MiB, GiB, or TiB",
        )
    amount, unit = match.groups()
    try:
        normalized_amount = int(amount)
    except ValueError:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Memory amount exceeds the supported numeric range",
        )
    return normalized_amount * _MEMORY_FACTORS[unit]


def _parse_walltime(
    value: object,
    source: Path,
    path: tuple[str | int, ...],
) -> timedelta:
    text = expect_string(value, source=source, path=path)
    match = _WALLTIME_PATTERN.fullmatch(text)
    if match is None:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Walltime must use HH:MM:SS with two-digit minutes and seconds",
        )
    try:
        hours, minutes, seconds = (int(component) for component in match.groups())
        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    except (OverflowError, ValueError):
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Walltime exceeds the supported duration range",
        )
    if duration <= timedelta(0):
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Walltime must be positive",
        )
    return duration


def _native_options(
    value: object,
    source: Path,
    path: tuple[str | int, ...],
) -> dict[str, dict[str, NativeValue]]:
    native = expect_mapping(value, source=source, path=path)
    result: dict[str, dict[str, NativeValue]] = {}
    for backend, raw_options in native.items():
        backend_path = (*path, backend)
        expect_string(backend, source=source, path=backend_path, nonblank=True)
        options = expect_mapping(raw_options, source=source, path=backend_path)
        result[backend] = {}
        for field, item in options.items():
            field_path = (*backend_path, field)
            if is_credential_field(field):
                fail(
                    source=source,
                    path=field_path,
                    code="FORBIDDEN_FIELD",
                    message="Credentials must not be stored in experiment configuration",
                )
            if type(item) not in (str, int, float, bool):
                fail(
                    source=source,
                    path=field_path,
                    code="INVALID_TYPE",
                    message="Native options must be scalar values",
                )
            if type(item) is float and not isfinite(item):
                fail(
                    source=source,
                    path=field_path,
                    code="INVALID_VALUE",
                    message="Native numeric options must be finite",
                )
            result[backend][field] = item
    return result


def _nested_string_list(
    document: dict[str, object],
    section_name: str,
    field_name: str,
    source: Path,
) -> tuple[str, ...]:
    if section_name not in document:
        return ()
    path = (section_name,)
    section = expect_mapping(document[section_name], source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({field_name}),
        required=frozenset({field_name}),
        source=source,
        path=path,
    )
    return expect_string_list(
        section[field_name],
        source=source,
        path=(*path, field_name),
    )
