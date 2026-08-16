from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Never, cast

from rundra.config.errors import ConfigError

type ConfigPath = tuple[str | int, ...]


def fail(
    *,
    source: Path,
    path: ConfigPath,
    code: str,
    message: str,
) -> Never:
    raise ConfigError(code=code, message=message, source=source, path=path)


def expect_mapping(
    value: object,
    *,
    source: Path,
    path: ConfigPath,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        fail(
            source=source,
            path=path,
            code="INVALID_TYPE",
            message="Expected a mapping",
        )
    result: dict[str, Any] = {}
    for key, item in value.items():
        if type(key) is not str:
            fail(
                source=source,
                path=path,
                code="INVALID_TYPE",
                message="Mapping field names must be strings",
            )
        result[key] = item
    return result


def check_fields(
    value: Mapping[str, object],
    *,
    allowed: frozenset[str],
    required: frozenset[str],
    source: Path,
    path: ConfigPath,
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        field = unknown[0]
        fail(
            source=source,
            path=(*path, field),
            code="UNKNOWN_FIELD",
            message=f"Unknown field '{field}'",
        )
    missing = sorted(required - set(value))
    if missing:
        field = missing[0]
        fail(
            source=source,
            path=(*path, field),
            code="MISSING_FIELD",
            message=f"Required field '{field}' is missing",
        )


def expect_string(
    value: object,
    *,
    source: Path,
    path: ConfigPath,
    nonblank: bool = False,
) -> str:
    if type(value) is not str:
        fail(
            source=source,
            path=path,
            code="INVALID_TYPE",
            message="Expected a string",
        )
    if nonblank and not value.strip():
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="Value must not be blank",
        )
    return value


def expect_boolean(value: object, *, source: Path, path: ConfigPath) -> bool:
    if type(value) is not bool:
        fail(
            source=source,
            path=path,
            code="INVALID_TYPE",
            message="Expected a boolean",
        )
    return value


def expect_integer(
    value: object,
    *,
    source: Path,
    path: ConfigPath,
    minimum: int,
) -> int:
    if type(value) is not int:
        fail(
            source=source,
            path=path,
            code="INVALID_TYPE",
            message="Expected an integer",
        )
    if value < minimum:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message=f"Value must be at least {minimum}",
        )
    return value


def expect_string_list(
    value: object,
    *,
    source: Path,
    path: ConfigPath,
    nonempty: bool = False,
) -> tuple[str, ...]:
    if type(value) is not list:
        fail(
            source=source,
            path=path,
            code="INVALID_TYPE",
            message="Expected a list of strings",
        )
    result: list[str] = []
    for index, item in enumerate(cast(list[object], value)):
        result.append(
            expect_string(
                item,
                source=source,
                path=(*path, index),
                nonblank=nonempty,
            )
        )
    if nonempty and not result:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message="List must not be empty",
        )
    return tuple(result)


def require_version_one(
    value: object,
    *,
    source: Path,
    path: ConfigPath = ("version",),
) -> int:
    version = expect_integer(value, source=source, path=path, minimum=1)
    if version != 1:
        fail(
            source=source,
            path=path,
            code="UNSUPPORTED_VERSION",
            message=f"Unsupported schema version {version}; expected version 1",
        )
    return version
