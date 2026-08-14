from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from shoal_run.config.errors import ConfigError


class _DuplicateFieldError(yaml.YAMLError):
    def __init__(self, field: object) -> None:
        self.field = field
        super().__init__(f"Duplicate YAML field: {field}")


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[object, object]:
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise _DuplicateFieldError(key)
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def read_yaml_text(source: Path) -> str:
    """Read UTF-8 configuration text, translating missing-file failures."""
    try:
        return source.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise ConfigError(
            code="CONFIG_NOT_FOUND",
            message="Configuration file does not exist",
            source=source,
        ) from error
    except UnicodeDecodeError as error:
        raise ConfigError(
            code="INVALID_ENCODING",
            message="Configuration file must be valid UTF-8",
            source=source,
        ) from error
    except OSError as error:
        raise ConfigError(
            code="CONFIG_IO",
            message=f"Could not read configuration file: {error.strerror or error}",
            source=source,
        ) from error


def parse_yaml_document(content: str, *, source: Path) -> Any:
    """Parse exactly one safe YAML document with unique mapping fields."""
    try:
        return yaml.load(content, Loader=_UniqueKeyLoader)
    except _DuplicateFieldError as error:
        raise ConfigError(
            code="DUPLICATE_FIELD",
            message=str(error),
            source=source,
            path=(str(error.field),),
        ) from error
    except yaml.YAMLError as error:
        raise ConfigError(
            code="INVALID_YAML",
            message=f"Invalid YAML: {error}",
            source=source,
        ) from error


def read_yaml_document(source: Path) -> Any:
    """Read and parse exactly one safe UTF-8 YAML document."""
    return parse_yaml_document(read_yaml_text(source), source=source)
