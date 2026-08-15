from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from rundra.config.errors import ConfigError

_YAML_MERGE_TAG = "tag:yaml.org,2002:merge"


class _DuplicateFieldError(yaml.YAMLError):
    def __init__(self, field: object, path: tuple[str | int, ...]) -> None:
        self.field = field
        self.path = path
        super().__init__(f"Duplicate YAML field: {field}")


class _InvalidFieldError(yaml.YAMLError):
    def __init__(self, path: tuple[str | int, ...]) -> None:
        self.path = path
        super().__init__("YAML mapping keys must be scalar hashable values")


def _validate_mapping_keys(
    loader: yaml.SafeLoader,
    node: yaml.Node,
    path: tuple[str | int, ...],
    visited: set[int],
) -> None:
    node_identity = id(node)
    if node_identity in visited:
        return
    visited.add(node_identity)
    if isinstance(node, yaml.MappingNode):
        seen: dict[object, None] = {}
        for key_node, value_node in node.value:
            if not isinstance(key_node, yaml.ScalarNode):
                raise _InvalidFieldError(path)
            key = (
                "<<"
                if key_node.tag == _YAML_MERGE_TAG
                else loader.construct_object(key_node, deep=False)
            )
            component = key if type(key) in (str, int) else str(key)
            try:
                duplicate = key in seen
            except TypeError as error:
                raise _InvalidFieldError(path) from error
            if duplicate:
                raise _DuplicateFieldError(key, (*path, component))
            seen[key] = None
            _validate_mapping_keys(
                loader,
                value_node,
                (*path, component),
                visited,
            )
    elif isinstance(node, yaml.SequenceNode):
        for index, item_node in enumerate(node.value):
            _validate_mapping_keys(loader, item_node, (*path, index), visited)


def read_yaml_text(source: Path) -> str:
    """Read UTF-8 configuration text, translating missing-file failures."""
    try:
        with source.open("r", encoding="utf-8", newline="") as stream:
            return stream.read()
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
    loader = yaml.SafeLoader(content)
    try:
        node = loader.get_single_node()
        if node is None:
            return None
        _validate_mapping_keys(loader, node, (), set())
        return loader.construct_document(node)  # type: ignore[no-untyped-call]
    except _DuplicateFieldError as error:
        raise ConfigError(
            code="DUPLICATE_FIELD",
            message=str(error),
            source=source,
            path=error.path,
        ) from error
    except _InvalidFieldError as error:
        raise ConfigError(
            code="INVALID_TYPE",
            message=str(error),
            source=source,
            path=error.path,
        ) from error
    except yaml.YAMLError as error:
        raise ConfigError(
            code="INVALID_YAML",
            message=f"Invalid YAML: {error}",
            source=source,
        ) from error
    finally:
        loader.dispose()  # type: ignore[no-untyped-call]


def read_yaml_document(source: Path) -> Any:
    """Read and parse exactly one safe UTF-8 YAML document."""
    return parse_yaml_document(read_yaml_text(source), source=source)
