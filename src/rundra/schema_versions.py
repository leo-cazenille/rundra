from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final


@dataclass(frozen=True, slots=True)
class SchemaVersions:
    """Supported and current versions for one public schema family."""

    current: int
    supported: frozenset[int]

    def __post_init__(self) -> None:
        if type(self.current) is not int or self.current < 1:
            raise ValueError("Current schema version must be positive")
        if not self.supported or any(
            type(version) is not int or version < 1 for version in self.supported
        ):
            raise ValueError("Supported schema versions must be positive integers")
        if self.current not in self.supported:
            raise ValueError("Current schema version must be supported")


RUN_RECORD_SCHEMA = SchemaVersions(6, frozenset({1, 2, 3, 4, 5, 6}))
STATUS_SCHEMA = SchemaVersions(6, RUN_RECORD_SCHEMA.supported)
TASKS_SCHEMA = SchemaVersions(6, RUN_RECORD_SCHEMA.supported)
LOGS_SCHEMA = SchemaVersions(6, RUN_RECORD_SCHEMA.supported)
INSPECT_SCHEMA = SchemaVersions(6, RUN_RECORD_SCHEMA.supported)
RUN_LIST_SCHEMA = SchemaVersions(2, frozenset({2}))
PLAN_SCHEMA = SchemaVersions(7, frozenset({1, 2, 3, 4, 5, 6, 7}))
PROJECT_CONFIG_SCHEMA = SchemaVersions(5, frozenset({1, 2, 3, 4, 5}))
USER_CONFIG_SCHEMA = SchemaVersions(2, frozenset({1, 2}))
TARGET_CONFIG_SCHEMA = SchemaVersions(8, frozenset({1, 2, 3, 4, 5, 6, 7, 8}))
REFERENCE_MANIFEST_SCHEMA = SchemaVersions(1, frozenset({1}))

PUBLIC_SCHEMA_VERSIONS: Final = MappingProxyType(
    {
        "inspect": INSPECT_SCHEMA,
        "logs": LOGS_SCHEMA,
        "plan": PLAN_SCHEMA,
        "project_config": PROJECT_CONFIG_SCHEMA,
        "reference_manifest": REFERENCE_MANIFEST_SCHEMA,
        "run_list": RUN_LIST_SCHEMA,
        "run_record": RUN_RECORD_SCHEMA,
        "status": STATUS_SCHEMA,
        "targets_config": TARGET_CONFIG_SCHEMA,
        "tasks": TASKS_SCHEMA,
        "user_config": USER_CONFIG_SCHEMA,
    }
)
