from __future__ import annotations

from dataclasses import dataclass


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
