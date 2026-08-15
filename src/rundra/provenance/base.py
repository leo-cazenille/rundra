from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePath
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class GitProvenance:
    """Accurately available Git identity for one source snapshot."""

    commit: str | None = None
    branch: str | None = None
    dirty: bool | None = None
    diff: str | None = None

    def __post_init__(self) -> None:
        for name in ("commit", "branch"):
            value = getattr(self, name)
            if value is not None and (type(value) is not str or not value.strip()):
                raise ValueError(f"GitProvenance {name} must be nonblank or None")
        if self.dirty is not None and type(self.dirty) is not bool:
            raise TypeError("GitProvenance dirty must be a boolean or None")
        if self.diff is not None and type(self.diff) is not str:
            raise TypeError("GitProvenance diff must be a string or None")
        if self.diff is not None and self.dirty is not True:
            raise ValueError("GitProvenance diff requires dirty=True")


@runtime_checkable
class ProvenanceProvider(Protocol):
    """Capture optional source provenance without controlling execution."""

    def capture(self, source_root: PurePath) -> GitProvenance: ...
