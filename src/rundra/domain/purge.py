from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import PurePath

from rundra.domain.models import RunId


class PurgeScope(StrEnum):
    OUTPUTS = "outputs"
    WORKSPACE = "workspace"


class PurgeOutcome(StrEnum):
    PLANNED = "planned"
    PURGED = "purged"
    ALREADY_ABSENT = "already_absent"
    RESUMED = "resumed"
    FAILED = "failed"
    PENDING = "pending"


@dataclass(frozen=True, slots=True)
class PurgeRequest:
    run_id: RunId
    run_root: PurePath
    target_workspace: PurePath
    scope: PurgeScope

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise TypeError("PurgeRequest run_id must be a RunId")
        for name in ("run_root", "target_workspace"):
            value = getattr(self, name)
            if not isinstance(value, PurePath) or "\x00" in str(value):
                raise TypeError(f"PurgeRequest {name} must be a safe path")
        if type(self.scope) is not PurgeScope:
            raise TypeError("PurgeRequest scope must be a PurgeScope")


@dataclass(frozen=True, slots=True)
class PurgeResult:
    path: PurePath
    tombstone: PurePath
    outcome: PurgeOutcome
    backend: str

    def __post_init__(self) -> None:
        if type(self.outcome) is not PurgeOutcome:
            raise TypeError("PurgeResult outcome must be a PurgeOutcome")
        if self.backend not in {"local", "ssh"}:
            raise ValueError("PurgeResult backend is unsupported")


@dataclass(frozen=True, slots=True)
class PurgeAttempt:
    attempt_id: str
    started_at: datetime
    finished_at: datetime | None
    scope: PurgeScope
    backend: str
    path: PurePath
    tombstone: PurePath
    outcome: PurgeOutcome
    error_code: str | None = None

    def __post_init__(self) -> None:
        if len(self.attempt_id) != 32 or any(
            value not in "0123456789abcdef" for value in self.attempt_id
        ):
            raise ValueError("PurgeAttempt attempt_id must be 128-bit lowercase hex")
        if self.started_at.utcoffset() is None or (
            self.finished_at is not None and self.finished_at.utcoffset() is None
        ):
            raise ValueError("PurgeAttempt timestamps must be timezone-aware")
        if self.finished_at is not None and self.finished_at < self.started_at:
            raise ValueError("PurgeAttempt cannot finish before it starts")
        if self.backend not in {"local", "ssh"}:
            raise ValueError("PurgeAttempt backend is unsupported")


@dataclass(frozen=True, slots=True)
class PurgeReceipt:
    format_version: int
    run_id: RunId
    attempts: tuple[PurgeAttempt, ...]

    def __post_init__(self) -> None:
        if self.format_version != 1:
            raise ValueError("PurgeReceipt format_version must be 1")
        if type(self.run_id) is not RunId:
            raise TypeError("PurgeReceipt run_id must be a RunId")
        if any(type(attempt) is not PurgeAttempt for attempt in self.attempts):
            raise TypeError("PurgeReceipt attempts must contain PurgeAttempt values")
