from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from rundra.domain.models import RunId


class ProgressPhase(StrEnum):
    """Portable phases exposed to interactive execution observers."""

    RESOLVE = "resolve"
    PREPARE = "prepare"
    STAGE = "stage"
    SUBMIT = "submit"
    WAIT = "wait"
    RETRIEVE = "retrieve"
    COMPLETE = "complete"


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    """One bounded lifecycle update suitable for human feedback."""

    phase: ProgressPhase
    completed: int
    total: int
    message: str
    run_id: RunId | None = None

    def __post_init__(self) -> None:
        if type(self.phase) is not ProgressPhase:
            raise TypeError("ProgressEvent phase must be a ProgressPhase")
        if type(self.completed) is not int or type(self.total) is not int:
            raise TypeError("ProgressEvent counts must be integers")
        if self.total <= 0 or not 0 <= self.completed <= self.total:
            raise ValueError("ProgressEvent counts must describe bounded progress")
        if type(self.message) is not str or not self.message.strip():
            raise ValueError("ProgressEvent message must be nonblank")
        if self.run_id is not None and type(self.run_id) is not RunId:
            raise TypeError("ProgressEvent run_id must be a RunId or None")


ProgressObserver = Callable[[ProgressEvent], None]
