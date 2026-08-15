from __future__ import annotations

from typing import Protocol, runtime_checkable

from rundra.domain.models import RunId
from rundra.domain.records import RunRecord


@runtime_checkable
class RunStore(Protocol):
    """Persistence boundary for durable Run records."""

    def create(self, record: RunRecord) -> None: ...

    def load(self, run_id: RunId) -> RunRecord: ...

    def update(self, record: RunRecord) -> None: ...

    def list(self) -> tuple[RunRecord, ...]: ...
