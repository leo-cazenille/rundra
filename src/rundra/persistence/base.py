from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol, runtime_checkable

from rundra.domain.models import RunId
from rundra.domain.records import RunRecord


@runtime_checkable
class RunStore(Protocol):
    """Persistence boundary for durable Run records."""

    def create(self, record: RunRecord) -> None: ...

    def load(self, run_id: RunId) -> RunRecord: ...

    def update(
        self,
        record: RunRecord,
        *,
        expected: RunRecord,
    ) -> None: ...

    def list(self) -> tuple[RunRecord, ...]: ...

    def operation_lock(self, run_id: RunId) -> AbstractContextManager[None]: ...


@runtime_checkable
class CompactRunStore(Protocol):
    """Optional persistence capability for one-way materialized compaction."""

    def compact(self, record: RunRecord, *, expected: RunRecord) -> None: ...
