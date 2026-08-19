from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType

from rundra.domain.models import RunId, TaskId
from rundra.persistence.errors import RunStoreError


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    """Separate durable evidence for one scheduler submission attempt."""

    format_version: int
    run_id: RunId
    task_ids: tuple[TaskId, ...]
    started_at: datetime
    scheduler_job_ids: tuple[str, ...] = ()
    task_scheduler_ids: Mapping[TaskId, str] | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.format_version != 1:
            raise ValueError("Submission receipt format_version must be 1")
        if type(self.run_id) is not RunId:
            raise TypeError("Submission receipt run_id must be a RunId")
        task_ids = tuple(self.task_ids)
        if not task_ids or any(type(item) is not TaskId for item in task_ids):
            raise TypeError("Submission receipt task_ids must contain TaskIds")
        if len(set(task_ids)) != len(task_ids):
            raise ValueError("Submission receipt task_ids must be unique")
        if (
            not isinstance(self.started_at, datetime)
            or self.started_at.utcoffset() is None
        ):
            raise ValueError("Submission receipt started_at must be timezone-aware")
        jobs = tuple(self.scheduler_job_ids)
        if any(type(item) is not str or not item.strip() for item in jobs):
            raise ValueError("Submission receipt scheduler IDs must be nonblank")
        mapping = dict(self.task_scheduler_ids or {})
        if any(
            type(task_id) is not TaskId
            or type(native_id) is not str
            or not native_id.strip()
            for task_id, native_id in mapping.items()
        ):
            raise TypeError("Submission receipt Task mapping is invalid")
        if self.completed_at is None:
            if jobs or mapping:
                raise ValueError(
                    "Pending submission receipt cannot contain scheduler IDs"
                )
        else:
            if (
                not isinstance(self.completed_at, datetime)
                or self.completed_at.utcoffset() is None
                or self.completed_at < self.started_at
            ):
                raise ValueError("Submission receipt completed_at is invalid")
            if not jobs or set(mapping) != set(task_ids):
                raise ValueError("Completed submission receipt must map every Task")
        object.__setattr__(self, "task_ids", task_ids)
        object.__setattr__(self, "scheduler_job_ids", jobs)
        object.__setattr__(self, "task_scheduler_ids", MappingProxyType(mapping))

    @property
    def completed(self) -> bool:
        return self.completed_at is not None


class SubmissionReceiptStore:
    """Atomic per-Run scheduler-submission receipts."""

    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise TypeError("SubmissionReceiptStore root must be a Path")
        self._root = root / "submission-receipts"

    def begin(
        self, run_id: RunId, task_ids: Sequence[TaskId], started_at: datetime
    ) -> SubmissionReceipt:
        receipt = SubmissionReceipt(1, run_id, tuple(task_ids), started_at)
        path = self.path(run_id)
        if path.exists():
            previous = self.load(run_id)
            if previous == receipt:
                return previous
            raise RunStoreError(
                f"Run {run_id} already has a scheduler submission receipt"
            )
        self._write(receipt, replace=False)
        return receipt

    def complete(
        self,
        pending: SubmissionReceipt,
        scheduler_job_ids: Sequence[str],
        task_scheduler_ids: Mapping[TaskId, str],
        completed_at: datetime,
    ) -> SubmissionReceipt:
        if pending.completed:
            return pending
        receipt = SubmissionReceipt(
            1,
            pending.run_id,
            pending.task_ids,
            pending.started_at,
            tuple(scheduler_job_ids),
            task_scheduler_ids,
            completed_at,
        )
        current = self.load(pending.run_id)
        if current == receipt:
            return current
        if current != pending:
            raise RunStoreError(
                f"Run {pending.run_id} submission receipt changed concurrently"
            )
        self._write(receipt, replace=True)
        return receipt

    def load(self, run_id: RunId) -> SubmissionReceipt:
        path = self.path(run_id)
        try:
            value: object = json.loads(path.read_text(encoding="utf-8"))
            return _receipt_from_document(value, run_id)
        except FileNotFoundError as error:
            raise RunStoreError(
                f"Run {run_id} has no scheduler submission receipt"
            ) from error
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
            raise RunStoreError(
                f"Invalid scheduler submission receipt for Run {run_id}: {error}"
            ) from error

    def path(self, run_id: RunId) -> Path:
        if type(run_id) is not RunId:
            raise TypeError("SubmissionReceiptStore requires a RunId")
        return self._root / f"{run_id}.json"

    def _write(self, receipt: SubmissionReceipt, *, replace: bool) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        destination = self.path(receipt.run_id)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix=f".{receipt.run_id}.",
                suffix=".tmp",
                dir=self._root,
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(_receipt_document(receipt), stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            if replace:
                os.replace(temporary, destination)
            else:
                os.link(temporary, destination)
            temporary.unlink(missing_ok=True)
            self._sync_root()
        except FileExistsError as error:
            raise RunStoreError(
                f"Run {receipt.run_id} already has a submission receipt"
            ) from error
        except OSError as error:
            raise RunStoreError(
                f"Could not persist submission receipt for Run {receipt.run_id}: {error}"
            ) from error
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _sync_root(self) -> None:
        descriptor = os.open(self._root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _receipt_document(receipt: SubmissionReceipt) -> dict[str, object]:
    return {
        "format_version": receipt.format_version,
        "run_id": str(receipt.run_id),
        "task_ids": [str(item) for item in receipt.task_ids],
        "started_at": receipt.started_at.isoformat(),
        "scheduler_job_ids": list(receipt.scheduler_job_ids),
        "task_scheduler_ids": {
            str(key): value for key, value in (receipt.task_scheduler_ids or {}).items()
        },
        "completed_at": (
            None if receipt.completed_at is None else receipt.completed_at.isoformat()
        ),
    }


def _receipt_from_document(value: object, run_id: RunId) -> SubmissionReceipt:
    if not isinstance(value, dict) or set(value) != {
        "format_version",
        "run_id",
        "task_ids",
        "started_at",
        "scheduler_job_ids",
        "task_scheduler_ids",
        "completed_at",
    }:
        raise ValueError("invalid receipt fields")
    if value["run_id"] != str(run_id):
        raise ValueError("receipt Run ID mismatch")
    task_ids = value["task_ids"]
    jobs = value["scheduler_job_ids"]
    mapping = value["task_scheduler_ids"]
    if (
        not isinstance(task_ids, list)
        or not isinstance(jobs, list)
        or not isinstance(mapping, dict)
    ):
        raise TypeError("invalid receipt collections")
    completed = value["completed_at"]
    return SubmissionReceipt(
        value["format_version"],
        run_id,
        tuple(TaskId(item) for item in task_ids),
        datetime.fromisoformat(value["started_at"]),
        tuple(jobs),
        {TaskId(key): item for key, item in mapping.items()},
        None if completed is None else datetime.fromisoformat(completed),
    )
