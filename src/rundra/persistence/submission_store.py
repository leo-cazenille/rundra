from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path, PurePath
from types import MappingProxyType

from rundra.domain.models import RunId, TaskId
from rundra.domain.scaling import SeedRange, TaskSpace
from rundra.persistence.errors import RunStoreError


class SubmissionReceiptOutcome(StrEnum):
    """Durable scheduler-submission attempt outcome."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNCERTAIN = "uncertain"
    OPERATOR_RESOLVED = "operator_resolved"


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
    outcome: SubmissionReceiptOutcome | None = None
    backend: str | None = None
    phase: str | None = None
    failure_classification: str | None = None
    exit_code: int | None = None
    updated_at: datetime | None = None
    task_space: TaskSpace | None = None
    execution_strategy: str | None = None
    retrieval_policy: str | None = None
    task_state_store: PurePath | None = None

    def __post_init__(self) -> None:
        if self.format_version not in {1, 2, 3}:
            raise ValueError("Submission receipt format_version must be 1, 2, or 3")
        if type(self.run_id) is not RunId:
            raise TypeError("Submission receipt run_id must be a RunId")
        task_ids = tuple(self.task_ids)
        if any(type(item) is not TaskId for item in task_ids):
            raise TypeError("Submission receipt task_ids must contain TaskIds")
        if len(set(task_ids)) != len(task_ids):
            raise ValueError("Submission receipt task_ids must be unique")
        compact_values = (
            self.task_space,
            self.execution_strategy,
            self.retrieval_policy,
            self.task_state_store,
        )
        if self.format_version == 3:
            if task_ids:
                raise ValueError("Version-3 receipt cannot materialize Task IDs")
            if type(self.task_space) is not TaskSpace:
                raise TypeError("Version-3 receipt requires a TaskSpace")
            if self.execution_strategy not in {"multi-array", "worker-pool"}:
                raise ValueError("Version-3 receipt execution strategy is unsupported")
            if self.retrieval_policy not in {"all", "manifest", "none"}:
                raise ValueError("Version-3 receipt retrieval policy is unsupported")
            if (
                not isinstance(self.task_state_store, PurePath)
                or self.task_state_store.is_absolute()
                or self.task_state_store.name != str(self.task_state_store)
            ):
                raise ValueError(
                    "Version-3 receipt task_state_store must be one relative filename"
                )
        else:
            if not task_ids:
                raise ValueError("Version-1 and version-2 receipts require Task IDs")
            if any(value is not None for value in compact_values):
                raise ValueError("Legacy receipts cannot contain compact Task metadata")
        if (
            not isinstance(self.started_at, datetime)
            or self.started_at.utcoffset() is None
        ):
            raise ValueError("Submission receipt started_at must be timezone-aware")
        outcome = self.outcome
        updated_at = self.updated_at
        if self.format_version == 1:
            if any(
                value is not None
                for value in (
                    outcome,
                    self.backend,
                    self.phase,
                    self.failure_classification,
                    self.exit_code,
                    updated_at,
                )
            ):
                raise ValueError("Version-1 receipt cannot contain version-2 fields")
            outcome = (
                SubmissionReceiptOutcome.ACCEPTED
                if self.completed_at is not None
                else SubmissionReceiptOutcome.PENDING
            )
            updated_at = self.completed_at or self.started_at
        else:
            if type(outcome) is not SubmissionReceiptOutcome:
                raise TypeError("Version-2 receipt outcome must be explicit")
            if (
                not isinstance(updated_at, datetime)
                or updated_at.utcoffset() is None
                or updated_at < self.started_at
            ):
                raise ValueError("Version-2 receipt updated_at is invalid")
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
        accepted = outcome is SubmissionReceiptOutcome.ACCEPTED
        if accepted:
            if (
                self.completed_at is None
                or not isinstance(self.completed_at, datetime)
                or self.completed_at.utcoffset() is None
                or self.completed_at < self.started_at
            ):
                raise ValueError("Submission receipt completed_at is invalid")
            if not jobs:
                raise ValueError("Accepted submission receipt requires scheduler IDs")
            if self.format_version != 3 and set(mapping) != set(task_ids):
                raise ValueError("Accepted submission receipt must map every Task")
            if self.format_version == 3 and mapping:
                raise ValueError("Compact receipt cannot materialize Task mappings")
        elif jobs or mapping:
            raise ValueError("Non-accepted receipt cannot contain scheduler IDs")
        if outcome in {
            SubmissionReceiptOutcome.REJECTED,
            SubmissionReceiptOutcome.OPERATOR_RESOLVED,
        }:
            if (
                self.completed_at is None
                or not isinstance(self.completed_at, datetime)
                or self.completed_at.utcoffset() is None
                or self.completed_at < self.started_at
            ):
                raise ValueError("Terminal submission receipt completed_at is invalid")
        elif not accepted and self.completed_at is not None:
            raise ValueError("Pending or uncertain receipt cannot be completed")
        failure = outcome in {
            SubmissionReceiptOutcome.REJECTED,
            SubmissionReceiptOutcome.UNCERTAIN,
            SubmissionReceiptOutcome.OPERATOR_RESOLVED,
        }
        details = (self.backend, self.phase, self.failure_classification)
        if failure:
            if any(
                type(value) is not str or not value.strip() or "\x00" in value
                for value in details
            ):
                raise ValueError("Submission receipt failure details must be safe")
        elif any(value is not None for value in details):
            raise ValueError(
                "Pending or accepted receipt cannot contain failure details"
            )
        if self.exit_code is not None and type(self.exit_code) is not int:
            raise TypeError("Submission receipt exit_code must be an integer or None")
        if not failure and self.exit_code is not None:
            raise ValueError("Only failed submission outcomes can contain exit_code")
        object.__setattr__(self, "task_ids", task_ids)
        object.__setattr__(self, "scheduler_job_ids", jobs)
        object.__setattr__(self, "task_scheduler_ids", MappingProxyType(mapping))
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "updated_at", updated_at)

    @property
    def completed(self) -> bool:
        return self.outcome is SubmissionReceiptOutcome.ACCEPTED

    @property
    def terminal(self) -> bool:
        return self.outcome in {
            SubmissionReceiptOutcome.ACCEPTED,
            SubmissionReceiptOutcome.REJECTED,
            SubmissionReceiptOutcome.OPERATOR_RESOLVED,
        }


class SubmissionReceiptStore:
    """Atomic per-Run scheduler-submission receipts."""

    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise TypeError("SubmissionReceiptStore root must be a Path")
        self._root = root / "submission-receipts"

    def begin(
        self,
        run_id: RunId,
        task_ids: Sequence[TaskId],
        started_at: datetime,
        *,
        backend: str | None = None,
    ) -> SubmissionReceipt:
        del backend
        receipt = SubmissionReceipt(
            2,
            run_id,
            tuple(task_ids),
            started_at,
            outcome=SubmissionReceiptOutcome.PENDING,
            updated_at=started_at,
        )
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

    def begin_compact(
        self,
        run_id: RunId,
        task_space: TaskSpace,
        started_at: datetime,
        *,
        execution_strategy: str,
        retrieval_policy: str,
        task_state_store: PurePath,
        backend: str | None = None,
    ) -> SubmissionReceipt:
        """Start a constant-size receipt for one compact submission."""
        del backend
        receipt = SubmissionReceipt(
            3,
            run_id,
            (),
            started_at,
            outcome=SubmissionReceiptOutcome.PENDING,
            updated_at=started_at,
            task_space=task_space,
            execution_strategy=execution_strategy,
            retrieval_policy=retrieval_policy,
            task_state_store=task_state_store,
        )
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
            2,
            pending.run_id,
            pending.task_ids,
            pending.started_at,
            tuple(scheduler_job_ids),
            task_scheduler_ids,
            completed_at,
            SubmissionReceiptOutcome.ACCEPTED,
            updated_at=completed_at,
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

    def complete_compact(
        self,
        pending: SubmissionReceipt,
        scheduler_job_ids: Sequence[str],
        completed_at: datetime,
    ) -> SubmissionReceipt:
        """Accept a compact receipt after its scheduler identities are durable."""
        if pending.format_version != 3:
            raise ValueError("complete_compact requires a version-3 receipt")
        if pending.completed:
            return pending
        receipt = SubmissionReceipt(
            3,
            pending.run_id,
            (),
            pending.started_at,
            tuple(scheduler_job_ids),
            completed_at=completed_at,
            outcome=SubmissionReceiptOutcome.ACCEPTED,
            updated_at=completed_at,
            task_space=pending.task_space,
            execution_strategy=pending.execution_strategy,
            retrieval_policy=pending.retrieval_policy,
            task_state_store=pending.task_state_store,
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

    def reject(
        self,
        pending: SubmissionReceipt,
        *,
        backend: str,
        phase: str,
        failure_classification: str,
        exit_code: int | None,
        updated_at: datetime,
    ) -> SubmissionReceipt:
        return self._fail(
            pending,
            outcome=SubmissionReceiptOutcome.REJECTED,
            backend=backend,
            phase=phase,
            failure_classification=failure_classification,
            exit_code=exit_code,
            updated_at=updated_at,
        )

    def mark_uncertain(
        self,
        pending: SubmissionReceipt,
        *,
        backend: str,
        phase: str,
        failure_classification: str,
        exit_code: int | None,
        updated_at: datetime,
    ) -> SubmissionReceipt:
        return self._fail(
            pending,
            outcome=SubmissionReceiptOutcome.UNCERTAIN,
            backend=backend,
            phase=phase,
            failure_classification=failure_classification,
            exit_code=exit_code,
            updated_at=updated_at,
        )

    def resolve_not_submitted(
        self,
        receipt: SubmissionReceipt,
        *,
        updated_at: datetime,
    ) -> SubmissionReceipt:
        """Persist an operator's explicit confirmation that no job was submitted."""
        return self._fail(
            receipt,
            outcome=SubmissionReceiptOutcome.OPERATOR_RESOLVED,
            backend=receipt.backend or "legacy",
            phase="operator_resolution",
            failure_classification="operator_verified_not_submitted",
            exit_code=None,
            updated_at=updated_at,
        )

    def _fail(
        self,
        pending: SubmissionReceipt,
        *,
        outcome: SubmissionReceiptOutcome,
        backend: str,
        phase: str,
        failure_classification: str,
        exit_code: int | None,
        updated_at: datetime,
    ) -> SubmissionReceipt:
        if pending.outcome is outcome:
            return pending
        resolvable = outcome is SubmissionReceiptOutcome.OPERATOR_RESOLVED and (
            pending.outcome is SubmissionReceiptOutcome.UNCERTAIN
            or (pending.format_version == 1 and pending.outcome is None)
        )
        if pending.outcome is not SubmissionReceiptOutcome.PENDING and not resolvable:
            current_outcome = pending.outcome
            if current_outcome is None:
                raise RunStoreError(
                    f"Run {pending.run_id} has a legacy submission receipt"
                )
            raise RunStoreError(
                f"Run {pending.run_id} submission receipt is already "
                f"{current_outcome.value}"
            )
        version = 3 if pending.format_version == 3 else 2
        receipt = SubmissionReceipt(
            version,
            pending.run_id,
            pending.task_ids,
            pending.started_at,
            completed_at=(
                updated_at
                if outcome
                in {
                    SubmissionReceiptOutcome.REJECTED,
                    SubmissionReceiptOutcome.OPERATOR_RESOLVED,
                }
                else None
            ),
            outcome=outcome,
            backend=backend,
            phase=phase,
            failure_classification=failure_classification,
            exit_code=exit_code,
            updated_at=updated_at,
            task_space=pending.task_space,
            execution_strategy=pending.execution_strategy,
            retrieval_policy=pending.retrieval_policy,
            task_state_store=pending.task_state_store,
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
    document: dict[str, object] = {
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
    if receipt.format_version in {2, 3}:
        if receipt.outcome is None or receipt.updated_at is None:
            raise ValueError("version-2 receipt is missing outcome metadata")
        document.update(
            {
                "outcome": receipt.outcome.value,
                "backend": receipt.backend,
                "phase": receipt.phase,
                "failure_classification": receipt.failure_classification,
                "exit_code": receipt.exit_code,
                "updated_at": receipt.updated_at.isoformat(),
            }
        )
    if receipt.format_version == 3:
        assert receipt.task_space is not None
        document.pop("task_ids")
        document.pop("task_scheduler_ids")
        document.update(
            {
                "task_space": {
                    "parameter_set_count": receipt.task_space.parameter_set_count,
                    "seeds": {
                        "start": receipt.task_space.seeds.start,
                        "stop": receipt.task_space.seeds.stop,
                        "step": receipt.task_space.seeds.step,
                    },
                    "task_count": receipt.task_space.task_count,
                },
                "execution_strategy": receipt.execution_strategy,
                "retrieval_policy": receipt.retrieval_policy,
                "task_state_store": str(receipt.task_state_store),
            }
        )
    return document


def _receipt_from_document(value: object, run_id: RunId) -> SubmissionReceipt:
    common = {
        "format_version",
        "run_id",
        "task_ids",
        "started_at",
        "scheduler_job_ids",
        "task_scheduler_ids",
        "completed_at",
    }
    version_two = {
        "outcome",
        "backend",
        "phase",
        "failure_classification",
        "exit_code",
        "updated_at",
    }
    version_three = (
        (common - {"task_ids", "task_scheduler_ids"})
        | version_two
        | {
            "task_space",
            "execution_strategy",
            "retrieval_policy",
            "task_state_store",
        }
    )
    if not isinstance(value, dict):
        raise ValueError("invalid receipt fields")
    version = value.get("format_version")
    if version not in (1, 2, 3):
        raise ValueError("unsupported receipt format version")
    expected = (
        common
        if version == 1
        else (common | version_two if version == 2 else version_three)
    )
    if set(value) != expected:
        raise ValueError("invalid receipt fields")
    if value["run_id"] != str(run_id):
        raise ValueError("receipt Run ID mismatch")
    task_ids = value.get("task_ids", [])
    jobs = value["scheduler_job_ids"]
    mapping = value.get("task_scheduler_ids", {})
    if (
        not isinstance(task_ids, list)
        or not isinstance(jobs, list)
        or not isinstance(mapping, dict)
    ):
        raise TypeError("invalid receipt collections")
    completed = value["completed_at"]
    task_space: TaskSpace | None = None
    if version == 3:
        task_space_document = value["task_space"]
        if not isinstance(task_space_document, dict) or set(task_space_document) != {
            "parameter_set_count",
            "seeds",
            "task_count",
        }:
            raise ValueError("invalid compact receipt TaskSpace")
        seeds = task_space_document["seeds"]
        if not isinstance(seeds, dict) or set(seeds) != {"start", "stop", "step"}:
            raise ValueError("invalid compact receipt seed range")
        task_space = TaskSpace(
            task_space_document["parameter_set_count"],
            SeedRange(seeds["start"], seeds["stop"], seeds["step"]),
        )
        if task_space_document["task_count"] != task_space.task_count:
            raise ValueError("compact receipt task_count does not match TaskSpace")
    return SubmissionReceipt(
        version,
        run_id,
        tuple(TaskId(item) for item in task_ids),
        datetime.fromisoformat(value["started_at"]),
        tuple(jobs),
        {TaskId(key): item for key, item in mapping.items()},
        None if completed is None else datetime.fromisoformat(completed),
        (None if version == 1 else SubmissionReceiptOutcome(value["outcome"])),
        None if version == 1 else value["backend"],
        None if version == 1 else value["phase"],
        None if version == 1 else value["failure_classification"],
        None if version == 1 else value["exit_code"],
        None if version == 1 else datetime.fromisoformat(value["updated_at"]),
        task_space,
        None if version != 3 else value["execution_strategy"],
        None if version != 3 else value["retrieval_policy"],
        None if version != 3 else PurePath(value["task_state_store"]),
    )
