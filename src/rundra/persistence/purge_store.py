from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path, PurePath

from rundra.domain.models import RunId
from rundra.domain.purge import (
    PurgeAttempt,
    PurgeOutcome,
    PurgeReceipt,
    PurgeScope,
)
from rundra.persistence.errors import RunStoreError


class PurgeReceiptStore:
    def __init__(self, run_store_root: Path) -> None:
        self._root = run_store_root / "purges"

    def path(self, run_id: RunId) -> Path:
        return self._root / f"{run_id}.json"

    def load(self, run_id: RunId) -> PurgeReceipt | None:
        path = self.path(run_id)
        if not path.exists():
            return None
        try:
            value: object = json.loads(path.read_text(encoding="utf-8"))
            return _receipt_from_dict(value, run_id)
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
            raise RunStoreError(f"Invalid purge receipt for Run {run_id}") from error

    def append(self, run_id: RunId, attempt: PurgeAttempt) -> PurgeReceipt:
        previous = self.load(run_id)
        receipt = PurgeReceipt(
            1,
            run_id,
            (*(() if previous is None else previous.attempts), attempt),
        )
        self._write(receipt)
        return receipt

    def replace_last(self, run_id: RunId, attempt: PurgeAttempt) -> PurgeReceipt:
        previous = self.load(run_id)
        if previous is None or not previous.attempts:
            raise RunStoreError(
                f"Purge receipt for Run {run_id} has no pending attempt"
            )
        receipt = PurgeReceipt(1, run_id, (*previous.attempts[:-1], attempt))
        self._write(receipt)
        return receipt

    def _write(self, receipt: PurgeReceipt) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._root,
                prefix=f".{receipt.run_id}.",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(_receipt_document(receipt), stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path(receipt.run_id))
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def receipt_document(receipt: PurgeReceipt) -> dict[str, object]:
    return _receipt_document(receipt)


def _receipt_document(receipt: PurgeReceipt) -> dict[str, object]:
    return {
        "format_version": receipt.format_version,
        "run_id": str(receipt.run_id),
        "attempts": [
            {
                "attempt_id": item.attempt_id,
                "started_at": item.started_at.isoformat(),
                "finished_at": (
                    None if item.finished_at is None else item.finished_at.isoformat()
                ),
                "scope": item.scope.value,
                "backend": item.backend,
                "path": str(item.path),
                "tombstone": str(item.tombstone),
                "outcome": item.outcome.value,
                "error_code": item.error_code,
            }
            for item in receipt.attempts
        ],
    }


def _receipt_from_dict(value: object, run_id: RunId) -> PurgeReceipt:
    if not isinstance(value, dict) or set(value) != {
        "format_version",
        "run_id",
        "attempts",
    }:
        raise ValueError("invalid receipt fields")
    if value["format_version"] != 1 or value["run_id"] != str(run_id):
        raise ValueError("invalid receipt identity")
    raw_attempts = value["attempts"]
    if not isinstance(raw_attempts, list):
        raise TypeError("receipt attempts must be a list")
    attempts: list[PurgeAttempt] = []
    for item in raw_attempts:
        if not isinstance(item, dict) or set(item) != {
            "attempt_id",
            "started_at",
            "finished_at",
            "scope",
            "backend",
            "path",
            "tombstone",
            "outcome",
            "error_code",
        }:
            raise ValueError("invalid purge attempt")
        if (
            type(item["attempt_id"]) is not str
            or type(item["started_at"]) is not str
            or (
                item["finished_at"] is not None and type(item["finished_at"]) is not str
            )
            or type(item["scope"]) is not str
            or type(item["backend"]) is not str
            or type(item["path"]) is not str
            or type(item["tombstone"]) is not str
            or type(item["outcome"]) is not str
            or (item["error_code"] is not None and type(item["error_code"]) is not str)
        ):
            raise TypeError("invalid purge attempt values")
        attempts.append(
            PurgeAttempt(
                attempt_id=item["attempt_id"],
                started_at=datetime.fromisoformat(item["started_at"]),
                finished_at=(
                    None
                    if item["finished_at"] is None
                    else datetime.fromisoformat(item["finished_at"])
                ),
                scope=PurgeScope(item["scope"]),
                backend=item["backend"],
                path=PurePath(item["path"]),
                tombstone=PurePath(item["tombstone"]),
                outcome=PurgeOutcome(item["outcome"]),
                error_code=item["error_code"],
            )
        )
    return PurgeReceipt(1, run_id, tuple(attempts))
