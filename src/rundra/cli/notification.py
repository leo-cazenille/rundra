from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from rundra.cli.operations import AwaitRunsValue, WaitValue


def write_wait_notification(path: Path, value: WaitValue) -> None:
    """Atomically publish one terminal Run notification with private permissions."""

    if not value.terminal:
        raise ValueError("A wait notification requires a terminal Run")
    if path.is_symlink():
        raise ValueError("Notification path must not be a symlink")
    run_id = str(value.status.run_id)
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing.get("run_id") != run_id:
            raise ValueError("Notification path belongs to a different Run")
    if not path.parent.is_dir() or path.parent.is_symlink():
        raise ValueError("Notification parent must be an existing directory")
    document = {
        "format_version": 1,
        "run_id": run_id,
        "state": value.status.state.value,
        "terminal": True,
        "observed_at": datetime.now(UTC).isoformat(),
    }
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(document, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_await_notification(path: Path, value: AwaitRunsValue) -> None:
    """Atomically publish one satisfied aggregate notification."""

    if not value.condition_met:
        raise ValueError("An aggregate notification requires a satisfied condition")
    if path.is_symlink():
        raise ValueError("Notification path must not be a symlink")
    run_ids = [str(status.run_id) for status in value.statuses]
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing.get("run_ids") != run_ids or existing.get("until") != value.until:
            raise ValueError("Notification path belongs to a different aggregate wait")
    if not path.parent.is_dir() or path.parent.is_symlink():
        raise ValueError("Notification parent must be an existing directory")
    document = {
        "condition_met": True,
        "format_version": 1,
        "observed_at": datetime.now(UTC).isoformat(),
        "run_ids": run_ids,
        "states": {str(status.run_id): status.state.value for status in value.statuses},
        "until": value.until,
    }
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(document, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
