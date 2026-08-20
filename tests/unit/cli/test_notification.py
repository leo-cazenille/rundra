from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from rundra.cli.notification import write_wait_notification
from rundra.cli.operations import StatusValue, WaitValue
from rundra.domain.models import RunId
from rundra.domain.states import ExecutionState, RetrievalState


def _wait(*, terminal: bool = True) -> WaitValue:
    return WaitValue(
        StatusValue(
            RunId("run_0123456789abcdef0123456789abcdef"),
            "example",
            "local",
            ExecutionState.SUCCEEDED,
            RetrievalState.NOT_REQUESTED,
            {"succeeded": 1, "total": 1},
        ),
        terminal,
        False,
        1.0,
    )


def test_wait_notification_is_atomic_private_json(tmp_path: Path) -> None:
    path = tmp_path / "complete.json"

    write_wait_notification(path, _wait())

    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["run_id"] == "run_0123456789abcdef0123456789abcdef"
    assert document["state"] == "SUCCEEDED"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_wait_notification_rejects_nonterminal_symlink_and_conflict(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="terminal"):
        write_wait_notification(tmp_path / "pending.json", _wait(terminal=False))
    target = tmp_path / "target"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        write_wait_notification(link, _wait())
    conflict = tmp_path / "conflict.json"
    conflict.write_text('{"run_id":"run_other"}', encoding="utf-8")
    with pytest.raises(ValueError, match="different Run"):
        write_wait_notification(conflict, _wait())
