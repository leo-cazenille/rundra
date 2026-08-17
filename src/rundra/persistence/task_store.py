from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import cast

from rundra.domain.models import RunId
from rundra.domain.scaling import SeedRange, TaskCoordinate, TaskSpace
from rundra.domain.states import (
    ExecutionState,
    RetrievalState,
    validate_execution_transition,
    validate_retrieval_transition,
)
from rundra.persistence.errors import RunNotFoundError, RunStoreError

_MAX_PAGE_SIZE = 1000


@dataclass(frozen=True, slots=True)
class TaskState:
    coordinate: TaskCoordinate
    execution_state: ExecutionState = ExecutionState.CREATED
    retrieval_state: RetrievalState = RetrievalState.NOT_REQUESTED
    scheduler_id: str | None = None
    native_state: str | None = None
    exit_code: int | None = None
    attempt: int = 0

    def __post_init__(self) -> None:
        if type(self.coordinate) is not TaskCoordinate:
            raise TypeError("TaskState coordinate must be a TaskCoordinate")
        if type(self.execution_state) is not ExecutionState:
            raise TypeError("TaskState execution_state must be an ExecutionState")
        if type(self.retrieval_state) is not RetrievalState:
            raise TypeError("TaskState retrieval_state must be a RetrievalState")
        for name in ("scheduler_id", "native_state"):
            value = getattr(self, name)
            if value is not None and (
                type(value) is not str or not value.strip() or "\x00" in value
            ):
                raise ValueError(f"TaskState {name} must be a safe nonblank string")
        if self.exit_code is not None and type(self.exit_code) is not int:
            raise TypeError("TaskState exit_code must be an integer or None")
        if type(self.attempt) is not int:
            raise TypeError("TaskState attempt must be an integer")
        if self.attempt < 0:
            raise ValueError("TaskState attempt must be non-negative")


@dataclass(frozen=True, slots=True)
class TaskStateCounts:
    total: int
    execution: Mapping[ExecutionState, int]
    retrieval: Mapping[RetrievalState, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "execution", MappingProxyType(dict(self.execution)))
        object.__setattr__(self, "retrieval", MappingProxyType(dict(self.retrieval)))


@dataclass(frozen=True, slots=True)
class TaskStatePage:
    total: int
    offset: int
    limit: int
    tasks: tuple[TaskState, ...] = field(default_factory=tuple)


class SqliteTaskStore:
    """Sparse per-Run Task state for compact version-4 TaskSpaces."""

    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise TypeError("SqliteTaskStore root must be a Path")
        self._root = root

    def create(self, run_id: RunId, task_space: TaskSpace) -> None:
        self._validate_identity(run_id, task_space)
        self._ensure_root()
        path = self.path(run_id)
        if path.is_symlink():
            raise RunStoreError(f"Task state path must not be a symlink: {path}")
        try:
            with self._connect(path) as connection:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value INTEGER NOT NULL
                    ) WITHOUT ROWID;
                    CREATE TABLE IF NOT EXISTS task_state (
                        ordinal INTEGER PRIMARY KEY,
                        execution_state TEXT NOT NULL,
                        retrieval_state TEXT NOT NULL,
                        scheduler_id TEXT,
                        native_state TEXT,
                        exit_code INTEGER,
                        attempt INTEGER NOT NULL
                    );
                    """
                )
                expected = {
                    "parameter_set_count": task_space.parameter_set_count,
                    "seed_start": task_space.seeds.start,
                    "seed_stop": task_space.seeds.stop,
                    "seed_step": task_space.seeds.step,
                    "task_count": task_space.task_count,
                }
                connection.executemany(
                    "INSERT OR IGNORE INTO metadata(key, value) VALUES (?, ?)",
                    expected.items(),
                )
                actual = dict(connection.execute("SELECT key, value FROM metadata"))
                if actual != expected:
                    raise RunStoreError(
                        f"Task state for Run {run_id} describes another TaskSpace"
                    )
            path.chmod(0o600)
        except sqlite3.Error as error:
            raise RunStoreError(
                f"Could not create Task state for Run {run_id}: {error}"
            ) from error

    def path(self, run_id: RunId) -> Path:
        if type(run_id) is not RunId:
            raise TypeError("Task state requires a RunId")
        return self._root / f"{run_id}.tasks.sqlite3"

    def task_space(self, run_id: RunId) -> TaskSpace:
        with self._open_existing(run_id) as connection:
            values = dict(connection.execute("SELECT key, value FROM metadata"))
        try:
            return TaskSpace(
                values["parameter_set_count"],
                SeedRange(
                    values["seed_start"], values["seed_stop"], values["seed_step"]
                ),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise RunStoreError(
                f"Task state for Run {run_id} has invalid metadata"
            ) from error

    def get(self, run_id: RunId, ordinal: int) -> TaskState:
        task_space = self.task_space(run_id)
        coordinate = task_space.coordinate(ordinal)
        with self._open_existing(run_id) as connection:
            row = connection.execute(
                "SELECT execution_state, retrieval_state, scheduler_id, "
                "native_state, exit_code, attempt FROM task_state WHERE ordinal = ?",
                (ordinal,),
            ).fetchone()
        return self._state(coordinate, row)

    def page(
        self, run_id: RunId, *, offset: int = 0, limit: int = 100
    ) -> TaskStatePage:
        if type(offset) is not int or offset < 0:
            raise ValueError("Task page offset must be a non-negative integer")
        if type(limit) is not int or not 1 <= limit <= _MAX_PAGE_SIZE:
            raise ValueError(f"Task page limit must be between 1 and {_MAX_PAGE_SIZE}")
        task_space = self.task_space(run_id)
        coordinates = task_space.page(offset=offset, limit=limit)
        if not coordinates:
            return TaskStatePage(task_space.task_count, offset, limit)
        stop = coordinates[-1].ordinal
        with self._open_existing(run_id) as connection:
            rows = {
                row[0]: row[1:]
                for row in connection.execute(
                    "SELECT ordinal, execution_state, retrieval_state, scheduler_id, "
                    "native_state, exit_code, attempt FROM task_state "
                    "WHERE ordinal BETWEEN ? AND ? ORDER BY ordinal",
                    (offset, stop),
                )
            }
        return TaskStatePage(
            task_space.task_count,
            offset,
            limit,
            tuple(self._state(item, rows.get(item.ordinal)) for item in coordinates),
        )

    def update_batch(self, run_id: RunId, states: Sequence[TaskState]) -> None:
        if not isinstance(states, Sequence) or isinstance(states, (str, bytes)):
            raise TypeError("Task state batch must be a sequence")
        updates = tuple(states)
        if not updates:
            raise ValueError("Task state batch must not be empty")
        if any(type(item) is not TaskState for item in updates):
            raise TypeError("Task state batch must contain TaskState values")
        ordinals = tuple(item.coordinate.ordinal for item in updates)
        if len(set(ordinals)) != len(ordinals):
            raise ValueError("Task state batch contains duplicate ordinals")
        task_space = self.task_space(run_id)
        if any(
            task_space.coordinate(item.coordinate.ordinal) != item.coordinate
            for item in updates
        ):
            raise ValueError(
                "Task state batch contains a coordinate from another TaskSpace"
            )
        try:
            with self._open_existing(run_id) as connection:
                connection.execute("BEGIN IMMEDIATE")
                for item in updates:
                    row = connection.execute(
                        "SELECT execution_state, retrieval_state, scheduler_id, "
                        "native_state, exit_code, attempt FROM task_state WHERE ordinal = ?",
                        (item.coordinate.ordinal,),
                    ).fetchone()
                    current = self._state(item.coordinate, row)
                    validate_execution_transition(
                        current.execution_state, item.execution_state
                    )
                    validate_retrieval_transition(
                        current.retrieval_state, item.retrieval_state
                    )
                    connection.execute(
                        "INSERT INTO task_state VALUES (?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(ordinal) DO UPDATE SET "
                        "execution_state=excluded.execution_state, "
                        "retrieval_state=excluded.retrieval_state, "
                        "scheduler_id=excluded.scheduler_id, "
                        "native_state=excluded.native_state, "
                        "exit_code=excluded.exit_code, attempt=excluded.attempt",
                        (
                            item.coordinate.ordinal,
                            item.execution_state.value,
                            item.retrieval_state.value,
                            item.scheduler_id,
                            item.native_state,
                            item.exit_code,
                            item.attempt,
                        ),
                    )
        except (sqlite3.Error, ValueError) as error:
            raise RunStoreError(
                f"Could not update Task state for Run {run_id}: {error}"
            ) from error

    def counts(self, run_id: RunId) -> TaskStateCounts:
        task_space = self.task_space(run_id)
        with self._open_existing(run_id) as connection:
            stored = connection.execute("SELECT COUNT(*) FROM task_state").fetchone()[0]
            execution_rows = connection.execute(
                "SELECT execution_state, COUNT(*) FROM task_state GROUP BY execution_state"
            )
            retrieval_rows = connection.execute(
                "SELECT retrieval_state, COUNT(*) FROM task_state GROUP BY retrieval_state"
            )
            execution = {state: 0 for state in ExecutionState}
            retrieval = {state: 0 for state in RetrievalState}
            for value, count in execution_rows:
                execution[ExecutionState(value)] = count
            for value, count in retrieval_rows:
                retrieval[RetrievalState(value)] = count
        implicit = task_space.task_count - stored
        execution[ExecutionState.CREATED] += implicit
        retrieval[RetrievalState.NOT_REQUESTED] += implicit
        return TaskStateCounts(task_space.task_count, execution, retrieval)

    def _open_existing(self, run_id: RunId) -> sqlite3.Connection:
        path = self.path(run_id)
        if not path.is_file() or path.is_symlink():
            raise RunNotFoundError(f"Task state for Run {run_id} was not found")
        return self._connect(path)

    @staticmethod
    def _connect(path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(path, timeout=30.0)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _ensure_root(self) -> None:
        try:
            self._root.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise RunStoreError(f"Could not create Task state root: {error}") from error
        if not self._root.is_dir():
            raise RunStoreError(f"Task state root is not a directory: {self._root}")

    @staticmethod
    def _state(coordinate: TaskCoordinate, row: tuple[object, ...] | None) -> TaskState:
        if row is None:
            return TaskState(coordinate)
        try:
            return TaskState(
                coordinate,
                ExecutionState(cast(str, row[0])),
                RetrievalState(cast(str, row[1])),
                cast(str | None, row[2]),
                cast(str | None, row[3]),
                cast(int | None, row[4]),
                cast(int, row[5]),
            )
        except (TypeError, ValueError) as error:
            raise RunStoreError(
                f"Task state row {coordinate.ordinal} is invalid: {error}"
            ) from error

    @staticmethod
    def _validate_identity(run_id: RunId, task_space: TaskSpace) -> None:
        if type(run_id) is not RunId:
            raise TypeError("Task state requires a RunId")
        if type(task_space) is not TaskSpace:
            raise TypeError("Task state requires a TaskSpace")
