from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import cast

from rundra.domain.models import RunId, TaskId
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
                    CREATE TABLE IF NOT EXISTS submission_job (
                        position INTEGER PRIMARY KEY,
                        native_id TEXT NOT NULL UNIQUE
                    );
                    CREATE TABLE IF NOT EXISTS result_shard (
                        ordinal INTEGER PRIMARY KEY,
                        shard_name TEXT NOT NULL,
                        exit_code INTEGER NOT NULL
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

    def initialize_submission(
        self,
        run_id: RunId,
        scheduler_ids: Mapping[TaskId, str],
        *,
        scheduler_job_ids: Sequence[str] = (),
    ) -> None:
        """Persist the accepted scheduler identity for every compact Task."""

        if not isinstance(scheduler_ids, Mapping):
            raise TypeError("Compact submission identities must be a mapping")
        task_space = self.task_space(run_id)
        if len(scheduler_ids) != task_space.task_count:
            raise RunStoreError(
                f"Compact submission for Run {run_id} does not map every Task"
            )
        jobs = tuple(scheduler_job_ids)
        if any(
            type(native_id) is not str or not native_id.strip() or "\x00" in native_id
            for native_id in jobs
        ) or len(set(jobs)) != len(jobs):
            raise RunStoreError(
                f"Compact submission for Run {run_id} has invalid root identities"
            )
        rows: list[tuple[object, ...]] = []
        for ordinal in range(task_space.task_count):
            coordinate = task_space.coordinate(ordinal)
            native_id = scheduler_ids.get(coordinate.task_id)
            if (
                type(native_id) is not str
                or not native_id.strip()
                or "\x00" in native_id
            ):
                raise RunStoreError(
                    f"Compact submission for Run {run_id} has an invalid Task identity"
                )
            rows.append(
                (
                    ordinal,
                    ExecutionState.SUBMITTED.value,
                    RetrievalState.NOT_REQUESTED.value,
                    native_id,
                    "SUBMITTED",
                    None,
                    0,
                )
            )
        try:
            with self._open_existing(run_id) as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT COUNT(*) FROM task_state"
                ).fetchone()[0]
                existing_jobs = connection.execute(
                    "SELECT COUNT(*) FROM submission_job"
                ).fetchone()[0]
                if existing or existing_jobs:
                    raise RunStoreError(
                        f"Compact submission for Run {run_id} is already initialized"
                    )
                connection.executemany(
                    "INSERT INTO task_state VALUES (?, ?, ?, ?, ?, ?, ?)", rows
                )
                connection.executemany(
                    "INSERT INTO submission_job VALUES (?, ?)", enumerate(jobs)
                )
        except sqlite3.Error as error:
            raise RunStoreError(
                f"Could not initialize compact submission for Run {run_id}: {error}"
            ) from error

    def submission_job_ids(self, run_id: RunId) -> tuple[str, ...]:
        """Return compact root scheduler identities in submission order."""

        with self._open_existing(run_id) as connection:
            return tuple(
                row[0]
                for row in connection.execute(
                    "SELECT native_id FROM submission_job ORDER BY position"
                )
            )

    def initialize_compact_submission(
        self,
        run_id: RunId,
        worker_native_ids: Sequence[str],
        *,
        scheduler_job_ids: Sequence[str],
    ) -> None:
        """Initialize Tasks from one bounded ordinal-modulo-worker assignment."""

        workers = tuple(worker_native_ids)
        jobs = tuple(scheduler_job_ids)
        for values, kind in ((workers, "worker"), (jobs, "root")):
            if (
                not values
                or any(
                    type(value) is not str or not value.strip() or "\x00" in value
                    for value in values
                )
                or len(set(values)) != len(values)
            ):
                raise RunStoreError(
                    f"Compact submission for Run {run_id} has invalid {kind} identities"
                )
        task_space = self.task_space(run_id)
        try:
            with self._open_existing(run_id) as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT COUNT(*) FROM task_state"
                ).fetchone()[0]
                existing_jobs = connection.execute(
                    "SELECT COUNT(*) FROM submission_job"
                ).fetchone()[0]
                if existing or existing_jobs:
                    raise RunStoreError(
                        f"Compact submission for Run {run_id} is already initialized"
                    )
                connection.executemany(
                    "INSERT INTO task_state VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        (
                            ordinal,
                            ExecutionState.SUBMITTED.value,
                            RetrievalState.NOT_REQUESTED.value,
                            workers[ordinal % len(workers)],
                            "SUBMITTED",
                            None,
                            0,
                        )
                        for ordinal in range(task_space.task_count)
                    ),
                )
                connection.executemany(
                    "INSERT INTO submission_job VALUES (?, ?)", enumerate(jobs)
                )
        except sqlite3.Error as error:
            raise RunStoreError(
                f"Could not initialize compact submission for Run {run_id}: {error}"
            ) from error

    def all_states(self, run_id: RunId) -> tuple[TaskState, ...]:
        """Return every explicitly initialized compact Task state."""

        task_space = self.task_space(run_id)
        with self._open_existing(run_id) as connection:
            rows = tuple(
                connection.execute(
                    "SELECT ordinal, execution_state, retrieval_state, scheduler_id, "
                    "native_state, exit_code, attempt FROM task_state ORDER BY ordinal"
                )
            )
        if len(rows) != task_space.task_count:
            raise RunStoreError(
                f"Compact Task state for Run {run_id} is not fully initialized"
            )
        return tuple(
            self._state(task_space.coordinate(cast(int, row[0])), row[1:])
            for row in rows
        )

    def set_all_retrieval(self, run_id: RunId, target: RetrievalState) -> None:
        """Transition retrieval state for every initialized compact Task."""

        if type(target) is not RetrievalState:
            raise TypeError("Compact retrieval target must be a RetrievalState")
        try:
            with self._open_existing(run_id) as connection:
                current_values = tuple(
                    RetrievalState(row[0])
                    for row in connection.execute(
                        "SELECT DISTINCT retrieval_state FROM task_state"
                    )
                )
                if not current_values:
                    raise RunStoreError(
                        f"Compact Task state for Run {run_id} is not initialized"
                    )
                for current in current_values:
                    validate_retrieval_transition(current, target)
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE task_state SET retrieval_state = ?", (target.value,)
                )
        except (sqlite3.Error, ValueError) as error:
            raise RunStoreError(
                f"Could not update compact retrieval for Run {run_id}: {error}"
            ) from error

    def prepare_all_retrieval(self, run_id: RunId) -> int:
        """Mark every non-retrieved compact Task pending without expanding it."""

        try:
            with self._open_existing(run_id) as connection:
                current_values = tuple(
                    RetrievalState(row[0])
                    for row in connection.execute(
                        "SELECT DISTINCT retrieval_state FROM task_state "
                        "WHERE retrieval_state != ?",
                        (RetrievalState.SUCCEEDED.value,),
                    )
                )
                for current in current_values:
                    validate_retrieval_transition(current, RetrievalState.PENDING)
                connection.execute("BEGIN IMMEDIATE")
                cursor = connection.execute(
                    "UPDATE task_state SET retrieval_state = ? "
                    "WHERE retrieval_state != ?",
                    (
                        RetrievalState.PENDING.value,
                        RetrievalState.SUCCEEDED.value,
                    ),
                )
                return cursor.rowcount
        except (sqlite3.Error, ValueError) as error:
            raise RunStoreError(
                f"Could not prepare compact retrieval for Run {run_id}: {error}"
            ) from error

    def fail_pending_retrieval(self, run_id: RunId) -> int:
        """Fail only compact Tasks left pending by an unsuccessful ingestion."""

        try:
            with self._open_existing(run_id) as connection:
                connection.execute("BEGIN IMMEDIATE")
                cursor = connection.execute(
                    "UPDATE task_state SET retrieval_state = ? "
                    "WHERE retrieval_state = ?",
                    (RetrievalState.FAILED.value, RetrievalState.PENDING.value),
                )
                return cursor.rowcount
        except sqlite3.Error as error:
            raise RunStoreError(
                f"Could not fail compact retrieval for Run {run_id}: {error}"
            ) from error

    def ingest_result_shards(
        self,
        run_id: RunId,
        shards: Iterable[tuple[str, Mapping[TaskId, int]]],
        *,
        selected: Sequence[TaskId] | None = None,
    ) -> int:
        """Atomically validate shard coverage and mark proven Tasks retrieved."""

        if not isinstance(shards, Iterable):
            raise TypeError("Result shard ingestion requires an iterable")
        requested = None if selected is None else tuple(selected)
        if requested is not None and (
            not requested
            or any(type(task_id) is not TaskId for task_id in requested)
            or len(set(requested)) != len(requested)
        ):
            raise ValueError("Selected shard Tasks must be nonempty and unique")
        task_space = self.task_space(run_id)
        requested_ordinals = (
            None
            if requested is None
            else {
                self._task_ordinal(task_id, task_space.task_count)
                for task_id in requested
            }
        )
        try:
            with self._open_existing(run_id) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS result_shard ("
                    "ordinal INTEGER PRIMARY KEY, "
                    "shard_name TEXT NOT NULL, exit_code INTEGER NOT NULL)"
                )
                for shard_name, task_exit_codes in shards:
                    if (
                        type(shard_name) is not str
                        or not shard_name
                        or "/" in shard_name
                        or "\\" in shard_name
                        or "\x00" in shard_name
                        or not isinstance(task_exit_codes, Mapping)
                    ):
                        raise RunStoreError("Result shard identity is invalid")
                    for task_id, exit_code in task_exit_codes.items():
                        if type(task_id) is not TaskId or type(exit_code) is not int:
                            raise RunStoreError("Result shard Task outcome is invalid")
                        ordinal = self._task_ordinal(task_id, task_space.task_count)
                        if (
                            requested_ordinals is not None
                            and ordinal not in requested_ordinals
                        ):
                            continue
                        row = connection.execute(
                            "SELECT execution_state, retrieval_state, exit_code "
                            "FROM task_state WHERE ordinal = ?",
                            (ordinal,),
                        ).fetchone()
                        if row is None:
                            raise RunStoreError(
                                f"Result shard Task {task_id} has no durable state"
                            )
                        execution = ExecutionState(cast(str, row[0]))
                        retrieval = RetrievalState(cast(str, row[1]))
                        durable_exit = cast(int | None, row[2])
                        if (
                            execution
                            not in {ExecutionState.SUCCEEDED, ExecutionState.FAILED}
                            or durable_exit != exit_code
                        ):
                            raise RunStoreError(
                                f"Result shard outcome disagrees with Task {task_id}"
                            )
                        existing = connection.execute(
                            "SELECT shard_name, exit_code FROM result_shard "
                            "WHERE ordinal = ?",
                            (ordinal,),
                        ).fetchone()
                        if existing is not None and existing != (
                            shard_name,
                            exit_code,
                        ):
                            raise RunStoreError(
                                f"Result shard Task {task_id} has duplicate coverage"
                            )
                        validate_retrieval_transition(
                            retrieval, RetrievalState.SUCCEEDED
                        )
                        connection.execute(
                            "INSERT OR IGNORE INTO result_shard VALUES (?, ?, ?)",
                            (ordinal, shard_name, exit_code),
                        )
                        connection.execute(
                            "UPDATE task_state SET retrieval_state = ? "
                            "WHERE ordinal = ?",
                            (RetrievalState.SUCCEEDED.value, ordinal),
                        )
                if requested_ordinals is None:
                    covered = cast(
                        int,
                        connection.execute(
                            "SELECT COUNT(*) FROM result_shard"
                        ).fetchone()[0],
                    )
                    expected = task_space.task_count
                else:
                    connection.execute(
                        "CREATE TEMP TABLE requested_ordinal ("
                        "ordinal INTEGER PRIMARY KEY)"
                    )
                    connection.executemany(
                        "INSERT INTO requested_ordinal VALUES (?)",
                        ((ordinal,) for ordinal in requested_ordinals),
                    )
                    covered = cast(
                        int,
                        connection.execute(
                            "SELECT COUNT(*) FROM result_shard "
                            "INNER JOIN requested_ordinal USING (ordinal)"
                        ).fetchone()[0],
                    )
                    expected = len(requested_ordinals)
                if covered != expected:
                    raise RunStoreError(
                        f"Result shards cover {covered} of {expected} requested Tasks"
                    )
                return covered
        except (sqlite3.Error, ValueError) as error:
            raise RunStoreError(
                f"Could not ingest result shards for Run {run_id}: {error}"
            ) from error

    def set_retrieval(
        self,
        run_id: RunId,
        task_ids: Sequence[TaskId],
        target: RetrievalState,
    ) -> None:
        """Transition retrieval state for selected compact Tasks."""

        if not isinstance(task_ids, Sequence) or isinstance(task_ids, (str, bytes)):
            raise TypeError("Compact retrieval Task IDs must be a sequence")
        selected = tuple(task_ids)
        if not selected or len(set(selected)) != len(selected):
            raise ValueError("Compact retrieval Task IDs must be nonempty and unique")
        if type(target) is not RetrievalState:
            raise TypeError("Compact retrieval target must be a RetrievalState")
        states = self.all_states(run_id)
        by_id = {state.coordinate.task_id: state for state in states}
        try:
            updates = tuple(
                replace(state, retrieval_state=target)
                for task_id in selected
                if (state := by_id[task_id])
            )
        except KeyError as error:
            raise ValueError(
                f"Unknown compact retrieval Task: {error.args[0]}"
            ) from error
        self.update_batch(run_id, updates)

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
    def _task_ordinal(task_id: TaskId, task_count: int) -> int:
        try:
            ordinal = int(task_id.value.removeprefix("task_"))
        except ValueError as error:
            raise RunStoreError(f"Invalid compact Task identity: {task_id}") from error
        if not 0 <= ordinal < task_count or TaskId.from_ordinal(ordinal) != task_id:
            raise RunStoreError(f"Compact Task is outside its TaskSpace: {task_id}")
        return ordinal

    @staticmethod
    def _validate_identity(run_id: RunId, task_space: TaskSpace) -> None:
        if type(run_id) is not RunId:
            raise TypeError("Task state requires a RunId")
        if type(task_space) is not TaskSpace:
            raise TypeError("Task state requires a TaskSpace")
