from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path

from rundra.domain.models import RunId, Task
from rundra.domain.records import RunRecord
from rundra.domain.states import (
    validate_execution_transition,
    validate_retrieval_transition,
)
from rundra.persistence.errors import (
    RunAlreadyExistsError,
    RunNotFoundError,
    RunRecordFormatError,
    RunStoreError,
)
from rundra.persistence.serialization import record_from_dict, record_to_dict


class JsonRunStore:
    """One atomic, versioned JSON record per Run under a configurable root."""

    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise TypeError("JsonRunStore root must be a Path")
        self._root = root

    def create(self, record: RunRecord) -> None:
        """Atomically create a record without replacing an existing Run."""
        self._require_record(record)
        self._ensure_root()
        destination = self._record_path(record.run.id)
        temporary = self._write_temporary(record)
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise RunAlreadyExistsError(
                f"Run {record.run.id} already exists in {self._root}"
            ) from error
        except OSError as error:
            raise RunStoreError(
                f"Could not atomically create Run {record.run.id}: {error}"
            ) from error
        finally:
            temporary.unlink(missing_ok=True)
        self._sync_root()

    def load(self, run_id: RunId) -> RunRecord:
        """Load and strictly validate one persisted record."""
        if type(run_id) is not RunId:
            raise TypeError("JsonRunStore.load requires a RunId")
        path = self._record_path(run_id)
        if not path.is_file():
            raise RunNotFoundError(f"Run {run_id} was not found in {self._root}")
        record = self._load_path(path)
        if record.run.id != run_id:
            raise RunRecordFormatError(
                f"Record {path} contains Run ID {record.run.id}, expected {run_id}"
            )
        return record

    def update(self, record: RunRecord) -> None:
        """Validate lifecycle changes and atomically replace a stored record."""
        self._require_record(record)
        previous = self.load(record.run.id)
        self._validate_update(previous, record)
        temporary = self._write_temporary(record)
        destination = self._record_path(record.run.id)
        try:
            os.replace(temporary, destination)
        except OSError as error:
            raise RunStoreError(
                f"Could not perform atomic update for Run {record.run.id}: {error}"
            ) from error
        finally:
            temporary.unlink(missing_ok=True)
        self._sync_root()

    def list(self) -> tuple[RunRecord, ...]:
        """Return all records in deterministic Run-ID order."""
        if not self._root.exists():
            return ()
        if not self._root.is_dir():
            raise RunStoreError(f"Run store root is not a directory: {self._root}")
        records: list[RunRecord] = []
        for path in sorted(self._root.glob("run_*.json")):
            record = self._load_path(path)
            if path.stem != str(record.run.id):
                raise RunRecordFormatError(
                    f"Record {path} contains mismatched Run ID {record.run.id}"
                )
            records.append(record)
        return tuple(records)

    def _ensure_root(self) -> None:
        try:
            self._root.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise RunStoreError(
                f"Could not create Run store root {self._root}: {error}"
            ) from error
        if not self._root.is_dir():
            raise RunStoreError(f"Run store root is not a directory: {self._root}")

    def _record_path(self, run_id: RunId) -> Path:
        return self._root / f"{run_id}.json"

    def _write_temporary(self, record: RunRecord) -> Path:
        document = record_to_dict(record)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                prefix=f".{record.run.id}.",
                suffix=".tmp",
                dir=self._root,
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(
                    document,
                    stream,
                    allow_nan=False,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        except (OSError, TypeError, ValueError) as error:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise RunStoreError(
                f"Could not write temporary record for Run {record.run.id}: {error}"
            ) from error
        if temporary is None:
            raise RunStoreError(
                f"Could not create temporary record for Run {record.run.id}"
            )
        return temporary

    def _load_path(self, path: Path) -> RunRecord:
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as error:
            raise RunStoreError(f"Could not read Run record {path}: {error}") from error
        try:
            value: object = json.loads(
                content,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_nonfinite,
            )
        except (json.JSONDecodeError, RunRecordFormatError) as error:
            raise RunRecordFormatError(f"Invalid Run record {path}: {error}") from error
        try:
            return record_from_dict(value)
        except RunRecordFormatError as error:
            raise RunRecordFormatError(f"Invalid Run record {path}: {error}") from error

    def _validate_update(self, previous: RunRecord, current: RunRecord) -> None:
        if _immutable_definition(previous) != _immutable_definition(current):
            raise RunStoreError(
                f"Run {current.run.id} update changes its immutable run definition"
            )
        try:
            validate_execution_transition(previous.run.state, current.run.state)
            validate_retrieval_transition(
                previous.run.retrieval_state,
                current.run.retrieval_state,
            )
            previous_tasks = {task.id: task for task in previous.run.tasks}
            for task in current.run.tasks:
                validate_execution_transition(previous_tasks[task.id].state, task.state)
        except ValueError as error:
            raise RunStoreError(str(error)) from error

    def _sync_root(self) -> None:
        try:
            descriptor = os.open(self._root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            raise RunStoreError(
                f"Could not synchronize Run store root {self._root}: {error}"
            ) from error

    @staticmethod
    def _require_record(record: RunRecord) -> None:
        if type(record) is not RunRecord:
            raise TypeError("JsonRunStore requires a RunRecord")


def _task_definition(task: Task) -> tuple[object, ...]:
    return (
        task.id,
        task.run_id,
        task.experiment_name,
        task.config,
        task.seed,
        task.resources,
    )


def _immutable_definition(record: RunRecord) -> tuple[object, ...]:
    return (
        record.format_version,
        record.framework_version,
        record.run.id,
        record.run.experiment_name,
        record.run.target,
        record.run.created_at,
        tuple(_task_definition(task) for task in record.run.tasks),
        record.experiment,
        record.source_root,
        record.experiment_source,
        record.task_array_mapping,
    )


def _unique_object(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RunRecordFormatError(f"duplicate field {key!r}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> object:
    raise RunRecordFormatError(f"non-finite JSON value {value!r} is not supported")
