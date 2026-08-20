from __future__ import annotations

import fcntl
import json
import os
import stat
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
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
    RunStoreConflictError,
    RunStoreError,
)
from rundra.persistence.serialization import record_from_dict, record_to_dict
from rundra.security import is_credential_field


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

    def update(
        self,
        record: RunRecord,
        *,
        expected: RunRecord,
    ) -> None:
        """Validate lifecycle changes and atomically replace a stored record."""
        self._require_record(record)
        self._require_record(expected)
        if expected.run.id != record.run.id:
            raise ValueError("Expected and updated Run IDs must match")
        with self._write_lock(record.run.id):
            previous = self.load(record.run.id)
            if previous == record:
                return
            if previous != expected:
                raise RunStoreConflictError(
                    f"Run {record.run.id} changed since it was loaded"
                )
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

    def compact(self, record: RunRecord, *, expected: RunRecord) -> None:
        """Atomically replace one materialized definition with verified v4 state."""

        self._require_record(record)
        self._require_record(expected)
        if expected.run.id != record.run.id:
            raise ValueError("Expected and compacted Run IDs must match")
        with self._write_lock(record.run.id):
            previous = self.load(record.run.id)
            if previous == record:
                return
            if previous != expected:
                raise RunStoreConflictError(
                    f"Run {record.run.id} changed since it was loaded"
                )
            _validate_compaction(previous, record)
            temporary = self._write_temporary(record)
            destination = self._record_path(record.run.id)
            try:
                os.replace(temporary, destination)
            except OSError as error:
                raise RunStoreError(
                    f"Could not compact Run {record.run.id}: {error}"
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

    @contextmanager
    def operation_lock(self, run_id: RunId) -> Iterator[None]:
        """Serialize result transfer and destructive retention for one Run."""
        if type(run_id) is not RunId:
            raise TypeError("JsonRunStore.operation_lock requires a RunId")
        self._ensure_root()
        lock_path = self._root / f".{run_id}.operation.lock"
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise RunStoreError("Run operation lock is not a regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

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

    @contextmanager
    def _write_lock(self, run_id: RunId) -> Iterator[None]:
        lock_path = self._root / f".{run_id}.lock"
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(lock_path, flags, 0o600)
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError("lock path is not a regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError as error:
            if "descriptor" in locals():
                os.close(descriptor)
            raise RunStoreError(
                f"Could not lock Run {run_id} for update: {error}"
            ) from error
        try:
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def _write_temporary(self, record: RunRecord) -> Path:
        document = record_to_dict(record)
        if _contains_credential_field(document):
            raise RunStoreError(
                f"Run {record.run.id} contains a forbidden credential field"
            )
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
        if _contains_credential_field(value):
            raise RunRecordFormatError(
                f"Invalid Run record {path}: forbidden credential field"
            )
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
            previous_retrieval = previous.task_retrieval_states or {
                task.id: previous.run.retrieval_state for task in previous.run.tasks
            }
            current_retrieval = current.task_retrieval_states or {
                task.id: current.run.retrieval_state for task in current.run.tasks
            }
            for task_id, state in current_retrieval.items():
                validate_retrieval_transition(previous_retrieval[task_id], state)
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
        task.parameter_set,
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
        record.retrieval_destination,
        record.fetch_mode,
        record.experiment_source,
        record.task_array_mapping,
        record.task_space,
        record.execution_strategy,
        record.retrieval_policy,
        record.task_state_store,
    )


def _validate_compaction(previous: RunRecord, current: RunRecord) -> None:
    legacy_transition = (
        previous.format_version in {1, 2, 3} and current.format_version == 4
    )
    canonical_transition = (
        previous.format_version in {5, 6}
        and current.format_version == previous.format_version
        and not previous.is_compact
        and current.is_compact
    )
    if not legacy_transition and not canonical_transition:
        raise RunStoreError(
            "Run compaction must convert v1-v3 to v4 or preserve a canonical v5+ version"
        )
    if current.task_space is None or current.task_space.task_count != len(
        previous.run.tasks
    ):
        raise RunStoreError("Run compaction TaskSpace does not match its Tasks")
    if any(
        current.task_space.coordinate(ordinal).task_id != task.id
        or current.task_space.coordinate(ordinal).seed != task.seed
        for ordinal, task in enumerate(previous.run.tasks)
    ):
        raise RunStoreError("Run compaction changes Task identity or seed order")
    previous_identity = (
        previous.framework_version,
        previous.run.id,
        previous.run.experiment_name,
        previous.run.target,
        previous.run.created_at,
        previous.experiment,
        previous.source_root,
        previous.experiment_source,
        previous.initiator,
        previous.git_commit,
        previous.git_branch,
        previous.git_dirty,
        previous.git_diff,
        previous.container_digest,
        previous.preparation,
        previous.retrieval_destination,
    )
    current_identity = (
        current.framework_version,
        current.run.id,
        current.run.experiment_name,
        current.run.target,
        current.run.created_at,
        current.experiment,
        current.source_root,
        current.experiment_source,
        current.initiator,
        current.git_commit,
        current.git_branch,
        current.git_dirty,
        current.git_diff,
        current.container_digest,
        current.preparation,
        current.retrieval_destination,
    )
    if previous_identity != current_identity:
        raise RunStoreError("Run compaction changes immutable provenance")
    if (
        current.run.state != previous.run.state
        or current.run.retrieval_state != previous.run.retrieval_state
        or current.task_array_mapping
        or current.task_scheduler_ids
        or current.task_native_states
        or current.task_retrieval_states
        or current.task_exit_codes
    ):
        raise RunStoreError("Run compaction contains incompatible lifecycle state")


def _unique_object(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RunRecordFormatError(f"duplicate field {key!r}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> object:
    raise RunRecordFormatError(f"non-finite JSON value {value!r} is not supported")


def _contains_credential_field(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            (type(key) is str and is_credential_field(key))
            or _contains_credential_field(item)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_contains_credential_field(item) for item in value)
    return False
