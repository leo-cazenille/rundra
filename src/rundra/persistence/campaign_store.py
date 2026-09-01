from __future__ import annotations

import fcntl
import json
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import cast

from rundra.domain.campaigns import (
    CampaignFailurePolicy,
    CampaignId,
    CampaignLaunchRecord,
    CampaignRecord,
    CampaignSubmissionState,
)
from rundra.domain.models import RunId
from rundra.persistence.errors import (
    CampaignAlreadyExistsError,
    CampaignNotFoundError,
    CampaignRecordFormatError,
    CampaignStoreConflictError,
    RunStoreError,
)
from rundra.security import is_credential_field


class JsonCampaignStore:
    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise TypeError("JsonCampaignStore root must be a Path")
        self._root = root

    def create(self, record: CampaignRecord) -> None:
        self._require(record)
        self._ensure_root()
        temporary = self._write_temporary(record)
        try:
            os.link(temporary, self._path(record.id))
        except FileExistsError as error:
            raise CampaignAlreadyExistsError(
                f"Campaign {record.id} already exists"
            ) from error
        finally:
            temporary.unlink(missing_ok=True)
        self._sync_root()

    def load(self, campaign_id: CampaignId) -> CampaignRecord:
        if type(campaign_id) is not CampaignId:
            raise TypeError("JsonCampaignStore.load requires a CampaignId")
        path = self._path(campaign_id)
        if not path.is_file():
            raise CampaignNotFoundError(
                f"Campaign {campaign_id} was not found in {self._root}"
            )
        record = campaign_record_from_dict(_read_document(path))
        if record.id != campaign_id:
            raise CampaignRecordFormatError(
                f"Campaign record {path} has a mismatched ID"
            )
        return record

    def update(self, record: CampaignRecord, *, expected: CampaignRecord) -> None:
        self._require(record)
        self._require(expected)
        if record.id != expected.id:
            raise ValueError("Campaign IDs must match")
        with self.operation_lock(record.id):
            previous = self.load(record.id)
            if previous == record:
                return
            if previous != expected:
                raise CampaignStoreConflictError(
                    f"Campaign {record.id} changed since it was loaded"
                )
            temporary = self._write_temporary(record)
            try:
                os.replace(temporary, self._path(record.id))
            finally:
                temporary.unlink(missing_ok=True)
            self._sync_root()

    def list(self) -> tuple[CampaignRecord, ...]:
        if not self._root.exists():
            return ()
        return tuple(
            campaign_record_from_dict(_read_document(path))
            for path in sorted(self._root.glob("campaign_*.json"))
        )

    def delete(self, campaign_id: CampaignId) -> None:
        with self.operation_lock(campaign_id):
            path = self._path(campaign_id)
            if not path.exists():
                raise CampaignNotFoundError(f"Campaign {campaign_id} was not found")
            path.unlink()
            self._sync_root()

    @contextmanager
    def operation_lock(self, campaign_id: CampaignId) -> Iterator[None]:
        self._ensure_root()
        path = self._root / f".{campaign_id}.lock"
        descriptor = os.open(
            path, os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0), 0o600
        )
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise RunStoreError("Campaign lock is not a regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _path(self, campaign_id: CampaignId) -> Path:
        return self._root / f"{campaign_id}.json"

    def _ensure_root(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if not self._root.is_dir():
            raise RunStoreError(f"Campaign store root is not a directory: {self._root}")

    def _write_temporary(self, record: CampaignRecord) -> Path:
        document = campaign_record_to_dict(record)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=f".{record.id}.",
            suffix=".tmp",
            dir=self._root,
            delete=False,
        ) as stream:
            json.dump(document, stream, allow_nan=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            return Path(stream.name)

    def _sync_root(self) -> None:
        descriptor = os.open(self._root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _require(record: CampaignRecord) -> None:
        if type(record) is not CampaignRecord:
            raise TypeError("Campaign store requires CampaignRecord")


def campaign_record_to_dict(record: CampaignRecord) -> dict[str, object]:
    if type(record) is not CampaignRecord:
        raise TypeError("campaign_record_to_dict requires CampaignRecord")
    return {
        "format_version": record.format_version,
        "framework_version": record.framework_version,
        "id": str(record.id),
        "name": record.name,
        "source": str(record.source),
        "experiment_source": str(record.experiment_source),
        "created_at": record.created_at.isoformat(),
        "submitted_at": record.submitted_at.isoformat()
        if record.submitted_at
        else None,
        "completed_at": record.completed_at.isoformat()
        if record.completed_at
        else None,
        "on_submit_failure": record.on_submit_failure.value,
        "allow_duplicate_tasks": record.allow_duplicate_tasks,
        "launches": [
            {
                "name": item.name,
                "run_id": str(item.run_id),
                "target": item.target,
                "task_count": item.task_count,
                "destination": str(item.destination),
                "submission_state": item.submission_state.value,
            }
            for item in record.launches
        ],
    }


def campaign_record_from_dict(value: object) -> CampaignRecord:
    if type(value) is not dict:
        raise CampaignRecordFormatError("Campaign record must be an object")
    document = value
    expected = {
        "format_version",
        "framework_version",
        "id",
        "name",
        "source",
        "experiment_source",
        "created_at",
        "submitted_at",
        "completed_at",
        "on_submit_failure",
        "allow_duplicate_tasks",
        "launches",
    }
    if set(document) != expected or _contains_credentials(document):
        raise CampaignRecordFormatError("Campaign record fields are invalid")
    try:
        launches_value = document["launches"]
        if type(launches_value) is not list:
            raise TypeError
        launches = tuple(
            CampaignLaunchRecord(
                name=_string(item, "name"),
                run_id=RunId(_string(item, "run_id")),
                target=_string(item, "target"),
                task_count=_integer(item, "task_count"),
                destination=PurePosixPath(_string(item, "destination")),
                submission_state=CampaignSubmissionState(
                    _string(item, "submission_state")
                ),
            )
            for item in launches_value
        )
        return CampaignRecord(
            format_version=_integer(document, "format_version"),
            framework_version=_string(document, "framework_version"),
            id=CampaignId(_string(document, "id")),
            name=_string(document, "name"),
            source=PurePosixPath(_string(document, "source")),
            experiment_source=PurePosixPath(_string(document, "experiment_source")),
            created_at=datetime.fromisoformat(_string(document, "created_at")),
            submitted_at=_optional_datetime(document["submitted_at"]),
            completed_at=_optional_datetime(document["completed_at"]),
            on_submit_failure=CampaignFailurePolicy(
                _string(document, "on_submit_failure")
            ),
            allow_duplicate_tasks=_boolean(document, "allow_duplicate_tasks"),
            launches=launches,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CampaignRecordFormatError(f"Invalid CampaignRecord: {error}") from error


def _read_document(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique)
    except (OSError, json.JSONDecodeError, CampaignRecordFormatError) as error:
        raise CampaignRecordFormatError(
            f"Invalid campaign record {path}: {error}"
        ) from error


def _unique(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise CampaignRecordFormatError(f"Duplicate CampaignRecord field: {key}")
        result[key] = value
    return result


def _string(value: object, key: str) -> str:
    if type(value) is not dict or type(value.get(key)) is not str:
        raise TypeError(f"{key} must be a string")
    return cast(str, value[key])


def _integer(value: object, key: str) -> int:
    if type(value) is not dict or type(value.get(key)) is not int:
        raise TypeError(f"{key} must be an integer")
    return cast(int, value[key])


def _boolean(value: object, key: str) -> bool:
    if type(value) is not dict or type(value.get(key)) is not bool:
        raise TypeError(f"{key} must be a boolean")
    return cast(bool, value[key])


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if type(value) is not str:
        raise TypeError("timestamp must be a string or null")
    return datetime.fromisoformat(value)


def _contains_credentials(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            is_credential_field(str(key)) or _contains_credentials(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_credentials(item) for item in value)
    return False
