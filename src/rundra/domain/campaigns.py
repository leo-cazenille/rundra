from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import PurePath
from uuid import uuid4

from rundra.domain.models import RunId
from rundra.domain.placement import PlacementDecision

_CAMPAIGN_ID_PATTERN = re.compile(r"campaign_[0-9a-f]{32}\Z")
_LAUNCH_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")


@dataclass(frozen=True, slots=True)
class CampaignId:
    value: str

    def __post_init__(self) -> None:
        if type(self.value) is not str:
            raise TypeError("CampaignId value must be a string")
        if _CAMPAIGN_ID_PATTERN.fullmatch(self.value) is None:
            raise ValueError(
                "Campaign ID must match 'campaign_' followed by 32 lowercase hex digits"
            )

    @classmethod
    def new(cls) -> CampaignId:
        return cls(f"campaign_{uuid4().hex}")

    def __str__(self) -> str:
        return self.value


class CampaignFailurePolicy(StrEnum):
    CANCEL = "cancel"
    STOP = "stop"
    CONTINUE = "continue"


class CampaignSubmissionState(StrEnum):
    PENDING = "PENDING"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    CANCELLED = "CANCELLED"
    NOT_ATTEMPTED = "NOT_ATTEMPTED"


@dataclass(frozen=True, slots=True)
class CampaignLaunchRecord:
    name: str
    run_id: RunId
    target: str
    task_count: int
    destination: PurePath
    submission_state: CampaignSubmissionState = CampaignSubmissionState.PENDING

    def __post_init__(self) -> None:
        if (
            type(self.name) is not str
            or _LAUNCH_NAME_PATTERN.fullmatch(self.name) is None
        ):
            raise ValueError("Campaign launch name is not filesystem-safe")
        if type(self.run_id) is not RunId:
            raise TypeError("Campaign launch run_id must be a RunId")
        if type(self.target) is not str or not self.target.strip():
            raise ValueError("Campaign launch target must be nonblank")
        if type(self.task_count) is not int or self.task_count < 1:
            raise ValueError("Campaign launch task_count must be positive")
        if (
            not isinstance(self.destination, PurePath)
            or not self.destination.is_absolute()
        ):
            raise ValueError("Campaign launch destination must be absolute")
        if type(self.submission_state) is not CampaignSubmissionState:
            raise TypeError("Campaign launch submission_state is invalid")


@dataclass(frozen=True, slots=True)
class CampaignRecord:
    format_version: int
    framework_version: str
    id: CampaignId
    name: str
    source: PurePath
    experiment_source: PurePath
    created_at: datetime
    on_submit_failure: CampaignFailurePolicy
    allow_duplicate_tasks: bool
    launches: tuple[CampaignLaunchRecord, ...]
    submitted_at: datetime | None = None
    completed_at: datetime | None = None
    placement: PlacementDecision | None = None

    def __post_init__(self) -> None:
        if self.format_version not in {1, 2}:
            raise ValueError("CampaignRecord format_version must be 1 or 2")
        if (
            type(self.framework_version) is not str
            or not self.framework_version.strip()
        ):
            raise ValueError("CampaignRecord framework_version must be nonblank")
        if type(self.id) is not CampaignId:
            raise TypeError("CampaignRecord id must be a CampaignId")
        if type(self.name) is not str or not self.name.strip():
            raise ValueError("CampaignRecord name must be nonblank")
        for name, path_value in (
            ("source", self.source),
            ("experiment_source", self.experiment_source),
        ):
            if not isinstance(path_value, PurePath) or not path_value.is_absolute():
                raise ValueError(f"CampaignRecord {name} must be absolute")
        if (
            not isinstance(self.created_at, datetime)
            or self.created_at.utcoffset() is None
        ):
            raise ValueError("CampaignRecord created_at must be timezone-aware")
        for name, timestamp in (
            ("submitted_at", self.submitted_at),
            ("completed_at", self.completed_at),
        ):
            if timestamp is not None and (
                not isinstance(timestamp, datetime) or timestamp.utcoffset() is None
            ):
                raise ValueError(
                    f"CampaignRecord {name} must be timezone-aware or None"
                )
        if type(self.on_submit_failure) is not CampaignFailurePolicy:
            raise TypeError("CampaignRecord failure policy is invalid")
        if type(self.allow_duplicate_tasks) is not bool:
            raise TypeError("CampaignRecord allow_duplicate_tasks must be bool")
        if not isinstance(self.launches, Sequence) or isinstance(
            self.launches, (str, bytes)
        ):
            raise TypeError("CampaignRecord launches must be a sequence")
        launches = tuple(self.launches)
        if not launches or any(
            type(item) is not CampaignLaunchRecord for item in launches
        ):
            raise ValueError("CampaignRecord requires campaign launch records")
        if len({item.name for item in launches}) != len(launches):
            raise ValueError("CampaignRecord launch names must be unique")
        if len({item.run_id for item in launches}) != len(launches):
            raise ValueError("CampaignRecord child Run IDs must be unique")
        if len({str(item.destination) for item in launches}) != len(launches):
            raise ValueError("CampaignRecord destinations must be unique")
        object.__setattr__(self, "launches", launches)
        if self.placement is not None and type(self.placement) is not PlacementDecision:
            raise TypeError("CampaignRecord placement is invalid")
        if self.format_version == 1 and self.placement is not None:
            raise ValueError("CampaignRecord v1 cannot preserve placement")


def valid_campaign_launch_name(value: str) -> bool:
    return type(value) is str and _LAUNCH_NAME_PATTERN.fullmatch(value) is not None
