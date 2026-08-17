from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite
from pathlib import PurePath
from types import MappingProxyType

from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import Artifact, ExperimentSpec, NativeValue, Run, TaskId
from rundra.domain.preparation import PreparationRecord
from rundra.domain.states import RetrievalState


def _freeze_strings(value: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{field_name} must be a sequence of strings")
    values = tuple(value)
    if any(type(item) is not str or not item for item in values):
        raise ValueError(f"{field_name} must contain nonempty strings")
    return values


def _optional_string(value: object, *, field_name: str) -> None:
    if value is not None and type(value) is not str:
        raise TypeError(f"{field_name} must be a string or None")


def _optional_nonblank_string(value: object, *, field_name: str) -> None:
    _optional_string(value, field_name=field_name)
    if type(value) is str and not value.strip():
        raise ValueError(f"{field_name} must not be blank")


def _optional_timestamp(value: object, *, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime or None")
    if value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Versioned durable state and available provenance for one logical Run."""

    format_version: int
    framework_version: str
    run: Run
    experiment: ExperimentSpec
    source_root: PurePath
    experiment_source: PurePath | None = None
    initiator: str | None = None
    git_commit: str | None = None
    git_branch: str | None = None
    git_dirty: bool | None = None
    git_diff: str | None = None
    container_digest: str | None = None
    preparation: PreparationRecord | None = None
    scheduler_job_ids: tuple[str, ...] = ()
    allocated_nodes: tuple[str, ...] = ()
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    native_state: str | None = None
    scheduler_metadata: Mapping[str, NativeValue] = field(default_factory=dict)
    task_array_mapping: tuple[ArrayTaskMapping, ...] = ()
    task_scheduler_ids: Mapping[TaskId, str] = field(default_factory=dict)
    task_native_states: Mapping[TaskId, str] = field(default_factory=dict)
    task_retrieval_states: Mapping[TaskId, RetrievalState] = field(default_factory=dict)
    task_exit_codes: Mapping[TaskId, int] = field(default_factory=dict)
    artifacts: tuple[Artifact, ...] = ()

    def __post_init__(self) -> None:
        if type(self.format_version) is not int:
            raise TypeError("RunRecord format_version must be an integer")
        if self.format_version not in {1, 2, 3}:
            raise ValueError("RunRecord format_version must be 1, 2, or 3")
        if type(self.framework_version) is not str:
            raise TypeError("RunRecord framework_version must be a string")
        if not self.framework_version.strip():
            raise ValueError("RunRecord framework_version must not be blank")
        if type(self.run) is not Run:
            raise TypeError("RunRecord run must be a Run")
        if type(self.experiment) is not ExperimentSpec:
            raise TypeError("RunRecord experiment must be an ExperimentSpec")
        if self.experiment.name != self.run.experiment_name:
            raise ValueError("RunRecord experiment must match its Run")
        if self.format_version == 1 and self.preparation is not None:
            raise ValueError("RunRecord v1 cannot contain preparation")
        if self.format_version == 2 and type(self.preparation) is not PreparationRecord:
            raise ValueError("RunRecord v2 requires preparation")
        if self.format_version == 3 and any(
            task.parameter_set is None for task in self.run.tasks
        ):
            raise ValueError("RunRecord v3 requires parameterized Tasks")
        if self.preparation is not None:
            if self.container_digest != self.preparation.image_sha256:
                raise ValueError(
                    "RunRecord container and preparation digests must match"
                )
            if (
                self.experiment.container is None
                or self.experiment.container.image != self.preparation.image_path
            ):
                raise ValueError(
                    "RunRecord experiment must use the prepared image path"
                )
        if not isinstance(self.source_root, PurePath):
            raise TypeError("RunRecord source_root must be a PurePath")
        if self.experiment_source is not None and not isinstance(
            self.experiment_source, PurePath
        ):
            raise TypeError("RunRecord experiment_source must be a PurePath or None")
        for field_name in (
            "initiator",
            "git_commit",
            "git_branch",
            "container_digest",
            "native_state",
        ):
            _optional_nonblank_string(
                getattr(self, field_name),
                field_name=f"RunRecord {field_name}",
            )
        _optional_string(self.git_diff, field_name="RunRecord git_diff")
        if self.git_dirty is not None and type(self.git_dirty) is not bool:
            raise TypeError("RunRecord git_dirty must be a boolean or None")
        object.__setattr__(
            self,
            "scheduler_job_ids",
            _freeze_strings(
                self.scheduler_job_ids,
                field_name="RunRecord scheduler_job_ids",
            ),
        )
        object.__setattr__(
            self,
            "allocated_nodes",
            _freeze_strings(
                self.allocated_nodes,
                field_name="RunRecord allocated_nodes",
            ),
        )
        for field_name in ("submitted_at", "started_at", "completed_at"):
            _optional_timestamp(
                getattr(self, field_name),
                field_name=f"RunRecord {field_name}",
            )
        timestamps = tuple(
            value
            for value in (
                self.run.created_at,
                self.submitted_at,
                self.started_at,
                self.completed_at,
            )
            if value is not None
        )
        if any(
            later < earlier
            for earlier, later in zip(timestamps, timestamps[1:], strict=False)
        ):
            raise ValueError("RunRecord timestamps must be chronological")
        task_ids = {task.id for task in self.run.tasks}
        task_array_mapping = tuple(self.task_array_mapping)
        if any(type(item) is not ArrayTaskMapping for item in task_array_mapping):
            raise TypeError(
                "RunRecord task_array_mapping must contain ArrayTaskMappings"
            )
        if task_array_mapping:
            if self.run.target.scheduler.kind != "slurm":
                raise ValueError("RunRecord task_array_mapping requires a Slurm target")
            expected_mapping = tuple(
                ArrayTaskMapping(task.id, task.seed, index)
                for index, task in enumerate(self.run.tasks)
            )
            if task_array_mapping != expected_mapping:
                raise ValueError(
                    "RunRecord task_array_mapping must match Task order and seeds"
                )
        object.__setattr__(self, "task_array_mapping", task_array_mapping)
        for field_name in ("task_scheduler_ids", "task_native_states"):
            value = getattr(self, field_name)
            if not isinstance(value, Mapping):
                raise TypeError(f"RunRecord {field_name} must be a mapping")
            mapping = dict(value)
            if any(
                type(task_id) is not TaskId
                or type(native_value) is not str
                or not native_value.strip()
                or "\x00" in native_value
                for task_id, native_value in mapping.items()
            ):
                raise TypeError(
                    f"RunRecord {field_name} must map TaskIds to safe strings"
                )
            if not set(mapping).issubset(task_ids):
                raise ValueError(f"RunRecord {field_name} contains an unknown TaskId")
            if field_name == "task_scheduler_ids" and mapping:
                if set(mapping) != task_ids:
                    raise ValueError(
                        "RunRecord task_scheduler_ids must identify every Task"
                    )
                if len(set(mapping.values())) != len(mapping):
                    raise ValueError(
                        "RunRecord task_scheduler_ids must contain unique identities"
                    )
            object.__setattr__(self, field_name, MappingProxyType(mapping))
        if not isinstance(self.task_retrieval_states, Mapping):
            raise TypeError("RunRecord task_retrieval_states must be a mapping")
        retrieval_states = dict(self.task_retrieval_states)
        if any(
            type(task_id) is not TaskId or type(state) is not RetrievalState
            for task_id, state in retrieval_states.items()
        ):
            raise TypeError(
                "RunRecord task_retrieval_states must map TaskIds to RetrievalStates"
            )
        if retrieval_states and set(retrieval_states) != task_ids:
            raise ValueError("RunRecord task_retrieval_states must describe every Task")
        object.__setattr__(
            self,
            "task_retrieval_states",
            MappingProxyType(retrieval_states),
        )
        if not isinstance(self.task_exit_codes, Mapping):
            raise TypeError("RunRecord task_exit_codes must be a mapping")
        exit_codes = dict(self.task_exit_codes)
        if any(
            type(task_id) is not TaskId or type(exit_code) is not int
            for task_id, exit_code in exit_codes.items()
        ):
            raise TypeError("RunRecord task_exit_codes must map TaskIds to integers")
        if not set(exit_codes).issubset(task_ids):
            raise ValueError("RunRecord task_exit_codes contain an unknown TaskId")
        if not isinstance(self.scheduler_metadata, Mapping) or any(
            type(key) is not str
            or not key.strip()
            or "\x00" in key
            or type(value) not in (str, int, float, bool)
            for key, value in self.scheduler_metadata.items()
        ):
            raise TypeError(
                "RunRecord scheduler_metadata must map safe strings to scalar values"
            )
        if any(
            type(value) is float and not isfinite(value)
            for value in self.scheduler_metadata.values()
        ):
            raise ValueError("RunRecord scheduler_metadata floats must be finite")
        object.__setattr__(
            self,
            "scheduler_metadata",
            MappingProxyType(dict(self.scheduler_metadata)),
        )
        object.__setattr__(
            self,
            "task_exit_codes",
            MappingProxyType(exit_codes),
        )
        if not isinstance(self.artifacts, Sequence) or isinstance(
            self.artifacts, (str, bytes)
        ):
            raise TypeError("RunRecord artifacts must be a sequence")
        artifacts = tuple(self.artifacts)
        if any(type(artifact) is not Artifact for artifact in artifacts):
            raise TypeError("RunRecord artifacts must contain only Artifacts")
        if any(
            artifact.task_id is not None and artifact.task_id not in task_ids
            for artifact in artifacts
        ):
            raise ValueError("RunRecord artifacts contain an unknown TaskId")
        object.__setattr__(self, "artifacts", artifacts)
