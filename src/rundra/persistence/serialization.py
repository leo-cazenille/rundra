from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from math import isfinite
from pathlib import PurePosixPath
from typing import NoReturn, cast

from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    BackendConfig,
    Command,
    ConfigSnapshot,
    ContainerSpec,
    ExperimentSpec,
    NativeValue,
    ResourceRequest,
    Run,
    RunId,
    Target,
    Task,
    TaskId,
)
from rundra.domain.parameters import ParameterSet
from rundra.domain.preparation import PreparationRecord, PreparedOutput
from rundra.domain.records import RunRecord
from rundra.domain.scaling import CompactRun, SeedRange, TaskSpace
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.domain.storage import SlurmScratchPolicy
from rundra.persistence.errors import RunRecordFormatError
from rundra.schema_versions import RUN_RECORD_SCHEMA

type JsonObject = dict[str, object]

_RECORD_FIELDS_V1 = frozenset(
    {
        "format_version",
        "framework_version",
        "run",
        "experiment",
        "source_root",
        "experiment_source",
        "initiator",
        "git_commit",
        "git_branch",
        "git_dirty",
        "git_diff",
        "container_digest",
        "scheduler_job_ids",
        "allocated_nodes",
        "submitted_at",
        "started_at",
        "completed_at",
        "native_state",
        "scheduler_metadata",
        "task_array_mapping",
        "task_scheduler_ids",
        "task_native_states",
        "task_retrieval_states",
        "task_exit_codes",
        "artifacts",
    }
)
_RECORD_FIELDS_V2 = _RECORD_FIELDS_V1 | {"preparation"}
_RECORD_FIELDS_V3 = _RECORD_FIELDS_V2
_RECORD_FIELDS_V4 = _RECORD_FIELDS_V2 | {
    "task_space",
    "execution_strategy",
    "retrieval_policy",
    "task_state_store",
}
_RECORD_FIELDS_V5 = _RECORD_FIELDS_V4 | {
    "run_kind",
    "retrieval_destination",
}
_RECORD_FIELDS_V6 = _RECORD_FIELDS_V5 | {"fetch_mode"}
_RECORD_FIELDS_BY_VERSION = {
    1: _RECORD_FIELDS_V1,
    2: _RECORD_FIELDS_V2,
    3: _RECORD_FIELDS_V3,
    4: _RECORD_FIELDS_V4,
    5: _RECORD_FIELDS_V5,
    6: _RECORD_FIELDS_V6,
}


def record_to_dict(record: RunRecord) -> JsonObject:
    """Convert a supported RunRecord into deterministic versioned JSON."""
    if type(record) is not RunRecord:
        raise TypeError("record_to_dict requires a RunRecord")
    document: JsonObject = {
        "format_version": record.format_version,
        "framework_version": record.framework_version,
        "run": _run_to_dict(record.run, version=record.format_version),
        "experiment": _experiment_to_dict(record.experiment),
        "source_root": str(record.source_root),
        "experiment_source": _path_or_none(record.experiment_source),
        "initiator": record.initiator,
        "git_commit": record.git_commit,
        "git_branch": record.git_branch,
        "git_dirty": record.git_dirty,
        "git_diff": record.git_diff,
        "container_digest": record.container_digest,
        "scheduler_job_ids": list(record.scheduler_job_ids),
        "allocated_nodes": list(record.allocated_nodes),
        "submitted_at": _datetime_or_none(record.submitted_at),
        "started_at": _datetime_or_none(record.started_at),
        "completed_at": _datetime_or_none(record.completed_at),
        "native_state": record.native_state,
        "scheduler_metadata": dict(sorted(record.scheduler_metadata.items())),
        "task_array_mapping": [
            {
                "task_id": str(item.task_id),
                "seed": item.seed,
                "array_index": item.array_index,
            }
            for item in record.task_array_mapping
        ],
        "task_scheduler_ids": _task_string_mapping(record.task_scheduler_ids),
        "task_native_states": _task_string_mapping(record.task_native_states),
        "task_retrieval_states": {
            str(task_id): state.value
            for task_id, state in sorted(
                record.task_retrieval_states.items(), key=lambda item: item[0].value
            )
        },
        "task_exit_codes": {
            str(task_id): exit_code
            for task_id, exit_code in sorted(
                record.task_exit_codes.items(), key=lambda item: item[0].value
            )
        },
        "artifacts": [_artifact_to_dict(artifact) for artifact in record.artifacts],
    }
    if record.format_version in {2, 3, 4, 5, 6}:
        document["preparation"] = (
            None
            if record.preparation is None
            else _preparation_to_dict(record.preparation, version=record.format_version)
        )
    if record.format_version in {4, 5, 6}:
        document.update(
            {
                "task_space": (
                    _task_space_to_dict(record.task_space)
                    if record.task_space is not None
                    else None
                ),
                "execution_strategy": record.execution_strategy,
                "retrieval_policy": record.retrieval_policy,
                "task_state_store": (
                    str(record.task_state_store)
                    if record.task_state_store is not None
                    else None
                ),
            }
        )
    if record.format_version in {5, 6}:
        assert record.retrieval_destination is not None
        document.update(
            {
                "run_kind": record.run_kind,
                "retrieval_destination": str(record.retrieval_destination),
            }
        )
    if record.format_version == 6:
        document["fetch_mode"] = record.fetch_mode
    return document


def record_from_dict(value: object) -> RunRecord:
    """Parse a strict supported RunRecord JSON value."""
    document = _object(value, path="record")
    version = document.get("format_version")
    if type(version) is not int:
        raise RunRecordFormatError("record.format_version must be an integer")
    if version not in RUN_RECORD_SCHEMA.supported:
        raise RunRecordFormatError(f"unsupported format_version {version}")
    document.setdefault("task_array_mapping", [])
    document.setdefault("task_scheduler_ids", {})
    document.setdefault("task_native_states", {})
    document.setdefault("task_retrieval_states", {})
    document.setdefault("scheduler_metadata", {})
    _exact_fields(document, _RECORD_FIELDS_BY_VERSION[version], path="record")
    run_kind = (
        _string(document["run_kind"], path="run_kind") if version in {5, 6} else None
    )
    if run_kind not in {None, "materialized", "compact"}:
        raise RunRecordFormatError("run_kind must be materialized or compact")
    run = _parse_run(
        document["run"],
        path="run",
        version=version,
        compact=version == 4 or run_kind == "compact",
    )
    try:
        return RunRecord(
            format_version=version,
            framework_version=_string(
                document["framework_version"], path="framework_version"
            ),
            run=run,
            experiment=_parse_experiment(document["experiment"], path="experiment"),
            source_root=_path(document["source_root"], path="source_root"),
            retrieval_destination=(
                _path(
                    document["retrieval_destination"],
                    path="retrieval_destination",
                )
                if version in {5, 6}
                else None
            ),
            fetch_mode=(
                _string(document["fetch_mode"], path="fetch_mode")
                if version == 6
                else None
            ),
            experiment_source=_optional_path(
                document["experiment_source"], path="experiment_source"
            ),
            initiator=_optional_string(document["initiator"], path="initiator"),
            git_commit=_optional_string(document["git_commit"], path="git_commit"),
            git_branch=_optional_string(document["git_branch"], path="git_branch"),
            git_dirty=_optional_bool(document["git_dirty"], path="git_dirty"),
            git_diff=_optional_string(document["git_diff"], path="git_diff"),
            container_digest=_optional_string(
                document["container_digest"], path="container_digest"
            ),
            preparation=(
                _parse_preparation(document["preparation"], version=version)
                if version in {2, 3, 4, 5, 6} and document["preparation"] is not None
                else None
            ),
            scheduler_job_ids=_string_tuple(
                document["scheduler_job_ids"], path="scheduler_job_ids"
            ),
            allocated_nodes=_string_tuple(
                document["allocated_nodes"], path="allocated_nodes"
            ),
            submitted_at=_optional_datetime(
                document["submitted_at"], path="submitted_at"
            ),
            started_at=_optional_datetime(document["started_at"], path="started_at"),
            completed_at=_optional_datetime(
                document["completed_at"], path="completed_at"
            ),
            native_state=_optional_string(
                document["native_state"], path="native_state"
            ),
            scheduler_metadata=_scalar_mapping(
                document["scheduler_metadata"], path="scheduler_metadata"
            ),
            task_array_mapping=_parse_task_array_mapping(
                document["task_array_mapping"]
            ),
            task_scheduler_ids=_parse_task_string_mapping(
                document["task_scheduler_ids"], path="task_scheduler_ids"
            ),
            task_native_states=_parse_task_string_mapping(
                document["task_native_states"], path="task_native_states"
            ),
            task_retrieval_states=_parse_task_retrieval_states(
                document["task_retrieval_states"]
            ),
            task_exit_codes=_parse_exit_codes(document["task_exit_codes"]),
            artifacts=_parse_artifacts(document["artifacts"]),
            task_space=(
                _parse_task_space(document["task_space"])
                if version in {4, 5, 6} and document["task_space"] is not None
                else None
            ),
            execution_strategy=(
                _string(document["execution_strategy"], path="execution_strategy")
                if version in {4, 5, 6} and document["execution_strategy"] is not None
                else None
            ),
            retrieval_policy=(
                _string(document["retrieval_policy"], path="retrieval_policy")
                if version in {4, 5, 6} and document["retrieval_policy"] is not None
                else None
            ),
            task_state_store=(
                _path(document["task_state_store"], path="task_state_store")
                if version in {4, 5, 6} and document["task_state_store"] is not None
                else None
            ),
        )
    except RunRecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        raise RunRecordFormatError(f"record: {error}") from error


def _path_or_none(value: object) -> str | None:
    return None if value is None else str(value)


def _task_space_to_dict(value: TaskSpace) -> JsonObject:
    return {
        "parameter_set_count": value.parameter_set_count,
        "seeds": {
            "start": value.seeds.start,
            "stop": value.seeds.stop,
            "step": value.seeds.step,
        },
        "task_count": value.task_count,
    }


def _parse_task_space(value: object) -> TaskSpace:
    document = _object(value, path="task_space")
    _exact_fields(
        document,
        frozenset({"parameter_set_count", "seeds", "task_count"}),
        path="task_space",
    )
    seeds = _object(document["seeds"], path="task_space.seeds")
    _exact_fields(
        seeds,
        frozenset({"start", "stop", "step"}),
        path="task_space.seeds",
    )
    try:
        task_space = TaskSpace(
            _integer(
                document["parameter_set_count"], path="task_space.parameter_set_count"
            ),
            SeedRange(
                _integer(seeds["start"], path="task_space.seeds.start"),
                _integer(seeds["stop"], path="task_space.seeds.stop"),
                _integer(seeds["step"], path="task_space.seeds.step"),
            ),
        )
        if (
            _integer(document["task_count"], path="task_space.task_count")
            != task_space.task_count
        ):
            raise ValueError("task_count does not match the TaskSpace product")
        return task_space
    except RunRecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        _invalid("task_space", error)


def _datetime_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _preparation_to_dict(value: PreparationRecord, *, version: int) -> JsonObject:
    document: JsonObject = {
        "source_identity": value.source_identity,
        "source_digest": value.source_digest,
        "source_action": value.source_action,
        "image_uri": value.image_uri,
        "image_sha256": value.image_sha256,
        "image_path": str(value.image_path),
        "image_action": value.image_action,
        "resolution_location": value.resolution_location,
        "build_cache_key": value.build_cache_key,
        "builder_location": value.builder_location,
        "builder_scheduler_id": value.builder_scheduler_id,
        "builder_status": value.builder_status,
        "builder_state": value.builder_state,
        "build_action": value.build_action,
        "build_outputs": [
            {
                "path": str(output.path),
                "sha256": output.sha256,
                "executable": output.executable,
            }
            for output in value.build_outputs
        ],
        "logs": [str(path) for path in value.logs],
    }
    if version >= 6:
        document["image_recipe_key"] = value.image_recipe_key
    return document


def _parse_preparation(value: object, *, version: int) -> PreparationRecord:
    path = "preparation"
    document = _object(value, path=path)
    document.setdefault("builder_status", None)
    document.setdefault("builder_state", None)
    document.setdefault("build_action", None)
    if version >= 6:
        document.setdefault("image_recipe_key", None)
    _exact_fields(
        document,
        frozenset(
            {
                "source_identity",
                "source_digest",
                "source_action",
                "image_uri",
                "image_sha256",
                *(("image_recipe_key",) if version >= 6 else ()),
                "image_path",
                "image_action",
                "resolution_location",
                "build_cache_key",
                "builder_location",
                "builder_scheduler_id",
                "builder_status",
                "builder_state",
                "build_action",
                "build_outputs",
                "logs",
            }
        ),
        path=path,
    )
    outputs: list[PreparedOutput] = []
    for index, raw_output in enumerate(
        _sequence(document["build_outputs"], path=f"{path}.build_outputs")
    ):
        output_path = f"{path}.build_outputs[{index}]"
        output = _object(raw_output, path=output_path)
        _exact_fields(
            output,
            frozenset({"path", "sha256", "executable"}),
            path=output_path,
        )
        outputs.append(
            PreparedOutput(
                path=_path(output["path"], path=f"{output_path}.path"),
                sha256=_string(output["sha256"], path=f"{output_path}.sha256"),
                executable=_boolean(
                    output["executable"], path=f"{output_path}.executable"
                ),
            )
        )
    try:
        return PreparationRecord(
            source_identity=_string(
                document["source_identity"], path=f"{path}.source_identity"
            ),
            source_digest=_string(
                document["source_digest"], path=f"{path}.source_digest"
            ),
            source_action=_string(
                document["source_action"], path=f"{path}.source_action"
            ),
            image_uri=_string(document["image_uri"], path=f"{path}.image_uri"),
            image_sha256=(
                _optional_string(document["image_sha256"], path=f"{path}.image_sha256")
                if version >= 6
                else _string(document["image_sha256"], path=f"{path}.image_sha256")
            ),
            image_path=_path(document["image_path"], path=f"{path}.image_path"),
            image_action=_string(document["image_action"], path=f"{path}.image_action"),
            resolution_location=_string(
                document["resolution_location"],
                path=f"{path}.resolution_location",
            ),
            image_recipe_key=(
                _optional_string(
                    document["image_recipe_key"], path=f"{path}.image_recipe_key"
                )
                if version >= 6
                else None
            ),
            build_cache_key=_optional_string(
                document["build_cache_key"], path=f"{path}.build_cache_key"
            ),
            builder_location=_optional_string(
                document["builder_location"], path=f"{path}.builder_location"
            ),
            builder_scheduler_id=_optional_string(
                document["builder_scheduler_id"],
                path=f"{path}.builder_scheduler_id",
            ),
            builder_status=_optional_string(
                document["builder_status"], path=f"{path}.builder_status"
            ),
            builder_state=_optional_string(
                document["builder_state"], path=f"{path}.builder_state"
            ),
            build_action=_optional_string(
                document["build_action"], path=f"{path}.build_action"
            ),
            build_outputs=tuple(outputs),
            logs=tuple(
                _path(item, path=f"{path}.logs[{index}]")
                for index, item in enumerate(
                    _sequence(document["logs"], path=f"{path}.logs")
                )
            ),
        )
    except (TypeError, ValueError) as error:
        raise RunRecordFormatError(f"{path}: {error}") from error


def _task_string_mapping(value: Mapping[TaskId, str]) -> JsonObject:
    return {
        str(task_id): native_value
        for task_id, native_value in sorted(
            value.items(), key=lambda item: item[0].value
        )
    }


def _command_to_dict(command: Command) -> JsonObject:
    return {
        "argv": list(command.argv),
        "environment": dict(sorted(command.environment.items())),
        "working_directory": _path_or_none(command.working_directory),
    }


def _resources_to_dict(resources: ResourceRequest) -> JsonObject:
    walltime_microseconds = (
        None
        if resources.walltime is None
        else (
            resources.walltime.days * 86_400_000_000
            + resources.walltime.seconds * 1_000_000
            + resources.walltime.microseconds
        )
    )
    return {
        "nodes": resources.nodes,
        "tasks": resources.tasks,
        "cpus_per_task": resources.cpus_per_task,
        "gpus_per_task": resources.gpus_per_task,
        "memory_bytes": resources.memory_bytes,
        "walltime_microseconds": walltime_microseconds,
        "native": {
            backend: dict(sorted(options.items()))
            for backend, options in sorted(resources.native.items())
        },
    }


def _container_to_dict(container: ContainerSpec | None) -> JsonObject | None:
    if container is None:
        return None
    return {"image": str(container.image), "gpu": container.gpu}


def _experiment_to_dict(experiment: ExperimentSpec) -> JsonObject:
    return {
        "version": experiment.version,
        "name": experiment.name,
        "command": _command_to_dict(experiment.command),
        "resources": _resources_to_dict(experiment.resources),
        "container": _container_to_dict(experiment.container),
        "outputs": list(experiment.outputs),
        "sync_excludes": list(experiment.sync_excludes),
    }


def _backend_to_dict(backend: BackendConfig) -> JsonObject:
    return {"kind": backend.kind, "options": dict(sorted(backend.options.items()))}


def _target_to_dict(target: Target, *, version: int) -> JsonObject:
    document: JsonObject = {
        "name": target.name,
        "transport": _backend_to_dict(target.transport),
        "scheduler": _backend_to_dict(target.scheduler),
        "staging": _backend_to_dict(target.staging),
        "container": _backend_to_dict(target.container),
        "workspace": str(target.workspace),
    }
    if target.execution_storage is not None:
        if version not in {6, 7}:
            raise RunRecordFormatError(
                "RunRecord v1-v5 cannot contain target execution_storage"
            )
        policy = target.execution_storage
        document["execution_storage"] = {
            "type": "slurm_scratch",
            "cpu_environment": policy.cpu_environment,
            "gpu_environment": policy.gpu_environment,
            "stage_image": policy.stage_image,
            "copy_back": policy.copy_back,
        }
    if target.partition_policy is not None:
        if version != 7:
            raise RunRecordFormatError(
                "RunRecord v1-v6 cannot contain target partition_policy"
            )
        document["partition_policy"] = {
            "type": "slurm_partition_routes",
            "routes": [
                {
                    "name": route.name,
                    "partition": route.partition,
                    "resource_class": route.resource_class,
                    "max_walltime_microseconds": int(
                        route.max_walltime.total_seconds() * 1_000_000
                    ),
                }
                for route in target.partition_policy.routes
            ],
        }
    return document


def _config_to_dict(config: ConfigSnapshot) -> JsonObject:
    return {"source": str(config.source), "content": config.content}


def _task_to_dict(task: Task, *, version: int) -> JsonObject:
    document: JsonObject = {
        "id": str(task.id),
        "run_id": str(task.run_id),
        "experiment_name": task.experiment_name,
        "config": _config_to_dict(task.config),
        "seed": task.seed,
        "resources": _resources_to_dict(task.resources),
        "state": task.state.value,
    }
    if version in {3, 5, 6, 7}:
        document["parameter_set"] = (
            {
                "id": task.parameter_set.id,
                "choices": dict(task.parameter_set.choices),
            }
            if task.parameter_set is not None
            else None
        )
    return document


def _run_to_dict(run: Run, *, version: int) -> JsonObject:
    document: JsonObject = {
        "id": str(run.id),
        "experiment_name": run.experiment_name,
        "target": _target_to_dict(run.target, version=version),
        "created_at": run.created_at.isoformat(),
        "state": run.state.value,
        "retrieval_state": run.retrieval_state.value,
    }
    if type(run) is not CompactRun:
        document["tasks"] = [_task_to_dict(task, version=version) for task in run.tasks]
    return document


def _artifact_to_dict(artifact: Artifact) -> JsonObject:
    return {
        "kind": artifact.kind.value,
        "path": str(artifact.path),
        "task_id": None if artifact.task_id is None else str(artifact.task_id),
        "size_bytes": artifact.size_bytes,
    }


def _parse_command(value: object, *, path: str) -> Command:
    document = _object(value, path=path)
    _exact_fields(
        document,
        frozenset({"argv", "environment", "working_directory"}),
        path=path,
    )
    environment = _string_mapping(document["environment"], path=f"{path}.environment")
    try:
        return Command(
            _string_tuple(document["argv"], path=f"{path}.argv"),
            environment=environment,
            working_directory=_optional_path(
                document["working_directory"], path=f"{path}.working_directory"
            ),
        )
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_resources(value: object, *, path: str) -> ResourceRequest:
    document = _object(value, path=path)
    _exact_fields(
        document,
        frozenset(
            {
                "nodes",
                "tasks",
                "cpus_per_task",
                "gpus_per_task",
                "memory_bytes",
                "walltime_microseconds",
                "native",
            }
        ),
        path=path,
    )
    walltime_value = document["walltime_microseconds"]
    walltime = (
        None
        if walltime_value is None
        else timedelta(
            microseconds=_integer(
                walltime_value,
                path=f"{path}.walltime_microseconds",
            )
        )
    )
    try:
        return ResourceRequest(
            nodes=_integer(document["nodes"], path=f"{path}.nodes"),
            tasks=_integer(document["tasks"], path=f"{path}.tasks"),
            cpus_per_task=_integer(
                document["cpus_per_task"], path=f"{path}.cpus_per_task"
            ),
            gpus_per_task=_integer(
                document["gpus_per_task"], path=f"{path}.gpus_per_task"
            ),
            memory_bytes=_optional_integer(
                document["memory_bytes"], path=f"{path}.memory_bytes"
            ),
            walltime=walltime,
            native=_native_options(document["native"], path=f"{path}.native"),
        )
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_container(value: object, *, path: str) -> ContainerSpec | None:
    if value is None:
        return None
    document = _object(value, path=path)
    _exact_fields(document, frozenset({"image", "gpu"}), path=path)
    try:
        return ContainerSpec(
            _path(document["image"], path=f"{path}.image"),
            gpu=_boolean(document["gpu"], path=f"{path}.gpu"),
        )
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_experiment(value: object, *, path: str) -> ExperimentSpec:
    document = _object(value, path=path)
    _exact_fields(
        document,
        frozenset(
            {
                "version",
                "name",
                "command",
                "resources",
                "container",
                "outputs",
                "sync_excludes",
            }
        ),
        path=path,
    )
    try:
        return ExperimentSpec(
            version=_integer(document["version"], path=f"{path}.version"),
            name=_string(document["name"], path=f"{path}.name"),
            command=_parse_command(document["command"], path=f"{path}.command"),
            resources=_parse_resources(document["resources"], path=f"{path}.resources"),
            container=_parse_container(document["container"], path=f"{path}.container"),
            outputs=_string_tuple(document["outputs"], path=f"{path}.outputs"),
            sync_excludes=_string_tuple(
                document["sync_excludes"], path=f"{path}.sync_excludes"
            ),
        )
    except RunRecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_backend(value: object, *, path: str) -> BackendConfig:
    document = _object(value, path=path)
    _exact_fields(document, frozenset({"kind", "options"}), path=path)
    try:
        return BackendConfig(
            _string(document["kind"], path=f"{path}.kind"),
            _scalar_mapping(document["options"], path=f"{path}.options"),
        )
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_target(value: object, *, path: str, version: int) -> Target:
    document = _object(value, path=path)
    storage_present = version in {6, 7} and "execution_storage" in document
    partition_policy_present = version == 7 and "partition_policy" in document
    _exact_fields(
        document,
        frozenset(
            {
                "name",
                "transport",
                "scheduler",
                "staging",
                "container",
                "workspace",
                *(("execution_storage",) if storage_present else ()),
                *(("partition_policy",) if partition_policy_present else ()),
            }
        ),
        path=path,
    )
    storage: SlurmScratchPolicy | None = None
    if storage_present:
        storage_path = f"{path}.execution_storage"
        storage_document = _object(document["execution_storage"], path=storage_path)
        _exact_fields(
            storage_document,
            frozenset(
                {
                    "type",
                    "cpu_environment",
                    "gpu_environment",
                    "stage_image",
                    "copy_back",
                }
            ),
            path=storage_path,
        )
        if _string(storage_document["type"], path=f"{storage_path}.type") != (
            "slurm_scratch"
        ):
            raise RunRecordFormatError(f"{storage_path}.type must be slurm_scratch")
        try:
            storage = SlurmScratchPolicy(
                cpu_environment=_string(
                    storage_document["cpu_environment"],
                    path=f"{storage_path}.cpu_environment",
                ),
                gpu_environment=_string(
                    storage_document["gpu_environment"],
                    path=f"{storage_path}.gpu_environment",
                ),
                stage_image=_boolean(
                    storage_document["stage_image"],
                    path=f"{storage_path}.stage_image",
                ),
                copy_back=_string(
                    storage_document["copy_back"],
                    path=f"{storage_path}.copy_back",
                ),
            )
        except (TypeError, ValueError) as error:
            _invalid(storage_path, error)
    partition_policy = None
    if partition_policy_present:
        from rundra.domain.scheduling import SlurmPartitionPolicy, SlurmPartitionRoute

        policy_path = f"{path}.partition_policy"
        policy_document = _object(document["partition_policy"], path=policy_path)
        _exact_fields(policy_document, frozenset({"type", "routes"}), path=policy_path)
        if _string(policy_document["type"], path=f"{policy_path}.type") != (
            "slurm_partition_routes"
        ):
            raise RunRecordFormatError(
                f"{policy_path}.type must be slurm_partition_routes"
            )
        raw_routes = policy_document["routes"]
        if not isinstance(raw_routes, list) or not raw_routes:
            raise RunRecordFormatError(f"{policy_path}.routes must be nonempty")
        parsed_routes = []
        for index, raw_route in enumerate(raw_routes):
            route_path = f"{policy_path}.routes[{index}]"
            route = _object(raw_route, path=route_path)
            _exact_fields(
                route,
                frozenset(
                    {
                        "name",
                        "partition",
                        "resource_class",
                        "max_walltime_microseconds",
                    }
                ),
                path=route_path,
            )
            parsed_routes.append(
                SlurmPartitionRoute(
                    _string(route["name"], path=f"{route_path}.name"),
                    _string(route["partition"], path=f"{route_path}.partition"),
                    _string(
                        route["resource_class"],
                        path=f"{route_path}.resource_class",
                    ),
                    timedelta(
                        microseconds=_integer(
                            route["max_walltime_microseconds"],
                            path=f"{route_path}.max_walltime_microseconds",
                        )
                    ),
                )
            )
        partition_policy = SlurmPartitionPolicy(tuple(parsed_routes))
    try:
        return Target(
            name=_string(document["name"], path=f"{path}.name"),
            transport=_parse_backend(document["transport"], path=f"{path}.transport"),
            scheduler=_parse_backend(document["scheduler"], path=f"{path}.scheduler"),
            staging=_parse_backend(document["staging"], path=f"{path}.staging"),
            container=_parse_backend(document["container"], path=f"{path}.container"),
            workspace=_path(document["workspace"], path=f"{path}.workspace"),
            execution_storage=storage,
            partition_policy=partition_policy,
        )
    except RunRecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_config(value: object, *, path: str) -> ConfigSnapshot:
    document = _object(value, path=path)
    _exact_fields(document, frozenset({"source", "content"}), path=path)
    try:
        return ConfigSnapshot(
            _path(document["source"], path=f"{path}.source"),
            _string(document["content"], path=f"{path}.content"),
        )
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_task(value: object, *, path: str, version: int) -> Task:
    document = _object(value, path=path)
    _exact_fields(
        document,
        frozenset(
            {
                "id",
                "run_id",
                "experiment_name",
                "config",
                "seed",
                "resources",
                "state",
                *(("parameter_set",) if version in {3, 5, 6} else ()),
            }
        ),
        path=path,
    )
    try:
        return Task(
            id=TaskId(_string(document["id"], path=f"{path}.id")),
            run_id=RunId(_string(document["run_id"], path=f"{path}.run_id")),
            experiment_name=_string(
                document["experiment_name"], path=f"{path}.experiment_name"
            ),
            config=_parse_config(document["config"], path=f"{path}.config"),
            seed=_integer(document["seed"], path=f"{path}.seed"),
            resources=_parse_resources(document["resources"], path=f"{path}.resources"),
            parameter_set=(
                _parse_parameter_set(
                    document["parameter_set"], path=f"{path}.parameter_set"
                )
                if version in {3, 5, 6} and document["parameter_set"] is not None
                else None
            ),
            state=_execution_state(document["state"], path=f"{path}.state"),
        )
    except RunRecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_run(value: object, *, path: str, version: int, compact: bool = False) -> Run:
    document = _object(value, path=path)
    _exact_fields(
        document,
        frozenset(
            {
                "id",
                "experiment_name",
                "target",
                "created_at",
                "state",
                "retrieval_state",
                *(("tasks",) if not compact else ()),
            }
        ),
        path=path,
    )
    task_values = (
        _sequence(document["tasks"], path=f"{path}.tasks") if not compact else ()
    )
    try:
        run_type = CompactRun if compact else Run
        return run_type(
            id=RunId(_string(document["id"], path=f"{path}.id")),
            experiment_name=_string(
                document["experiment_name"], path=f"{path}.experiment_name"
            ),
            target=_parse_target(
                document["target"], path=f"{path}.target", version=version
            ),
            tasks=tuple(
                _parse_task(item, path=f"{path}.tasks[{index}]", version=version)
                for index, item in enumerate(task_values)
            ),
            created_at=_datetime(document["created_at"], path=f"{path}.created_at"),
            state=_execution_state(document["state"], path=f"{path}.state"),
            retrieval_state=_retrieval_state(
                document["retrieval_state"], path=f"{path}.retrieval_state"
            ),
        )
    except RunRecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_parameter_set(value: object, *, path: str) -> ParameterSet:
    document = _object(value, path=path)
    _exact_fields(document, frozenset({"id", "choices"}), path=path)
    choices = _object(document["choices"], path=f"{path}.choices")
    try:
        return ParameterSet(
            _string(document["id"], path=f"{path}.id"),
            choices,
        )
    except (TypeError, ValueError) as error:
        _invalid(path, error)


def _parse_artifacts(value: object) -> tuple[Artifact, ...]:
    values = _sequence(value, path="artifacts")
    artifacts: list[Artifact] = []
    for index, item in enumerate(values):
        path = f"artifacts[{index}]"
        document = _object(item, path=path)
        _exact_fields(
            document,
            frozenset({"kind", "path", "task_id", "size_bytes"}),
            path=path,
        )
        task_id_value = document["task_id"]
        try:
            artifacts.append(
                Artifact(
                    kind=ArtifactKind(_string(document["kind"], path=f"{path}.kind")),
                    path=_path(document["path"], path=f"{path}.path"),
                    task_id=(
                        None
                        if task_id_value is None
                        else TaskId(_string(task_id_value, path=f"{path}.task_id"))
                    ),
                    size_bytes=_optional_integer(
                        document["size_bytes"], path=f"{path}.size_bytes"
                    ),
                )
            )
        except (TypeError, ValueError) as error:
            _invalid(path, error)
    return tuple(artifacts)


def _parse_exit_codes(value: object) -> dict[TaskId, int]:
    document = _object(value, path="task_exit_codes")
    result: dict[TaskId, int] = {}
    for task_id, exit_code in document.items():
        try:
            parsed_task_id = TaskId(task_id)
        except (TypeError, ValueError) as error:
            _invalid(f"task_exit_codes.{task_id}", error)
        result[parsed_task_id] = _integer(exit_code, path=f"task_exit_codes.{task_id}")
    return result


def _parse_task_string_mapping(value: object, *, path: str) -> dict[TaskId, str]:
    document = _object(value, path=path)
    result: dict[TaskId, str] = {}
    for task_id, native_value in document.items():
        try:
            parsed_task_id = TaskId(task_id)
        except (TypeError, ValueError) as error:
            _invalid(f"{path}.{task_id}", error)
        result[parsed_task_id] = _string(native_value, path=f"{path}.{task_id}")
    return result


def _parse_task_retrieval_states(value: object) -> dict[TaskId, RetrievalState]:
    path = "task_retrieval_states"
    document = _object(value, path=path)
    result: dict[TaskId, RetrievalState] = {}
    for task_id, state in document.items():
        try:
            parsed_task_id = TaskId(task_id)
            parsed_state = RetrievalState(_string(state, path=f"{path}.{task_id}"))
        except (TypeError, ValueError) as error:
            _invalid(f"{path}.{task_id}", error)
        result[parsed_task_id] = parsed_state
    return result


def _parse_task_array_mapping(value: object) -> tuple[ArrayTaskMapping, ...]:
    values = _sequence(value, path="task_array_mapping")
    result: list[ArrayTaskMapping] = []
    for index, item in enumerate(values):
        path = f"task_array_mapping[{index}]"
        document = _object(item, path=path)
        _exact_fields(
            document,
            frozenset({"task_id", "seed", "array_index"}),
            path=path,
        )
        try:
            result.append(
                ArrayTaskMapping(
                    task_id=TaskId(
                        _string(document["task_id"], path=f"{path}.task_id")
                    ),
                    seed=_integer(document["seed"], path=f"{path}.seed"),
                    array_index=_integer(
                        document["array_index"], path=f"{path}.array_index"
                    ),
                )
            )
        except (TypeError, ValueError) as error:
            _invalid(path, error)
    return tuple(result)


def _object(value: object, *, path: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise RunRecordFormatError(f"{path} must be an object")
    if any(type(key) is not str for key in value):
        raise RunRecordFormatError(f"{path} field names must be strings")
    return dict(value)


def _exact_fields(
    document: Mapping[str, object], expected: frozenset[str], *, path: str
) -> None:
    unknown = sorted(set(document) - expected)
    if unknown:
        raise RunRecordFormatError(f"{path} has unknown field(s): {', '.join(unknown)}")
    missing = sorted(expected - set(document))
    if missing:
        raise RunRecordFormatError(f"{path} is missing field(s): {', '.join(missing)}")


def _sequence(value: object, *, path: str) -> tuple[object, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RunRecordFormatError(f"{path} must be an array")
    return tuple(value)


def _string_tuple(value: object, *, path: str) -> tuple[str, ...]:
    values = _sequence(value, path=path)
    return tuple(
        _string(item, path=f"{path}[{index}]") for index, item in enumerate(values)
    )


def _string(value: object, *, path: str) -> str:
    if type(value) is not str:
        raise RunRecordFormatError(f"{path} must be a string")
    return value


def _optional_string(value: object, *, path: str) -> str | None:
    return None if value is None else _string(value, path=path)


def _integer(value: object, *, path: str) -> int:
    if type(value) is not int:
        raise RunRecordFormatError(f"{path} must be an integer")
    return value


def _optional_integer(value: object, *, path: str) -> int | None:
    return None if value is None else _integer(value, path=path)


def _boolean(value: object, *, path: str) -> bool:
    if type(value) is not bool:
        raise RunRecordFormatError(f"{path} must be a boolean")
    return value


def _optional_bool(value: object, *, path: str) -> bool | None:
    return None if value is None else _boolean(value, path=path)


def _path(value: object, *, path: str) -> PurePosixPath:
    return PurePosixPath(_string(value, path=path))


def _optional_path(value: object, *, path: str) -> PurePosixPath | None:
    return None if value is None else _path(value, path=path)


def _datetime(value: object, *, path: str) -> datetime:
    text = _string(value, path=path)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise RunRecordFormatError(f"{path} must be an ISO 8601 datetime") from error
    if parsed.utcoffset() is None:
        raise RunRecordFormatError(f"{path} must be timezone-aware")
    return parsed


def _optional_datetime(value: object, *, path: str) -> datetime | None:
    return None if value is None else _datetime(value, path=path)


def _execution_state(value: object, *, path: str) -> ExecutionState:
    try:
        return ExecutionState(_string(value, path=path))
    except ValueError as error:
        raise RunRecordFormatError(f"{path} is not a known execution state") from error


def _retrieval_state(value: object, *, path: str) -> RetrievalState:
    try:
        return RetrievalState(_string(value, path=path))
    except ValueError as error:
        raise RunRecordFormatError(f"{path} is not a known retrieval state") from error


def _string_mapping(value: object, *, path: str) -> dict[str, str]:
    document = _object(value, path=path)
    return {key: _string(item, path=f"{path}.{key}") for key, item in document.items()}


def _scalar_mapping(value: object, *, path: str) -> dict[str, NativeValue]:
    document = _object(value, path=path)
    result: dict[str, NativeValue] = {}
    for key, item in document.items():
        if type(item) not in (str, int, float, bool):
            raise RunRecordFormatError(f"{path}.{key} must be a JSON scalar")
        if type(item) is float and not isfinite(item):
            raise RunRecordFormatError(f"{path}.{key} must be finite")
        result[key] = cast(NativeValue, item)
    return result


def _native_options(value: object, *, path: str) -> dict[str, dict[str, NativeValue]]:
    document = _object(value, path=path)
    return {
        backend: _scalar_mapping(options, path=f"{path}.{backend}")
        for backend, options in document.items()
    }


def _invalid(path: str, error: Exception) -> NoReturn:
    raise RunRecordFormatError(f"{path}: {error}") from error
