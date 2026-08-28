from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import timedelta
from pathlib import Path, PurePath
from types import MappingProxyType

from rundra.config._schema import (
    check_fields,
    expect_boolean,
    expect_integer,
    expect_mapping,
    expect_string,
    expect_string_list,
    fail,
)
from rundra.config._yaml import read_yaml_document
from rundra.domain.models import BackendConfig, NativeValue, ResourceRequest, Target
from rundra.domain.preparation import DefinitionBuildPolicy, PreparationStorageConfig
from rundra.domain.scaling import (
    DEFAULT_MAX_CONCURRENT_JOBS,
    ExecutionPolicy,
    WorkerPoolPolicy,
)
from rundra.domain.scheduling import SlurmPartitionPolicy, SlurmPartitionRoute
from rundra.domain.storage import SlurmScratchPolicy
from rundra.scheduler_registry import scheduler_kinds
from rundra.schema_versions import TARGET_CONFIG_SCHEMA
from rundra.security import is_credential_field, is_safe_ssh_destination

_TARGET_V1_FIELDS = frozenset(
    {"transport", "scheduler", "staging", "container", "workspace"}
)
_TARGET_V2_FIELDS = _TARGET_V1_FIELDS | {"preparation"}
_TARGET_V3_FIELDS = _TARGET_V2_FIELDS | {"execution"}
_TARGET_V4_FIELDS = _TARGET_V3_FIELDS
_TARGET_V5_FIELDS = _TARGET_V4_FIELDS
_TARGET_V6_FIELDS = _TARGET_V5_FIELDS
_TARGET_V7_FIELDS = _TARGET_V6_FIELDS
_TARGET_V8_FIELDS = _TARGET_V7_FIELDS
_TARGET_V9_FIELDS = _TARGET_V8_FIELDS
_TARGET_V10_FIELDS = _TARGET_V9_FIELDS | {"execution_storage"}
_TARGET_V11_FIELDS = _TARGET_V10_FIELDS
_MEMORY_PATTERN = re.compile(r"([1-9][0-9]*)(B|KiB|MiB|GiB|TiB)\Z")
_WALLTIME_PATTERN = re.compile(r"([0-9]{2,}):([0-5][0-9]):([0-5][0-9])\Z")
_MEMORY_FACTORS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}
_BACKENDS_BY_ROLE = {
    "transport": frozenset({"local", "ssh"}),
    "scheduler": scheduler_kinds(),
    "staging": frozenset({"local", "rsync", "shared"}),
    "container": frozenset({"apptainer", "native"}),
}
_SUPPORTED_BACKEND_STACKS = frozenset(
    {
        ("local", "local", "local", "apptainer"),
        ("local", "local", "local", "native"),
        ("ssh", "slurm", "rsync", "apptainer"),
        ("ssh", "slurm", "shared", "apptainer"),
        ("ssh", "pbs", "rsync", "apptainer"),
        ("ssh", "pbs", "shared", "apptainer"),
        ("ssh", "htcondor", "rsync", "apptainer"),
        ("ssh", "htcondor", "shared", "apptainer"),
    }
)


def builtin_targets_source() -> Path:
    """Return the packaged safe local target definition."""
    return Path(__file__).parents[1] / "defaults" / "targets.yaml"


@dataclass(frozen=True, slots=True)
class TargetsConfig:
    version: int
    targets: Mapping[str, Target]
    preparation: Mapping[str, PreparationStorageConfig]
    execution: Mapping[str, ExecutionPolicy] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.version not in TARGET_CONFIG_SCHEMA.supported:
            raise ValueError("TargetsConfig version must be 1 through 10")
        object.__setattr__(self, "targets", MappingProxyType(dict(self.targets)))
        object.__setattr__(
            self, "preparation", MappingProxyType(dict(self.preparation))
        )
        object.__setattr__(self, "execution", MappingProxyType(dict(self.execution)))


def load_targets(source: Path) -> Mapping[str, Target]:
    """Load strict site targets without constructing adapters."""
    return load_targets_config(source).targets


def load_targets_config(source: Path) -> TargetsConfig:
    """Load targets and preparation storage without mixing their domains."""
    document = expect_mapping(read_yaml_document(source), source=source, path=())
    check_fields(
        document,
        allowed=frozenset({"version", "targets"}),
        required=frozenset({"version", "targets"}),
        source=source,
        path=(),
    )
    version = expect_integer(
        document["version"], source=source, path=("version",), minimum=1
    )
    if version not in TARGET_CONFIG_SCHEMA.supported:
        fail(
            source=source,
            path=("version",),
            code="UNSUPPORTED_VERSION",
            message=(
                "Unsupported targets version; supported versions are 1 through 10"
            ),
        )
    raw_targets = expect_mapping(document["targets"], source=source, path=("targets",))
    targets: dict[str, Target] = {}
    preparation: dict[str, PreparationStorageConfig] = {}
    execution: dict[str, ExecutionPolicy] = {}
    for name, raw_target in raw_targets.items():
        path = ("targets", name)
        expect_string(name, source=source, path=path, nonblank=True)
        section = expect_mapping(raw_target, source=source, path=path)
        check_fields(
            section,
            allowed=(
                _TARGET_V1_FIELDS
                if version == 1
                else (
                    _TARGET_V2_FIELDS
                    if version == 2
                    else (
                        _TARGET_V3_FIELDS
                        if version == 3
                        else (
                            _TARGET_V4_FIELDS
                            if version == 4
                            else (
                                _TARGET_V5_FIELDS
                                if version == 5
                                else (
                                    _TARGET_V6_FIELDS
                                    if version == 6
                                    else (
                                        _TARGET_V7_FIELDS
                                        if version == 7
                                        else (
                                            _TARGET_V8_FIELDS
                                            if version == 8
                                            else (
                                                _TARGET_V9_FIELDS
                                                if version == 9
                                                else (
                                                    _TARGET_V10_FIELDS
                                                    if version == 10
                                                    else _TARGET_V11_FIELDS
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ),
            required=(
                _TARGET_V1_FIELDS if version < 3 else _TARGET_V1_FIELDS | {"execution"}
            ),
            source=source,
            path=path,
        )
        workspace = PurePath(
            expect_string(
                section["workspace"],
                source=source,
                path=(*path, "workspace"),
                nonblank=True,
            )
        )
        scheduler_section = expect_mapping(
            section["scheduler"], source=source, path=(*path, "scheduler")
        )
        partition_policy = (
            _partition_policy(
                scheduler_section["partition_routes"],
                source,
                (*path, "scheduler", "partition_routes"),
            )
            if "partition_routes" in scheduler_section
            else None
        )
        target = Target(
            name=name,
            transport=_backend_config(
                section["transport"], "transport", source, path, version=version
            ),
            scheduler=_backend_config(
                section["scheduler"], "scheduler", source, path, version=version
            ),
            staging=_backend_config(
                section["staging"], "staging", source, path, version=version
            ),
            container=_backend_config(
                section["container"], "container", source, path, version=version
            ),
            workspace=workspace,
            execution_storage=(
                _execution_storage(
                    section["execution_storage"],
                    source,
                    (*path, "execution_storage"),
                )
                if "execution_storage" in section
                else None
            ),
            partition_policy=partition_policy,
        )
        backend_stack = (
            target.transport.kind,
            target.scheduler.kind,
            target.staging.kind,
            target.container.kind,
        )
        if backend_stack not in _SUPPORTED_BACKEND_STACKS:
            fail(
                source=source,
                path=path,
                code="INVALID_BACKEND_COMBINATION",
                message=(
                    "Target backends must use an all-local stack or the supported "
                    "SSH stack with Slurm or OpenPBS, rsync or shared staging, "
                    "and Apptainer"
                ),
            )
        if target.execution_storage is not None and target.scheduler.kind != "slurm":
            fail(
                source=source,
                path=(*path, "execution_storage"),
                code="INVALID_EXECUTION_STORAGE",
                message="Slurm scratch execution requires scheduler.type: slurm",
            )
        if target.partition_policy is not None and target.scheduler.kind != "slurm":
            fail(
                source=source,
                path=(*path, "scheduler", "partition_routes"),
                code="INVALID_PARTITION_POLICY",
                message="Partition routes require scheduler.type: slurm",
            )
        if target.transport.kind == "ssh" and (
            not target.workspace.is_absolute() or target.workspace == PurePath("/")
        ):
            fail(
                source=source,
                path=(*path, "workspace"),
                code="INVALID_REMOTE_WORKSPACE",
                message="SSH target workspace must be an absolute non-root path",
            )
        targets[name] = target
        if "preparation" in section:
            preparation[name] = _preparation_storage(
                section["preparation"],
                source,
                (*path, "preparation"),
                version=version,
            )
        if "execution" in section:
            execution[name] = _execution_policy(
                section["execution"], source, (*path, "execution"), version=version
            )
    return TargetsConfig(version, targets, preparation, execution)


def _partition_policy(
    value: object,
    source: Path,
    path: tuple[str | int, ...],
) -> SlurmPartitionPolicy:
    if not isinstance(value, list) or not value:
        fail(
            source=source,
            path=path,
            code="INVALID_PARTITION_POLICY",
            message="partition_routes must be a nonempty list",
        )
    routes: list[SlurmPartitionRoute] = []
    for index, raw_route in enumerate(value):
        route_path = (*path, index)
        route = expect_mapping(raw_route, source=source, path=route_path)
        check_fields(
            route,
            allowed=frozenset({"name", "partition", "resource_class", "max_walltime"}),
            required=frozenset({"name", "partition", "resource_class", "max_walltime"}),
            source=source,
            path=route_path,
        )
        values = {
            name: expect_string(
                route[name], source=source, path=(*route_path, name), nonblank=True
            )
            for name in ("name", "partition", "resource_class", "max_walltime")
        }
        match = _WALLTIME_PATTERN.fullmatch(values["max_walltime"])
        if match is None:
            fail(
                source=source,
                path=(*route_path, "max_walltime"),
                code="INVALID_PARTITION_POLICY",
                message="max_walltime must use HH:MM:SS",
            )
        hours, minutes, seconds = (int(part) for part in match.groups())
        try:
            routes.append(
                SlurmPartitionRoute(
                    values["name"],
                    values["partition"],
                    values["resource_class"],
                    timedelta(hours=hours, minutes=minutes, seconds=seconds),
                )
            )
        except (TypeError, ValueError) as error:
            fail(
                source=source,
                path=route_path,
                code="INVALID_PARTITION_POLICY",
                message=str(error),
            )
    try:
        return SlurmPartitionPolicy(tuple(routes))
    except (TypeError, ValueError) as error:
        fail(
            source=source,
            path=path,
            code="INVALID_PARTITION_POLICY",
            message=str(error),
        )


def _execution_storage(
    value: object,
    source: Path,
    path: tuple[str | int, ...],
) -> SlurmScratchPolicy:
    section = expect_mapping(value, source=source, path=path)
    fields = frozenset(
        {"type", "cpu_environment", "gpu_environment", "stage_image", "copy_back"}
    )
    check_fields(
        section,
        allowed=fields,
        required=fields,
        source=source,
        path=path,
    )
    kind = expect_string(
        section["type"], source=source, path=(*path, "type"), nonblank=True
    )
    if kind != "slurm_scratch":
        fail(
            source=source,
            path=(*path, "type"),
            code="INVALID_VALUE",
            message="Execution storage type must be 'slurm_scratch'",
        )
    cpu_environment = expect_string(
        section["cpu_environment"],
        source=source,
        path=(*path, "cpu_environment"),
        nonblank=True,
    )
    gpu_environment = expect_string(
        section["gpu_environment"],
        source=source,
        path=(*path, "gpu_environment"),
        nonblank=True,
    )
    stage_image = expect_boolean(
        section["stage_image"], source=source, path=(*path, "stage_image")
    )
    copy_back = expect_string(
        section["copy_back"],
        source=source,
        path=(*path, "copy_back"),
        nonblank=True,
    )
    try:
        return SlurmScratchPolicy(
            cpu_environment=cpu_environment,
            gpu_environment=gpu_environment,
            stage_image=stage_image,
            copy_back=copy_back,
        )
    except ValueError as error:
        fail(
            source=source,
            path=path,
            code="INVALID_EXECUTION_STORAGE",
            message=str(error),
        )


def _execution_policy(
    value: object,
    source: Path,
    path: tuple[str, ...],
    *,
    version: int,
) -> ExecutionPolicy:
    section = expect_mapping(value, source=source, path=path)
    fields = frozenset(
        {
            "hard_task_limit",
            "confirmation_threshold",
            "max_active_tasks",
            "max_concurrent_jobs",
            "max_array_size",
            "output_shard_tasks",
            "automatic_retrieval_threshold",
            "worker_pool",
        }
    )
    if version >= 7:
        fields |= {"max_memory_per_worker"}
    check_fields(
        section,
        allowed=fields,
        required=fields - {"max_concurrent_jobs"},
        source=source,
        path=path,
    )
    worker_path = (*path, "worker_pool")
    worker = expect_mapping(section["worker_pool"], source=source, path=worker_path)
    worker_fields = frozenset(
        {
            "activation_threshold",
            "max_workers",
            "tasks_per_lease",
            "infrastructure_retry_limit",
            "requeue_limit",
        }
    )
    if 4 <= version <= 5:
        worker_fields |= {"task_slots_per_worker"}
    elif version >= 6:
        worker_fields |= {
            "default_workers",
            "default_task_slots_per_worker",
            "max_task_slots_per_worker",
        }
    check_fields(
        worker,
        allowed=worker_fields,
        required=worker_fields,
        source=source,
        path=worker_path,
    )

    def integer(section_value: Mapping[str, object], name: str, minimum: int) -> int:
        return expect_integer(
            section_value[name], source=source, path=(*path, name), minimum=minimum
        )

    try:
        return ExecutionPolicy(
            hard_task_limit=integer(section, "hard_task_limit", 1),
            confirmation_threshold=integer(section, "confirmation_threshold", 1),
            max_active_tasks=integer(section, "max_active_tasks", 1),
            max_array_size=integer(section, "max_array_size", 2),
            output_shard_tasks=integer(section, "output_shard_tasks", 1),
            automatic_retrieval_threshold=integer(
                section, "automatic_retrieval_threshold", 0
            ),
            worker_pool=WorkerPoolPolicy(
                activation_threshold=expect_integer(
                    worker["activation_threshold"],
                    source=source,
                    path=(*worker_path, "activation_threshold"),
                    minimum=2,
                ),
                max_workers=expect_integer(
                    worker["max_workers"],
                    source=source,
                    path=(*worker_path, "max_workers"),
                    minimum=1,
                ),
                tasks_per_lease=expect_integer(
                    worker["tasks_per_lease"],
                    source=source,
                    path=(*worker_path, "tasks_per_lease"),
                    minimum=1,
                ),
                infrastructure_retry_limit=expect_integer(
                    worker["infrastructure_retry_limit"],
                    source=source,
                    path=(*worker_path, "infrastructure_retry_limit"),
                    minimum=0,
                ),
                requeue_limit=expect_integer(
                    worker["requeue_limit"],
                    source=source,
                    path=(*worker_path, "requeue_limit"),
                    minimum=0,
                ),
                task_slots_per_worker=(
                    expect_integer(
                        worker[
                            "default_task_slots_per_worker"
                            if version >= 6
                            else "task_slots_per_worker"
                        ],
                        source=source,
                        path=(
                            *worker_path,
                            "default_task_slots_per_worker"
                            if version >= 6
                            else "task_slots_per_worker",
                        ),
                        minimum=1,
                    )
                    if version >= 4
                    else 1
                ),
                default_workers=(
                    expect_integer(
                        worker["default_workers"],
                        source=source,
                        path=(*worker_path, "default_workers"),
                        minimum=1,
                    )
                    if version >= 6
                    else None
                ),
                max_task_slots_per_worker=(
                    expect_integer(
                        worker["max_task_slots_per_worker"],
                        source=source,
                        path=(*worker_path, "max_task_slots_per_worker"),
                        minimum=1,
                    )
                    if version >= 6
                    else None
                ),
            ),
            max_concurrent_jobs=expect_integer(
                section.get("max_concurrent_jobs", DEFAULT_MAX_CONCURRENT_JOBS),
                source=source,
                path=(*path, "max_concurrent_jobs"),
                minimum=1,
            ),
            max_memory_per_worker=(
                _parse_memory(
                    section["max_memory_per_worker"],
                    source,
                    (*path, "max_memory_per_worker"),
                )
                if version >= 7 and "max_memory_per_worker" in section
                else None
            ),
        )
    except (TypeError, ValueError) as error:
        fail(
            source=source,
            path=path,
            code="INVALID_EXECUTION_POLICY",
            message=str(error),
        )


def _parse_memory(value: object, source: Path, path: tuple[str, ...]) -> int:
    text = expect_string(value, source=source, path=path)
    match = _MEMORY_PATTERN.fullmatch(text)
    if match is None:
        fail(
            source=source,
            path=path,
            code="INVALID_VALUE",
            message=(
                "Memory must be a positive integer followed by B, KiB, MiB, GiB, or TiB"
            ),
        )
    amount, unit = match.groups()
    return int(amount) * _MEMORY_FACTORS[unit]


def _preparation_storage(
    value: object,
    source: Path,
    path: tuple[str, ...],
    *,
    version: int,
) -> PreparationStorageConfig:
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=(
            frozenset({"cache_root", "image_search_paths", "definition_build"})
            if version >= 8
            else frozenset({"cache_root", "image_search_paths"})
        ),
        required=frozenset(),
        source=source,
        path=path,
    )
    if not section:
        fail(
            source=source,
            path=path,
            code="EMPTY_PREPARATION_STORAGE",
            message="Target preparation storage must not be empty",
        )

    def absolute_path(raw: str, field_path: tuple[str | int, ...]) -> PurePath:
        result = PurePath(raw)
        if not result.is_absolute() or result == PurePath("/"):
            fail(
                source=source,
                path=field_path,
                code="INVALID_PREPARATION_PATH",
                message="Target preparation paths must be absolute and non-root",
            )
        return result

    cache_root = None
    if "cache_root" in section:
        raw_cache = expect_string(
            section["cache_root"],
            source=source,
            path=(*path, "cache_root"),
            nonblank=True,
        )
        cache_root = absolute_path(raw_cache, (*path, "cache_root"))
    search_paths = tuple(
        absolute_path(raw, (*path, "image_search_paths", index))
        for index, raw in enumerate(
            expect_string_list(
                section.get("image_search_paths", []),
                source=source,
                path=(*path, "image_search_paths"),
            )
        )
    )
    definition_build = None
    if "definition_build" in section:
        policy_path = (*path, "definition_build")
        policy = expect_mapping(
            section["definition_build"], source=source, path=policy_path
        )
        check_fields(
            policy,
            allowed=frozenset({"allowed_locations", "mode", "max_resources"}),
            required=frozenset({"allowed_locations", "mode", "max_resources"}),
            source=source,
            path=policy_path,
        )
        resources_path = (*policy_path, "max_resources")
        resources = expect_mapping(
            policy["max_resources"], source=source, path=resources_path
        )
        resource_fields = frozenset({"cpus_per_task", "memory", "walltime"})
        check_fields(
            resources,
            allowed=resource_fields,
            required=resource_fields,
            source=source,
            path=resources_path,
        )
        walltime_text = expect_string(
            resources["walltime"],
            source=source,
            path=(*resources_path, "walltime"),
            nonblank=True,
        )
        walltime_match = _WALLTIME_PATTERN.fullmatch(walltime_text)
        if walltime_match is None:
            fail(
                source=source,
                path=(*resources_path, "walltime"),
                code="INVALID_VALUE",
                message=(
                    "Walltime must use HH:MM:SS with two-digit minutes and seconds"
                ),
            )
        hours, minutes, seconds = (int(part) for part in walltime_match.groups())
        walltime = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        if walltime <= timedelta(0):
            fail(
                source=source,
                path=(*resources_path, "walltime"),
                code="INVALID_VALUE",
                message="Walltime must be positive",
            )
        try:
            definition_build = DefinitionBuildPolicy(
                allowed_locations=tuple(
                    expect_string_list(
                        policy["allowed_locations"],
                        source=source,
                        path=(*policy_path, "allowed_locations"),
                    )
                ),
                mode=expect_string(
                    policy["mode"],
                    source=source,
                    path=(*policy_path, "mode"),
                    nonblank=True,
                ),
                max_resources=ResourceRequest(
                    cpus_per_task=expect_integer(
                        resources["cpus_per_task"],
                        source=source,
                        path=(*resources_path, "cpus_per_task"),
                        minimum=1,
                    ),
                    memory_bytes=_parse_memory(
                        resources["memory"], source, (*resources_path, "memory")
                    ),
                    walltime=walltime,
                ),
            )
        except (TypeError, ValueError) as error:
            fail(
                source=source,
                path=policy_path,
                code="INVALID_DEFINITION_BUILD_POLICY",
                message=str(error),
            )
    return PreparationStorageConfig(cache_root, search_paths, definition_build)


def _backend_config(
    value: object,
    role: str,
    source: Path,
    target_path: tuple[str, str],
    *,
    version: int,
) -> BackendConfig:
    path = (*target_path, role)
    section = expect_mapping(value, source=source, path=path)
    for field in section:
        if is_credential_field(field):
            fail(
                source=source,
                path=(*path, field),
                code="FORBIDDEN_FIELD",
                message="Credentials must not be stored in target configuration",
            )
    check_fields(
        section,
        allowed=(
            frozenset({"type", "host", "executable", "config_file"})
            if role == "transport"
            else (
                frozenset({"type", "executable"})
                if role == "container" and version >= 11
                else (
                    frozenset({"type", "root"})
                    if role == "staging" and version >= 5
                    else (
                        frozenset(
                            {
                                "type",
                                "shared_workspace",
                                *(("partition_routes",) if version >= 11 else ()),
                            }
                        )
                        if role == "scheduler" and version >= 9
                        else frozenset({"type"})
                    )
                )
            )
        ),
        required=frozenset({"type"}),
        source=source,
        path=path,
    )
    kind = expect_string(
        section["type"], source=source, path=(*path, "type"), nonblank=True
    )
    if kind not in _BACKENDS_BY_ROLE[role]:
        fail(
            source=source,
            path=(*path, "type"),
            code="UNKNOWN_BACKEND",
            message=f"Backend '{kind}' is not supported for {role}",
        )
    if role == "staging" and kind == "shared" and version < 5:
        fail(
            source=source,
            path=(*path, "type"),
            code="UNKNOWN_BACKEND",
            message="Shared staging requires targets version 5",
        )
    if role == "scheduler" and kind == "htcondor":
        if version < 9:
            fail(
                source=source,
                path=(*path, "type"),
                code="UNKNOWN_BACKEND",
                message="HTCondor requires targets version 9",
            )
        if section.get("shared_workspace") is not True:
            fail(
                source=source,
                path=(*path, "shared_workspace"),
                code="INVALID_VALUE",
                message="HTCondor requires explicit shared_workspace: true",
            )
    options: dict[str, NativeValue] = {}
    if role == "scheduler" and kind == "htcondor":
        options["shared_workspace"] = True
    if role == "transport" and kind == "ssh":
        if "host" not in section:
            fail(
                source=source,
                path=(*path, "host"),
                code="MISSING_FIELD",
                message="SSH transport requires field 'host'",
            )
        host = expect_string(
            section["host"],
            source=source,
            path=(*path, "host"),
            nonblank=True,
        )
        if not is_safe_ssh_destination(host):
            fail(
                source=source,
                path=(*path, "host"),
                code="INVALID_VALUE",
                message="SSH host must be a safe host alias or user@host destination",
            )
        options["host"] = host
        if "executable" in section:
            executable = expect_string(
                section["executable"],
                source=source,
                path=(*path, "executable"),
                nonblank=True,
            )
            if "\x00" in executable or any(
                character.isspace() for character in executable
            ):
                fail(
                    source=source,
                    path=(*path, "executable"),
                    code="INVALID_VALUE",
                    message="SSH executable must be one safe argument",
                )
            options["executable"] = executable
        if "config_file" in section:
            config_file = expect_string(
                section["config_file"],
                source=source,
                path=(*path, "config_file"),
                nonblank=True,
            )
            config_path = PurePath(config_file)
            if (
                not config_path.is_absolute()
                or config_path == PurePath("/")
                or "\x00" in config_file
            ):
                fail(
                    source=source,
                    path=(*path, "config_file"),
                    code="INVALID_VALUE",
                    message="SSH config_file must be an absolute non-root path",
                )
            options["config_file"] = config_file
    elif role == "container" and kind == "apptainer":
        if "executable" in section:
            executable = expect_string(
                section["executable"],
                source=source,
                path=(*path, "executable"),
                nonblank=True,
            )
            if "\x00" in executable or any(
                character.isspace() for character in executable
            ):
                fail(
                    source=source,
                    path=(*path, "executable"),
                    code="INVALID_VALUE",
                    message="Container executable must be one safe argument",
                )
            options["executable"] = executable
    elif role == "staging" and kind == "shared":
        if "root" not in section:
            fail(
                source=source,
                path=(*path, "root"),
                code="MISSING_FIELD",
                message="Shared staging requires field 'root'",
            )
        root = expect_string(
            section["root"], source=source, path=(*path, "root"), nonblank=True
        )
        root_path = PurePath(root)
        if not root_path.is_absolute() or root_path == PurePath("/") or "\x00" in root:
            fail(
                source=source,
                path=(*path, "root"),
                code="INVALID_VALUE",
                message="Shared root must be an absolute non-root path",
            )
        options["root"] = root
    elif not (role == "scheduler" and kind == "htcondor") and any(
        field in section
        for field in ("host", "executable", "config_file", "root", "shared_workspace")
    ):
        field = next(
            field
            for field in (
                "host",
                "executable",
                "config_file",
                "root",
                "shared_workspace",
            )
            if field in section
        )
        fail(
            source=source,
            path=(*path, field),
            code="UNKNOWN_FIELD",
            message=f"Field '{field}' is not valid for {kind} {role}",
        )
    return BackendConfig(kind=kind, options=options)
