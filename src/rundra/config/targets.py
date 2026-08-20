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
    "scheduler": frozenset({"local", "pbs", "slurm"}),
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
    }
)


@dataclass(frozen=True, slots=True)
class TargetsConfig:
    version: int
    targets: Mapping[str, Target]
    preparation: Mapping[str, PreparationStorageConfig]
    execution: Mapping[str, ExecutionPolicy] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.version not in {1, 2, 3, 4, 5, 6, 7, 8}:
            raise ValueError("TargetsConfig version must be 1 through 8")
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
    if version not in {1, 2, 3, 4, 5, 6, 7, 8}:
        fail(
            source=source,
            path=("version",),
            code="UNSUPPORTED_VERSION",
            message=("Unsupported targets version; supported versions are 1 through 8"),
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
                                        else _TARGET_V8_FIELDS
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
                frozenset({"type", "root"})
                if role == "staging" and version >= 5
                else frozenset({"type"})
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
    options: dict[str, NativeValue] = {}
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
    elif any(
        field in section for field in ("host", "executable", "config_file", "root")
    ):
        field = next(
            field
            for field in ("host", "executable", "config_file", "root")
            if field in section
        )
        fail(
            source=source,
            path=(*path, field),
            code="UNKNOWN_FIELD",
            message=f"Field '{field}' is only valid for SSH transport",
        )
    return BackendConfig(kind=kind, options=options)
