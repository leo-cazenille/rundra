from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
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
from rundra.domain.models import BackendConfig, NativeValue, Target
from rundra.domain.preparation import PreparationStorageConfig
from rundra.security import is_credential_field, is_safe_ssh_destination

_TARGET_V1_FIELDS = frozenset(
    {"transport", "scheduler", "staging", "container", "workspace"}
)
_TARGET_V2_FIELDS = _TARGET_V1_FIELDS | {"preparation"}
_BACKENDS_BY_ROLE = {
    "transport": frozenset({"local", "ssh"}),
    "scheduler": frozenset({"local", "slurm"}),
    "staging": frozenset({"local", "rsync"}),
    "container": frozenset({"apptainer", "native"}),
}
_SUPPORTED_BACKEND_STACKS = frozenset(
    {
        ("local", "local", "local", "apptainer"),
        ("local", "local", "local", "native"),
        ("ssh", "slurm", "rsync", "apptainer"),
    }
)


@dataclass(frozen=True, slots=True)
class TargetsConfig:
    version: int
    targets: Mapping[str, Target]
    preparation: Mapping[str, PreparationStorageConfig]

    def __post_init__(self) -> None:
        if self.version not in {1, 2}:
            raise ValueError("TargetsConfig version must be 1 or 2")
        object.__setattr__(self, "targets", MappingProxyType(dict(self.targets)))
        object.__setattr__(
            self, "preparation", MappingProxyType(dict(self.preparation))
        )


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
    if version not in {1, 2}:
        fail(
            source=source,
            path=("version",),
            code="UNSUPPORTED_VERSION",
            message="Unsupported targets version; supported versions are 1 and 2",
        )
    raw_targets = expect_mapping(document["targets"], source=source, path=("targets",))
    targets: dict[str, Target] = {}
    preparation: dict[str, PreparationStorageConfig] = {}
    for name, raw_target in raw_targets.items():
        path = ("targets", name)
        expect_string(name, source=source, path=path, nonblank=True)
        section = expect_mapping(raw_target, source=source, path=path)
        check_fields(
            section,
            allowed=_TARGET_V1_FIELDS if version == 1 else _TARGET_V2_FIELDS,
            required=_TARGET_V1_FIELDS,
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
            transport=_backend_config(section["transport"], "transport", source, path),
            scheduler=_backend_config(section["scheduler"], "scheduler", source, path),
            staging=_backend_config(section["staging"], "staging", source, path),
            container=_backend_config(section["container"], "container", source, path),
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
                    "SSH/Slurm/rsync/Apptainer stack"
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
                section["preparation"], source, (*path, "preparation")
            )
    return TargetsConfig(version, targets, preparation)


def _preparation_storage(
    value: object,
    source: Path,
    path: tuple[str, ...],
) -> PreparationStorageConfig:
    section = expect_mapping(value, source=source, path=path)
    check_fields(
        section,
        allowed=frozenset({"cache_root", "image_search_paths"}),
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
    return PreparationStorageConfig(cache_root, search_paths)


def _backend_config(
    value: object,
    role: str,
    source: Path,
    target_path: tuple[str, str],
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
        allowed=frozenset({"type", "host"})
        if role == "transport"
        else frozenset({"type"}),
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
    elif "host" in section:
        fail(
            source=source,
            path=(*path, "host"),
            code="UNKNOWN_FIELD",
            message="Field 'host' is only valid for SSH transport",
        )
    return BackendConfig(kind=kind, options=options)
