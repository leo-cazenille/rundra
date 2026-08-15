from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path, PurePath
from types import MappingProxyType

from rundra.config._schema import (
    check_fields,
    expect_mapping,
    expect_string,
    fail,
    is_credential_field,
    require_version_one,
)
from rundra.config._yaml import read_yaml_document
from rundra.domain.models import BackendConfig, NativeValue, Target

_TARGET_FIELDS = frozenset(
    {"transport", "scheduler", "staging", "container", "workspace"}
)
_BACKENDS_BY_ROLE = {
    "transport": frozenset({"local", "ssh"}),
    "scheduler": frozenset({"local", "slurm"}),
    "staging": frozenset({"local", "rsync"}),
    "container": frozenset({"apptainer"}),
}


def load_targets(source: Path) -> Mapping[str, Target]:
    """Load strict version-1 site targets without constructing adapters."""
    document = expect_mapping(read_yaml_document(source), source=source, path=())
    check_fields(
        document,
        allowed=frozenset({"version", "targets"}),
        required=frozenset({"version", "targets"}),
        source=source,
        path=(),
    )
    require_version_one(document["version"], source=source)
    raw_targets = expect_mapping(document["targets"], source=source, path=("targets",))
    targets: dict[str, Target] = {}
    for name, raw_target in raw_targets.items():
        path = ("targets", name)
        expect_string(name, source=source, path=path, nonblank=True)
        section = expect_mapping(raw_target, source=source, path=path)
        check_fields(
            section,
            allowed=_TARGET_FIELDS,
            required=_TARGET_FIELDS,
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
        targets[name] = Target(
            name=name,
            transport=_backend_config(section["transport"], "transport", source, path),
            scheduler=_backend_config(section["scheduler"], "scheduler", source, path),
            staging=_backend_config(section["staging"], "staging", source, path),
            container=_backend_config(section["container"], "container", source, path),
            workspace=workspace,
        )
    return MappingProxyType(targets)


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
        options["host"] = expect_string(
            section["host"],
            source=source,
            path=(*path, "host"),
            nonblank=True,
        )
    elif "host" in section:
        fail(
            source=source,
            path=(*path, "host"),
            code="UNKNOWN_FIELD",
            message="Field 'host' is only valid for SSH transport",
        )
    return BackendConfig(kind=kind, options=options)
