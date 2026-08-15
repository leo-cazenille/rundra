from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from rundra.config._schema import (
    ConfigPath,
    check_fields,
    expect_integer,
    expect_mapping,
    expect_string,
    fail,
    is_credential_field,
    require_version_one,
)
from rundra.config._yaml import read_yaml_document

_PROJECT_FIELDS = frozenset({"version", "default_profile", "defaults", "profiles"})
_LAUNCH_VALUE_FIELDS = frozenset(
    {"config", "seed", "target", "source_root", "destination"}
)


@dataclass(frozen=True, slots=True)
class LaunchValues:
    """Optional launch values after declaring-file-relative path resolution."""

    config: Path | None = None
    seed: int | None = None
    target: str | None = None
    source_root: Path | None = None
    destination: Path | None = None

    def __post_init__(self) -> None:
        for name in ("config", "source_root", "destination"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, Path):
                raise TypeError(f"LaunchValues {name} must be a Path or None")
        if self.seed is not None and type(self.seed) is not int:
            raise TypeError("LaunchValues seed must be an integer or None")
        if self.target is not None and (
            type(self.target) is not str or not self.target.strip()
        ):
            raise ValueError("LaunchValues target must be a nonblank string or None")


@dataclass(frozen=True, slots=True)
class ProjectLaunchConfig:
    """Strict version-1 project launch defaults and named profiles."""

    version: int
    source: Path
    defaults: LaunchValues
    profiles: Mapping[str, LaunchValues]
    default_profile: str | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("ProjectLaunchConfig version must be 1")
        if not isinstance(self.source, Path) or not self.source.is_absolute():
            raise ValueError("ProjectLaunchConfig source must be an absolute Path")
        if type(self.defaults) is not LaunchValues:
            raise TypeError("ProjectLaunchConfig defaults must be LaunchValues")
        if not isinstance(self.profiles, Mapping):
            raise TypeError("ProjectLaunchConfig profiles must be a mapping")
        profiles = dict(self.profiles)
        if any(
            type(name) is not str
            or not name.strip()
            or type(values) is not LaunchValues
            for name, values in profiles.items()
        ):
            raise ValueError("ProjectLaunchConfig profiles are invalid")
        if self.default_profile is not None:
            if (
                type(self.default_profile) is not str
                or self.default_profile not in profiles
            ):
                raise ValueError("ProjectLaunchConfig default profile is invalid")
        object.__setattr__(self, "profiles", MappingProxyType(profiles))

    @property
    def project_root(self) -> Path:
        """Return the directory that owns relative project launch paths."""
        return self.source.parent


def load_project_launch(source: Path) -> ProjectLaunchConfig:
    """Load a strict project launch document from an explicit path."""
    normalized_source = source.expanduser().resolve()
    document = expect_mapping(
        read_yaml_document(normalized_source), source=normalized_source, path=()
    )
    _reject_credential_fields(document, normalized_source, ())
    check_fields(
        document,
        allowed=_PROJECT_FIELDS,
        required=frozenset({"version"}),
        source=normalized_source,
        path=(),
    )
    version = require_version_one(document["version"], source=normalized_source)
    defaults = (
        _launch_values(document["defaults"], normalized_source, ("defaults",))
        if "defaults" in document
        else LaunchValues()
    )
    profiles: dict[str, LaunchValues] = {}
    if "profiles" in document:
        raw_profiles = expect_mapping(
            document["profiles"], source=normalized_source, path=("profiles",)
        )
        for name, raw_profile in raw_profiles.items():
            expect_string(
                name,
                source=normalized_source,
                path=("profiles", name),
                nonblank=True,
            )
            profiles[name] = _launch_values(
                raw_profile, normalized_source, ("profiles", name)
            )
    if defaults == LaunchValues() and not profiles:
        fail(
            source=normalized_source,
            path=(),
            code="EMPTY_LAUNCH_CONFIG",
            message="Project launch configuration must define defaults or profiles",
        )
    default_profile = None
    if "default_profile" in document:
        default_profile = expect_string(
            document["default_profile"],
            source=normalized_source,
            path=("default_profile",),
            nonblank=True,
        )
        if default_profile not in profiles:
            fail(
                source=normalized_source,
                path=("default_profile",),
                code="UNKNOWN_PROFILE",
                message=f"Default profile '{default_profile}' is not defined",
            )
    return ProjectLaunchConfig(
        version=version,
        source=normalized_source,
        defaults=defaults,
        profiles=profiles,
        default_profile=default_profile,
    )


def discover_project_launch(
    experiment_source: Path,
    *,
    project_file: Path | None = None,
) -> ProjectLaunchConfig | None:
    """Load an explicit project file or conservatively discover an adjacent one."""
    if project_file is not None:
        return load_project_launch(project_file)
    experiment = experiment_source.expanduser().resolve()
    candidate = experiment.parent / "rundra.yaml"
    if not candidate.exists():
        return None
    return load_project_launch(candidate)


def _launch_values(
    value: object,
    source: Path,
    path: ConfigPath,
) -> LaunchValues:
    section = expect_mapping(value, source=source, path=path)
    _reject_credential_fields(section, source, path)
    check_fields(
        section,
        allowed=_LAUNCH_VALUE_FIELDS,
        required=frozenset(),
        source=source,
        path=path,
    )
    return LaunchValues(
        config=_optional_path(section, "config", source, path),
        seed=(
            expect_integer(
                section["seed"], source=source, path=(*path, "seed"), minimum=0
            )
            if "seed" in section
            else None
        ),
        target=(
            expect_string(
                section["target"],
                source=source,
                path=(*path, "target"),
                nonblank=True,
            )
            if "target" in section
            else None
        ),
        source_root=_optional_path(section, "source_root", source, path),
        destination=_optional_path(section, "destination", source, path),
    )


def _optional_path(
    section: Mapping[str, object],
    field: str,
    source: Path,
    path: ConfigPath,
) -> Path | None:
    if field not in section:
        return None
    raw = expect_string(
        section[field], source=source, path=(*path, field), nonblank=True
    )
    declared = Path(raw).expanduser()
    if not declared.is_absolute():
        declared = source.parent / declared
    return declared.resolve()


def _reject_credential_fields(
    section: Mapping[str, object],
    source: Path,
    path: ConfigPath,
) -> None:
    for field in section:
        if is_credential_field(field):
            fail(
                source=source,
                path=(*path, field),
                code="FORBIDDEN_FIELD",
                message="Credentials must not be stored in launch configuration",
            )
