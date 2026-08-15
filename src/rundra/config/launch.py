from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from rundra.config._schema import (
    ConfigPath,
    check_fields,
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
_USER_FIELDS = frozenset({"version", "defaults"})
_USER_VALUE_FIELDS = frozenset(
    {
        "config",
        "seed",
        "target",
        "source_root",
        "destination",
        "targets_file",
        "data_dir",
    }
)
_VALUE_NAMES = (
    "config",
    "seed",
    "target",
    "source_root",
    "destination",
    "targets_file",
    "data_dir",
)


@dataclass(frozen=True, slots=True)
class LaunchValues:
    """Optional launch values after declaring-file-relative path resolution."""

    config: Path | None = None
    seed: int | None = None
    target: str | None = None
    source_root: Path | None = None
    destination: Path | None = None
    targets_file: Path | None = None
    data_dir: Path | None = None

    def __post_init__(self) -> None:
        for name in (
            "config",
            "source_root",
            "destination",
            "targets_file",
            "data_dir",
        ):
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


@dataclass(frozen=True, slots=True)
class UserLaunchConfig:
    """Strict version-1 per-user launch defaults, separate from targets."""

    version: int
    source: Path
    defaults: LaunchValues

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("UserLaunchConfig version must be 1")
        if not isinstance(self.source, Path) or not self.source.is_absolute():
            raise ValueError("UserLaunchConfig source must be an absolute Path")
        if type(self.defaults) is not LaunchValues:
            raise TypeError("UserLaunchConfig defaults must be LaunchValues")


@dataclass(frozen=True, slots=True)
class ResolvedLaunch:
    """Resolved launch values plus the layer selected for every present field."""

    values: LaunchValues
    sources: Mapping[str, str]
    profile: str | None = None

    def __post_init__(self) -> None:
        if type(self.values) is not LaunchValues:
            raise TypeError("ResolvedLaunch values must be LaunchValues")
        if not isinstance(self.sources, Mapping):
            raise TypeError("ResolvedLaunch sources must be a mapping")
        sources = dict(self.sources)
        if any(
            name not in _VALUE_NAMES or type(source) is not str or not source
            for name, source in sources.items()
        ):
            raise ValueError("ResolvedLaunch sources are invalid")
        if self.profile is not None and (
            type(self.profile) is not str or not self.profile
        ):
            raise ValueError("ResolvedLaunch profile must be nonblank or None")
        object.__setattr__(self, "sources", MappingProxyType(sources))


class LaunchResolutionError(Exception):
    """A stable failure while selecting layered launch inputs."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


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


def load_user_launch(source: Path) -> UserLaunchConfig:
    """Load strict per-user defaults without reading target definitions."""
    normalized_source = source.expanduser().resolve()
    document = expect_mapping(
        read_yaml_document(normalized_source), source=normalized_source, path=()
    )
    _reject_credential_fields(document, normalized_source, ())
    check_fields(
        document,
        allowed=_USER_FIELDS,
        required=frozenset({"version", "defaults"}),
        source=normalized_source,
        path=(),
    )
    version = require_version_one(document["version"], source=normalized_source)
    defaults = _launch_values(
        document["defaults"],
        normalized_source,
        ("defaults",),
        allowed=_USER_VALUE_FIELDS,
    )
    if defaults == LaunchValues():
        fail(
            source=normalized_source,
            path=("defaults",),
            code="EMPTY_LAUNCH_CONFIG",
            message="User launch defaults must not be empty",
        )
    return UserLaunchConfig(version, normalized_source, defaults)


def discover_user_launch(source: Path | None = None) -> UserLaunchConfig | None:
    """Load optional user defaults from the standard or caller-selected path."""
    candidate = (
        Path("~/.config/rundra/config.yaml").expanduser()
        if source is None
        else source.expanduser()
    ).resolve()
    if not candidate.exists():
        return None
    return load_user_launch(candidate)


def resolve_launch(
    *,
    cli: LaunchValues | None = None,
    project: ProjectLaunchConfig | None = None,
    user: UserLaunchConfig | None = None,
    builtins: LaunchValues | None = None,
    profile: str | None = None,
) -> ResolvedLaunch:
    """Resolve launch layers without I/O, prompting, planning, or entropy."""
    cli = cli or LaunchValues()
    builtins = builtins or LaunchValues()
    for name, value, expected in (
        ("cli", cli, LaunchValues),
        ("project", project, ProjectLaunchConfig),
        ("user", user, UserLaunchConfig),
        ("builtins", builtins, LaunchValues),
    ):
        if value is not None and type(value) is not expected:
            raise TypeError(f"resolve_launch {name} has an invalid type")
    if profile is not None and (type(profile) is not str or not profile.strip()):
        raise LaunchResolutionError("INVALID_PROFILE", "Profile must be nonblank")
    selected_profile = profile
    if selected_profile is None and project is not None:
        selected_profile = project.default_profile
    if selected_profile is not None:
        if project is None or selected_profile not in project.profiles:
            raise LaunchResolutionError(
                "PROFILE_NOT_FOUND",
                f"Launch profile '{selected_profile}' is not defined",
            )

    values = LaunchValues()
    sources: dict[str, str] = {}
    layers: list[tuple[str, LaunchValues]] = [("built_in", builtins)]
    if user is not None:
        layers.append(("user", user.defaults))
    if project is not None:
        layers.append(("project", project.defaults))
        if selected_profile is not None:
            layers.append(
                (
                    f"project_profile:{selected_profile}",
                    project.profiles[selected_profile],
                )
            )
    layers.append(("cli", cli))
    for source_name, layer in layers:
        values = _overlay(values, layer)
        for field in _VALUE_NAMES:
            if getattr(layer, field) is not None:
                sources[field] = source_name
    return ResolvedLaunch(values, sources, selected_profile)


def _launch_values(
    value: object,
    source: Path,
    path: ConfigPath,
    *,
    allowed: frozenset[str] = _LAUNCH_VALUE_FIELDS,
) -> LaunchValues:
    section = expect_mapping(value, source=source, path=path)
    _reject_credential_fields(section, source, path)
    check_fields(
        section,
        allowed=allowed,
        required=frozenset(),
        source=source,
        path=path,
    )
    return LaunchValues(
        config=_optional_path(section, "config", source, path),
        seed=_optional_seed(section, source, path),
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
        targets_file=_optional_path(section, "targets_file", source, path),
        data_dir=_optional_path(section, "data_dir", source, path),
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


def _optional_seed(
    section: Mapping[str, object],
    source: Path,
    path: ConfigPath,
) -> int | None:
    if "seed" not in section:
        return None
    value = section["seed"]
    if type(value) is not int:
        fail(
            source=source,
            path=(*path, "seed"),
            code="INVALID_TYPE",
            message="Expected an integer",
        )
    return value


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


def _overlay(base: LaunchValues, override: LaunchValues) -> LaunchValues:
    return LaunchValues(
        config=override.config if override.config is not None else base.config,
        seed=override.seed if override.seed is not None else base.seed,
        target=override.target if override.target is not None else base.target,
        source_root=(
            override.source_root
            if override.source_root is not None
            else base.source_root
        ),
        destination=(
            override.destination
            if override.destination is not None
            else base.destination
        ),
        targets_file=(
            override.targets_file
            if override.targets_file is not None
            else base.targets_file
        ),
        data_dir=override.data_dir if override.data_dir is not None else base.data_dir,
    )
