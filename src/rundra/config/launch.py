from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType

from rundra.config._schema import (
    ConfigPath,
    check_fields,
    expect_integer,
    expect_mapping,
    expect_string,
    expect_string_list,
    fail,
)
from rundra.config._yaml import read_yaml_document
from rundra.config.preparation import parse_preparation
from rundra.domain.preparation import PreparationConfig, PreparationStorageConfig
from rundra.schema_versions import PROJECT_CONFIG_SCHEMA, USER_CONFIG_SCHEMA
from rundra.security import is_credential_field

_PROJECT_V1_FIELDS = frozenset({"version", "default_profile", "defaults", "profiles"})
_PROJECT_V2_FIELDS = _PROJECT_V1_FIELDS | {"preparation"}
_PROJECT_V3_FIELDS = _PROJECT_V2_FIELDS
_PROJECT_V4_FIELDS = _PROJECT_V3_FIELDS
_PROJECT_V5_FIELDS = _PROJECT_V4_FIELDS
_PROJECT_V6_FIELDS = _PROJECT_V5_FIELDS
_LAUNCH_VALUE_FIELDS = frozenset(
    {
        "config",
        "seed",
        "target",
        "source_root",
        "destination",
        "workers",
        "task_slots_per_worker",
        "fetch_mode",
    }
)
_USER_V1_FIELDS = frozenset({"version", "defaults"})
_USER_V2_FIELDS = _USER_V1_FIELDS | {"preparation"}
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
    "workers",
    "task_slots_per_worker",
    "fetch_mode",
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
    workers: int | None = None
    task_slots_per_worker: int | None = None
    fetch_mode: str | None = None

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
        for name in ("workers", "task_slots_per_worker"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"LaunchValues {name} must be positive or None")
        if self.fetch_mode is not None and self.fetch_mode not in {
            "auto",
            "copy",
            "reference",
            "archive",
        }:
            raise ValueError("LaunchValues fetch_mode is unsupported")


@dataclass(frozen=True, slots=True)
class ProjectLaunchConfig:
    """Strict versioned project launch defaults and preparation recipe."""

    version: int
    source: Path
    defaults: LaunchValues
    profiles: Mapping[str, LaunchValues]
    default_profile: str | None = None
    preparation: PreparationConfig | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int:
            raise ValueError("ProjectLaunchConfig version must be an int")
        if self.version not in PROJECT_CONFIG_SCHEMA.supported:
            raise ValueError(
                "ProjectLaunchConfig version must be 1, 2, 3, 4, 5, or 6"
            )
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
        if self.version == 1 and self.preparation is not None:
            raise ValueError("ProjectLaunchConfig v1 cannot define preparation")
        if (
            self.version in {2, 3, 4, 5, 6}
            and type(self.preparation) is not PreparationConfig
        ):
            raise ValueError("ProjectLaunchConfig v2+ requires preparation")
        object.__setattr__(self, "profiles", MappingProxyType(profiles))

    @property
    def project_root(self) -> Path:
        """Return the directory that owns relative project launch paths."""
        return self.source.parent


@dataclass(frozen=True, slots=True)
class UserLaunchConfig:
    """Strict per-user launch defaults and local preparation storage."""

    version: int
    source: Path
    defaults: LaunchValues
    preparation: PreparationStorageConfig = PreparationStorageConfig()

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version not in {1, 2}:
            raise ValueError("UserLaunchConfig version must be 1 or 2")
        if not isinstance(self.source, Path) or not self.source.is_absolute():
            raise ValueError("UserLaunchConfig source must be an absolute Path")
        if type(self.defaults) is not LaunchValues:
            raise TypeError("UserLaunchConfig defaults must be LaunchValues")
        if type(self.preparation) is not PreparationStorageConfig:
            raise TypeError("UserLaunchConfig preparation has an invalid type")
        if self.version == 1 and self.preparation != PreparationStorageConfig():
            raise ValueError("UserLaunchConfig v1 cannot define preparation")


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
    if "version" not in document:
        fail(
            source=normalized_source,
            path=("version",),
            code="MISSING_FIELD",
            message="Required field 'version' is missing",
        )
    version = expect_integer(
        document["version"],
        source=normalized_source,
        path=("version",),
        minimum=1,
    )
    if version not in PROJECT_CONFIG_SCHEMA.supported:
        fail(
            source=normalized_source,
            path=("version",),
            code="UNSUPPORTED_VERSION",
            message=(
                "Unsupported project config version; supported versions are 1 through 6"
            ),
        )
    check_fields(
        document,
        allowed=(
            _PROJECT_V1_FIELDS
            if version == 1
            else _PROJECT_V2_FIELDS
            if version == 2
            else _PROJECT_V3_FIELDS
            if version == 3
            else _PROJECT_V4_FIELDS
            if version == 4
            else _PROJECT_V5_FIELDS
            if version == 5
            else _PROJECT_V6_FIELDS
        ),
        required=(
            frozenset({"version"})
            if version == 1
            else frozenset({"version", "preparation"})
        ),
        source=normalized_source,
        path=(),
    )
    version_number = version
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
    if version_number < 5:
        if defaults.fetch_mode is not None:
            fail(
                source=normalized_source,
                path=("defaults", "fetch_mode"),
                code="UNKNOWN_FIELD",
                message="fetch_mode requires project configuration version 5",
            )
        for name, values in profiles.items():
            if values.fetch_mode is not None:
                fail(
                    source=normalized_source,
                    path=("profiles", name, "fetch_mode"),
                    code="UNKNOWN_FIELD",
                    message="fetch_mode requires project configuration version 5",
                )
    if version_number == 1 and defaults == LaunchValues() and not profiles:
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
    preparation = (
        parse_preparation(
            document["preparation"],
            source=normalized_source,
            version=version,
        )
        if version_number in {2, 3, 4, 5, 6}
        else None
    )
    return ProjectLaunchConfig(
        version=version_number,
        source=normalized_source,
        defaults=defaults,
        profiles=profiles,
        default_profile=default_profile,
        preparation=preparation,
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
    if "version" not in document:
        fail(
            source=normalized_source,
            path=("version",),
            code="MISSING_FIELD",
            message="Required field 'version' is missing",
        )
    version = expect_integer(
        document["version"], source=normalized_source, path=("version",), minimum=1
    )
    if version not in USER_CONFIG_SCHEMA.supported:
        fail(
            source=normalized_source,
            path=("version",),
            code="UNSUPPORTED_VERSION",
            message="Unsupported user config version; supported versions are 1 and 2",
        )
    check_fields(
        document,
        allowed=_USER_V1_FIELDS if version == 1 else _USER_V2_FIELDS,
        required=(
            frozenset({"version", "defaults"})
            if version == 1
            else frozenset({"version", "preparation"})
        ),
        source=normalized_source,
        path=(),
    )
    defaults = (
        _launch_values(
            document["defaults"],
            normalized_source,
            ("defaults",),
            allowed=_USER_VALUE_FIELDS,
        )
        if "defaults" in document
        else LaunchValues()
    )
    if version == 1 and defaults == LaunchValues():
        fail(
            source=normalized_source,
            path=("defaults",),
            code="EMPTY_LAUNCH_CONFIG",
            message="User launch defaults must not be empty",
        )
    preparation = (
        _user_preparation_storage(
            document["preparation"], normalized_source, ("preparation",)
        )
        if version == 2
        else PreparationStorageConfig()
    )
    return UserLaunchConfig(version, normalized_source, defaults, preparation)


def _user_preparation_storage(
    value: object,
    source: Path,
    path: ConfigPath,
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
            message="User preparation storage must not be empty",
        )
    cache_root = _optional_path(section, "cache_root", source, path)
    search_paths = tuple(
        _resolve_declared_path(raw, source)
        for raw in expect_string_list(
            section.get("image_search_paths", []),
            source=source,
            path=(*path, "image_search_paths"),
        )
    )
    return PreparationStorageConfig(cache_root, search_paths)


def _resolve_declared_path(raw: str, source: Path) -> Path:
    declared = Path(raw).expanduser()
    if not declared.is_absolute():
        declared = source.parent / declared
    return declared.resolve()


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
    implicit_target_profile = selected_profile if project is None else None
    if selected_profile is not None:
        if project is not None and selected_profile not in project.profiles:
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
        project_defaults = project.defaults
        profile_values = (
            project.profiles[selected_profile] if selected_profile is not None else None
        )
        project_target = (
            profile_values.target
            if profile_values is not None and profile_values.target is not None
            else project_defaults.target
        )
        if (
            cli.target is not None
            and project_target is not None
            and cli.target != project_target
        ):
            project_defaults = _without_worker_scale(project_defaults)
            if profile_values is not None:
                profile_values = _without_worker_scale(profile_values)
        layers.append(("project", project_defaults))
        if selected_profile is not None:
            assert profile_values is not None
            layers.append(
                (
                    f"project_profile:{selected_profile}",
                    profile_values,
                )
            )
    if implicit_target_profile is not None:
        layers.append(
            (
                f"target_profile:{implicit_target_profile}",
                LaunchValues(target=implicit_target_profile),
            )
        )
    layers.append(("cli", cli))
    for source_name, layer in layers:
        values = _overlay(values, layer)
        for field in _VALUE_NAMES:
            if getattr(layer, field) is not None:
                sources[field] = source_name
    return ResolvedLaunch(values, sources, selected_profile)


def _without_worker_scale(values: LaunchValues) -> LaunchValues:
    return replace(values, workers=None, task_slots_per_worker=None)


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
        workers=_optional_positive_integer(section, "workers", source, path),
        task_slots_per_worker=_optional_positive_integer(
            section, "task_slots_per_worker", source, path
        ),
        fetch_mode=_optional_fetch_mode(section, source, path),
    )


def _optional_fetch_mode(
    section: Mapping[str, object], source: Path, path: ConfigPath
) -> str | None:
    if "fetch_mode" not in section:
        return None
    mode = expect_string(
        section["fetch_mode"],
        source=source,
        path=(*path, "fetch_mode"),
        nonblank=True,
    )
    if mode not in {"auto", "copy", "reference", "archive"}:
        fail(
            source=source,
            path=(*path, "fetch_mode"),
            code="INVALID_VALUE",
            message="fetch_mode must be auto, copy, reference, or archive",
        )
    return mode


def _optional_positive_integer(
    section: Mapping[str, object],
    field: str,
    source: Path,
    path: ConfigPath,
) -> int | None:
    if field not in section:
        return None
    return expect_integer(section[field], source=source, path=(*path, field), minimum=1)


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
        workers=override.workers if override.workers is not None else base.workers,
        task_slots_per_worker=(
            override.task_slots_per_worker
            if override.task_slots_per_worker is not None
            else base.task_slots_per_worker
        ),
        fetch_mode=(
            override.fetch_mode if override.fetch_mode is not None else base.fetch_mode
        ),
    )
