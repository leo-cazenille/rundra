from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from rundra.config._schema import (
    ConfigPath,
    check_fields,
    expect_boolean,
    expect_integer,
    expect_mapping,
    expect_string,
    fail,
    require_version_one,
)
from rundra.config._yaml import read_yaml_document
from rundra.config.errors import ConfigError
from rundra.domain.campaigns import CampaignFailurePolicy, valid_campaign_launch_name
from rundra.security import is_credential_field

_RANGE = re.compile(r"(-?[0-9]+):(-?[0-9]+)\Z")
_CAMPAIGN_FIELDS = frozenset({"on_submit_failure", "allow_duplicate_tasks", "launches"})
_STANDALONE_FIELDS = _CAMPAIGN_FIELDS | {
    "kind",
    "version",
    "name",
    "experiment",
    "project_file",
}
_LAUNCH_FIELDS = frozenset(
    {
        "name",
        "profile",
        "target",
        "config",
        "source_root",
        "destination",
        "seed",
        "seeds",
        "workers",
        "task_slots_per_worker",
        "fetch_mode",
    }
)


@dataclass(frozen=True, slots=True)
class CampaignSeedSelector:
    start: int
    stop: int

    def __post_init__(self) -> None:
        if type(self.start) is not int or type(self.stop) is not int:
            raise TypeError("Campaign seeds must be integers")
        if self.stop < self.start:
            raise ValueError("Campaign seed range stop must be at least start")

    @property
    def count(self) -> int:
        return self.stop - self.start + 1

    def values(self) -> range:
        return range(self.start, self.stop + 1)


@dataclass(frozen=True, slots=True)
class CampaignLaunchConfig:
    name: str
    seeds: CampaignSeedSelector
    profile: str | None = None
    target: str | None = None
    config: Path | None = None
    source_root: Path | None = None
    destination: Path | None = None
    workers: int | None = None
    task_slots_per_worker: int | None = None
    fetch_mode: str | None = None

    def __post_init__(self) -> None:
        if not valid_campaign_launch_name(self.name):
            raise ValueError("Campaign launch name is not filesystem-safe")
        if type(self.seeds) is not CampaignSeedSelector:
            raise TypeError("Campaign launch seeds are invalid")


@dataclass(frozen=True, slots=True)
class CampaignDefinition:
    version: int
    name: str
    source: Path
    experiment: Path | None
    project_file: Path | None
    on_submit_failure: CampaignFailurePolicy
    allow_duplicate_tasks: bool
    launches: tuple[CampaignLaunchConfig, ...]

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("CampaignDefinition version must be 1")
        if type(self.name) is not str or not self.name.strip():
            raise ValueError("CampaignDefinition name must be nonblank")
        if not isinstance(self.source, Path) or not self.source.is_absolute():
            raise ValueError("CampaignDefinition source must be absolute")
        launches = tuple(self.launches)
        if not launches or any(
            type(item) is not CampaignLaunchConfig for item in launches
        ):
            raise ValueError("CampaignDefinition launches are invalid")
        if len({item.name for item in launches}) != len(launches):
            raise ValueError("Campaign launch names must be unique")
        destinations = [str(item.destination) for item in launches if item.destination]
        if len(set(destinations)) != len(destinations):
            raise ValueError("Explicit campaign destinations must be unique")
        object.__setattr__(self, "launches", launches)


@dataclass(frozen=True, slots=True)
class ProjectCampaigns:
    campaigns: Mapping[str, CampaignDefinition]

    def __post_init__(self) -> None:
        object.__setattr__(self, "campaigns", MappingProxyType(dict(self.campaigns)))


def load_campaign(source: Path) -> CampaignDefinition:
    normalized = source.expanduser().resolve()
    document = expect_mapping(
        read_yaml_document(normalized), source=normalized, path=()
    )
    _reject_credentials(document, normalized, ())
    check_fields(
        document,
        allowed=_STANDALONE_FIELDS,
        required=frozenset({"kind", "version", "name", "experiment", "launches"}),
        source=normalized,
        path=(),
    )
    kind = expect_string(document["kind"], source=normalized, path=("kind",))
    if kind != "campaign":
        fail(
            source=normalized,
            path=("kind",),
            code="INVALID_VALUE",
            message="kind must be 'campaign'",
        )
    require_version_one(document["version"], source=normalized)
    name = expect_string(
        document["name"], source=normalized, path=("name",), nonblank=True
    )
    experiment = _path(document["experiment"], normalized, ("experiment",))
    project_file = (
        _path(document["project_file"], normalized, ("project_file",))
        if "project_file" in document
        else None
    )
    return _definition(document, normalized, name, experiment, project_file, ())


def is_campaign_source(source: Path) -> bool:
    """Return whether a readable YAML document explicitly declares a campaign."""
    try:
        document = read_yaml_document(source.expanduser().resolve())
    except ConfigError:
        return False
    return isinstance(document, Mapping) and document.get("kind") == "campaign"


def parse_project_campaigns(
    value: object, *, source: Path
) -> Mapping[str, CampaignDefinition]:
    raw = expect_mapping(value, source=source, path=("campaigns",))
    campaigns: dict[str, CampaignDefinition] = {}
    for name, item in raw.items():
        expect_string(name, source=source, path=("campaigns", name), nonblank=True)
        document = expect_mapping(item, source=source, path=("campaigns", name))
        _reject_credentials(document, source, ("campaigns", name))
        check_fields(
            document,
            allowed=_CAMPAIGN_FIELDS,
            required=frozenset({"launches"}),
            source=source,
            path=("campaigns", name),
        )
        campaigns[name] = _definition(
            document, source, name, None, None, ("campaigns", name)
        )
    return MappingProxyType(campaigns)


def _definition(
    document: Mapping[str, object],
    source: Path,
    name: str,
    experiment: Path | None,
    project_file: Path | None,
    path: ConfigPath,
) -> CampaignDefinition:
    policy_text = (
        expect_string(
            document["on_submit_failure"],
            source=source,
            path=(*path, "on_submit_failure"),
        )
        if "on_submit_failure" in document
        else "cancel"
    )
    try:
        policy = CampaignFailurePolicy(policy_text)
    except ValueError:
        fail(
            source=source,
            path=(*path, "on_submit_failure"),
            code="INVALID_VALUE",
            message="on_submit_failure must be cancel, stop, or continue",
        )
    allow_duplicates = (
        expect_boolean(
            document["allow_duplicate_tasks"],
            source=source,
            path=(*path, "allow_duplicate_tasks"),
        )
        if "allow_duplicate_tasks" in document
        else False
    )
    raw_launches = document["launches"]
    if type(raw_launches) is not list or not raw_launches:
        fail(
            source=source,
            path=(*path, "launches"),
            code="INVALID_TYPE",
            message="launches must be a nonempty list",
        )
    launches = tuple(
        _launch(item, source, (*path, "launches", index))
        for index, item in enumerate(raw_launches)
    )
    names = [item.name for item in launches]
    if len(set(names)) != len(names):
        fail(
            source=source,
            path=(*path, "launches"),
            code="DUPLICATE_CAMPAIGN_LAUNCH",
            message="Campaign launch names must be unique",
        )
    destinations = [str(item.destination) for item in launches if item.destination]
    if len(set(destinations)) != len(destinations):
        fail(
            source=source,
            path=(*path, "launches"),
            code="DUPLICATE_CAMPAIGN_DESTINATION",
            message="Explicit campaign destinations must be unique",
        )
    return CampaignDefinition(
        1, name, source, experiment, project_file, policy, allow_duplicates, launches
    )


def _launch(value: object, source: Path, path: ConfigPath) -> CampaignLaunchConfig:
    document = expect_mapping(value, source=source, path=path)
    _reject_credentials(document, source, path)
    check_fields(
        document,
        allowed=_LAUNCH_FIELDS,
        required=frozenset({"name"}),
        source=source,
        path=path,
    )
    name = expect_string(
        document["name"], source=source, path=(*path, "name"), nonblank=True
    )
    if not valid_campaign_launch_name(name):
        fail(
            source=source,
            path=(*path, "name"),
            code="INVALID_VALUE",
            message="Launch name must match [a-z0-9][a-z0-9_-]{0,63}",
        )
    if ("seed" in document) == ("seeds" in document):
        fail(
            source=source,
            path=path,
            code="CAMPAIGN_SEEDS_REQUIRED",
            message="Exactly one of seed or seeds is required",
        )
    seeds = (
        CampaignSeedSelector(
            expect_integer(
                document["seed"], source=source, path=(*path, "seed"), minimum=-(2**63)
            ),
            expect_integer(
                document["seed"], source=source, path=(*path, "seed"), minimum=-(2**63)
            ),
        )
        if "seed" in document
        else _seed_range(document["seeds"], source, (*path, "seeds"))
    )
    text_fields = {
        name: expect_string(
            document[name], source=source, path=(*path, name), nonblank=True
        )
        if name in document
        else None
        for name in ("profile", "target", "fetch_mode")
    }
    if text_fields["fetch_mode"] not in {None, "auto", "copy", "reference", "archive"}:
        fail(
            source=source,
            path=(*path, "fetch_mode"),
            code="INVALID_VALUE",
            message="Unsupported fetch mode",
        )
    return CampaignLaunchConfig(
        name=name,
        seeds=seeds,
        profile=text_fields["profile"],
        target=text_fields["target"],
        config=_optional_path(document, "config", source, path),
        source_root=_optional_path(document, "source_root", source, path),
        destination=_optional_path(document, "destination", source, path),
        workers=_optional_positive(document, "workers", source, path),
        task_slots_per_worker=_optional_positive(
            document, "task_slots_per_worker", source, path
        ),
        fetch_mode=text_fields["fetch_mode"],
    )


def _seed_range(value: object, source: Path, path: ConfigPath) -> CampaignSeedSelector:
    text = expect_string(value, source=source, path=path)
    match = _RANGE.fullmatch(text)
    if match is None:
        fail(
            source=source,
            path=path,
            code="INVALID_SEED_RANGE",
            message="Expected inclusive START:STOP seed range",
        )
    start, stop = (int(match.group(1)), int(match.group(2)))
    if stop < start:
        fail(
            source=source,
            path=path,
            code="INVALID_SEED_RANGE",
            message="Seed range stop must be at least start",
        )
    return CampaignSeedSelector(start, stop)


def _path(value: object, source: Path, path: ConfigPath) -> Path:
    text = expect_string(value, source=source, path=path, nonblank=True)
    candidate = Path(text).expanduser()
    return (
        (source.parent / candidate).resolve()
        if not candidate.is_absolute()
        else candidate.resolve()
    )


def _optional_path(
    document: Mapping[str, object], name: str, source: Path, path: ConfigPath
) -> Path | None:
    return _path(document[name], source, (*path, name)) if name in document else None


def _optional_positive(
    document: Mapping[str, object], name: str, source: Path, path: ConfigPath
) -> int | None:
    return (
        expect_integer(document[name], source=source, path=(*path, name), minimum=1)
        if name in document
        else None
    )


def _reject_credentials(value: object, source: Path, path: ConfigPath) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if type(key) is str and is_credential_field(key):
                fail(
                    source=source,
                    path=(*path, key),
                    code="FORBIDDEN_FIELD",
                    message=f"Credential-bearing field '{key}' is forbidden",
                )
            _reject_credentials(item, source, (*path, str(key)))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_credentials(item, source, (*path, index))
