from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePath

from rundra.domain.models import ResourceRequest

PREPARE_LOCATIONS = frozenset({"auto", "local", "target"})
CACHE_SCOPES = frozenset({"target", "architecture"})
DEFINITION_BUILD_MODES = frozenset({"unprivileged", "fakeroot"})


@dataclass(frozen=True, slots=True)
class PreparationSourceGit:
    """One immutable Git source selected by a full commit identity."""

    url: str
    revision: str

    def __post_init__(self) -> None:
        if type(self.url) is not str or not self.url.strip() or "\x00" in self.url:
            raise ValueError("Preparation Git URL must be nonblank and safe")
        if (
            type(self.revision) is not str
            or len(self.revision) != 40
            or any(character not in "0123456789abcdef" for character in self.revision)
        ):
            raise ValueError("Preparation Git revision must be a lowercase full SHA-1")


@dataclass(frozen=True, slots=True)
class PreparationSourceWorkingTree:
    """An explicit working-tree-only source recipe."""


@dataclass(frozen=True, slots=True)
class PreparationOutput:
    """One output that must exist after application compilation."""

    path: PurePath
    executable: bool = False

    def __post_init__(self) -> None:
        _require_safe_relative(self.path, field_name="Preparation output path")
        if type(self.executable) is not bool:
            raise TypeError("Preparation output executable must be a boolean")


@dataclass(frozen=True, slots=True)
class PreparationBuild:
    """A shell-free build recipe and its bounded resources."""

    argv: tuple[str, ...]
    outputs: tuple[PreparationOutput, ...]
    cache_scope: str
    resources: ResourceRequest

    def __post_init__(self) -> None:
        argv = _string_sequence(self.argv, field_name="Preparation build argv")
        outputs = tuple(self.outputs)
        if not outputs or any(type(item) is not PreparationOutput for item in outputs):
            raise ValueError("Preparation build outputs must not be empty")
        if len({str(item.path) for item in outputs}) != len(outputs):
            raise ValueError("Preparation build outputs must be unique")
        if self.cache_scope not in CACHE_SCOPES:
            raise ValueError("Preparation cache scope is unsupported")
        if type(self.resources) is not ResourceRequest:
            raise TypeError("Preparation build resources must be a ResourceRequest")
        if self.resources.memory_bytes is None or self.resources.walltime is None:
            raise ValueError("Preparation build memory and walltime must be bounded")
        object.__setattr__(self, "argv", argv)
        object.__setattr__(self, "outputs", outputs)


@dataclass(frozen=True, slots=True)
class PreparationImage:
    """A logical image filename and immutable external identity."""

    name: PurePath
    uri: str
    sha256: str

    def __post_init__(self) -> None:
        _require_safe_relative(self.name, field_name="Preparation image name")
        if len(self.name.parts) != 1:
            raise ValueError("Preparation image name must be a filename")
        if type(self.uri) is not str or not self.uri.strip() or "\x00" in self.uri:
            raise ValueError("Preparation image URI must be nonblank and safe")
        if (
            type(self.sha256) is not str
            or len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
        ):
            raise ValueError("Preparation image SHA-256 must be lowercase hex")


@dataclass(frozen=True, slots=True)
class PreparationImageDefinition:
    """A SIF image built from one definition inside the source snapshot."""

    name: PurePath
    path: PurePath
    resources: ResourceRequest
    context: tuple[PurePath, ...] | None = None

    def __post_init__(self) -> None:
        _require_safe_relative(self.name, field_name="Preparation image name")
        if len(self.name.parts) != 1 or self.name.suffix != ".sif":
            raise ValueError("Preparation image name must be one .sif filename")
        _require_safe_relative(self.path, field_name="Definition path")
        if type(self.resources) is not ResourceRequest:
            raise TypeError("Definition build resources must be a ResourceRequest")
        if self.resources.memory_bytes is None or self.resources.walltime is None:
            raise ValueError("Definition build memory and walltime must be bounded")
        if self.context is not None:
            context = tuple(self.context)
            if any(not isinstance(item, PurePath) for item in context):
                raise TypeError("Definition context must contain paths")
            for item in context:
                _require_safe_relative(item, field_name="Definition context path")
            if len(set(context)) != len(context):
                raise ValueError("Definition context paths must be unique")
            object.__setattr__(self, "context", context)


@dataclass(frozen=True, slots=True)
class PreparationConfig:
    """Project-owned source, image, and optional application build recipe."""

    source: PreparationSourceGit | PreparationSourceWorkingTree
    image: PreparationImage | PreparationImageDefinition
    build: PreparationBuild | None

    def __post_init__(self) -> None:
        if type(self.source) not in {
            PreparationSourceGit,
            PreparationSourceWorkingTree,
        }:
            raise TypeError("Preparation source is unsupported")
        if type(self.image) not in {PreparationImage, PreparationImageDefinition}:
            raise TypeError("Preparation image recipe is unsupported")
        if self.build is not None and type(self.build) is not PreparationBuild:
            raise TypeError("Preparation build must be a build recipe or None")


@dataclass(frozen=True, slots=True)
class PreparationPlan:
    """Pure preparation intent; no candidate has been probed or resolved."""

    recipe: PreparationConfig
    source_mode: str
    source_root: PurePath | None
    requested_location: str = "auto"
    selected_location: str | None = None
    rebuild: bool = False
    rebuild_image: bool = False
    offline: bool = False
    possible_actions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.recipe) is not PreparationConfig:
            raise TypeError("PreparationPlan recipe must be PreparationConfig")
        if self.source_mode not in {"git", "working_tree"}:
            raise ValueError("PreparationPlan source mode is unsupported")
        if self.source_mode == "working_tree" and self.source_root is None:
            raise ValueError("Working-tree preparation requires a source root")
        if self.source_root is not None and not isinstance(self.source_root, PurePath):
            raise TypeError("PreparationPlan source root must be a path or None")
        if self.requested_location not in PREPARE_LOCATIONS:
            raise ValueError("PreparationPlan location is unsupported")
        if self.selected_location is not None and self.selected_location not in {
            "local",
            "target",
        }:
            raise ValueError("PreparationPlan selected location is unsupported")
        if any(
            type(value) is not bool
            for value in (self.rebuild, self.rebuild_image, self.offline)
        ):
            raise TypeError("PreparationPlan flags must be booleans")
        object.__setattr__(
            self,
            "possible_actions",
            _string_sequence(
                self.possible_actions,
                field_name="PreparationPlan possible actions",
                allow_empty=True,
            ),
        )


@dataclass(frozen=True, slots=True)
class PreparedOutput:
    """One verified output in a prepared source snapshot."""

    path: PurePath
    sha256: str
    executable: bool

    def __post_init__(self) -> None:
        _require_safe_relative(self.path, field_name="Prepared output path")
        _require_sha256(self.sha256, field_name="Prepared output SHA-256")
        if type(self.executable) is not bool:
            raise TypeError("Prepared output executable must be a boolean")


@dataclass(frozen=True, slots=True)
class PreparationRecord:
    """Resolved preparation provenance persisted with a version-2 Run."""

    source_identity: str
    source_digest: str
    source_action: str
    image_uri: str
    image_sha256: str | None
    image_path: PurePath
    image_action: str
    resolution_location: str
    image_recipe_key: str | None = None
    build_cache_key: str | None = None
    builder_location: str | None = None
    builder_scheduler_id: str | None = None
    builder_status: str | None = None
    builder_state: str | None = None
    build_action: str | None = None
    build_outputs: tuple[PreparedOutput, ...] = ()
    logs: tuple[PurePath, ...] = ()

    def __post_init__(self) -> None:
        for name in ("source_identity", "source_action", "image_uri", "image_action"):
            value = getattr(self, name)
            if type(value) is not str or not value or "\x00" in value:
                raise ValueError(f"PreparationRecord {name} must be safe and nonblank")
        _require_sha256(self.source_digest, field_name="Source digest")
        if self.image_sha256 is not None:
            _require_sha256(self.image_sha256, field_name="Image digest")
        if self.image_recipe_key is not None:
            _require_sha256(self.image_recipe_key, field_name="Image recipe key")
        if self.image_sha256 is None and self.image_recipe_key is None:
            raise ValueError("PreparationRecord requires an image digest or recipe key")
        if (
            not isinstance(self.image_path, PurePath)
            or not self.image_path.is_absolute()
        ):
            raise ValueError("PreparationRecord image path must be absolute")
        if self.resolution_location not in {"local", "target"}:
            raise ValueError("PreparationRecord resolution location is unsupported")
        for name in (
            "build_cache_key",
            "builder_location",
            "builder_scheduler_id",
            "builder_status",
            "builder_state",
            "build_action",
        ):
            value = getattr(self, name)
            if value is not None and (
                type(value) is not str or not value or "\x00" in value
            ):
                raise ValueError(f"PreparationRecord {name} must be safe or None")
        if self.build_cache_key is not None:
            _require_sha256(self.build_cache_key, field_name="Build cache key")
        if self.builder_status is not None and self.builder_status not in {
            "SUBMITTED",
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
            "UNKNOWN",
        }:
            raise ValueError("PreparationRecord builder status is unsupported")
        outputs = tuple(self.build_outputs)
        if any(type(item) is not PreparedOutput for item in outputs):
            raise TypeError("PreparationRecord outputs must be PreparedOutputs")
        logs = tuple(self.logs)
        if any(not isinstance(item, PurePath) for item in logs):
            raise TypeError("PreparationRecord logs must be paths")
        object.__setattr__(self, "build_outputs", outputs)
        object.__setattr__(self, "logs", logs)


@dataclass(frozen=True, slots=True)
class PreparationStorageConfig:
    """Operator-selected cache root and non-recursive image search paths."""

    cache_root: PurePath | None = None
    image_search_paths: tuple[PurePath, ...] = ()
    definition_build: DefinitionBuildPolicy | None = None

    def __post_init__(self) -> None:
        if self.cache_root is not None and not isinstance(self.cache_root, PurePath):
            raise TypeError("Preparation cache root must be a path or None")
        paths = tuple(self.image_search_paths)
        if any(not isinstance(path, PurePath) for path in paths):
            raise TypeError("Preparation image search paths must be paths")
        if len(set(paths)) != len(paths):
            raise ValueError("Preparation image search paths must be unique")
        if (
            self.definition_build is not None
            and type(self.definition_build) is not DefinitionBuildPolicy
        ):
            raise TypeError("Definition build policy has an invalid type")
        object.__setattr__(self, "image_search_paths", paths)


@dataclass(frozen=True, slots=True)
class DefinitionBuildPolicy:
    """Target-owned privilege, location, and resource limits for SIF builds."""

    allowed_locations: tuple[str, ...]
    mode: str
    max_resources: ResourceRequest

    def __post_init__(self) -> None:
        locations = tuple(self.allowed_locations)
        if not locations or any(item not in {"local", "target"} for item in locations):
            raise ValueError("Definition build locations must be local or target")
        if len(set(locations)) != len(locations):
            raise ValueError("Definition build locations must be unique")
        if self.mode not in DEFINITION_BUILD_MODES:
            raise ValueError("Definition build mode is unsupported")
        if type(self.max_resources) is not ResourceRequest:
            raise TypeError("Definition build resource ceiling is invalid")
        if (
            self.max_resources.memory_bytes is None
            or self.max_resources.walltime is None
        ):
            raise ValueError("Definition build resource ceilings must be bounded")
        object.__setattr__(self, "allowed_locations", locations)


def source_recipe_identity(
    source: PreparationSourceGit | PreparationSourceWorkingTree,
) -> str:
    """Return a deterministic identity for an acquired-source recipe."""
    if type(source) is PreparationSourceGit:
        return _digest({"kind": "git", "revision": source.revision, "url": source.url})
    if type(source) is PreparationSourceWorkingTree:
        return _digest({"kind": "working_tree"})
    raise TypeError("source_recipe_identity source is unsupported")


def definition_image_recipe_key(
    *,
    source_digest: str,
    image: PreparationImageDefinition,
    target_name: str,
    mode: str,
    platform_fingerprint: str,
    builder_version: str,
) -> str:
    """Return a deterministic cache key for one definition image build."""
    _require_sha256(source_digest, field_name="Source digest")
    if type(image) is not PreparationImageDefinition:
        raise TypeError("definition image key requires a definition recipe")
    for name, value in (
        ("target_name", target_name),
        ("platform_fingerprint", platform_fingerprint),
        ("builder_version", builder_version),
    ):
        if type(value) is not str or not value.strip() or "\x00" in value:
            raise ValueError(f"{name} must be safe and nonblank")
    if mode not in DEFINITION_BUILD_MODES:
        raise ValueError("definition build mode is unsupported")
    return _digest(
        {
            "builder_version": builder_version,
            "definition": str(image.path),
            "mode": mode,
            "name": str(image.name),
            "platform": platform_fingerprint,
            "source_digest": source_digest,
            "target": target_name,
        }
    )


def build_cache_key(
    *,
    source_digest: str,
    image_digest: str,
    build: PreparationBuild,
    builder_scope: str,
    platform_fingerprint: str,
) -> str:
    """Return the deterministic prepared-source cache key."""
    for name, value in (
        ("source_digest", source_digest),
        ("image_digest", image_digest),
        ("builder_scope", builder_scope),
        ("platform_fingerprint", platform_fingerprint),
    ):
        if type(value) is not str or not value:
            raise ValueError(f"{name} must be nonblank")
    if type(build) is not PreparationBuild:
        raise TypeError("build_cache_key build must be PreparationBuild")
    return _digest(
        {
            "build": {
                "argv": list(build.argv),
                "cache_scope": build.cache_scope,
                "outputs": [
                    {"executable": output.executable, "path": str(output.path)}
                    for output in build.outputs
                ],
            },
            "builder_scope": builder_scope,
            "image_digest": image_digest,
            "platform_fingerprint": platform_fingerprint,
            "source_digest": source_digest,
        }
    )


def _digest(value: object) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _require_safe_relative(value: PurePath, *, field_name: str) -> None:
    if not isinstance(value, PurePath):
        raise TypeError(f"{field_name} must be a path")
    if value.is_absolute() or not value.parts or value == PurePath("."):
        raise ValueError(f"{field_name} must be relative")
    if ".." in value.parts or any("\x00" in part for part in value.parts):
        raise ValueError(f"{field_name} must not escape its root")


def _require_sha256(value: str, *, field_name: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field_name} must be lowercase hexadecimal")


def _string_sequence(
    value: Sequence[str], *, field_name: str, allow_empty: bool = False
) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{field_name} must be a sequence")
    normalized = tuple(value)
    if (not normalized and not allow_empty) or any(
        type(item) is not str or not item or "\x00" in item for item in normalized
    ):
        raise ValueError(f"{field_name} must contain safe nonempty strings")
    return normalized
