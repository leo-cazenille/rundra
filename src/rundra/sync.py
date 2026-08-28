from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath

DEFAULT_SYNC_EXCLUDES = (
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".rundra",
    ".agents",
    "retrieved",
    "tmp",
    "downloads",
    "*.py[cod]",
    "*.sif",
    "*.simg",
)


def with_default_sync_excludes(patterns: Iterable[str]) -> tuple[str, ...]:
    """Combine portable transient-file defaults with validated user patterns."""
    return (*DEFAULT_SYNC_EXCLUDES, *patterns)


class SyncExclusionError(ValueError):
    """Raised when a source synchronization exclusion is unsafe."""


class SourceSnapshotPreviewError(RuntimeError):
    """Raised when a source root cannot be inspected safely."""


@dataclass(frozen=True, slots=True)
class SourceSnapshotEntry:
    """One top-level contributor to a source snapshot estimate."""

    path: PurePosixPath
    file_count: int
    size_bytes: int


@dataclass(frozen=True, slots=True)
class SourceSnapshotPreview:
    """A network-free estimate of files included in source staging."""

    source_root: Path
    file_count: int
    size_bytes: int
    largest_entries: tuple[SourceSnapshotEntry, ...]
    excluded_patterns: tuple[str, ...]
    unreadable_entries: int = 0
    symlink_entries: int = 0

    @property
    def exact(self) -> bool:
        """Whether every included entry had an ordinary measurable representation."""
        return self.unreadable_entries == 0 and self.symlink_entries == 0


def validated_sync_excludes(patterns: Iterable[str]) -> tuple[str, ...]:
    """Return normalized built-in and user source exclusions."""
    normalized: list[str] = []
    for pattern in patterns:
        value = pattern.removeprefix("./").rstrip("/")
        path = PurePosixPath(value)
        if not value or path.is_absolute() or ".." in path.parts:
            raise SyncExclusionError(
                "Sync exclusion must be a nonempty safe relative exclusion: "
                f"{pattern!r}"
            )
        normalized.append(value)
    return with_default_sync_excludes(normalized)


def is_sync_excluded(relative: Path | PurePosixPath, patterns: Iterable[str]) -> bool:
    """Return whether a source-relative path matches an effective exclusion."""
    candidate = PurePosixPath(relative.as_posix())
    return any(
        fnmatchcase(candidate.as_posix(), pattern)
        or fnmatchcase(candidate.name, pattern)
        for pattern in patterns
    )


def preview_source_snapshot(
    source_root: Path,
    patterns: Iterable[str],
    *,
    workspace_root: Path | None = None,
    largest_limit: int = 5,
) -> SourceSnapshotPreview:
    """Estimate source bytes staged after exclusions without following symlinks."""
    if type(largest_limit) is not int or largest_limit < 0:
        raise ValueError("largest_limit must be a non-negative integer")
    try:
        source = source_root.expanduser().resolve(strict=True)
    except OSError as error:
        raise SourceSnapshotPreviewError(
            f"Source root does not exist: {source_root}"
        ) from error
    if not source.is_dir():
        raise SourceSnapshotPreviewError(f"Source root is not a directory: {source}")
    exclusions = validated_sync_excludes(patterns)
    workspace_relative: Path | None = None
    if workspace_root is not None:
        try:
            workspace_relative = workspace_root.expanduser().resolve().relative_to(source)
        except ValueError:
            pass
    totals: dict[str, list[int]] = {}
    unreadable = 0
    symlinks = 0

    def scan(directory: Path) -> None:
        nonlocal unreadable, symlinks
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError:
            unreadable += 1
            return
        for entry in entries:
            path = Path(entry.path)
            relative = path.relative_to(source)
            if workspace_relative is not None and (
                relative == workspace_relative or workspace_relative in relative.parents
            ):
                continue
            if is_sync_excluded(relative, exclusions):
                continue
            top_level = relative.parts[0]
            totals.setdefault(top_level, [0, 0])
            try:
                if entry.is_symlink():
                    symlinks += 1
                    totals[top_level][0] += 1
                    totals[top_level][1] += entry.stat(follow_symlinks=False).st_size
                elif entry.is_dir(follow_symlinks=False):
                    scan(path)
                elif entry.is_file(follow_symlinks=False):
                    totals[top_level][0] += 1
                    totals[top_level][1] += entry.stat(follow_symlinks=False).st_size
                else:
                    unreadable += 1
            except OSError:
                unreadable += 1

    scan(source)
    contributors = tuple(
        sorted(
            (
                SourceSnapshotEntry(PurePosixPath(name), values[0], values[1])
                for name, values in totals.items()
            ),
            key=lambda item: (-item.size_bytes, item.path.as_posix()),
        )[:largest_limit]
    )
    return SourceSnapshotPreview(
        source_root=source,
        file_count=sum(values[0] for values in totals.values()),
        size_bytes=sum(values[1] for values in totals.values()),
        largest_entries=contributors,
        excluded_patterns=exclusions,
        unreadable_entries=unreadable,
        symlink_entries=symlinks,
    )
