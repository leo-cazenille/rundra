from __future__ import annotations

from collections.abc import Iterable

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
