from __future__ import annotations

import os
from pathlib import Path, PurePath


class UnsafeLocalPathError(ValueError):
    """Raised when a local write destination can escape through a symlink."""


def resolve_write_destination(value: PurePath) -> Path:
    """Resolve a local destination only after rejecting symlinked components."""
    candidate = Path(str(value)).expanduser()
    absolute = Path(os.path.abspath(candidate))
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        if current.is_symlink():
            raise UnsafeLocalPathError(
                "Local write destination must not contain symbolic links"
            )
        if not current.exists():
            break
    return absolute.resolve()


def reject_destination_tree_symlinks(root: Path) -> None:
    """Reject existing descendants that could redirect later file writes."""
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_symlink():
            raise UnsafeLocalPathError(
                "Local write destination tree must not contain symbolic links"
            )
