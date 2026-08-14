from __future__ import annotations

from pathlib import Path


class ConfigError(Exception):
    """Actionable configuration failure with stable machine-readable fields."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        source: Path,
        path: tuple[str | int, ...] = (),
    ) -> None:
        self.code = code
        self.message = message
        self.source = source
        self.path = path
        location = ".".join(str(component) for component in path) or "<document>"
        super().__init__(f"{source}:{location}: {message}")
