from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from rundra.domain.models import NativeValue


class PreflightStatus(StrEnum):
    """Outcome of one non-submitting infrastructure check."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    """One bounded check with an actionable failure and safe scalar details."""

    name: str
    layer: str
    status: PreflightStatus
    message: str
    corrective_action: str | None = None
    details: Mapping[str, NativeValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("name", "layer", "message"):
            value = getattr(self, name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"PreflightCheck {name} must be nonblank")
        if type(self.status) is not PreflightStatus:
            raise TypeError("PreflightCheck status must be a PreflightStatus")
        if self.corrective_action is not None and (
            type(self.corrective_action) is not str
            or not self.corrective_action.strip()
        ):
            raise ValueError("Preflight corrective action must be nonblank or None")
        if not isinstance(self.details, Mapping) or any(
            type(key) is not str or type(value) not in (str, int, bool)
            for key, value in self.details.items()
        ):
            raise TypeError("Preflight details must contain safe scalar values")
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))


@dataclass(frozen=True, slots=True)
class PreflightReport:
    """Complete non-submitting check set for one experiment and target."""

    target: str
    experiment: str
    checks: tuple[PreflightCheck, ...]

    def __post_init__(self) -> None:
        if type(self.target) is not str or not self.target.strip():
            raise ValueError("PreflightReport target must be nonblank")
        if type(self.experiment) is not str or not self.experiment.strip():
            raise ValueError("PreflightReport experiment must be nonblank")
        checks = tuple(self.checks)
        if not checks or any(type(check) is not PreflightCheck for check in checks):
            raise ValueError("PreflightReport requires PreflightCheck values")
        if len({check.name for check in checks}) != len(checks):
            raise ValueError("PreflightReport check names must be unique")
        object.__setattr__(self, "checks", checks)

    @property
    def ok(self) -> bool:
        return all(check.status is PreflightStatus.PASSED for check in self.checks)
