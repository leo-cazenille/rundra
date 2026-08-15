from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Self

type ErrorDetail = str | int | bool | tuple[str | int, ...]


@dataclass(frozen=True, slots=True)
class OperationError:
    """A structured, renderer-independent operation failure."""

    code: str
    message: str
    details: Mapping[str, ErrorDetail] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.code) is not str or type(self.message) is not str:
            raise TypeError("Operation error code and message must be strings")
        if not self.code or not self.message:
            raise ValueError("Operation errors require a code and message")
        if not isinstance(self.details, Mapping) or any(
            type(key) is not str or not _is_error_detail(value)
            for key, value in self.details.items()
        ):
            raise TypeError("Operation error details must contain JSON-safe values")
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))


@dataclass(frozen=True, slots=True)
class OperationResult[T]:
    """One value consumed by both human and machine-readable renderers."""

    operation: str
    value: T | None = None
    error: OperationError | None = None

    def __post_init__(self) -> None:
        if type(self.operation) is not str:
            raise TypeError("Operation result names must be strings")
        if not self.operation:
            raise ValueError("Operation results require an operation name")
        if self.error is not None and type(self.error) is not OperationError:
            raise TypeError("Operation result error must be an OperationError")
        if (self.value is None) == (self.error is None):
            raise ValueError("Operation results contain exactly one of value or error")

    @property
    def ok(self) -> bool:
        return self.error is None

    @classmethod
    def success(cls, operation: str, value: T) -> Self:
        return cls(operation=operation, value=value)

    @classmethod
    def failure(cls, operation: str, error: OperationError) -> Self:
        return cls(operation=operation, error=error)


def _is_error_detail(value: object) -> bool:
    if type(value) in (str, int, bool):
        return True
    return isinstance(value, tuple) and all(type(item) in (str, int) for item in value)
