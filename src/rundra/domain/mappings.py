from __future__ import annotations

from dataclasses import dataclass

from rundra.domain.models import TaskId


@dataclass(frozen=True, slots=True)
class ArrayTaskMapping:
    """Explicit backend extension mapping a logical Task to an array index."""

    task_id: TaskId
    seed: int
    array_index: int

    def __post_init__(self) -> None:
        if type(self.task_id) is not TaskId:
            raise TypeError("ArrayTaskMapping task_id must be a TaskId")
        if type(self.seed) is not int:
            raise TypeError("ArrayTaskMapping seed must be an integer")
        if type(self.array_index) is not int:
            raise TypeError("ArrayTaskMapping array_index must be an integer")
        if self.array_index < 0:
            raise ValueError("ArrayTaskMapping array_index must be non-negative")
