from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from rundra.domain.models import Run, RunId, Target, TaskId
from rundra.domain.states import ExecutionState, RetrievalState

DEFAULT_MAX_CONCURRENT_JOBS = 256


def _integer_at_least(value: object, minimum: int, field_name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


@dataclass(frozen=True, slots=True)
class SeedRange:
    """An inclusive arithmetic seed range with constant-size storage."""

    start: int
    stop: int
    step: int = 1

    def __post_init__(self) -> None:
        if type(self.start) is not int or type(self.stop) is not int:
            raise TypeError("SeedRange bounds must be integers")
        _integer_at_least(self.step, 1, "SeedRange step")
        if self.stop < self.start:
            raise ValueError("SeedRange stop must not precede start")
        if (self.stop - self.start) % self.step != 0:
            raise ValueError("SeedRange stop must be reachable using step")

    @property
    def count(self) -> int:
        return (self.stop - self.start) // self.step + 1

    def at(self, ordinal: int) -> int:
        _integer_at_least(ordinal, 0, "Seed ordinal")
        if ordinal >= self.count:
            raise IndexError("Seed ordinal is outside the range")
        return self.start + ordinal * self.step


@dataclass(frozen=True, slots=True)
class TaskCoordinate:
    """One deterministic logical position in a compact TaskSpace."""

    task_id: TaskId
    ordinal: int
    parameter_set_ordinal: int
    seed_ordinal: int
    seed: int


@dataclass(frozen=True, slots=True)
class TaskSpace:
    """A constant-size parameter-set by seed Cartesian product."""

    parameter_set_count: int
    seeds: SeedRange

    def __post_init__(self) -> None:
        _integer_at_least(self.parameter_set_count, 1, "TaskSpace parameter_set_count")
        if type(self.seeds) is not SeedRange:
            raise TypeError("TaskSpace seeds must be a SeedRange")

    @property
    def task_count(self) -> int:
        return self.parameter_set_count * self.seeds.count

    def coordinate(self, ordinal: int) -> TaskCoordinate:
        _integer_at_least(ordinal, 0, "Task ordinal")
        if ordinal >= self.task_count:
            raise IndexError("Task ordinal is outside the TaskSpace")
        parameter_set_ordinal, seed_ordinal = divmod(ordinal, self.seeds.count)
        return TaskCoordinate(
            task_id=TaskId.from_ordinal(ordinal),
            ordinal=ordinal,
            parameter_set_ordinal=parameter_set_ordinal,
            seed_ordinal=seed_ordinal,
            seed=self.seeds.at(seed_ordinal),
        )

    def page(self, *, offset: int, limit: int) -> tuple[TaskCoordinate, ...]:
        _integer_at_least(offset, 0, "Task page offset")
        _integer_at_least(limit, 1, "Task page limit")
        stop = min(self.task_count, offset + limit)
        if offset >= stop:
            return ()
        return tuple(self.coordinate(ordinal) for ordinal in range(offset, stop))


@dataclass(frozen=True, slots=True)
class CompactRun(Run):
    """Version-4 Run summary whose Tasks live in a TaskSpace sidecar."""

    def __post_init__(self) -> None:
        if type(self.id) is not RunId:
            raise TypeError("CompactRun id must be a RunId")
        if type(self.experiment_name) is not str or not self.experiment_name.strip():
            raise ValueError("CompactRun experiment_name must be nonblank")
        if type(self.target) is not Target:
            raise TypeError("CompactRun target must be a Target")
        if tuple(self.tasks):
            raise ValueError("CompactRun must not materialize Tasks")
        if not isinstance(self.created_at, datetime):
            raise TypeError("CompactRun created_at must be a datetime")
        if self.created_at.utcoffset() is None:
            raise ValueError("CompactRun creation time must be timezone-aware")
        if type(self.state) is not ExecutionState:
            raise TypeError("CompactRun state must be an ExecutionState")
        if type(self.retrieval_state) is not RetrievalState:
            raise TypeError("CompactRun retrieval_state must be a RetrievalState")
        object.__setattr__(self, "tasks", ())


@dataclass(frozen=True, slots=True)
class WorkerPoolPolicy:
    """Site-enforced bounds for scheduler-allocated sequential workers."""

    activation_threshold: int
    max_workers: int
    tasks_per_lease: int
    infrastructure_retry_limit: int
    requeue_limit: int

    def __post_init__(self) -> None:
        _integer_at_least(self.activation_threshold, 2, "activation_threshold")
        _integer_at_least(self.max_workers, 1, "max_workers")
        _integer_at_least(self.tasks_per_lease, 1, "tasks_per_lease")
        _integer_at_least(
            self.infrastructure_retry_limit, 0, "infrastructure_retry_limit"
        )
        _integer_at_least(self.requeue_limit, 0, "requeue_limit")


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """Explicit target-owned safety and scaling limits."""

    hard_task_limit: int
    confirmation_threshold: int
    max_active_tasks: int
    max_array_size: int
    output_shard_tasks: int
    automatic_retrieval_threshold: int
    worker_pool: WorkerPoolPolicy
    max_concurrent_jobs: int = DEFAULT_MAX_CONCURRENT_JOBS

    def __post_init__(self) -> None:
        _integer_at_least(self.hard_task_limit, 1, "hard_task_limit")
        _integer_at_least(self.confirmation_threshold, 1, "confirmation_threshold")
        _integer_at_least(self.max_active_tasks, 1, "max_active_tasks")
        _integer_at_least(self.max_array_size, 2, "max_array_size")
        _integer_at_least(self.output_shard_tasks, 1, "output_shard_tasks")
        _integer_at_least(
            self.automatic_retrieval_threshold, 0, "automatic_retrieval_threshold"
        )
        _integer_at_least(self.max_concurrent_jobs, 1, "max_concurrent_jobs")
        if type(self.worker_pool) is not WorkerPoolPolicy:
            raise TypeError("worker_pool must be a WorkerPoolPolicy")
        if self.confirmation_threshold > self.hard_task_limit:
            raise ValueError("confirmation_threshold must not exceed hard_task_limit")
        if self.max_active_tasks > self.hard_task_limit:
            raise ValueError("max_active_tasks must not exceed hard_task_limit")
        if self.worker_pool.activation_threshold > self.hard_task_limit:
            raise ValueError("worker_pool activation_threshold exceeds hard_task_limit")
