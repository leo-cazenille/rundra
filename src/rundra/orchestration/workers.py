from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
import tempfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from rundra.domain.scaling import TaskCoordinate, TaskSpace


@dataclass(frozen=True, slots=True)
class WorkerLease:
    ordinal: int
    task_start: int
    task_stop: int

    @property
    def task_count(self) -> int:
        return self.task_stop - self.task_start


@dataclass(frozen=True, slots=True)
class WorkerAssignment:
    task_count: int
    worker_index: int
    worker_count: int
    tasks_per_lease: int

    def __post_init__(self) -> None:
        for name in ("task_count", "worker_index", "worker_count", "tasks_per_lease"):
            if type(getattr(self, name)) is not int:
                raise TypeError(f"WorkerAssignment {name} must be an integer")
        if self.task_count < 1:
            raise ValueError("WorkerAssignment task_count must be positive")
        if self.worker_count < 1 or self.tasks_per_lease < 1:
            raise ValueError("Worker count and lease size must be positive")
        if not 0 <= self.worker_index < self.worker_count:
            raise ValueError("Worker index must be inside the worker pool")

    @property
    def lease_count(self) -> int:
        return (self.task_count + self.tasks_per_lease - 1) // self.tasks_per_lease

    @property
    def assigned_task_count(self) -> int:
        if self.worker_index >= self.lease_count:
            return 0
        assigned_leases = (
            self.lease_count - 1 - self.worker_index
        ) // self.worker_count + 1
        count = assigned_leases * self.tasks_per_lease
        final_size = self.task_count % self.tasks_per_lease
        if (
            final_size
            and (self.lease_count - 1) % self.worker_count == self.worker_index
        ):
            count -= self.tasks_per_lease - final_size
        return count

    def leases(self) -> Iterator[WorkerLease]:
        for lease_ordinal in range(
            self.worker_index, self.lease_count, self.worker_count
        ):
            start = lease_ordinal * self.tasks_per_lease
            yield WorkerLease(
                lease_ordinal,
                start,
                min(self.task_count, start + self.tasks_per_lease),
            )


@dataclass(frozen=True, slots=True)
class TaskOutcome:
    coordinate: TaskCoordinate
    exit_code: int
    timed_out: bool = False

    def __post_init__(self) -> None:
        if type(self.coordinate) is not TaskCoordinate:
            raise TypeError("TaskOutcome coordinate must be a TaskCoordinate")
        if type(self.exit_code) is not int:
            raise TypeError("TaskOutcome exit_code must be an integer")
        if type(self.timed_out) is not bool:
            raise TypeError("TaskOutcome timed_out must be a boolean")

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


@dataclass(frozen=True, slots=True)
class ShardMember:
    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class OutputShard:
    path: Path
    sha256: str
    lease: WorkerLease
    members: tuple[ShardMember, ...]


@dataclass(frozen=True, slots=True)
class WorkerResult:
    completed_leases: int
    completed_tasks: int
    scientific_failures: int
    needs_requeue: bool
    shards: tuple[OutputShard, ...]


type TaskRunner = Callable[[TaskCoordinate, Path], TaskOutcome]
type RemainingTime = Callable[[], float]


def execute_worker(
    task_space: TaskSpace,
    assignment: WorkerAssignment,
    *,
    workspace: Path,
    runner: TaskRunner,
    remaining_time: RemainingTime | None = None,
    allocation_guard_seconds: float = 60.0,
) -> WorkerResult:
    """Execute this worker's leases; scientific failures never stop the worker."""

    if type(task_space) is not TaskSpace:
        raise TypeError("execute_worker task_space must be a TaskSpace")
    if assignment.task_count != task_space.task_count:
        raise ValueError("WorkerAssignment task count must match TaskSpace")
    if not isinstance(workspace, Path):
        raise TypeError("execute_worker workspace must be a Path")
    if not callable(runner):
        raise TypeError("execute_worker runner must be callable")
    if type(allocation_guard_seconds) not in (int, float):
        raise TypeError("allocation_guard_seconds must be numeric")
    if allocation_guard_seconds < 0:
        raise ValueError("allocation_guard_seconds must be non-negative")
    workspace.mkdir(parents=True, exist_ok=True)
    shards_root = workspace / "shards"
    shards_root.mkdir(exist_ok=True)
    journal_path = workspace / f"worker-{assignment.worker_index:06d}.jsonl"
    completed_tasks = 0
    failures = 0
    shards: list[OutputShard] = []
    for lease in assignment.leases():
        if remaining_time is not None and remaining_time() <= allocation_guard_seconds:
            return WorkerResult(
                len(shards), completed_tasks, failures, True, tuple(shards)
            )
        with tempfile.TemporaryDirectory(
            prefix=f".lease-{lease.ordinal:09d}-", dir=workspace
        ) as temporary:
            lease_root = Path(temporary)
            outcomes: list[TaskOutcome] = []
            for ordinal in range(lease.task_start, lease.task_stop):
                coordinate = task_space.coordinate(ordinal)
                task_root = lease_root / str(coordinate.task_id)
                task_root.mkdir()
                outcome = runner(coordinate, task_root)
                if outcome.coordinate != coordinate:
                    raise ValueError("Task runner returned another Task coordinate")
                outcomes.append(outcome)
                completed_tasks += 1
                failures += not outcome.succeeded
                _append_journal(journal_path, outcome, lease.ordinal)
            shards.append(seal_output_shard(lease_root, shards_root, lease, outcomes))
    return WorkerResult(len(shards), completed_tasks, failures, False, tuple(shards))


def seal_output_shard(
    source: Path,
    destination: Path,
    lease: WorkerLease,
    outcomes: Sequence[TaskOutcome],
) -> OutputShard:
    """Atomically publish one immutable, indexed, uncompressed tar shard."""

    if not source.is_dir():
        raise ValueError("Output shard source must be a directory")
    destination.mkdir(parents=True, exist_ok=True)
    members = tuple(_source_members(source))
    index = {
        "format_version": 1,
        "lease": {
            "ordinal": lease.ordinal,
            "task_start": lease.task_start,
            "task_stop": lease.task_stop,
        },
        "tasks": [
            {
                "task_id": str(item.coordinate.task_id),
                "ordinal": item.coordinate.ordinal,
                "exit_code": item.exit_code,
                "timed_out": item.timed_out,
            }
            for item in outcomes
        ],
        "members": [
            {
                "path": item.path,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
            }
            for item in members
        ],
    }
    final = destination / f"lease-{lease.ordinal:09d}.tar"
    if final.exists():
        raise FileExistsError(f"Output shard already exists: {final}")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{final.name}.", suffix=".tmp", dir=destination, delete=False
        ) as stream:
            temporary = Path(stream.name)
            with tarfile.open(fileobj=stream, mode="w") as archive:
                for member in members:
                    archive.add(
                        source / member.path,
                        arcname=member.path,
                        recursive=False,
                    )
                encoded = json.dumps(
                    index, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
                info = tarfile.TarInfo("index.json")
                info.size = len(encoded)
                info.mode = 0o444
                info.mtime = 0
                archive.addfile(info, io.BytesIO(encoded))
            stream.flush()
            os.fsync(stream.fileno())
        digest = _file_sha256(temporary)
        os.replace(temporary, final)
        final.chmod(0o444)
        _sync_directory(destination)
        return OutputShard(final, digest, lease, members)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _source_members(source: Path) -> Iterator[ShardMember]:
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise ValueError("Output shards must not contain symlinks")
        if not path.is_file():
            continue
        relative = path.relative_to(source).as_posix()
        yield ShardMember(relative, path.stat().st_size, _file_sha256(path))


def _append_journal(path: Path, outcome: TaskOutcome, lease_ordinal: int) -> None:
    document = {
        "task_id": str(outcome.coordinate.task_id),
        "ordinal": outcome.coordinate.ordinal,
        "parameter_set_ordinal": outcome.coordinate.parameter_set_ordinal,
        "seed": outcome.coordinate.seed,
        "lease": lease_ordinal,
        "exit_code": outcome.exit_code,
        "timed_out": outcome.timed_out,
    }
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(document, sort_keys=True, separators=(",", ":")))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
