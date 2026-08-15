from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import pytest

from rundra.cli.operations import (
    CancelValue,
    LogsValue,
    cancel_operation,
    logs_operation,
)
from rundra.domain.models import (
    BackendConfig,
    Command,
    ConfigSnapshot,
    ExperimentSpec,
    ResourceRequest,
    Run,
    RunId,
    Target,
    Task,
    TaskId,
)
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState
from rundra.orchestration.service import OrchestrationError, SchedulerLifecycleService
from rundra.persistence import JsonRunStore
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmission,
)

_RUN_ID = RunId("run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
_TASK_ID = TaskId.from_ordinal(0)
_REFERENCE = SchedulerReference("12345")
_CREATED = datetime(2026, 8, 15, 10, tzinfo=UTC)


@dataclass
class SequenceScheduler:
    queries: deque[SchedulerObservation | Exception]
    cancellations: deque[SchedulerObservation | Exception] = field(
        default_factory=deque
    )
    query_calls: int = 0
    cancel_calls: int = 0

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        raise AssertionError("lifecycle reconciliation must not submit")

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.query_calls += 1
        outcome = self.queries.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return (outcome,)

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.cancel_calls += 1
        outcome = self.cancellations.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return (outcome,)


class LogTransport:
    def __init__(self) -> None:
        self.calls: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("logs")

    def run(self, command: Command) -> CommandResult:
        self.calls.append(command)
        content = "remote stdout\n" if str(command.argv[-1]).endswith("stdout") else ""
        return CommandResult(command, 0, content, "", _CREATED, _CREATED)


def _record() -> RunRecord:
    target = Target(
        "cluster",
        BackendConfig("ssh", {"host": "cluster"}),
        BackendConfig("slurm"),
        BackendConfig("rsync"),
        BackendConfig("apptainer"),
        PurePosixPath("/remote/work"),
    )
    config = ConfigSnapshot(PurePosixPath("config.yaml"), "value: 1\n")
    resources = ResourceRequest()
    experiment = ExperimentSpec(1, "lifecycle", Command(("program",)), resources)
    task = Task(
        _TASK_ID,
        _RUN_ID,
        experiment.name,
        config,
        17,
        resources,
        ExecutionState.SUBMITTED,
    )
    run = Run(
        _RUN_ID,
        experiment.name,
        target,
        (task,),
        _CREATED,
        ExecutionState.SUBMITTED,
    )
    return RunRecord(
        1,
        "0.1.0.dev0",
        run,
        experiment,
        PurePosixPath("/source"),
        scheduler_job_ids=("12345",),
        submitted_at=_CREATED + timedelta(seconds=1),
    )


def _observation(
    state: ExecutionState,
    native: str,
    *,
    exit_code: int | None = None,
) -> SchedulerObservation:
    return SchedulerObservation(
        _REFERENCE,
        state,
        native,
        exit_code=exit_code,
        started_at=(
            _CREATED + timedelta(seconds=2)
            if state
            in {
                ExecutionState.RUNNING,
                ExecutionState.SUCCEEDED,
                ExecutionState.FAILED,
                ExecutionState.CANCELLED,
            }
            else None
        ),
        finished_at=(
            _CREATED + timedelta(seconds=3)
            if state
            in {
                ExecutionState.SUCCEEDED,
                ExecutionState.FAILED,
                ExecutionState.CANCELLED,
            }
            else None
        ),
        metadata={"allocated_nodes": "node01"},
    )


def test_new_process_can_wait_from_durable_reference_to_terminal(tmp_path) -> None:
    store_path = tmp_path / "records"
    JsonRunStore(store_path).create(_record())
    scheduler = SequenceScheduler(
        deque(
            [
                _observation(ExecutionState.QUEUED, "PENDING"),
                _observation(ExecutionState.RUNNING, "RUNNING"),
                _observation(ExecutionState.SUCCEEDED, "COMPLETED", exit_code=0),
            ]
        )
    )
    reloaded_store = JsonRunStore(store_path)
    service = SchedulerLifecycleService(
        store=reloaded_store,
        scheduler=scheduler,
        clock=lambda: _CREATED + timedelta(seconds=4),
        sleeper=lambda delay: None,
    )

    completed = service.wait(reloaded_store.load(_RUN_ID), poll_interval=0.01)

    assert completed.run.state is ExecutionState.SUCCEEDED
    assert completed.native_state == "COMPLETED"
    assert completed.task_exit_codes == {_TASK_ID: 0}
    assert completed.allocated_nodes == ("node01",)
    assert reloaded_store.load(_RUN_ID) == completed
    assert scheduler.query_calls == 3


def test_wait_timeout_preserves_last_nonterminal_scheduler_state(tmp_path) -> None:
    store = JsonRunStore(tmp_path / "records")
    store.create(_record())
    scheduler = SequenceScheduler(
        deque([_observation(ExecutionState.QUEUED, "PENDING")])
    )
    service = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        monotonic_clock=lambda: 0.0,
        sleeper=lambda delay: None,
    )

    with pytest.raises(OrchestrationError) as caught:
        service.wait(store.load(_RUN_ID), timeout=0)

    assert caught.value.code == "SCHEDULER_TIMEOUT"
    assert store.load(_RUN_ID).run.state is ExecutionState.QUEUED


def test_cancel_reconciles_a_race_and_is_idempotent_after_terminal(tmp_path) -> None:
    store = JsonRunStore(tmp_path / "records")
    store.create(_record())
    scheduler = SequenceScheduler(
        deque([_observation(ExecutionState.CANCELLED, "CANCELLED")]),
        deque([_observation(ExecutionState.RUNNING, "RUNNING")]),
    )
    service = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        clock=lambda: _CREATED + timedelta(seconds=4),
        sleeper=lambda delay: None,
    )

    cancelled = service.cancel(store.load(_RUN_ID), poll_interval=0.01)
    repeated = service.cancel(cancelled)

    assert cancelled.run.state is ExecutionState.CANCELLED
    assert cancelled.native_state == "CANCELLED"
    assert repeated == cancelled
    assert scheduler.cancel_calls == 1
    assert scheduler.query_calls == 1


def test_logs_use_normalized_artifacts_without_exposing_slurm_filenames(
    tmp_path: Path,
) -> None:
    store = JsonRunStore(tmp_path / "records")
    store.create(_record())
    terminal = SchedulerObservation(
        _REFERENCE,
        ExecutionState.SUCCEEDED,
        "COMPLETED",
        exit_code=0,
        started_at=_CREATED + timedelta(seconds=2),
        finished_at=_CREATED + timedelta(seconds=3),
        metadata={
            "source": "sacct",
            "stdout_path": "/remote/logs/12345.stdout",
            "stderr_path": "/remote/logs/12345.stderr",
        },
    )
    scheduler = SequenceScheduler(deque([terminal]))
    transport = LogTransport()

    result = logs_operation(
        str(_RUN_ID), store, scheduler=scheduler, transport=transport
    )

    assert result.ok and isinstance(result.value, LogsValue)
    assert result.value.stdout == "remote stdout\n"
    assert result.value.stderr == ""
    assert result.value.task_id == _TASK_ID
    assert transport.calls == [
        Command(("cat", "--", "/remote/logs/12345.stdout")),
        Command(("cat", "--", "/remote/logs/12345.stderr")),
    ]
    persisted = store.load(_RUN_ID)
    assert persisted.scheduler_metadata["source"] == "sacct"


def test_cancel_operation_reconciles_by_run_id(tmp_path: Path) -> None:
    store = JsonRunStore(tmp_path / "records")
    store.create(_record())
    scheduler = SequenceScheduler(
        deque([]),
        deque([_observation(ExecutionState.CANCELLED, "CANCELLED")]),
    )

    result = cancel_operation(str(_RUN_ID), store, scheduler=scheduler)

    assert result.ok and isinstance(result.value, CancelValue)
    assert result.value.status.state is ExecutionState.CANCELLED
    assert store.load(_RUN_ID).run.state is ExecutionState.CANCELLED
