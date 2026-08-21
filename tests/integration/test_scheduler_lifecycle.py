from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import pytest

from rundra.cli.operations import (
    CancelValue,
    LogsValue,
    cancel_operation,
    logs_operation,
    status_operation,
)
from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import (
    Artifact,
    ArtifactKind,
    BackendConfig,
    Command,
    ConfigSnapshot,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    Run,
    RunId,
    Target,
    Task,
    TaskId,
)
from rundra.domain.preparation import PreparationRecord
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
_TERMINAL_TEST_STATES = {
    ExecutionState.SUCCEEDED,
    ExecutionState.FAILED,
    ExecutionState.CANCELLED,
}


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


@dataclass
class ArrayCancellationScheduler:
    refreshed: tuple[SchedulerObservation, ...]
    cancelled: tuple[SchedulerObservation, ...]
    query_references: tuple[SchedulerReference, ...] = ()
    cancel_references: tuple[SchedulerReference, ...] = ()
    cancellation_requested: bool = False

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        raise AssertionError("lifecycle reconciliation must not submit")

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.query_references = references
        return self.cancelled if self.cancellation_requested else self.refreshed

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.cancel_references = references
        self.cancellation_requested = True
        return self.cancelled


@dataclass
class PreparedLifecycleScheduler:
    preparation_observation: SchedulerObservation
    task_observation: SchedulerObservation
    queried: list[tuple[SchedulerReference, ...]] = field(default_factory=list)
    cancelled: list[tuple[SchedulerReference, ...]] = field(default_factory=list)

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        raise AssertionError("lifecycle reconciliation must not submit")

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.queried.append(references)
        if references == (SchedulerReference("900"),):
            return (self.preparation_observation,)
        assert references == (_REFERENCE,)
        return (self.task_observation,)

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.cancelled.append(references)
        if references == (SchedulerReference("900"),):
            return (
                SchedulerObservation(
                    SchedulerReference("900"),
                    ExecutionState.CANCELLED,
                    "CANCELLED",
                ),
            )
        assert references == (_REFERENCE,)
        return (_observation(ExecutionState.CANCELLED, "CANCELLED"),)


@dataclass
class PreparationRaceScheduler(PreparedLifecycleScheduler):
    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.cancelled.append(references)
        if references == (SchedulerReference("900"),):
            return (self.preparation_observation,)
        return (_observation(ExecutionState.CANCELLED, "CANCELLED"),)


class LogTransport:
    def __init__(self) -> None:
        self.calls: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("logs")

    def run(self, command: Command) -> CommandResult:
        self.calls.append(command)
        content = "remote stdout\n" if str(command.argv[-1]).endswith("stdout") else ""
        return CommandResult(command, 0, content, "", _CREATED, _CREATED)


class PreparationManifestTransport(LogTransport):
    def run(self, command: Command) -> CommandResult:
        self.calls.append(command)
        path = str(command.argv[-1])
        content = (
            "image_action\tpull_image\nbuild_action\tbuild_and_publish\n"
            if path.endswith("preparation-actions.tsv")
            else f"{'12' * 32}\t1\texamples/model\n"
        )
        return CommandResult(command, 0, content, "", _CREATED, _CREATED)


class V6PreparationManifestTransport(LogTransport):
    def run(self, command: Command) -> CommandResult:
        self.calls.append(command)
        path = str(command.argv[-1])
        if path.endswith("preparation-actions.tsv"):
            content = "image_action\tbuild_definition_image\nbuild_action\tnone\n"
        elif path.endswith("preparation-outputs.tsv"):
            content = ""
        elif path.endswith("preparation-image.tsv"):
            content = f"{'34' * 32}\t/remote/work/cache/images/final.sif\n"
        elif path.endswith("preparation-build.txt"):
            content = f"{'56' * 32}\n"
        else:
            return CommandResult(command, 1, "", "missing", _CREATED, _CREATED)
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


def _prepared_record() -> RunRecord:
    record = _record()
    image = PurePosixPath("/remote/work/cache/images/application.sif")
    return replace(
        record,
        format_version=2,
        experiment=replace(record.experiment, container=ContainerSpec(image)),
        container_digest="cd" * 32,
        preparation=PreparationRecord(
            source_identity="git-recipe",
            source_digest="ab" * 32,
            source_action="checkout_git_cache",
            image_uri="library://example/application:v1",
            image_sha256="cd" * 32,
            image_path=image,
            image_action="resolve_in_preparation_job",
            resolution_location="target",
            build_cache_key="ef" * 32,
            builder_location="target",
            builder_scheduler_id="900",
            builder_status="SUBMITTED",
            logs=(
                PurePosixPath("/remote/logs/900.stdout"),
                PurePosixPath("/remote/logs/900.stderr"),
            ),
        ),
    )


def _pending_v6_prepared_record() -> RunRecord:
    record = _prepared_record()
    preparation = record.preparation
    assert preparation is not None
    image = PurePosixPath("/remote/work/cache/images/final.sif")
    assert record.experiment.container is not None
    return replace(
        record,
        format_version=6,
        experiment=replace(
            record.experiment,
            container=replace(record.experiment.container, image=image),
        ),
        retrieval_destination=PurePosixPath("/retrieved"),
        fetch_mode="auto",
        container_digest=None,
        preparation=replace(
            preparation,
            image_sha256=None,
            image_path=image,
            image_recipe_key="12" * 32,
            build_cache_key=None,
        ),
    )


def _array_record() -> RunRecord:
    record = _record()
    first = record.run.tasks[0]
    tasks = (
        first,
        replace(first, id=TaskId.from_ordinal(1), seed=23),
        replace(first, id=TaskId.from_ordinal(2), seed=29),
    )
    return replace(
        record,
        run=replace(record.run, tasks=tasks),
        task_array_mapping=tuple(
            ArrayTaskMapping(task.id, task.seed, index)
            for index, task in enumerate(tasks)
        ),
        task_scheduler_ids={
            task.id: f"777_{index}" for index, task in enumerate(tasks)
        },
    )


def _array_observation(
    index: int,
    state: ExecutionState,
    native: str,
    *,
    exit_code: int | None = None,
) -> SchedulerObservation:
    return SchedulerObservation(
        SchedulerReference(f"777_{index}"),
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
            _CREATED + timedelta(seconds=3) if state in _TERMINAL_TEST_STATES else None
        ),
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
    from rundra.orchestration.progress import ProgressEvent, ProgressPhase

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
    progress: list[ProgressEvent] = []
    service = SchedulerLifecycleService(
        store=reloaded_store,
        scheduler=scheduler,
        clock=lambda: _CREATED + timedelta(seconds=4),
        sleeper=lambda delay: None,
        progress=progress.append,
    )

    completed = service.wait(reloaded_store.load(_RUN_ID), poll_interval=0.01)

    assert completed.run.state is ExecutionState.SUCCEEDED
    assert completed.native_state == "COMPLETED"
    assert completed.task_exit_codes == {_TASK_ID: 0}
    assert completed.allocated_nodes == ("node01",)
    assert reloaded_store.load(_RUN_ID) == completed
    assert scheduler.query_calls == 3
    assert [event.phase for event in progress] == [ProgressPhase.WAIT] * 4
    assert [event.message.split()[0] for event in progress] == [
        "run=SUBMITTED",
        "run=QUEUED",
        "run=RUNNING",
        "run=SUCCEEDED",
    ]
    assert progress[-1].completed == 6
    assert progress[-1].total == 7
    assert "tasks=1/1" in progress[-1].message


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


def test_failed_preparation_prevents_scientific_status_progression(tmp_path) -> None:
    record = _prepared_record()
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    scheduler = PreparedLifecycleScheduler(
        SchedulerObservation(
            SchedulerReference("900"),
            ExecutionState.FAILED,
            "FAILED",
            exit_code=2,
            finished_at=_CREATED + timedelta(seconds=3),
        ),
        _observation(ExecutionState.SUCCEEDED, "COMPLETED", exit_code=0),
    )

    failed = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        clock=lambda: _CREATED + timedelta(seconds=4),
    ).refresh(record)

    assert failed.run.state is ExecutionState.FAILED
    assert failed.native_state == "PREPARATION_FAILED"
    assert failed.preparation is not None
    assert failed.preparation.builder_status == "FAILED"
    assert scheduler.queried == [(SchedulerReference("900"),)]


def test_queued_preparation_persists_as_submitted(tmp_path) -> None:
    record = _prepared_record()
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    scheduler = PreparedLifecycleScheduler(
        SchedulerObservation(
            SchedulerReference("900"), ExecutionState.QUEUED, "PENDING"
        ),
        _observation(ExecutionState.RUNNING, "RUNNING"),
    )

    refreshed = SchedulerLifecycleService(store=store, scheduler=scheduler).refresh(
        record
    )

    assert refreshed.preparation is not None
    assert refreshed.preparation.builder_status == "SUBMITTED"
    assert refreshed.preparation.builder_state == "PENDING"


def test_refresh_repairs_terminal_aggregate_from_durable_task_facts(
    tmp_path: Path,
) -> None:
    record = _record()
    task = replace(record.run.tasks[0], state=ExecutionState.SUCCEEDED)
    stale = replace(
        record,
        run=replace(record.run, state=ExecutionState.RUNNING, tasks=(task,)),
        native_state="MIXED",
        task_native_states={task.id: "BUNDLED_TASK_SUCCEEDED"},
        task_exit_codes={task.id: 0},
    )
    store = JsonRunStore(tmp_path / "records")
    store.create(stale)
    scheduler = SequenceScheduler(deque())

    repaired = SchedulerLifecycleService(store=store, scheduler=scheduler).refresh(
        stale
    )

    assert repaired.run.state is ExecutionState.SUCCEEDED
    assert repaired.native_state == "BUNDLED_TASK_SUCCEEDED"
    assert scheduler.query_calls == 0
    assert store.load(stale.run.id) == repaired


def test_v6_completed_preparation_atomically_persists_verified_image(
    tmp_path: Path,
) -> None:
    record = _pending_v6_prepared_record()
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    scheduler = PreparedLifecycleScheduler(
        SchedulerObservation(
            SchedulerReference("900"), ExecutionState.SUCCEEDED, "COMPLETED"
        ),
        _observation(ExecutionState.SUCCEEDED, "COMPLETED", exit_code=0),
    )

    refreshed = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        transport=V6PreparationManifestTransport(),
    ).refresh(record)

    assert refreshed.preparation is not None
    assert refreshed.preparation.builder_status == "SUCCEEDED"
    assert refreshed.preparation.image_sha256 == "34" * 32
    assert refreshed.preparation.image_path == PurePosixPath(
        "/remote/work/cache/images/final.sif"
    )
    assert refreshed.preparation.build_cache_key == "56" * 32
    assert refreshed.container_digest == "34" * 32
    assert refreshed.experiment.container is not None
    assert refreshed.experiment.container.image == refreshed.preparation.image_path
    assert refreshed.run.state is ExecutionState.SUCCEEDED


def test_status_accepts_preparation_without_scientific_identities(tmp_path) -> None:
    record = replace(_prepared_record(), scheduler_job_ids=(), task_scheduler_ids={})
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    scheduler = PreparedLifecycleScheduler(
        SchedulerObservation(
            SchedulerReference("900"), ExecutionState.RUNNING, "RUNNING"
        ),
        _observation(ExecutionState.RUNNING, "RUNNING"),
    )

    status = status_operation(str(record.run.id), store, scheduler=scheduler)

    assert status.ok
    assert status.value is not None
    assert status.value.preparation is not None
    assert status.value.preparation.state == ExecutionState.RUNNING.value


def test_status_finalizes_completed_async_preparation_provenance(tmp_path) -> None:
    submitted = _prepared_record()
    preparation = submitted.preparation
    assert preparation is not None
    completed = replace(
        submitted,
        run=replace(
            submitted.run,
            state=ExecutionState.SUCCEEDED,
            tasks=tuple(
                replace(task, state=ExecutionState.SUCCEEDED)
                for task in submitted.run.tasks
            ),
        ),
        preparation=replace(
            preparation,
            builder_status="SUCCEEDED",
            builder_state="COMPLETED",
        ),
        completed_at=_CREATED + timedelta(seconds=4),
    )
    store = JsonRunStore(tmp_path / "records")
    store.create(completed)
    transport = PreparationManifestTransport()

    status = status_operation(
        str(_RUN_ID),
        store,
        transport=transport,
    )

    assert status.ok
    restored = store.load(_RUN_ID)
    assert restored.preparation is not None
    assert restored.preparation.image_action == "pull_image"
    assert restored.preparation.build_action == "build_and_publish"
    assert restored.preparation.build_outputs[0].sha256 == "12" * 32


def test_cancel_covers_preparation_and_dependent_scientific_job(tmp_path) -> None:
    record = _prepared_record()
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    scheduler = PreparedLifecycleScheduler(
        SchedulerObservation(
            SchedulerReference("900"), ExecutionState.RUNNING, "RUNNING"
        ),
        _observation(ExecutionState.CANCELLED, "CANCELLED"),
    )

    cancelled = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        clock=lambda: _CREATED + timedelta(seconds=4),
        sleeper=lambda delay: None,
    ).cancel(record, poll_interval=0.01)

    assert cancelled.run.state is ExecutionState.CANCELLED
    assert cancelled.preparation is not None
    assert cancelled.preparation.builder_status == "CANCELLED"
    assert scheduler.cancelled == [
        (SchedulerReference("900"),),
        (_REFERENCE,),
    ]


def test_cancel_reconciles_preparation_that_completed_during_cancellation(
    tmp_path: Path,
) -> None:
    record = _pending_v6_prepared_record()
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    completed = SchedulerObservation(
        SchedulerReference("900"), ExecutionState.SUCCEEDED, "COMPLETED"
    )
    scheduler = PreparationRaceScheduler(
        completed,
        _observation(ExecutionState.CANCELLED, "CANCELLED"),
    )

    cancelled = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        transport=V6PreparationManifestTransport(),
    ).cancel(record)

    assert cancelled.preparation is not None
    assert cancelled.preparation.image_sha256 == "34" * 32
    assert cancelled.container_digest == "34" * 32


def test_cancel_closes_interrupted_preparation_only_run(tmp_path) -> None:
    record = replace(_prepared_record(), scheduler_job_ids=(), task_scheduler_ids={})
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    scheduler = PreparedLifecycleScheduler(
        SchedulerObservation(
            SchedulerReference("900"), ExecutionState.RUNNING, "RUNNING"
        ),
        _observation(ExecutionState.CANCELLED, "CANCELLED"),
    )

    cancelled = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        clock=lambda: _CREATED + timedelta(seconds=4),
    ).cancel(record)

    assert cancelled.run.state is ExecutionState.CANCELLED
    assert cancelled.native_state == "PREPARATION_ONLY_CANCELLED"
    assert cancelled.preparation is not None
    assert cancelled.preparation.builder_status == "CANCELLED"
    assert scheduler.cancelled == [(SchedulerReference("900"),)]


def test_concurrent_status_refreshes_converge_without_record_corruption(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "records"
    JsonRunStore(store_path).create(_record())

    class TerminalScheduler:
        def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
            raise AssertionError("status must not submit")

        def query(
            self, references: tuple[SchedulerReference, ...]
        ) -> tuple[SchedulerObservation, ...]:
            assert references == (_REFERENCE,)
            return (
                _observation(
                    ExecutionState.SUCCEEDED,
                    "COMPLETED",
                    exit_code=0,
                ),
            )

        def cancel(
            self, references: tuple[SchedulerReference, ...]
        ) -> tuple[SchedulerObservation, ...]:
            raise AssertionError("status must not cancel")

    scheduler = TerminalScheduler()

    def refresh() -> bool:
        return status_operation(
            str(_RUN_ID), JsonRunStore(store_path), scheduler=scheduler
        ).ok

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = tuple(executor.map(lambda _index: refresh(), range(32)))

    assert all(outcomes)
    completed = JsonRunStore(store_path).load(_RUN_ID)
    assert completed.run.state is ExecutionState.SUCCEEDED
    assert completed.task_exit_codes == {_TASK_ID: 0}


def test_concurrent_cancel_requests_are_idempotent(tmp_path: Path) -> None:
    store_path = tmp_path / "records"
    JsonRunStore(store_path).create(_record())

    class TerminalCancellationScheduler:
        def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
            raise AssertionError("cancel must not submit")

        def query(
            self, references: tuple[SchedulerReference, ...]
        ) -> tuple[SchedulerObservation, ...]:
            raise AssertionError("terminal cancellation must not poll")

        def cancel(
            self, references: tuple[SchedulerReference, ...]
        ) -> tuple[SchedulerObservation, ...]:
            assert references == (_REFERENCE,)
            return (_observation(ExecutionState.CANCELLED, "CANCELLED"),)

    scheduler = TerminalCancellationScheduler()

    def cancel() -> bool:
        return cancel_operation(
            str(_RUN_ID), JsonRunStore(store_path), scheduler=scheduler
        ).ok

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = tuple(executor.map(lambda _index: cancel(), range(32)))

    assert all(outcomes)
    cancelled = JsonRunStore(store_path).load(_RUN_ID)
    assert cancelled.run.state is ExecutionState.CANCELLED
    assert cancelled.native_state == "CANCELLED"


def test_array_cancel_reconciles_first_and_cancels_scheduler_roots(
    tmp_path: Path,
) -> None:
    record = _array_record()
    store = JsonRunStore(tmp_path / "records")
    store.create(record)
    scheduler = ArrayCancellationScheduler(
        refreshed=(
            _array_observation(0, ExecutionState.SUCCEEDED, "COMPLETED", exit_code=0),
            _array_observation(1, ExecutionState.RUNNING, "RUNNING"),
            _array_observation(2, ExecutionState.QUEUED, "PENDING"),
        ),
        cancelled=(
            _array_observation(0, ExecutionState.SUCCEEDED, "COMPLETED", exit_code=0),
            _array_observation(1, ExecutionState.CANCELLED, "CANCELLED", exit_code=0),
            _array_observation(2, ExecutionState.SUCCEEDED, "COMPLETED", exit_code=0),
        ),
    )
    service = SchedulerLifecycleService(
        store=store,
        scheduler=scheduler,
        clock=lambda: _CREATED + timedelta(seconds=4),
        sleeper=lambda delay: None,
    )

    cancelled = service.cancel(record, poll_interval=0.01)
    repeated = service.cancel(cancelled)

    assert [task.state for task in cancelled.run.tasks] == [
        ExecutionState.SUCCEEDED,
        ExecutionState.CANCELLED,
        ExecutionState.SUCCEEDED,
    ]
    assert cancelled.run.state is ExecutionState.CANCELLED
    assert cancelled.task_exit_codes == {
        TaskId.from_ordinal(0): 0,
        TaskId.from_ordinal(1): 0,
        TaskId.from_ordinal(2): 0,
    }
    assert scheduler.query_references == (
        SchedulerReference("777_0"),
        SchedulerReference("777_1"),
        SchedulerReference("777_2"),
    )
    assert scheduler.cancel_references == (SchedulerReference("12345"),)
    assert repeated == cancelled


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


def test_terminal_array_logs_select_stable_task_without_scheduler_query(
    tmp_path: Path,
) -> None:
    record = _array_record()
    tasks = tuple(
        replace(task, state=ExecutionState.SUCCEEDED) for task in record.run.tasks
    )
    artifacts = tuple(
        Artifact(
            kind,
            PurePosixPath(f"/remote/logs/777_{index}.{suffix}"),
            task_id=task.id,
        )
        for index, task in enumerate(tasks)
        for kind, suffix in (
            (ArtifactKind.STDOUT, "stdout"),
            (ArtifactKind.STDERR, "stderr"),
        )
    )
    terminal = replace(
        record,
        run=replace(record.run, tasks=tasks, state=ExecutionState.SUCCEEDED),
        artifacts=artifacts,
    )
    store = JsonRunStore(tmp_path / "records")
    store.create(terminal)
    transport = LogTransport()

    by_ordinal = logs_operation(str(_RUN_ID), store, task="1", transport=transport)
    by_id = logs_operation(str(_RUN_ID), store, task="task_000002", transport=transport)
    unspecified = logs_operation(str(_RUN_ID), store, transport=transport)

    assert by_ordinal.ok and isinstance(by_ordinal.value, LogsValue)
    assert by_ordinal.value.task_id == TaskId.from_ordinal(1)
    assert str(by_ordinal.value.stdout_path).endswith("777_1.stdout")
    assert by_id.ok and isinstance(by_id.value, LogsValue)
    assert by_id.value.task_id == TaskId.from_ordinal(2)
    assert str(by_id.value.stderr_path).endswith("777_2.stderr")
    assert unspecified.error is not None
    assert unspecified.error.code == "TASK_REQUIRED"


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
