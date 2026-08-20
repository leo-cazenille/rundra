from __future__ import annotations

import base64
import gzip
from collections import deque
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import pytest

from rundra.adapters import SlurmScheduler
from rundra.cli.operations import CancelValue, cancel_operation
from rundra.domain.models import (
    BackendConfig,
    Command,
    ConfigSnapshot,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    RunId,
    Target,
)
from rundra.domain.records import RunRecord
from rundra.domain.scaling import ExecutionPolicy, SeedRange, WorkerPoolPolicy
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.domain.sweeps import ExpandedConfig
from rundra.orchestration.planner import create_plan, create_scalable_plan
from rundra.orchestration.service import (
    OrchestrationError,
    OrchestrationService,
    RunExecutionRequest,
    SchedulerLifecycleService,
)
from rundra.persistence import JsonRunStore, SqliteTaskStore
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    ContainerRequest,
    FetchRequest,
    FetchResult,
    StagedWorkspace,
    StageRequest,
)

_RUN_ID = RunId("run_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
_NOW = datetime(2026, 8, 15, 10, tzinfo=UTC)


class ScriptedTransport:
    def __init__(self, outcomes: deque[tuple[int, str, str] | Exception]) -> None:
        self.outcomes = outcomes
        self.commands: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("fake-ssh")

    def run(self, command: Command) -> CommandResult:
        self.commands.append(command)
        outcome = self.outcomes.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        exit_code, stdout, stderr = outcome
        return CommandResult(command, exit_code, stdout, stderr, _NOW, _NOW)


class FakeRemoteStager:
    def __init__(self) -> None:
        root = PurePosixPath(f"/remote/runs/{_RUN_ID}")
        self.workspace = StagedWorkspace(
            root,
            root / "source",
            root / "input",
            root / "input/config.yaml",
            root / "runtime",
            root / "output",
            root / "logs",
            root / "metadata",
        )

    def stage(self, request: StageRequest) -> StagedWorkspace:
        return self.workspace

    def fetch(self, request: FetchRequest) -> FetchResult:
        return FetchResult(())


class FakeRuntime:
    def check(self) -> CapabilityCheck:
        return CapabilityCheck("fake-apptainer")

    def build_command(self, request: ContainerRequest) -> Command:
        return Command(
            ("apptainer", "exec", "/images/test.sif", *request.command.argv),
            environment=request.command.environment,
            working_directory=request.command.working_directory,
        )


def _request(tmp_path: Path, *, seeds: tuple[int, ...] = (17,)) -> RunExecutionRequest:
    resources = ResourceRequest(cpus_per_task=2)
    experiment = ExperimentSpec(
        1,
        "fake-slurm",
        Command(("program", "--config", "{config}", "--seed", "{seed}")),
        resources,
        container=ContainerSpec(PurePosixPath("/images/test.sif")),
    )
    target = Target(
        "cluster",
        BackendConfig("ssh", {"host": "cluster"}),
        BackendConfig("slurm"),
        BackendConfig("rsync"),
        BackendConfig("apptainer"),
        PurePosixPath("/remote"),
    )
    config = ConfigSnapshot(PurePosixPath("config.yaml"), "value: 1\n")
    return RunExecutionRequest(
        create_plan(experiment, config, target, seeds=seeds),
        experiment,
        tmp_path,
        tmp_path / "retrieved",
    )


def _service(
    tmp_path: Path,
    outcomes: deque[tuple[int, str, str] | Exception],
    *,
    task_store: SqliteTaskStore | None = None,
) -> tuple[OrchestrationService, ScriptedTransport, JsonRunStore]:
    transport = ScriptedTransport(outcomes)
    store = JsonRunStore(tmp_path / "records")
    scheduler = SlurmScheduler(
        transport,
        timezone=UTC,
        log_directory=PurePosixPath("/remote/.scheduler-logs"),
    )
    service = OrchestrationService(
        store=store,
        stager=FakeRemoteStager(),
        runtime=FakeRuntime(),
        scheduler=scheduler,
        transport=transport,
        run_id_factory=lambda: _RUN_ID,
        clock=lambda: _NOW,
        framework_version="0.1.0.dev0",
        task_store=task_store,
    )
    return service, transport, store


def _compact_plan(request: RunExecutionRequest):
    policy = ExecutionPolicy(
        hard_task_limit=2_000,
        confirmation_threshold=2_000,
        max_active_tasks=2,
        max_array_size=1_001,
        output_shard_tasks=1_000,
        automatic_retrieval_threshold=20_000,
        worker_pool=WorkerPoolPolicy(
            activation_threshold=100,
            max_workers=2,
            tasks_per_lease=100,
            infrastructure_retry_limit=2,
            requeue_limit=2,
            task_slots_per_worker=1,
            default_workers=2,
            max_task_slots_per_worker=1,
        ),
        max_concurrent_jobs=2,
    )
    return create_scalable_plan(
        request.experiment,
        (ExpandedConfig(request.plan.units[0].config),),
        request.plan.target,
        seeds=SeedRange(0, 999),
        policy=policy,
        strategy="worker-pool",
        version=7,
        workers=2,
    )


def _terminal_rows(state: str, exit_code: int) -> tuple[tuple[int, str, str], ...]:
    accounting = (
        f"42|{state}|{exit_code}:0|2026-08-15T10:00:00|2026-08-15T10:00:00|node01|\n"
    )
    return ((0, "", ""), (0, accounting, ""))


def test_bundled_array_reconciles_atomic_task_journals(tmp_path: Path) -> None:
    service, transport, store = _service(
        tmp_path,
        deque(
            [
                (0, "MaxArraySize = 1001\n", ""),
                (0, "", ""),
                (0, "", ""),
                (0, "42\n", ""),
            ]
        ),
    )
    request = replace(
        _request(tmp_path, seeds=(10, 11, 12, 13, 14)),
        max_concurrent_jobs=2,
    )

    submitted = service.submit_one(request).record

    assert submitted.scheduler_job_ids == ("42",)
    assert tuple(submitted.task_scheduler_ids.values()) == (
        "42_0",
        "42_1",
        "42_0",
        "42_1",
        "42_0",
    )
    accounting = (
        "42_0|COMPLETED|0:0|2026-08-15T10:00:00|"
        "2026-08-15T10:01:00|node01|\n"
        "42_1|COMPLETED|0:0|2026-08-15T10:00:00|"
        "2026-08-15T10:01:00|node02|\n"
    )
    journals = "\n".join(
        (
            "task_000000\t0",
            "task_000002\t9",
            "task_000004\t0",
            "task_000001\t0",
            "task_000003\t0",
        )
    )
    transport.outcomes.extend(((0, "", ""), (0, accounting, ""), (0, journals, "")))

    refreshed = SchedulerLifecycleService(
        store=store,
        scheduler=service._scheduler,
        transport=transport,
        clock=lambda: _NOW,
    ).refresh(submitted)

    assert [task.state for task in refreshed.run.tasks] == [
        ExecutionState.SUCCEEDED,
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.SUCCEEDED,
        ExecutionState.SUCCEEDED,
    ]
    assert refreshed.task_exit_codes[refreshed.run.tasks[2].id] == 9
    assert refreshed.run.state is ExecutionState.FAILED


def test_large_bundled_cancel_reaches_scancel_before_task_reconciliation(
    tmp_path: Path,
) -> None:
    service, transport, store = _service(
        tmp_path,
        deque(
            [
                (0, "MaxArraySize = 1001\n", ""),
                (0, "", ""),
                (0, "", ""),
                (0, "42\n", ""),
            ]
        ),
    )
    request = replace(
        _request(tmp_path, seeds=tuple(range(1_000))),
        max_concurrent_jobs=2,
    )
    submitted = service.submit_one(request).record
    command_count = len(transport.commands)
    cancelled_workers = "42_0|CANCELLED|N/A|(null)\n42_1|CANCELLED|N/A|(null)\n"
    transport.outcomes.extend(
        (
            (0, "", ""),
            (0, "42|CANCELLED|N/A|(null)\n", ""),
            (0, cancelled_workers, ""),
            (0, "", ""),
        )
    )

    cancelled = SchedulerLifecycleService(
        store=store,
        scheduler=service._scheduler,
        transport=transport,
        clock=lambda: _NOW,
    ).cancel(submitted)

    cancellation_commands = transport.commands[command_count:]
    assert cancellation_commands[0] == Command(("scancel", "--", "42"))
    assert len(cancellation_commands) == 4
    assert cancelled.run.state is ExecutionState.CANCELLED
    assert {task.state for task in cancelled.run.tasks} == {ExecutionState.CANCELLED}
    assert len(cancelled.artifacts) >= 2_000

    result = cancel_operation(
        str(cancelled.run.id), store, scheduler=service._scheduler
    )

    assert result.ok and isinstance(result.value, CancelValue)
    assert result.value.status.state is ExecutionState.CANCELLED
    assert len(transport.commands) == command_count + 4


def test_large_worker_pool_persists_and_reconciles_compact_task_state(
    tmp_path: Path,
) -> None:
    task_store = SqliteTaskStore(tmp_path / "records")
    service, transport, store = _service(
        tmp_path,
        deque(
            [
                (0, "MaxArraySize = 1001\n", ""),
                (0, "", ""),
                (0, "", ""),
                (0, "42\n", ""),
            ]
        ),
        task_store=task_store,
    )
    request = _request(tmp_path, seeds=tuple(range(1_000)))
    compact_plan = _compact_plan(request)
    request = replace(
        request,
        max_concurrent_jobs=2,
        max_workers=2,
        compact_plan=compact_plan,
        worker_resources=compact_plan.worker_resources,
    )

    submitted = service.submit_one(request).record

    assert submitted.format_version == 4
    assert submitted.run.tasks == ()
    assert submitted.task_space is not None
    assert submitted.task_space.task_count == 1_000
    assert task_store.counts(_RUN_ID).execution[ExecutionState.SUBMITTED] == 1_000
    assert (tmp_path / "records" / f"{_RUN_ID}.json").stat().st_size < 100_000

    accounting = (
        "42_0|COMPLETED|0:0|2026-08-15T10:00:00|"
        "2026-08-15T10:01:00|node01|\n"
        "42_1|COMPLETED|0:0|2026-08-15T10:00:00|"
        "2026-08-15T10:01:00|node02|\n"
    )
    journals = "\n".join(f"task_{ordinal:06d}\t0" for ordinal in range(1_000))
    transport.outcomes.extend(((0, "", ""), (0, accounting, ""), (0, journals, "")))

    refreshed = SchedulerLifecycleService(
        store=store,
        scheduler=service._scheduler,
        transport=transport,
        clock=lambda: _NOW,
        task_store=task_store,
    ).refresh(submitted)

    assert refreshed.run.state is ExecutionState.SUCCEEDED
    assert task_store.counts(_RUN_ID).execution[ExecutionState.SUCCEEDED] == 1_000


@pytest.mark.parametrize(
    ("native_state", "exit_code", "expected"),
    [
        ("COMPLETED", 0, ExecutionState.SUCCEEDED),
        ("OUT_OF_MEMORY", 137, ExecutionState.FAILED),
    ],
)
def test_scripted_slurm_run_reconciles_terminal_success_and_failure(
    tmp_path: Path,
    native_state: str,
    exit_code: int,
    expected: ExecutionState,
) -> None:
    outcomes = deque(
        [
            (0, "42\n", ""),
            *_terminal_rows(native_state, exit_code),
            *_terminal_rows(native_state, exit_code),
        ]
    )
    service, transport, store = _service(tmp_path, outcomes)

    result = service.execute_one(_request(tmp_path))

    assert result.record.run.state is expected
    assert result.record.native_state == native_state
    assert result.record.task_exit_codes == {result.record.run.tasks[0].id: exit_code}
    assert result.record.run.retrieval_state is RetrievalState.SUCCEEDED
    assert store.load(_RUN_ID) == result.record
    assert transport.commands[0].argv[0:2] == ("/bin/sh", "-c")


def test_scripted_submission_failure_is_durable_and_actionable(tmp_path: Path) -> None:
    service, _, store = _service(tmp_path, deque([(1, "", "invalid account or qos")]))

    with pytest.raises(OrchestrationError) as caught:
        service.submit_one(_request(tmp_path))

    assert caught.value.code == "SCHEDULER_SUBMISSION_FAILED"
    record = store.load(_RUN_ID)
    assert record.run.state is ExecutionState.FAILED
    assert record.native_state == "SCHEDULER_SUBMISSION_FAILED"


def test_scripted_slurm_array_reconciles_every_task_and_mixed_outcome(
    tmp_path: Path,
) -> None:
    accounting = (
        "42_0|COMPLETED|0:0|2026-08-15T10:00:00|"
        "2026-08-15T10:01:00|node01|\n"
        "42_1|FAILED|9:0|2026-08-15T10:00:00|"
        "2026-08-15T10:02:00|node02|\n"
    )
    service, transport, store = _service(
        tmp_path,
        deque(
            [
                (0, "MaxArraySize = 1001\n", ""),
                (0, "42\n", ""),
                (0, "", ""),
                (0, accounting, ""),
            ]
        ),
    )

    result = service.execute_one(_request(tmp_path, seeds=(17, 23)))

    first, second = result.record.run.tasks
    assert [task.state for task in result.record.run.tasks] == [
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
    ]
    assert result.record.run.state is ExecutionState.FAILED
    assert result.record.native_state == "MIXED"
    assert result.record.scheduler_job_ids == ("42",)
    assert result.record.task_scheduler_ids == {first.id: "42_0", second.id: "42_1"}
    assert result.record.task_native_states == {
        first.id: "COMPLETED",
        second.id: "FAILED",
    }
    assert result.record.task_exit_codes == {first.id: 0, second.id: 9}
    assert result.record.allocated_nodes == ("node01", "node02")
    assert {
        (artifact.kind.value, artifact.task_id, str(artifact.path))
        for artifact in result.record.artifacts
    } >= {
        ("stdout", first.id, "/remote/.scheduler-logs/42_0.stdout"),
        ("stderr", first.id, "/remote/.scheduler-logs/42_0.stderr"),
        ("stdout", second.id, "/remote/.scheduler-logs/42_1.stdout"),
        ("stderr", second.id, "/remote/.scheduler-logs/42_1.stderr"),
    }
    assert result.record.run.retrieval_state is RetrievalState.SUCCEEDED
    assert store.load(_RUN_ID) == result.record
    submission_command = transport.commands[1]
    assert "#SBATCH --array=0-1" in submission_command.argv[6]
    manifest = gzip.decompress(base64.b64decode(submission_command.argv[5])).decode(
        "utf-8"
    )
    assert "task_id=task_000000 seed=17" in manifest
    assert "task_id=task_000001 seed=23" in manifest


def test_scripted_slurm_array_is_reproducible_for_the_same_seed_set(
    tmp_path: Path,
) -> None:
    accounting = (
        "42_0|COMPLETED|0:0|2026-08-15T10:00:00|"
        "2026-08-15T10:01:00|node01|\n"
        "42_1|COMPLETED|0:0|2026-08-15T10:00:00|"
        "2026-08-15T10:01:30|node02|\n"
    )

    def execute(root: Path) -> tuple[RunRecord, str]:
        service, transport, _ = _service(
            root,
            deque(
                [
                    (0, "MaxArraySize = 1001\n", ""),
                    (0, "42\n", ""),
                    (0, "", ""),
                    (0, accounting, ""),
                ]
            ),
        )
        result = service.execute_one(_request(root, seeds=(17, 23)))
        manifest = gzip.decompress(
            base64.b64decode(transport.commands[1].argv[5])
        ).decode("utf-8")
        return result.record, manifest

    first, first_manifest = execute(tmp_path / "first")
    second, second_manifest = execute(tmp_path / "second")

    assert type(first) is type(second)
    assert [task.id for task in first.run.tasks] == [
        task.id for task in second.run.tasks
    ]
    assert [task.seed for task in first.run.tasks] == [17, 23]
    assert [task.seed for task in first.run.tasks] == [
        task.seed for task in second.run.tasks
    ]
    assert [task.config for task in first.run.tasks] == [
        task.config for task in second.run.tasks
    ]
    assert first.task_array_mapping == second.task_array_mapping
    assert first_manifest == second_manifest
    assert "--seed 17" in first_manifest
    assert "--seed 23" in first_manifest
    assert first.run.state is ExecutionState.SUCCEEDED
    assert first.task_exit_codes == {
        first.run.tasks[0].id: 0,
        first.run.tasks[1].id: 0,
    }
    assert first.task_retrieval_states == {
        first.run.tasks[0].id: RetrievalState.SUCCEEDED,
        first.run.tasks[1].id: RetrievalState.SUCCEEDED,
    }


def test_accounting_disappearance_times_out_without_failing_the_run(
    tmp_path: Path,
) -> None:
    service, _, store = _service(tmp_path, deque([(0, "42\n", "")]))
    submitted = service.submit_one(_request(tmp_path)).record
    continuation_transport = ScriptedTransport(deque([(0, "", ""), (0, "", "")]))
    continuation = SchedulerLifecycleService(
        store=JsonRunStore(tmp_path / "records"),
        scheduler=SlurmScheduler(continuation_transport),
        monotonic_clock=lambda: 0.0,
        sleeper=lambda delay: None,
    )

    with pytest.raises(OrchestrationError) as caught:
        continuation.wait(store.load(submitted.run.id), timeout=0)

    assert caught.value.code == "SCHEDULER_TIMEOUT"
    record = store.load(_RUN_ID)
    assert record.run.state is ExecutionState.UNKNOWN
    assert record.native_state == "ACCOUNTING_PENDING"
    assert record.scheduler_job_ids == ("42",)


def test_new_client_can_cancel_from_only_the_persisted_record(tmp_path: Path) -> None:
    service, _, store = _service(tmp_path, deque([(0, "42\n", "")]))
    service.submit_one(_request(tmp_path))
    cancel_transport = ScriptedTransport(
        deque(
            [
                (0, "", ""),
                (0, "", ""),
                *_terminal_rows("CANCELLED", 0)[1:],
            ]
        )
    )
    reloaded_store = JsonRunStore(tmp_path / "records")
    continuation = SchedulerLifecycleService(
        store=reloaded_store,
        scheduler=SlurmScheduler(cancel_transport),
        clock=lambda: _NOW,
    )

    cancelled = continuation.cancel(reloaded_store.load(_RUN_ID))

    assert cancelled.run.state is ExecutionState.CANCELLED
    assert cancelled.native_state == "CANCELLED"
    assert cancel_transport.commands[0] == Command(("scancel", "--", "42"))
