from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from shoal_run.domain.models import Command
from shoal_run.orchestration.models import ExecutionUnit
from shoal_run.ports import (
    CapabilityCheck,
    CommandResult,
    ContainerRequest,
    FetchRequest,
    FetchResult,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmission,
    StagedWorkspace,
    StageRequest,
)


def _next[T](script: deque[T | Exception], *, operation: str) -> T:
    if not script:
        raise AssertionError(f"No scripted {operation} outcome remains")
    outcome = script.popleft()
    if isinstance(outcome, Exception):
        raise outcome
    return outcome


@dataclass
class FakeTransport:
    check_script: deque[CapabilityCheck | Exception]
    run_script: deque[CommandResult | Exception]
    check_calls: int = 0
    run_calls: list[Command] = field(default_factory=list)

    def check(self) -> CapabilityCheck:
        self.check_calls += 1
        return _next(self.check_script, operation="transport check")

    def run(self, command: Command) -> CommandResult:
        self.run_calls.append(command)
        return _next(self.run_script, operation="transport run")


@dataclass
class FakeScheduler:
    submit_script: deque[SchedulerSubmission | Exception]
    query_script: deque[tuple[SchedulerObservation, ...] | Exception]
    cancel_script: deque[tuple[SchedulerObservation, ...] | Exception]
    submit_calls: list[tuple[ExecutionUnit, ...]] = field(default_factory=list)
    query_calls: list[tuple[SchedulerReference, ...]] = field(default_factory=list)
    cancel_calls: list[tuple[SchedulerReference, ...]] = field(default_factory=list)

    def submit(self, units: tuple[ExecutionUnit, ...]) -> SchedulerSubmission:
        self.submit_calls.append(units)
        return _next(self.submit_script, operation="scheduler submit")

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.query_calls.append(references)
        return _next(self.query_script, operation="scheduler query")

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.cancel_calls.append(references)
        return _next(self.cancel_script, operation="scheduler cancel")


@dataclass
class FakeStager:
    stage_script: deque[StagedWorkspace | Exception]
    fetch_script: deque[FetchResult | Exception]
    stage_calls: list[StageRequest] = field(default_factory=list)
    fetch_calls: list[FetchRequest] = field(default_factory=list)

    def stage(self, request: StageRequest) -> StagedWorkspace:
        self.stage_calls.append(request)
        return _next(self.stage_script, operation="stage")

    def fetch(self, request: FetchRequest) -> FetchResult:
        self.fetch_calls.append(request)
        return _next(self.fetch_script, operation="fetch")


@dataclass
class RecordingContainerRuntime:
    check_result: CapabilityCheck
    command: Command
    check_calls: int = 0
    build_calls: list[ContainerRequest] = field(default_factory=list)

    def check(self) -> CapabilityCheck:
        self.check_calls += 1
        return self.check_result

    def build_command(self, request: ContainerRequest) -> Command:
        self.build_calls.append(request)
        return self.command
