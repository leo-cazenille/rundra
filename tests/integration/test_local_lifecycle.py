from __future__ import annotations

import os
import stat
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path, PurePath, PurePosixPath

import pytest

from rundra.adapters.local import LocalScheduler, LocalStager, LocalTransport
from rundra.domain.models import (
    ArtifactKind,
    BackendConfig,
    Command,
    ConfigSnapshot,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    RunId,
    Target,
)
from rundra.domain.preparation import (
    PreparationBuild,
    PreparationConfig,
    PreparationImage,
    PreparationOutput,
    PreparationPlan,
    PreparationRecord,
    PreparationSourceGit,
)
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.planner import create_plan
from rundra.orchestration.preparation import RemotePreparationSpec
from rundra.orchestration.service import (
    OrchestrationError,
    OrchestrationService,
    RunExecutionRequest,
)
from rundra.persistence import (
    JsonRunStore,
    SubmissionReceiptOutcome,
    SubmissionReceiptStore,
)
from rundra.persistence.errors import RunStoreError
from rundra.ports import (
    CapabilityCheck,
    ContainerRequest,
    FetchRequest,
    FetchResult,
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerSubmissionRole,
    SchedulerSubmission,
    SchedulerSubmissionFailure,
    SchedulerSubmissionOutcome,
    StagedWorkspace,
    StageRequest,
)
from rundra.provenance import GitProvenance

_RUN_ID = RunId("run_0123456789abcdef0123456789abcdef")


class HostMappedRuntime:
    def __init__(self) -> None:
        self.check_calls = 0
        self.build_calls: list[ContainerRequest] = []

    def check(self) -> CapabilityCheck:
        self.check_calls += 1
        return CapabilityCheck("host-mapped-test-runtime")

    def build_command(self, request: ContainerRequest) -> Command:
        self.build_calls.append(request)
        replacements = tuple(
            sorted(
                ((str(bind.destination), str(bind.source)) for bind in request.binds),
                key=lambda item: len(item[0]),
                reverse=True,
            )
        )

        def host_value(value: str) -> str:
            for container_path, host_path in replacements:
                value = value.replace(container_path, host_path)
            return value

        working_directory = request.command.working_directory
        return Command(
            tuple(host_value(argument) for argument in request.command.argv),
            environment=request.command.environment,
            working_directory=(
                None
                if working_directory is None
                else PurePath(host_value(str(working_directory)))
            ),
        )


class FailingFetchStager:
    def __init__(self, delegate: LocalStager) -> None:
        self.delegate = delegate

    def stage(self, request: StageRequest) -> StagedWorkspace:
        return self.delegate.stage(request)

    def fetch(self, request: FetchRequest) -> FetchResult:
        raise RuntimeError("simulated retrieval failure")


class FailingStageStager:
    def stage(self, request: StageRequest) -> StagedWorkspace:
        raise RuntimeError("simulated staging failure")

    def fetch(self, request: FetchRequest) -> FetchResult:
        raise AssertionError("fetch must not follow failed staging")


class QueryFailScheduler:
    def __init__(self, submission: SchedulerSubmission) -> None:
        self.submission = submission
        self.query_calls = 0

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        return self.submission

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        self.query_calls += 1
        raise RuntimeError("accounting unavailable")

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        raise AssertionError("cancel must not be called")


class FailingSubmissionScheduler:
    def __init__(self, failure: SchedulerSubmissionFailure) -> None:
        self.failure = failure

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        raise self.failure

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        raise AssertionError("query must not follow failed submission")

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        raise AssertionError("cancel must not follow failed submission")


class DependencyLocalScheduler:
    def __init__(self, transport: LocalTransport) -> None:
        self.delegate = LocalScheduler(
            transport,
            reference_factory=lambda: "local-science-reference",
        )
        self.preparation_groups: list[SchedulerGroup] = []
        self.dependencies: list[SchedulerReference] = []

    def submit(self, group: SchedulerGroup) -> SchedulerSubmission:
        self.preparation_groups.append(group)
        return SchedulerSubmission(
            SchedulerReference("900"),
            {group.units[0].task_id: "900"},
        )

    def submit_afterok(
        self,
        group: SchedulerGroup,
        dependency: SchedulerReference,
    ) -> SchedulerSubmission:
        self.dependencies.append(dependency)
        return self.delegate.submit(group)

    def submit_array_afterok(
        self,
        request: SchedulerArrayRequest,
        dependency: SchedulerReference,
    ) -> SchedulerSubmission:
        raise AssertionError("array submission is not expected")

    def query(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        if references == (SchedulerReference("900"),):
            return (
                SchedulerObservation(
                    references[0],
                    ExecutionState.SUCCEEDED,
                    "COMPLETED",
                    exit_code=0,
                ),
            )
        return self.delegate.query(references)

    def cancel(
        self, references: tuple[SchedulerReference, ...]
    ) -> tuple[SchedulerObservation, ...]:
        return self.delegate.cancel(references)


def _target(workspace: Path) -> Target:
    return Target(
        name="local",
        transport=BackendConfig("local"),
        scheduler=BackendConfig("local"),
        staging=BackendConfig("local"),
        container=BackendConfig("apptainer"),
        workspace=workspace,
    )


def _experiment(*, exit_code: int) -> ExperimentSpec:
    return ExperimentSpec(
        version=1,
        name="local-lifecycle",
        command=Command(
            (
                sys.executable,
                "main.py",
                "--config",
                "{config}",
                "--seed",
                "{seed}",
                "--exit-code",
                str(exit_code),
            ),
            environment={"RUNDRA_MODE": "integration"},
        ),
        resources=ResourceRequest(),
        container=ContainerSpec(PurePosixPath("image.sif")),
        outputs=("results/**",),
    )


def _source(tmp_path: Path) -> tuple[Path, ConfigSnapshot]:
    source = tmp_path / "project"
    source.mkdir()
    (source / "main.py").write_text(
        """\
import argparse
import os
import sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
parser.add_argument("--seed", required=True, type=int)
parser.add_argument("--exit-code", required=True, type=int)
args = parser.parse_args()
output = Path("../output/results")
output.mkdir(parents=True, exist_ok=True)
content = Path(args.config).read_text(encoding="utf-8").strip()
(output / "result.txt").write_text(
    f"seed={args.seed};config={content};mode={os.environ['RUNDRA_MODE']}\\n",
    encoding="utf-8",
)
print(f"stdout seed={args.seed}")
print(f"stderr exit={args.exit_code}", file=sys.stderr)
raise SystemExit(args.exit_code)
""",
        encoding="utf-8",
    )
    config_path = source / "config.yaml"
    config_path.write_text("value: 41\n", encoding="utf-8")
    return source, ConfigSnapshot(config_path, "value: 41\n")


def _restore_writes(root: Path) -> None:
    if not root.exists():
        return
    for path in (root, *root.rglob("*")):
        if not path.is_symlink():
            os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)


def _service(
    tmp_path: Path,
    runtime: HostMappedRuntime,
    *,
    stager: object | None = None,
    provenance: object | None = None,
    scheduler: object | None = None,
) -> OrchestrationService:
    transport = LocalTransport()
    return OrchestrationService(
        store=JsonRunStore(tmp_path / "records"),
        stager=stager or LocalStager(),
        runtime=runtime,
        scheduler=scheduler
        or LocalScheduler(
            transport, reference_factory=lambda: "local-lifecycle-reference"
        ),
        transport=transport,
        run_id_factory=lambda: _RUN_ID,
        framework_version="0.1.0.dev0",
        provenance=provenance,
    )


def _request(
    tmp_path: Path,
    *,
    exit_code: int,
) -> tuple[RunExecutionRequest, Path]:
    source, config = _source(tmp_path)
    experiment = _experiment(exit_code=exit_code)
    plan = create_plan(experiment, config, _target(tmp_path / "workspace"), seeds=(17,))
    return (
        RunExecutionRequest(
            plan=plan,
            experiment=experiment,
            source_root=source,
            fetch_destination=tmp_path / "retrieved",
            experiment_source=source / "experiment.yaml",
        ),
        source,
    )


def test_one_task_local_lifecycle_persists_success_logs_manifest_and_fetch(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    service = _service(tmp_path, runtime)

    result = service.execute_one(request)

    record = result.record
    assert record.run.state is ExecutionState.SUCCEEDED
    assert record.run.tasks[0].state is ExecutionState.SUCCEEDED
    assert record.run.retrieval_state is RetrievalState.SUCCEEDED
    assert record.task_exit_codes == {record.run.tasks[0].id: 0}
    assert record.scheduler_job_ids == ("local-lifecycle-reference",)
    assert record.native_state == "EXITED"
    assert record.submitted_at is not None
    assert record.started_at is not None
    assert record.submitted_at <= record.started_at
    assert record.completed_at is not None
    assert record.completed_at >= record.started_at
    assert service.store.load(_RUN_ID) == record
    assert (tmp_path / "retrieved/results/result.txt").read_text(
        encoding="utf-8"
    ) == "seed=17;config=value: 41;mode=integration\n"
    stdout = result.workspace.logs / "task_000000.stdout"
    stderr = result.workspace.logs / "task_000000.stderr"
    assert stdout.read_text(encoding="utf-8") == "stdout seed=17\n"
    assert stderr.read_text(encoding="utf-8") == "stderr exit=0\n"


def test_preparation_job_is_submitted_before_dependent_scientific_work(
    tmp_path: Path,
) -> None:
    source, config = _source(tmp_path)
    target = _target(tmp_path / "workspace")
    image = target.workspace / "cache/images" / f"{'ab' * 32}.sif"
    experiment = replace(
        _experiment(exit_code=0),
        container=ContainerSpec(image),
    )
    build = PreparationBuild(
        argv=("make", "model"),
        outputs=(PreparationOutput(PurePosixPath("main.py")),),
        cache_scope="target",
        resources=ResourceRequest(
            memory_bytes=1024**2,
            walltime=timedelta(minutes=1),
        ),
    )
    preparation = PreparationPlan(
        PreparationConfig(
            source=PreparationSourceGit(
                "https://example.test/project.git",
                "01" * 20,
            ),
            image=PreparationImage(
                PurePosixPath("image.sif"),
                "library://example/image:v1",
                "ab" * 32,
            ),
            build=build,
        ),
        source_mode="working_tree",
        source_root=source,
        offline=True,
    )
    remote = RemotePreparationSpec(
        preparation,
        source_digest="cd" * 32,
        source_action="snapshot_working_tree",
        source_identity="working-tree",
        platform_fingerprint="ef" * 32,
        build_key="12" * 32,
    )
    provenance = PreparationRecord(
        source_identity=remote.source_identity,
        source_digest=remote.source_digest,
        source_action=remote.source_action,
        image_uri=preparation.recipe.image.uri,
        image_sha256=preparation.recipe.image.sha256,
        image_path=image,
        image_action="resolve_in_preparation_job",
        resolution_location="target",
        build_cache_key=remote.build_key,
        builder_location="target",
    )
    plan = create_plan(
        experiment,
        config,
        target,
        seeds=(17,),
        preparation=preparation,
    )
    request = RunExecutionRequest(
        plan=plan,
        experiment=experiment,
        source_root=source,
        fetch_destination=tmp_path / "retrieved",
        preparation=provenance,
        remote_preparation=remote,
    )
    transport = LocalTransport()
    scheduler = DependencyLocalScheduler(transport)
    runtime = HostMappedRuntime()
    service = OrchestrationService(
        store=JsonRunStore(tmp_path / "records"),
        stager=LocalStager(),
        runtime=runtime,
        scheduler=scheduler,
        transport=transport,
        run_id_factory=lambda: _RUN_ID,
        framework_version="0.1.0.dev0",
    )

    result = service.execute_one(request)

    assert scheduler.dependencies == [SchedulerReference("900")]
    assert len(scheduler.preparation_groups) == 1
    assert scheduler.preparation_groups[0].role is SchedulerSubmissionRole.PREPARATION
    assert scheduler.preparation_groups[0].units[0].resources == build.resources
    assert result.record.scheduler_job_ids == ("local-science-reference",)
    assert result.record.preparation is not None
    assert result.record.preparation.builder_scheduler_id == "900"
    assert result.record.preparation.logs == (
        target.workspace / ".rundra-scheduler-logs/900.stdout",
        target.workspace / ".rundra-scheduler-logs/900.stderr",
    )
    record = result.record
    assert [artifact.kind for artifact in record.artifacts] == [
        ArtifactKind.SOURCE_SNAPSHOT,
        ArtifactKind.EFFECTIVE_CONFIG,
        ArtifactKind.STDOUT,
        ArtifactKind.STDERR,
        ArtifactKind.RAW_RESULT,
    ]
    assert all(
        artifact.task_id == record.run.tasks[0].id for artifact in record.artifacts[2:]
    )
    assert runtime.check_calls == 1
    container_request = runtime.build_calls[0]
    assert container_request.command.argv[-2:] == ("--exit-code", "0")
    assert str(container_request.command.working_directory) == "/workspace/source"
    assert [bind.read_only for bind in container_request.binds] == [
        True,
        True,
        False,
        False,
    ]
    _restore_writes(Path(result.workspace.source))
    _restore_writes(Path(result.workspace.inputs))


def test_nonzero_task_still_persists_logs_and_fetches_partial_outputs(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=7)
    service = _service(tmp_path, runtime)

    result = service.execute_one(request)

    record = result.record
    assert record.run.state is ExecutionState.FAILED
    assert record.run.tasks[0].state is ExecutionState.FAILED
    assert record.run.retrieval_state is RetrievalState.SUCCEEDED
    assert record.task_exit_codes == {record.run.tasks[0].id: 7}
    assert (tmp_path / "retrieved/results/result.txt").is_file()
    assert (result.workspace.logs / "task_000000.stdout").is_file()
    assert (result.workspace.logs / "task_000000.stderr").read_text(
        encoding="utf-8"
    ) == "stderr exit=7\n"
    _restore_writes(Path(result.workspace.source))
    _restore_writes(Path(result.workspace.inputs))


def test_retrieval_failure_does_not_change_successful_computation(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    service = _service(
        tmp_path,
        runtime,
        stager=FailingFetchStager(LocalStager()),
    )

    with pytest.raises(OrchestrationError, match="simulated retrieval failure") as exc:
        service.execute_one(request)

    assert exc.value.code == "RESULT_RETRIEVAL_FAILED"
    assert exc.value.run_id == _RUN_ID
    record = service.store.load(_RUN_ID)
    assert record.run.state is ExecutionState.SUCCEEDED
    assert record.run.retrieval_state is RetrievalState.FAILED
    assert record.task_exit_codes == {record.run.tasks[0].id: 0}
    workspace = tmp_path / "workspace/runs" / str(_RUN_ID)
    assert (workspace / "logs/task_000000.stdout").is_file()
    _restore_writes(workspace / "source")
    _restore_writes(workspace / "input")


def test_service_rejects_multi_task_plans_before_creating_a_record(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    source, config = _source(tmp_path)
    experiment = _experiment(exit_code=0)
    plan = create_plan(
        experiment,
        config,
        _target(tmp_path / "workspace"),
        seeds=(1, 2),
    )
    service = _service(tmp_path, runtime)

    with pytest.raises(OrchestrationError, match="requires a Slurm array") as exc:
        service.execute_one(
            RunExecutionRequest(
                plan,
                experiment,
                source,
                tmp_path / "retrieved",
            )
        )

    assert exc.value.code == "UNSUPPORTED_TASK_COUNT"
    assert service.store.list() == ()


def test_service_rejects_a_plan_that_does_not_match_the_recorded_experiment(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    unit = replace(request.plan.units[0], command=Command(("unexpected",)))
    request = replace(request, plan=replace(request.plan, units=(unit,)))
    service = _service(tmp_path, runtime)

    with pytest.raises(OrchestrationError, match="does not match") as exc:
        service.execute_one(request)

    assert exc.value.code == "PLAN_MISMATCH"
    assert service.store.list() == ()


def test_staging_failure_is_persisted_with_an_actionable_phase(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    service = _service(tmp_path, runtime, stager=FailingStageStager())

    with pytest.raises(OrchestrationError, match="simulated staging failure") as exc:
        service.execute_one(request)

    record = service.store.load(_RUN_ID)
    assert exc.value.code == "STAGING_FAILED"
    assert record.run.state is ExecutionState.FAILED
    assert record.native_state == "STAGING_FAILED"
    assert record.completed_at is not None


def test_capability_failure_is_persisted_as_a_failed_run(tmp_path: Path) -> None:
    class MissingRuntime(HostMappedRuntime):
        def check(self) -> CapabilityCheck:
            raise RuntimeError("runtime unavailable")

    runtime = MissingRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    service = _service(tmp_path, runtime)

    with pytest.raises(OrchestrationError, match="runtime unavailable") as exc:
        service.execute_one(request)

    record = service.store.load(_RUN_ID)
    assert exc.value.code == "CAPABILITY_CHECK_FAILED"
    assert record.run.state is ExecutionState.FAILED
    assert record.run.tasks[0].state is ExecutionState.FAILED
    assert record.native_state == "CAPABILITY_CHECK_FAILED"
    assert not (tmp_path / "workspace/runs" / str(_RUN_ID)).exists()


def test_scheduler_reference_is_durable_before_the_first_query(tmp_path: Path) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    task_id = request.plan.units[0].task_id
    reference = SchedulerReference("918273")
    scheduler = QueryFailScheduler(SchedulerSubmission(reference, {task_id: "918273"}))
    service = _service(tmp_path, runtime, scheduler=scheduler)

    with pytest.raises(OrchestrationError, match="accounting unavailable"):
        service.execute_one(request)

    record = service.store.load(_RUN_ID)
    assert record.scheduler_job_ids == ("918273",)
    assert record.submitted_at is not None
    assert record.run.state is ExecutionState.SUBMITTED


def test_async_submit_returns_after_durable_submission_without_querying(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    task_id = request.plan.units[0].task_id
    reference = SchedulerReference("918274")
    scheduler = QueryFailScheduler(SchedulerSubmission(reference, {task_id: "918274"}))
    service = _service(tmp_path, runtime, scheduler=scheduler)

    result = service.submit_one(request)

    assert result.record.run.state is ExecutionState.SUBMITTED
    assert result.record.scheduler_job_ids == ("918274",)
    assert service.store.load(_RUN_ID) == result.record
    assert scheduler.query_calls == 0


def test_completed_receipt_recovers_interrupted_run_record_update(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    task_id = request.plan.units[0].task_id
    scheduler = QueryFailScheduler(
        SchedulerSubmission(SchedulerReference("918275"), {task_id: "918275"})
    )
    service = _service(tmp_path, runtime, scheduler=scheduler)
    receipts = SubmissionReceiptStore(tmp_path / "records")
    service._submission_receipts = receipts
    original_update = service.store.update

    def interrupt_submitted_update(record: RunRecord, *, expected: RunRecord) -> None:
        if record.run.state is ExecutionState.SUBMITTED:
            raise RunStoreError("simulated client interruption")
        original_update(record, expected=expected)

    service.store.update = interrupt_submitted_update  # type: ignore[method-assign]
    with pytest.raises(RunStoreError, match="simulated client interruption"):
        service.submit_one(request)

    service.store.update = original_update  # type: ignore[method-assign]
    recovered, action = service.recover_submission(_RUN_ID)

    assert action == "resumed"
    assert recovered.run.state is ExecutionState.SUBMITTED
    assert recovered.scheduler_job_ids == ("918275",)
    assert service.store.load(_RUN_ID) == recovered


def test_recovery_finds_an_already_durable_submission(tmp_path: Path) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    task_id = request.plan.units[0].task_id
    scheduler = QueryFailScheduler(
        SchedulerSubmission(SchedulerReference("918276"), {task_id: "918276"})
    )
    service = _service(tmp_path, runtime, scheduler=scheduler)
    service._submission_receipts = SubmissionReceiptStore(tmp_path / "records")
    submitted = service.submit_one(request).record

    found, action = service.recover_submission(_RUN_ID)

    assert action == "found"
    assert found == submitted


@pytest.mark.parametrize(
    ("outcome", "error_code", "run_state", "receipt_outcome"),
    [
        (
            SchedulerSubmissionOutcome.REJECTED,
            "SCHEDULER_SUBMISSION_FAILED",
            ExecutionState.FAILED,
            SubmissionReceiptOutcome.REJECTED,
        ),
        (
            SchedulerSubmissionOutcome.UNCERTAIN,
            "SUBMISSION_OUTCOME_UNKNOWN",
            ExecutionState.STAGING,
            SubmissionReceiptOutcome.UNCERTAIN,
        ),
    ],
)
def test_submission_outcome_controls_durable_run_state(
    tmp_path: Path,
    outcome: SchedulerSubmissionOutcome,
    error_code: str,
    run_state: ExecutionState,
    receipt_outcome: SubmissionReceiptOutcome,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    failure = SchedulerSubmissionFailure(
        "safe scheduler failure",
        backend="slurm",
        phase="scheduler_submit",
        outcome=outcome,
        exit_code=1 if outcome is SchedulerSubmissionOutcome.REJECTED else None,
    )
    service = _service(
        tmp_path,
        runtime,
        scheduler=FailingSubmissionScheduler(failure),
    )
    receipts = SubmissionReceiptStore(tmp_path / "records")
    service._submission_receipts = receipts

    with pytest.raises(OrchestrationError) as caught:
        service.submit_one(request)

    record = service.store.load(_RUN_ID)
    receipt = receipts.load(_RUN_ID)
    assert caught.value.code == error_code
    assert record.run.state is run_state
    assert receipt.outcome is receipt_outcome
    if outcome is SchedulerSubmissionOutcome.REJECTED:
        recovered, action = service.recover_submission(_RUN_ID)
        assert action == "rejected"
        assert recovered.run.state is ExecutionState.FAILED
    else:
        with pytest.raises(OrchestrationError) as recovery:
            service.recover_submission(_RUN_ID)
        assert recovery.value.code == "SUBMISSION_OUTCOME_UNKNOWN"


def test_operator_can_resolve_an_uncertain_submission_as_not_submitted(
    tmp_path: Path,
) -> None:
    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    failure = SchedulerSubmissionFailure(
        "safe uncertain failure",
        backend="slurm",
        phase="scheduler_submit",
        outcome=SchedulerSubmissionOutcome.UNCERTAIN,
    )
    service = _service(
        tmp_path,
        runtime,
        scheduler=FailingSubmissionScheduler(failure),
    )
    receipts = SubmissionReceiptStore(tmp_path / "records")
    service._submission_receipts = receipts
    with pytest.raises(OrchestrationError):
        service.submit_one(request)

    with pytest.raises(OrchestrationError) as mismatch:
        service.resolve_submission(
            _RUN_ID,
            confirmation=RunId("run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
    assert mismatch.value.code == "SUBMISSION_CONFIRMATION_MISMATCH"

    resolved = service.resolve_submission(_RUN_ID, confirmation=_RUN_ID)

    assert resolved.run.state is ExecutionState.FAILED
    assert resolved.native_state == "SUBMISSION_CONFIRMED_NOT_SUBMITTED"
    receipt = receipts.load(_RUN_ID)
    assert receipt.outcome is SubmissionReceiptOutcome.OPERATOR_RESOLVED
    assert receipt.failure_classification == "operator_verified_not_submitted"
    assert service.resolve_submission(_RUN_ID, confirmation=_RUN_ID) == resolved


def test_available_source_provenance_is_persisted_before_execution(
    tmp_path: Path,
) -> None:
    class StaticProvenance:
        def capture(self, source_root: PurePath) -> GitProvenance:
            return GitProvenance(
                commit="0123456789abcdef",
                branch="feature/m1.6",
                dirty=True,
                diff="diff --git a/main.py b/main.py\n",
            )

    runtime = HostMappedRuntime()
    request, _ = _request(tmp_path, exit_code=0)
    service = _service(tmp_path, runtime, provenance=StaticProvenance())

    result = service.execute_one(request)

    assert result.record.git_commit == "0123456789abcdef"
    assert result.record.git_branch == "feature/m1.6"
    assert result.record.git_dirty is True
    assert result.record.git_diff == "diff --git a/main.py b/main.py\n"
    _restore_writes(Path(result.workspace.source))
    _restore_writes(Path(result.workspace.inputs))
