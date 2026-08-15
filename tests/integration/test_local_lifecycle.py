from __future__ import annotations

import os
import stat
import sys
from dataclasses import replace
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
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.orchestration.planner import create_plan
from rundra.orchestration.service import (
    OrchestrationError,
    OrchestrationService,
    RunExecutionRequest,
)
from rundra.persistence import JsonRunStore
from rundra.ports import (
    CapabilityCheck,
    ContainerRequest,
    FetchRequest,
    FetchResult,
    StagedWorkspace,
    StageRequest,
)

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
) -> OrchestrationService:
    transport = LocalTransport()
    return OrchestrationService(
        store=JsonRunStore(tmp_path / "records"),
        stager=stager or LocalStager(),
        runtime=runtime,
        scheduler=LocalScheduler(
            transport, reference_factory=lambda: "local-lifecycle-reference"
        ),
        transport=transport,
        run_id_factory=lambda: _RUN_ID,
        framework_version="0.1.0.dev0",
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
    assert record.submitted_at == record.started_at
    assert record.completed_at is not None
    assert record.started_at is not None
    assert record.completed_at >= record.started_at
    assert service.store.load(_RUN_ID) == record
    assert (tmp_path / "retrieved/results/result.txt").read_text(
        encoding="utf-8"
    ) == "seed=17;config=value: 41;mode=integration\n"
    stdout = result.workspace.logs / "task_000000.stdout"
    stderr = result.workspace.logs / "task_000000.stderr"
    assert stdout.read_text(encoding="utf-8") == "stdout seed=17\n"
    assert stderr.read_text(encoding="utf-8") == "stderr exit=0\n"
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

    with pytest.raises(OrchestrationError, match="exactly one Task") as exc:
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
