from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pytest

from rundra.domain.models import RunId


def test_run_id_new_generates_portable_unique_values() -> None:
    first = RunId.new()
    second = RunId.new()

    assert re.fullmatch(r"run_[0-9a-f]{32}", str(first))
    assert re.fullmatch(r"run_[0-9a-f]{32}", str(second))
    assert first != second


@pytest.mark.parametrize(
    "value",
    ["", "01abcdef", "run_ABCDEF", "run_1234", "run_" + "g" * 32],
)
def test_run_id_rejects_values_outside_its_public_format(value: str) -> None:
    with pytest.raises(ValueError, match="Run ID"):
        RunId(value)


def test_task_id_from_ordinal_is_deterministic_and_zero_padded() -> None:
    try:
        from rundra.domain.models import TaskId
    except ImportError:
        pytest.fail("TaskId is not implemented")

    assert str(TaskId.from_ordinal(0)) == "task_000000"
    assert str(TaskId.from_ordinal(17)) == "task_000017"
    assert TaskId.from_ordinal(17) == TaskId.from_ordinal(17)


@pytest.mark.parametrize("ordinal", [True, 1.5, "1"])
def test_task_id_rejects_non_integer_ordinals(ordinal: object) -> None:
    from rundra.domain.models import TaskId

    with pytest.raises(TypeError, match="ordinal"):
        TaskId.from_ordinal(ordinal)


def test_command_copies_argv_and_environment_into_immutable_values() -> None:
    try:
        from rundra.domain.models import Command
    except ImportError:
        pytest.fail("Command is not implemented")

    argv = ["python", "main.py"]
    environment = {"SEED": "17"}
    command = Command(
        argv=argv,
        environment=environment,
        working_directory=PurePosixPath("/work"),
    )

    argv.append("--unexpected")
    environment["SEED"] = "99"

    assert command.argv == ("python", "main.py")
    assert command.environment == {"SEED": "17"}
    assert command.working_directory == PurePosixPath("/work")
    with pytest.raises(TypeError):
        command.environment["SEED"] = "99"


@pytest.mark.parametrize("argv", [(), ("python", "")])
def test_command_rejects_missing_or_empty_arguments(argv: tuple[str, ...]) -> None:
    from rundra.domain.models import Command

    with pytest.raises(ValueError, match="argv"):
        Command(argv=argv)


@pytest.mark.parametrize("argv", ["python", (1,)])
def test_command_rejects_non_string_argument_vectors(argv: object) -> None:
    from rundra.domain.models import Command

    with pytest.raises(TypeError, match="argv"):
        Command(argv=argv)


@pytest.mark.parametrize(
    "environment",
    [[("SEED", "17")], {1: "17"}, {"SEED": 17}],
)
def test_command_rejects_invalid_environment_values(environment: object) -> None:
    from rundra.domain.models import Command

    with pytest.raises(TypeError, match="environment"):
        Command(argv=("python",), environment=environment)


def test_command_rejects_invalid_working_directory() -> None:
    from rundra.domain.models import Command

    with pytest.raises(TypeError, match="working_directory"):
        Command(argv=("python",), working_directory="/work")


def test_resource_request_preserves_portable_and_namespaced_native_values() -> None:
    try:
        from rundra.domain.models import ResourceRequest
    except ImportError:
        pytest.fail("ResourceRequest is not implemented")

    native = {"slurm": {"partition": "gpu"}}
    request = ResourceRequest(
        nodes=2,
        tasks=4,
        cpus_per_task=8,
        gpus_per_task=1,
        memory_bytes=32 * 1024**3,
        walltime=timedelta(hours=4),
        native=native,
    )
    native["slurm"]["partition"] = "cpu"

    assert request.nodes == 2
    assert request.memory_bytes == 32 * 1024**3
    assert request.walltime == timedelta(hours=4)
    assert request.native == {"slurm": {"partition": "gpu"}}
    with pytest.raises(TypeError):
        request.native["slurm"]["partition"] = "cpu"


@pytest.mark.parametrize(
    "field, value",
    [
        ("nodes", 0),
        ("tasks", -1),
        ("cpus_per_task", 0),
        ("gpus_per_task", -1),
        ("memory_bytes", 0),
        ("walltime", timedelta(0)),
    ],
)
def test_resource_request_rejects_non_positive_portable_values(
    field: str,
    value: object,
) -> None:
    from rundra.domain.models import ResourceRequest

    with pytest.raises(ValueError, match=field):
        ResourceRequest(**{field: value})


@pytest.mark.parametrize(
    "field, value",
    [
        ("nodes", True),
        ("tasks", 1.0),
        ("cpus_per_task", "1"),
        ("gpus_per_task", False),
        ("memory_bytes", 1.0),
        ("walltime", 60),
    ],
)
def test_resource_request_rejects_wrong_portable_value_types(
    field: str,
    value: object,
) -> None:
    from rundra.domain.models import ResourceRequest

    with pytest.raises(TypeError, match=field):
        ResourceRequest(**{field: value})


@pytest.mark.parametrize(
    "native",
    [
        [("slurm", {"partition": "gpu"})],
        {1: {"partition": "gpu"}},
        {"slurm": [("partition", "gpu")]},
        {"slurm": {1: "gpu"}},
        {"slurm": {"constraint": ["gpu"]}},
    ],
)
def test_resource_request_rejects_invalid_native_options(native: object) -> None:
    from rundra.domain.models import ResourceRequest

    with pytest.raises(TypeError, match="native"):
        ResourceRequest(native=native)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_resource_request_rejects_nonfinite_native_numbers(value: float) -> None:
    """Keeps portable resource values representable in strict JSON."""
    from rundra.domain.models import ResourceRequest

    with pytest.raises(ValueError, match="finite"):
        ResourceRequest(native={"slurm": {"priority": value}})


def test_artifact_represents_only_raw_execution_categories() -> None:
    try:
        from rundra.domain.models import Artifact, ArtifactKind
    except ImportError:
        pytest.fail("Artifact domain values are not implemented")

    artifact = Artifact(
        kind=ArtifactKind.RAW_RESULT,
        path=PurePosixPath("output/result.json"),
        size_bytes=128,
    )

    assert artifact.kind is ArtifactKind.RAW_RESULT
    assert artifact.path == PurePosixPath("output/result.json")
    assert artifact.size_bytes == 128
    assert "derived_analysis" not in {kind.value for kind in ArtifactKind}


@pytest.mark.parametrize(
    "field, value",
    [
        ("kind", "raw_result"),
        ("path", "result.json"),
        ("task_id", "task_000000"),
        ("size_bytes", True),
    ],
)
def test_artifact_rejects_wrong_value_types(field: str, value: object) -> None:
    from rundra.domain.models import Artifact, ArtifactKind

    values = {"kind": ArtifactKind.RAW_RESULT, "path": PurePosixPath("result")}
    values[field] = value
    with pytest.raises(TypeError, match=field):
        Artifact(**values)


def test_target_keeps_backend_selection_and_site_options_outside_experiment() -> None:
    try:
        from rundra.domain.models import BackendConfig, Target
    except ImportError:
        pytest.fail("Target domain values are not implemented")

    ssh_options = {"host": "fishvision"}
    target = Target(
        name="shoal",
        transport=BackendConfig(kind="ssh", options=ssh_options),
        scheduler=BackendConfig(kind="slurm"),
        staging=BackendConfig(kind="rsync"),
        container=BackendConfig(kind="apptainer"),
        workspace=PurePosixPath("/shoalhome/user/.rundra"),
    )
    ssh_options["host"] = "unexpected"

    assert target.name == "shoal"
    assert target.transport.kind == "ssh"
    assert target.transport.options == {"host": "fishvision"}
    with pytest.raises(TypeError):
        target.transport.options["host"] = "unexpected"


def test_backend_config_rejects_blank_kind() -> None:
    from rundra.domain.models import BackendConfig

    with pytest.raises(ValueError, match="kind"):
        BackendConfig(kind=" ")


@pytest.mark.parametrize(
    "options",
    [[("host", "cluster")], {1: "cluster"}, {"host": object()}],
)
def test_backend_config_rejects_invalid_options(options: object) -> None:
    from rundra.domain.models import BackendConfig

    with pytest.raises(TypeError, match="options"):
        BackendConfig(kind="ssh", options=options)


def test_experiment_spec_contains_only_portable_scientific_execution_values() -> None:
    try:
        from rundra.domain.models import (
            Command,
            ContainerSpec,
            ExperimentSpec,
            ResourceRequest,
        )
    except ImportError:
        pytest.fail("ExperimentSpec domain values are not implemented")

    outputs = ["results/**"]
    spec = ExperimentSpec(
        version=1,
        name="collective-departure",
        command=Command(
            argv=(
                "python",
                "main.py",
                "--config",
                "{config}",
                "--seed",
                "{seed}",
            )
        ),
        container=ContainerSpec(
            image=PurePosixPath("containers/project.sif"),
            gpu=True,
        ),
        resources=ResourceRequest(cpus_per_task=4, gpus_per_task=1),
        outputs=outputs,
        sync_excludes=[".git/"],
    )
    outputs.append("unexpected/**")

    assert spec.outputs == ("results/**",)
    assert spec.sync_excludes == (".git/",)
    assert spec.container is not None
    assert spec.container.gpu is True
    assert not hasattr(spec, "scheduler")
    assert not hasattr(spec, "transport")


@pytest.mark.parametrize(
    "version, name, message",
    [(0, "experiment", "version"), (1, " ", "name")],
)
def test_experiment_spec_rejects_invalid_identity(
    version: int,
    name: str,
    message: str,
) -> None:
    from rundra.domain.models import Command, ExperimentSpec, ResourceRequest

    with pytest.raises(ValueError, match=message):
        ExperimentSpec(
            version=version,
            name=name,
            command=Command(argv=("executable",)),
            resources=ResourceRequest(),
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("version", True),
        ("name", []),
        ("command", []),
        ("resources", []),
        ("container", []),
        ("outputs", "results/**"),
        ("outputs", [[]]),
        ("sync_excludes", ".git/"),
        ("sync_excludes", [[]]),
    ],
)
def test_experiment_spec_rejects_wrong_value_types(field: str, value: object) -> None:
    from rundra.domain.models import Command, ExperimentSpec, ResourceRequest

    values = {
        "version": 1,
        "name": "experiment",
        "command": Command(argv=("python",)),
        "resources": ResourceRequest(),
    }
    values[field] = value
    with pytest.raises(TypeError, match=field):
        ExperimentSpec(**values)


@pytest.mark.parametrize("field, value", [("image", []), ("gpu", 1)])
def test_container_spec_rejects_wrong_value_types(field: str, value: object) -> None:
    from rundra.domain.models import ContainerSpec

    values = {"image": PurePosixPath("image.sif"), "gpu": False}
    values[field] = value
    with pytest.raises(TypeError, match=field):
        ContainerSpec(**values)


def test_run_groups_logical_tasks_with_explicit_seeds_and_stable_ids() -> None:
    try:
        from rundra.domain.models import (
            BackendConfig,
            ConfigSnapshot,
            ResourceRequest,
            Run,
            RunId,
            Target,
            Task,
            TaskId,
        )
        from rundra.domain.states import ExecutionState
    except ImportError:
        pytest.fail("Run and Task domain values are not implemented")

    run_id = RunId.new()
    task = Task(
        id=TaskId.from_ordinal(0),
        run_id=run_id,
        experiment_name="experiment",
        config=ConfigSnapshot(
            source=PurePosixPath("configs/test.yaml"),
            content="value: 1\n",
        ),
        seed=42,
        resources=ResourceRequest(),
    )
    tasks = [task]
    target = Target(
        name="local",
        transport=BackendConfig(kind="local"),
        scheduler=BackendConfig(kind="local"),
        staging=BackendConfig(kind="local"),
        container=BackendConfig(kind="apptainer"),
        workspace=PurePosixPath("/tmp/rundra"),
    )
    run = Run(
        id=run_id,
        experiment_name="experiment",
        target=target,
        tasks=tasks,
        created_at=datetime(2026, 8, 15, tzinfo=UTC),
    )
    tasks.clear()

    assert run.tasks == (task,)
    assert task.seed == 42
    assert task.id == TaskId.from_ordinal(0)
    assert task.state is ExecutionState.CREATED
    assert run.state is ExecutionState.CREATED
    assert not hasattr(task, "array_index")
    assert not hasattr(run, "scheduler_job_id")


def test_run_tracks_execution_and_retrieval_states_independently() -> None:
    from rundra.domain.models import Run, RunId
    from rundra.domain.states import ExecutionState, RetrievalState

    run_id = RunId.new()
    run = Run(
        id=run_id,
        experiment_name="experiment",
        target=_target_for_run_tests(),
        tasks=(_task_for_run_tests(run_id),),
        created_at=datetime(2026, 8, 15, tzinfo=UTC),
        state=ExecutionState.SUCCEEDED,
        retrieval_state=RetrievalState.FAILED,
    )

    assert run.state is ExecutionState.SUCCEEDED
    assert run.retrieval_state is RetrievalState.FAILED


@pytest.mark.parametrize("model", ["task", "run"])
def test_run_and_task_reject_non_enum_execution_states(model: str) -> None:
    from rundra.domain.models import Run, RunId

    run_id = RunId.new()
    task = _task_for_run_tests(run_id)
    if model == "task":
        from rundra.domain.models import Task

        with pytest.raises(TypeError, match="state"):
            Task(
                id=task.id,
                run_id=task.run_id,
                experiment_name=task.experiment_name,
                config=task.config,
                seed=task.seed,
                resources=task.resources,
                state="CREATED",
            )
    else:
        with pytest.raises(TypeError, match="state"):
            Run(
                id=run_id,
                experiment_name="experiment",
                target=_target_for_run_tests(),
                tasks=(task,),
                created_at=datetime(2026, 8, 15, tzinfo=UTC),
                state="CREATED",
            )


@pytest.mark.parametrize("seed", [None, 1.5, True, "17"])
def test_task_requires_an_explicit_integer_seed(seed: object) -> None:
    from rundra.domain.models import (
        ConfigSnapshot,
        ResourceRequest,
        RunId,
        Task,
        TaskId,
    )

    with pytest.raises(TypeError, match="seed"):
        Task(
            id=TaskId.from_ordinal(0),
            run_id=RunId.new(),
            experiment_name="experiment",
            config=ConfigSnapshot(
                source=PurePosixPath("configs/test.yaml"),
                content="value: 1\n",
            ),
            seed=seed,
            resources=ResourceRequest(),
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("id", []),
        ("run_id", []),
        ("experiment_name", []),
        ("config", []),
        ("resources", []),
    ],
)
def test_task_rejects_wrong_nested_value_types(field: str, value: object) -> None:
    from rundra.domain.models import (
        ConfigSnapshot,
        ResourceRequest,
        RunId,
        Task,
        TaskId,
    )

    values = {
        "id": TaskId.from_ordinal(0),
        "run_id": RunId.new(),
        "experiment_name": "experiment",
        "config": ConfigSnapshot(PurePosixPath("config.yaml"), "value: 1\n"),
        "seed": 1,
        "resources": ResourceRequest(),
    }
    values[field] = value
    with pytest.raises(TypeError, match=field):
        Task(**values)


@pytest.mark.parametrize("field, value", [("source", []), ("content", [])])
def test_config_snapshot_rejects_wrong_value_types(field: str, value: object) -> None:
    from rundra.domain.models import ConfigSnapshot

    values = {"source": PurePosixPath("config.yaml"), "content": "value: 1\n"}
    values[field] = value
    with pytest.raises(TypeError, match=field):
        ConfigSnapshot(**values)


def _target_for_run_tests():
    from rundra.domain.models import BackendConfig, Target

    local = BackendConfig(kind="local")
    return Target(
        name="local",
        transport=local,
        scheduler=local,
        staging=local,
        container=BackendConfig(kind="apptainer"),
        workspace=PurePosixPath("/tmp/rundra"),
    )


@pytest.mark.parametrize(
    "field, value",
    [
        ("name", []),
        ("transport", []),
        ("scheduler", []),
        ("staging", []),
        ("container", []),
        ("workspace", []),
    ],
)
def test_target_rejects_wrong_value_types(field: str, value: object) -> None:
    from rundra.domain.models import BackendConfig, Target

    local = BackendConfig(kind="local")
    values = {
        "name": "local",
        "transport": local,
        "scheduler": local,
        "staging": local,
        "container": local,
        "workspace": PurePosixPath("/tmp/run"),
    }
    values[field] = value
    with pytest.raises(TypeError, match=field):
        Target(**values)


def _task_for_run_tests(run_id, ordinal: int = 0):
    from rundra.domain.models import (
        ConfigSnapshot,
        ResourceRequest,
        Task,
        TaskId,
    )

    return Task(
        id=TaskId.from_ordinal(ordinal),
        run_id=run_id,
        experiment_name="experiment",
        config=ConfigSnapshot(PurePosixPath("config.yaml"), "value: 1\n"),
        seed=ordinal,
        resources=ResourceRequest(),
    )


def test_run_requires_at_least_one_task() -> None:
    from rundra.domain.models import Run, RunId

    with pytest.raises(ValueError, match="at least one Task"):
        Run(
            id=RunId.new(),
            experiment_name="experiment",
            target=_target_for_run_tests(),
            tasks=(),
            created_at=datetime(2026, 8, 15, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("id", []),
        ("experiment_name", []),
        ("target", []),
        ("tasks", "task"),
        ("created_at", "2026-08-15T00:00:00Z"),
        ("retrieval_state", "NOT_REQUESTED"),
    ],
)
def test_run_rejects_wrong_value_types(field: str, value: object) -> None:
    from rundra.domain.models import Run, RunId

    run_id = RunId.new()
    values = {
        "id": run_id,
        "experiment_name": "experiment",
        "target": _target_for_run_tests(),
        "tasks": (_task_for_run_tests(run_id),),
        "created_at": datetime(2026, 8, 15, tzinfo=UTC),
    }
    values[field] = value
    with pytest.raises(TypeError, match=field):
        Run(**values)


@pytest.mark.parametrize("id_type", ["run", "task"])
def test_identifiers_reject_non_string_values(id_type: str) -> None:
    from rundra.domain.models import RunId, TaskId

    cls = RunId if id_type == "run" else TaskId
    with pytest.raises(TypeError, match="value"):
        cls([])


def test_run_rejects_task_from_another_run() -> None:
    from rundra.domain.models import Run, RunId

    with pytest.raises(ValueError, match="same Run ID"):
        Run(
            id=RunId.new(),
            experiment_name="experiment",
            target=_target_for_run_tests(),
            tasks=(_task_for_run_tests(RunId.new()),),
            created_at=datetime(2026, 8, 15, tzinfo=UTC),
        )


def test_run_rejects_duplicate_task_ids() -> None:
    from rundra.domain.models import Run, RunId

    run_id = RunId.new()
    with pytest.raises(ValueError, match="unique Task IDs"):
        Run(
            id=run_id,
            experiment_name="experiment",
            target=_target_for_run_tests(),
            tasks=(
                _task_for_run_tests(run_id),
                _task_for_run_tests(run_id),
            ),
            created_at=datetime(2026, 8, 15, tzinfo=UTC),
        )


def test_run_rejects_task_from_another_experiment() -> None:
    from dataclasses import replace

    from rundra.domain.models import Run, RunId

    run_id = RunId.new()
    task = replace(
        _task_for_run_tests(run_id),
        experiment_name="different-experiment",
    )
    with pytest.raises(ValueError, match="same experiment"):
        Run(
            id=run_id,
            experiment_name="experiment",
            target=_target_for_run_tests(),
            tasks=(task,),
            created_at=datetime(2026, 8, 15, tzinfo=UTC),
        )


def test_run_requires_timezone_aware_creation_time() -> None:
    from rundra.domain.models import Run, RunId

    run_id = RunId.new()
    with pytest.raises(ValueError, match="timezone-aware"):
        Run(
            id=run_id,
            experiment_name="experiment",
            target=_target_for_run_tests(),
            tasks=(_task_for_run_tests(run_id),),
            created_at=datetime(2026, 8, 15),
        )
