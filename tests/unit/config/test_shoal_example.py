from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).parents[3]


def test_checked_shoal_target_example_matches_supported_reference_path() -> None:
    from rundra.config.targets import load_targets

    targets = load_targets(_REPOSITORY_ROOT / "examples/shoal/targets.yaml")

    target = targets["shoal"]
    assert target.transport.kind == "ssh"
    assert target.transport.options == {"host": "fishvision"}
    assert target.scheduler.kind == "slurm"
    assert target.staging.kind == "rsync"
    assert target.container.kind == "apptainer"
    assert str(target.workspace) == "/shoalhome/YOUR_USERNAME/.rundra"


def test_checked_shoal_cpu_example_is_bounded_and_containerized() -> None:
    from rundra.config.experiments import load_experiment

    experiment = load_experiment(
        _REPOSITORY_ROOT / "examples/shoal/cpu/experiment.yaml"
    )

    assert experiment.name == "shoal-cpu"
    assert experiment.container is not None
    assert not experiment.container.gpu
    assert experiment.resources.nodes == 1
    assert experiment.resources.tasks == 1
    assert experiment.resources.cpus_per_task == 1
    assert experiment.resources.gpus_per_task == 0
    assert experiment.resources.memory_bytes == 1024**3
    assert experiment.resources.walltime is not None
    assert experiment.resources.walltime.total_seconds() == 300
    assert experiment.outputs == ("results/**",)


def test_checked_shoal_gpu_example_is_bounded_and_enables_both_gpu_layers() -> None:
    from rundra.adapters.slurm import render_sbatch_script
    from rundra.config.experiments import load_experiment
    from rundra.domain.models import Command, TaskId
    from rundra.ports import SchedulerGroup, SchedulerUnit

    experiment = load_experiment(
        _REPOSITORY_ROOT / "examples/shoal/gpu/experiment.yaml"
    )

    assert experiment.name == "shoal-gpu"
    assert experiment.container is not None
    assert experiment.container.gpu
    assert experiment.resources.nodes == 1
    assert experiment.resources.tasks == 1
    assert experiment.resources.cpus_per_task == 1
    assert experiment.resources.gpus_per_task == 1
    assert experiment.resources.memory_bytes == 1024**3
    assert experiment.resources.walltime is not None
    assert experiment.resources.walltime.total_seconds() == 300
    assert experiment.outputs == ("results/**",)

    unit = SchedulerUnit(
        TaskId.from_ordinal(0),
        Command(("apptainer", "--nv")),
        experiment.resources,
    )
    script = render_sbatch_script(SchedulerGroup((unit,)))
    assert "#SBATCH --gpus-per-task=1\n" in script
    assert "apptainer --nv" in script


def test_checked_shoal_failure_example_is_bounded_and_cpu_only() -> None:
    from rundra.config.experiments import load_experiment

    experiment = load_experiment(
        _REPOSITORY_ROOT / "examples/shoal/failure/experiment.yaml"
    )

    assert experiment.name == "shoal-failure"
    assert experiment.container is not None
    assert not experiment.container.gpu
    assert experiment.resources.nodes == 1
    assert experiment.resources.tasks == 1
    assert experiment.resources.cpus_per_task == 1
    assert experiment.resources.gpus_per_task == 0
    assert experiment.resources.memory_bytes == 1024**3
    assert experiment.resources.walltime is not None
    assert experiment.resources.walltime.total_seconds() == 300
    assert experiment.outputs == ("results/**",)
