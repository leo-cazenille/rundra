from pathlib import Path

from rundra.config.experiments import load_experiment
from rundra.config.launch import load_project_launch

_ROOT = Path(__file__).parents[3]
_EXAMPLE = _ROOT / "examples/python-multiprocessing"


def test_python_multiprocessing_experiments_declare_bounded_cpu_resources() -> None:
    local = load_experiment(_EXAMPLE / "experiment-local.yaml")
    shoal = load_experiment(_EXAMPLE / "experiment-shoal.yaml")

    assert local.container is None
    assert shoal.container is not None
    assert str(shoal.container.image) == (
        "/absolute/path/to/python-capable-cpu-image.sif"
    )
    for experiment in (local, shoal):
        assert experiment.command.argv == (
            "python3",
            "main.py",
            "--config",
            "{config}",
            "--seed",
            "{seed}",
        )
        assert experiment.resources.nodes == 1
        assert experiment.resources.tasks == 1
        assert experiment.resources.cpus_per_task == 4
        assert experiment.resources.gpus_per_task == 0
        assert experiment.resources.memory_bytes == 256 * 1024**2
        assert experiment.resources.walltime is not None
        assert experiment.resources.walltime.total_seconds() == 300
        assert experiment.outputs == ("results/**",)


def test_python_multiprocessing_project_profiles_separate_local_and_shoal() -> None:
    project = load_project_launch(_EXAMPLE / "rundra.yaml")

    assert project.default_profile == "local"
    assert project.defaults.config == _EXAMPLE / "config.json"
    assert project.defaults.source_root == _EXAMPLE
    local = project.profiles["local"]
    shoal = project.profiles["shoal"]
    assert local.target == "local"
    assert local.destination == _EXAMPLE / "retrieved/local"
    assert shoal.target == "shoal"
    assert shoal.destination == _EXAMPLE / "retrieved/shoal"
    assert shoal.workers == 2
    assert shoal.task_slots_per_worker == 10


def test_python_multiprocessing_prepared_project_builds_logical_image() -> None:
    experiment = load_experiment(_EXAMPLE / "prepared/experiment.yaml")
    project = load_project_launch(_EXAMPLE / "prepared/rundra.yaml")

    assert experiment.container is not None
    assert str(experiment.container.image) == "python-multiprocessing.sif"
    assert project.version == 3
    assert project.preparation is not None
    assert str(project.preparation.image.name) == "python-multiprocessing.sif"
