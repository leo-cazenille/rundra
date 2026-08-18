from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pytest

from rundra.config.errors import ConfigError
from rundra.config.experiments import load_experiment
from rundra.config.targets import load_targets
from rundra.domain.models import ExperimentSpec, ResourceRequest, Target


def _required_file(variable: str) -> Path:
    source_value = os.environ.get(variable)
    if source_value is None or not source_value.strip():
        pytest.fail(f"{variable} must name an explicit file when enabled")
    source = Path(source_value).expanduser()
    if not source.is_file():
        pytest.fail(f"{variable} file does not exist: {source}")
    return source


@pytest.fixture(scope="session")
def shoal_targets_source() -> Path:
    return _required_file("RUNDRA_SHOAL_TARGETS_FILE")


@pytest.fixture(scope="session")
def shoal_target_name() -> str:
    return os.environ.get("RUNDRA_SHOAL_TARGET", "shoal")


@pytest.fixture(scope="session")
def shoal_target(shoal_targets_source: Path, shoal_target_name: str) -> Target:
    try:
        targets = load_targets(shoal_targets_source)
    except ConfigError as error:
        pytest.fail(f"Could not load Shoal target file: {error}")
    if shoal_target_name not in targets:
        pytest.fail(f"Shoal target file has no target named {shoal_target_name!r}")
    target = targets[shoal_target_name]
    actual_backends = (
        target.transport.kind,
        target.scheduler.kind,
        target.staging.kind,
        target.container.kind,
    )
    expected_backends = ("ssh", "slurm", "rsync", "apptainer")
    if actual_backends != expected_backends:
        pytest.fail(
            "Shoal system tests require SSH/Slurm/rsync/Apptainer; got "
            + "/".join(actual_backends)
        )
    if not target.workspace.is_absolute() or str(target.workspace) == "/":
        pytest.fail("Shoal workspace must be an absolute, non-root path")
    if "YOUR_USERNAME" in str(target.workspace):
        pytest.fail("Replace YOUR_USERNAME in the copied Shoal target before opting in")
    return target


@pytest.fixture(scope="session")
def shoal_experiment_source() -> Path:
    return _required_file("RUNDRA_SHOAL_EXPERIMENT")


@pytest.fixture(scope="session")
def shoal_config_source() -> Path:
    return _required_file("RUNDRA_SHOAL_CONFIG")


@pytest.fixture(scope="session")
def shoal_cpu_image() -> Path:
    image = _required_file("RUNDRA_SHOAL_CPU_IMAGE")
    if not image.is_absolute():
        pytest.fail("RUNDRA_SHOAL_CPU_IMAGE must be an absolute path")
    return image


@pytest.fixture(scope="session")
def shoal_gpu_image() -> Path:
    image = _required_file("RUNDRA_SHOAL_GPU_IMAGE")
    if not image.is_absolute():
        pytest.fail("RUNDRA_SHOAL_GPU_IMAGE must be an absolute path")
    return image


@pytest.fixture(scope="session")
def docker_slurm_targets_source() -> Path:
    return _required_file("RUNDRA_DOCKER_SLURM_TARGETS_FILE")


@pytest.fixture(scope="session")
def docker_slurm_target_name() -> str:
    return os.environ.get("RUNDRA_DOCKER_SLURM_TARGET", "docker-slurm")


@pytest.fixture(scope="session")
def shoal_experiment(shoal_experiment_source: Path) -> ExperimentSpec:
    try:
        return load_experiment(shoal_experiment_source)
    except ConfigError as error:
        pytest.fail(f"Could not load Shoal experiment: {error}")


@pytest.fixture(scope="session")
def shoal_cpu_resources() -> ResourceRequest:
    """Bounded defaults for later CPU system checks; this fixture submits nothing."""
    return ResourceRequest(
        nodes=1,
        tasks=1,
        cpus_per_task=1,
        gpus_per_task=0,
        memory_bytes=1024**3,
        walltime=timedelta(minutes=5),
    )


@pytest.fixture(scope="session")
def shoal_gpu_resources() -> ResourceRequest:
    """Bounded defaults for the separately authorized GPU system check."""
    return ResourceRequest(
        nodes=1,
        tasks=1,
        cpus_per_task=1,
        gpus_per_task=1,
        memory_bytes=1024**3,
        walltime=timedelta(minutes=5),
    )
