from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pytest

from rundra.config.errors import ConfigError
from rundra.config.targets import load_targets
from rundra.domain.models import ResourceRequest, Target


@pytest.fixture(scope="session")
def shoal_target() -> Target:
    source_value = os.environ.get("RUNDRA_SHOAL_TARGETS_FILE")
    if source_value is None or not source_value.strip():
        pytest.fail(
            "RUNDRA_SHOAL_TARGETS_FILE must name an explicit, non-secret target "
            "file when Shoal system tests are enabled"
        )
    source = Path(source_value).expanduser()
    if not source.is_file():
        pytest.fail(f"Shoal target file does not exist: {source}")
    try:
        targets = load_targets(source)
    except ConfigError as error:
        pytest.fail(f"Could not load Shoal target file: {error}")
    target_name = os.environ.get("RUNDRA_SHOAL_TARGET", "shoal")
    if target_name not in targets:
        pytest.fail(f"Shoal target file has no target named {target_name!r}")
    target = targets[target_name]
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
