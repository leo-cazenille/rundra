from pathlib import PurePath

import pytest

from rundra.domain.models import ResourceRequest, Target

pytestmark = pytest.mark.shoal_system


def test_shoal_target_and_resource_budget_are_explicit(
    shoal_target: Target,
    shoal_cpu_resources: ResourceRequest,
) -> None:
    assert shoal_target.transport.kind == "ssh"
    assert shoal_target.scheduler.kind == "slurm"
    assert shoal_target.staging.kind == "rsync"
    assert shoal_target.container.kind == "apptainer"
    assert shoal_target.workspace.is_absolute()
    assert shoal_target.workspace != PurePath("/")
    assert "YOUR_USERNAME" not in str(shoal_target.workspace)

    assert shoal_cpu_resources.nodes == 1
    assert shoal_cpu_resources.tasks == 1
    assert shoal_cpu_resources.cpus_per_task == 1
    assert shoal_cpu_resources.gpus_per_task == 0
    assert shoal_cpu_resources.memory_bytes == 1024**3
    assert shoal_cpu_resources.walltime is not None
    assert shoal_cpu_resources.walltime.total_seconds() == 300
