from pathlib import Path

import pytest

from rundra.adapters import RemoteApptainerRuntime, RsyncStager, SSHTransport
from rundra.cli.operations import plan_operation
from rundra.domain.models import ExperimentSpec, ResourceRequest, Target
from rundra.orchestration.preflight import PreflightStatus, RemotePreflight

pytestmark = pytest.mark.shoal_system


def test_shoal_plan_is_single_cpu_and_resource_conscious(
    shoal_experiment_source: Path,
    shoal_config_source: Path,
    shoal_targets_source: Path,
    shoal_target_name: str,
    shoal_cpu_resources: ResourceRequest,
) -> None:
    result = plan_operation(
        shoal_experiment_source,
        shoal_config_source,
        shoal_targets_source,
        shoal_target_name,
        seed=17,
    )
    if result.error is not None:
        pytest.fail(f"Shoal plan failed [{result.error.code}]: {result.error.message}")
    assert result.value is not None
    assert len(result.value.plan.units) == 1
    resources = result.value.plan.units[0].resources
    assert resources.nodes == shoal_cpu_resources.nodes
    assert resources.tasks == shoal_cpu_resources.tasks
    assert resources.cpus_per_task <= shoal_cpu_resources.cpus_per_task
    assert resources.gpus_per_task == 0
    assert resources.memory_bytes is not None
    assert resources.memory_bytes <= shoal_cpu_resources.memory_bytes
    assert resources.walltime is not None
    assert shoal_cpu_resources.walltime is not None
    assert resources.walltime <= shoal_cpu_resources.walltime


def test_shoal_remote_preflight_passes_without_submitting(
    shoal_target: Target,
    shoal_experiment: ExperimentSpec,
) -> None:
    host = shoal_target.transport.options.get("host")
    if type(host) is not str:
        pytest.fail("Shoal SSH target has no string host option")
    transport = SSHTransport(host)
    stager = RsyncStager(transport, host=host)
    report = RemotePreflight(
        shoal_target,
        shoal_experiment,
        transport,
        rsync_check=stager.check,
        runtime=RemoteApptainerRuntime(transport),
    ).run()

    failures = [
        check for check in report.checks if check.status is not PreflightStatus.PASSED
    ]
    if failures:
        diagnostic = "\n".join(
            f"{check.layer}/{check.name} [{check.status}]: {check.message}; "
            f"action: {check.corrective_action or 'none'}"
            for check in failures
        )
        pytest.fail("Shoal preflight did not pass:\n" + diagnostic)
