from __future__ import annotations

from pathlib import Path

import pytest

from rundra.cli.operations import PlanValue, plan_operation
from rundra.results import OperationResult


def _write_targets(tmp_path: Path, *, remote: bool) -> Path:
    source = tmp_path / ("remote-targets.yaml" if remote else "local-targets.yaml")
    if remote:
        backends = """\
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /tmp/rundra-plan
"""
    else:
        backends = f"""\
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: apptainer}}
    workspace: {tmp_path / "workspace"}
"""
    source.write_text(
        f"version: 1\ntargets:\n  selected:\n{backends}", encoding="utf-8"
    )
    return source


def _write_experiment(
    tmp_path: Path,
    *,
    gpu: bool = False,
    gpus_per_task: int = 0,
    native: str = "",
) -> Path:
    source = tmp_path / "experiment.yaml"
    source.write_text(
        f"""\
version: 1
experiment:
  name: capability-test
command:
  argv: [python, main.py, --config, "{{config}}", --seed, "{{seed}}"]
container:
  image: image.sif
  gpu: {str(gpu).lower()}
resources:
  gpus_per_task: {gpus_per_task}
{native}outputs:
  include: [results/**]
""",
        encoding="utf-8",
    )
    return source


def _plan(
    tmp_path: Path, experiment: Path, targets: Path
) -> OperationResult[PlanValue]:
    config = tmp_path / "config.yaml"
    config.write_text("value: 1\n", encoding="utf-8")
    return plan_operation(experiment, config, targets, "selected", seed=7)


@pytest.mark.parametrize(
    ("gpu", "gpus_per_task"),
    [(False, 1), (True, 0)],
)
def test_plan_rejects_gpu_allocation_and_passthrough_mismatches(
    tmp_path: Path, gpu: bool, gpus_per_task: int
) -> None:
    result = _plan(
        tmp_path,
        _write_experiment(tmp_path, gpu=gpu, gpus_per_task=gpus_per_task),
        _write_targets(tmp_path, remote=False),
    )

    assert result.error is not None
    assert result.error.code == "GPU_CONFIGURATION_MISMATCH"
    assert result.error.details == {
        "gpus_per_task": gpus_per_task,
        "target": "selected",
    }


@pytest.mark.parametrize(
    ("remote", "native"),
    [
        (False, "  native:\n    slurm: {partition: gpu}\n"),
        (True, "  native:\n    slurm: {output: stolen.log}\n"),
        (True, "  native:\n    pbs: {queue: batch}\n"),
    ],
)
def test_plan_rejects_native_options_the_selected_scheduler_cannot_represent(
    tmp_path: Path, remote: bool, native: str
) -> None:
    result = _plan(
        tmp_path,
        _write_experiment(tmp_path, native=native),
        _write_targets(tmp_path, remote=remote),
    )

    assert result.error is not None
    assert result.error.code == "NATIVE_OPTIONS_UNSUPPORTED"
    assert result.error.details["target"] == "selected"


def test_plan_accepts_allowlisted_slurm_options_without_contacting_the_target(
    tmp_path: Path,
) -> None:
    result = _plan(
        tmp_path,
        _write_experiment(
            tmp_path,
            native="  native:\n    slurm: {partition: gpu, exclusive: true}\n",
        ),
        _write_targets(tmp_path, remote=True),
    )

    assert result.error is None
    assert result.value is not None
    assert result.value.plan.units[0].resources.native == {
        "slurm": {"partition": "gpu", "exclusive": True}
    }
