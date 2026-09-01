from __future__ import annotations

from pathlib import Path

from rundra.cli.placement_operations import placement_plan_operation
from rundra.ports import SchedulerPartition
from rundra.results import OperationError, OperationResult


def _inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: automatic-placement-test}
command:
  argv: [python3, task.py, --config, '{config}', --seed, '{seed}']
container: {image: application.sif}
resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 1
  gpus_per_task: 0
  memory: 1GiB
  walltime: 00:05:00
outputs: {include: ['results/**']}
""",
        encoding="utf-8",
    )
    (tmp_path / "config.yaml").write_text("value: 1\n", encoding="utf-8")
    targets = tmp_path / "targets.yaml"
    target = f"""\
    transport: {{type: ssh, host: cluster}}
    scheduler: {{type: slurm}}
    staging: {{type: rsync}}
    container: {{type: apptainer}}
    workspace: {tmp_path / "workspace"}
"""
    targets.write_text(
        "version: 1\ntargets:\n"
        + "".join(f"  {name}:\n{target}" for name in ("alpha", "beta", "gamma")),
        encoding="utf-8",
    )
    project = tmp_path / "rundra.yaml"
    project.write_text(
        """\
version: 8
defaults:
  config: config.yaml
  source_root: .
  placement: balanced
placements:
  balanced:
    candidates: [alpha, beta, gamma]
    max_utilization_percent: 90
""",
        encoding="utf-8",
    )
    return experiment, targets, project


def _partition(
    *, idle: int, allocated: int, availability: str = "up"
) -> SchedulerPartition:
    total = idle + allocated
    return SchedulerPartition(
        "cpu",
        True,
        availability,
        3600,
        "01:00:00",
        "(null)",
        2,
        allocated,
        idle,
        0,
        total,
    )


def test_placement_splits_contiguous_seeds_by_available_capacity(
    tmp_path: Path,
) -> None:
    experiment, targets, project = _inputs(tmp_path)
    inventory = {
        "alpha": (_partition(idle=8, allocated=0),),
        "beta": (_partition(idle=4, allocated=4),),
        "gamma": (_partition(idle=0, allocated=8),),
    }

    result = placement_plan_operation(
        experiment,
        seeds="0:11",
        targets_file=targets,
        project_file=project,
        observer=lambda _, target: OperationResult.success("plan", inventory[target]),
    )

    assert result.ok and result.value is not None
    assert [item.target for item in result.value.launches] == ["alpha", "beta"]
    assert [item.task_count for item in result.value.launches] == [8, 4]
    assert result.value.placement is not None
    assert result.value.placement.selected_targets == ("alpha", "beta")
    decisions = {item.target: item for item in result.value.placement.targets}
    assert decisions["alpha"].assigned_seed_start == 0
    assert decisions["alpha"].assigned_seed_stop == 7
    assert decisions["beta"].assigned_seed_start == 8
    assert decisions["beta"].assigned_seed_stop == 11
    assert not decisions["gamma"].accepted
    assert decisions["gamma"].reason == "utilization threshold reached"


def test_placement_uses_one_target_when_its_capacity_covers_all_tasks(
    tmp_path: Path,
) -> None:
    experiment, targets, project = _inputs(tmp_path)
    inventory = {
        "alpha": (_partition(idle=16, allocated=0),),
        "beta": (_partition(idle=8, allocated=0),),
        "gamma": (_partition(idle=4, allocated=0),),
    }

    result = placement_plan_operation(
        experiment,
        seeds="0:11",
        targets_file=targets,
        project_file=project,
        observer=lambda _, target: OperationResult.success("plan", inventory[target]),
    )

    assert result.ok and result.value is not None
    assert [item.target for item in result.value.launches] == ["alpha"]
    assert result.value.launches[0].task_count == 12


def test_placement_fails_when_every_candidate_is_unavailable(tmp_path: Path) -> None:
    experiment, targets, project = _inputs(tmp_path)

    result = placement_plan_operation(
        experiment,
        seeds="0:3",
        targets_file=targets,
        project_file=project,
        observer=lambda _, target: OperationResult.failure(
            "plan", OperationError("PLACEMENT_TARGET_UNREACHABLE", target)
        ),
    )

    assert not result.ok and result.error is not None
    assert result.error.code == "PLACEMENT_NO_ELIGIBLE_TARGETS"
    assert result.error.details["rejections"] == (
        "alpha:placement_target_unreachable",
        "beta:placement_target_unreachable",
        "gamma:placement_target_unreachable",
    )
