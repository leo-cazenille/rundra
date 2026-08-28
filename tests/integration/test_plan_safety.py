from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import rundra.cli.operations as operations
from rundra.cli.render import result_document
from rundra.domain.models import RunId


def _snapshot(root: Path) -> dict[str, tuple[str, bytes]]:
    snapshot: dict[str, tuple[str, bytes]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        snapshot[relative] = (
            "directory" if path.is_dir() else "file",
            b"" if path.is_dir() else path.read_bytes(),
        )
    return snapshot


def test_plan_is_offline_and_has_no_workspace_run_or_submission_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment:
  name: safe-plan
command:
  argv: [/bin/sh, run.sh, --config, "{config}", --seed, "{seed}"]
container:
  image: image.sif
  gpu: false
resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 2
  memory: 2GiB
  walltime: "00:10:00"
  native:
    slurm: {partition: batch, exclusive: true}
outputs:
  include: [results/**]
""",
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text("learning_rate: 0.1\n", encoding="utf-8")
    workspace = tmp_path / "remote-workspace"
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""\
version: 1
targets:
  cluster:
    transport: {{type: ssh, host: unreachable.invalid}}
    scheduler: {{type: slurm}}
    staging: {{type: rsync}}
    container: {{type: apptainer}}
    workspace: {workspace}
""",
        encoding="utf-8",
    )
    before = _snapshot(tmp_path)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        pytest.fail("plan crossed an execution or mutation boundary")

    monkeypatch.setattr(operations, "_execution_adapters", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(RunId, "new", forbidden)

    result = operations.plan_operation(
        experiment,
        config,
        targets,
        "cluster",
        seeds="17:18",
    )

    assert result.error is None
    assert result.value is not None
    assert result_document(result)["plan"]["safety"] == {
        "contacts_target": False,
        "creates_run": False,
        "creates_workspace": False,
        "submits": False,
    }
    assert _snapshot(tmp_path) == before
    assert not workspace.exists()
    assert not (tmp_path / "records").exists()


def test_plan_expands_parameter_sets_and_seeds_as_v3(tmp_path: Path) -> None:
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """version: 1
experiment: {name: sweep-plan}
command:
  argv: [/bin/sh, run.sh, --config, "{config}", --seed, "{seed}"]
container: {image: image.sif, gpu: false}
resources: {nodes: 1, tasks: 1, cpus_per_task: 1, memory: 1GiB, walltime: "00:05:00"}
outputs: {include: [results/**]}
""",
        encoding="utf-8",
    )
    config = tmp_path / "sweep.yaml"
    config.write_text(
        """_rundr: {version: 1}
parameters:
  speed: {batch_options: [1, 2]}
""",
        encoding="utf-8",
    )
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 1
targets:
  cluster:
    transport: {{type: ssh, host: cluster}}
    scheduler: {{type: slurm}}
    staging: {{type: rsync}}
    container: {{type: apptainer}}
    workspace: {tmp_path / "workspace"}
""",
        encoding="utf-8",
    )

    result = operations.plan_operation(
        experiment, config, targets, "cluster", seeds="0:1"
    )

    assert result.error is None
    document = result_document(result)
    assert document["format_version"] == 10
    assert document["plan"]["version"] == 3
    units = document["plan"]["units"]
    assert len(units) == 4
    assert [unit["seed"] for unit in units] == [0, 1, 0, 1]
    assert [unit["parameter_set"]["choices"] for unit in units] == [
        {"parameters.speed": 1},
        {"parameters.speed": 1},
        {"parameters.speed": 2},
        {"parameters.speed": 2},
    ]
    assert all(len(unit["config_sha256"]) == 64 for unit in units)
