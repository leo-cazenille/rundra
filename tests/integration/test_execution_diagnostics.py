from __future__ import annotations

import json
import subprocess
import sys
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from rundra.adapters.local import LocalTransport
from rundra.adapters.slurm import render_sbatch_script
from rundra.cli import capability_doctor
from rundra.config.experiments import load_experiment
from rundra.domain.models import BackendConfig, Command, ResourceRequest, Target, TaskId
from rundra.domain.states import ExecutionState
from rundra.domain.storage import SlurmScratchPolicy
from rundra.execution_diagnostics import (
    DIAGNOSTICS_DIRECTORY,
    read_execution_diagnostics,
)
from rundra.ports import (
    AllocationScratch,
    SchedulerGroup,
    SchedulerObservation,
    SchedulerReference,
    SchedulerUnit,
)


@pytest.mark.parametrize("create_output", [True, False])
@pytest.mark.parametrize("task_count", [1, 2])
def test_required_outputs_and_diagnostics_through_local_cli(
    tmp_path: Path, create_output: bool, task_count: int
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "config.yaml").write_text("{}\n", encoding="utf-8")
    (project / "task.py").write_text(
        "from pathlib import Path\nfrom sys import argv\n"
        + ("Path(argv[2]).write_text('ok\\n')\n" if create_output else "pass\n"),
        encoding="utf-8",
    )
    experiment = project / "experiment.yaml"
    experiment.write_text(
        """\
version: 2
experiment: {name: required-outputs}
command:
  argv: [python3, task.py, "{config}", "/workspace/output/result-{seed}.txt"]
resources: {}
outputs:
  include: ["*.txt"]
  required: ["result-{seed}.txt"]
""",
        encoding="utf-8",
    )
    assert load_experiment(experiment).required_outputs == ("result-{seed}.txt",)
    targets = tmp_path / "targets.yaml"
    settings = yaml.safe_load(
        files("rundra").joinpath("defaults", "targets.yaml").read_text(encoding="utf-8")
    )
    settings["targets"]["local"]["workspace"] = str(tmp_path / "workspace")
    targets.write_text(yaml.safe_dump(settings), encoding="utf-8")
    records = tmp_path / "records"

    def run_cli(*arguments: str):
        process = subprocess.run(
            (sys.executable, "-m", "rundra", *arguments, "--json"),
            cwd=project,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        return json.loads(process.stdout)

    launch = (str(experiment), "--targets-file", str(targets), "--target", "local")
    selection = ("--seed", "3") if task_count == 1 else ("--seeds", "3:4")
    planned = run_cli("plan", *launch, *selection)
    assert planned["ok"], planned
    assert planned["plan"]["required_outputs"] == ["result-{seed}.txt"]
    submitted = run_cli("run", *launch, *selection, "--data-dir", str(records))
    assert submitted["ok"], submitted
    run_id = submitted["run"]["run_id"]
    selection = (run_id, "--data-dir", str(records))
    assert submitted["run"]["state"] == ("SUCCEEDED" if create_output else "FAILED")
    legacy = run_cli("inspect", *selection)
    assert legacy["record"]["experiment"]["required_outputs"] == ["result-{seed}.txt"]
    assert "diagnostics" not in legacy
    diagnostics = run_cli("inspect", *selection, "--diagnostics")["diagnostics"]
    assert diagnostics["format_version"] == 1
    assert diagnostics["run_id"] == run_id
    events = diagnostics["events"]
    if create_output:
        assert events == []
    else:
        assert [
            (event["scope"], event["code"], event["exit_code"]) for event in events
        ] == [("task", "REQUIRED_OUTPUT_MISSING", 86)] * task_count
    assert run_cli("status", *selection, "--diagnostics")["diagnostics"] == diagnostics
    awaited = run_cli("await", *selection, "--diagnostics")
    assert awaited["await"]["runs"][0]["diagnostics"] == diagnostics


def test_slurm_scratch_startup_diagnostic_preserves_worker_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "workspace"
    directory = tmp_path / "actual-scratch"
    directory.mkdir()
    symlink = tmp_path / "linked-scratch"
    symlink.symlink_to(directory, target_is_directory=True)
    monkeypatch.setenv("SLURM_TMPDIR", str(symlink))
    monkeypatch.setenv("SLURM_JOB_ID", "37")
    command = Command(("/bin/true",))
    unit = SchedulerUnit(TaskId("task_000000"), command, ResourceRequest())
    script = render_sbatch_script(
        SchedulerGroup(
            (unit,), scratch=AllocationScratch(workspace, SlurmScratchPolicy())
        )
    )
    transport = LocalTransport()
    result = transport.run(Command(("sh", "-c", script)))
    assert result.exit_code == 72
    diagnostics_dir = workspace / "metadata" / DIAGNOSTICS_DIRECTORY
    availability, events, truncated = read_execution_diagnostics(
        transport, diagnostics_dir
    )
    assert availability == "available"
    assert not truncated
    assert [
        (event.scope, event.identifier, event.code, event.exit_code) for event in events
    ] == [("worker", "37", "SCRATCH_SYMLINK", 72)]
    assert str(symlink) not in events[0].message


@pytest.mark.parametrize("container_exit", [0, 23])
def test_extended_probe_runs_the_existing_image_and_reports_tested_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, container_exit: int
) -> None:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setenv("SLURM_TMPDIR", str(scratch))
    image = tmp_path / "existing.sif"
    image.touch()
    runtime = tmp_path / "singularity"
    runtime.write_text(
        "#!/bin/sh\n"
        'test "$#" = 4 && test "$1" = exec && test "$2" = --cleanenv && '
        'test -f "$3" && test "$4" = /bin/true || exit 27\n'
        f"exit {container_exit}\n",
        encoding="utf-8",
    )
    runtime.chmod(0o700)
    target = Target(
        name="slurm-test",
        transport=BackendConfig("local"),
        scheduler=BackendConfig("slurm"),
        staging=BackendConfig("local"),
        container=BackendConfig("apptainer", {"executable": str(runtime)}),
        workspace=tmp_path,
        execution_storage=SlurmScratchPolicy(),
    )

    class ProbeScheduler:
        def submit(self, group: SchedulerGroup) -> SimpleNamespace:
            self.result = LocalTransport().run(group.units[0].command)
            return SimpleNamespace(reference=SchedulerReference("17"))

        def query(self, references: tuple[SchedulerReference, ...]):
            return (
                SchedulerObservation(
                    references[0],
                    ExecutionState.SUCCEEDED
                    if self.result.exit_code == 0
                    else ExecutionState.FAILED,
                    "COMPLETED" if self.result.exit_code == 0 else "FAILED",
                    exit_code=self.result.exit_code,
                ),
            )

    monkeypatch.setattr(
        capability_doctor,
        "scheduler_for_target",
        lambda *args, **kwargs: ProbeScheduler(),
    )
    check, probe = capability_doctor._scheduler_probe(
        target,
        1,
        ResourceRequest(native={"slurm": {"partition": "cpu-hour"}}),
        image=image,
    )
    assert check.status == ("pass" if container_exit == 0 else "fail")
    assert probe is not None
    assert probe.partition == "cpu-hour"
    assert probe.node == subprocess.check_output(("hostname",), text=True).strip()
    assert probe.stage == ("complete" if container_exit == 0 else "container")
