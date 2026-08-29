from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

from rundra.persistence import JsonRunStore

_EXAMPLE = Path(__file__).parents[2] / "examples/minimal"
_RUNDR = os.environ.get("RUNDRA_LOCAL_DEPLOYMENT_EXECUTABLE", "rundr")


def test_one_argument_run_uses_project_profile_and_user_defaults(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    for name in ("experiment.yaml", "config.yaml", "main.py"):
        shutil.copy2(_EXAMPLE / name, project / name)
    (project / "rundra.yaml").write_text(
        """\
version: 1
default_profile: local
profiles:
  local:
    config: config.yaml
    target: local
    source_root: .
    destination: retrieved
""",
        encoding="utf-8",
    )
    home = tmp_path / "home"
    user_config = home / ".config/rundra"
    user_config.mkdir(parents=True)
    targets = tmp_path / "targets.yaml"
    workspace = tmp_path / "workspace"
    records = tmp_path / "records"
    targets.write_text(
        f"""\
version: 6
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: native}}
    workspace: {workspace}
""",
        encoding="utf-8",
    )
    (user_config / "config.yaml").write_text(
        f"""\
version: 1
defaults:
  targets_file: {targets}
  data_dir: {records}
""",
        encoding="utf-8",
    )

    environment = {**os.environ, "HOME": str(home)}
    planned = subprocess.run(
        [_RUNDR, "plan", "experiment.yaml", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    multi_task_plan = subprocess.run(
        [_RUNDR, "plan", "experiment.yaml", "--seeds", "0:3", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    human_plan = subprocess.run(
        [_RUNDR, "plan", "experiment.yaml"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    result = subprocess.run(
        [_RUNDR, "run", "experiment.yaml", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        assert planned.returncode == 0, planned.stderr or planned.stdout
        assert multi_task_plan.returncode == 0, (
            multi_task_plan.stderr or multi_task_plan.stdout
        )
        assert human_plan.returncode == 0, human_plan.stderr or human_plan.stdout
        assert f"Config: {project / 'config.yaml'} (project profile)" in (
            human_plan.stdout
        )
        preview_seed = json.loads(planned.stdout)["plan"]["units"][0]["seed"]
        assert type(preview_seed) is int and 0 <= preview_seed < 2**63
        planned_launch = json.loads(planned.stdout)["launch"]
        assert planned_launch["profile"] == "local"
        assert planned_launch["sources"]["seed"] == "generated"
        assert planned_launch["sources"]["config"] == "project_profile:local"
        assert planned_launch["sources"]["targets_file"] == "user"
        available_cpus = (
            len(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else (os.cpu_count() or 1)
        )
        multi_plan = json.loads(multi_task_plan.stdout)["plan"]
        assert multi_plan["strategy"] == "worker-pool"
        assert multi_plan["scheduling"]["concurrent_task_capacity"] == min(
            4, available_cpus
        )
        assert result.returncode == 0, result.stderr or result.stdout
        document = json.loads(result.stdout)
        assert document["run"]["state"] == "SUCCEEDED"
        seed = document["run"]["seed"]
        assert type(seed) is int and 0 <= seed < 2**63
        assert document["launch"]["values"]["seed"] == seed
        assert document["launch"]["sources"]["seed"] == "generated"
        assert document["launch"]["sources"]["data_dir"] == "user"
        result_path = project / f"retrieved/results/result-{seed}.json"
        assert result_path.is_file()
        result_document = json.loads(result_path.read_text(encoding="utf-8"))
        assert result_document["seed"] == seed
        replay = subprocess.run(
            [
                "rundr",
                "run",
                "experiment.yaml",
                "--seed",
                str(seed),
                "--destination",
                "replayed",
                "--json",
            ],
            cwd=project,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        assert replay.returncode == 0, replay.stderr or replay.stdout
        assert json.loads(replay.stdout)["launch"]["sources"]["seed"] == "cli"
        assert (
            result_path.read_bytes()
            == (project / f"replayed/results/result-{seed}.json").read_bytes()
        )
        stored = JsonRunStore(records).list()
        assert len(stored) == 2
        assert all(record.run.tasks[0].seed == seed for record in stored)
    finally:
        for run_root in (workspace / "runs").glob("run_*"):
            _restore_writes(run_root / "source")
            _restore_writes(run_root / "input")


def test_one_argument_run_uses_packaged_local_defaults(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    for name in ("experiment.yaml", "config.yaml", "main.py"):
        shutil.copy2(_EXAMPLE / name, project / name)
    home = tmp_path / "home"
    home.mkdir()

    environment = {**os.environ, "HOME": str(home)}
    planned = subprocess.run(
        [_RUNDR, "plan", "experiment.yaml", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    multi_task_plan = subprocess.run(
        [_RUNDR, "plan", "experiment.yaml", "--seeds", "4:12", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    human_plan = subprocess.run(
        [_RUNDR, "plan", "experiment.yaml"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    result = subprocess.run(
        [_RUNDR, "run", "experiment.yaml", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    multi_task_result = subprocess.run(
        [
            _RUNDR,
            "run",
            "experiment.yaml",
            "--seeds",
            "4:12",
            "--destination",
            str(project / "retrieved/multi-task"),
            "--json",
        ],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    workspace = home / ".local/share/rundra/workspaces"
    try:
        assert planned.returncode == 0, planned.stderr or planned.stdout
        assert multi_task_plan.returncode == 0, (
            multi_task_plan.stderr or multi_task_plan.stdout
        )
        assert human_plan.returncode == 0, human_plan.stderr or human_plan.stdout
        assert f"Config: {project / 'config.yaml'} (adjacent default)" in (
            human_plan.stdout
        )
        planned_launch = json.loads(planned.stdout)["launch"]
        assert planned_launch["values"]["config"] == str(project / "config.yaml")
        assert planned_launch["values"]["target"] == "local"
        assert planned_launch["sources"]["config"] == "built_in"
        assert planned_launch["sources"]["target"] == "built_in"
        available_cpus = (
            len(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else (os.cpu_count() or 1)
        )
        multi_plan = json.loads(multi_task_plan.stdout)["plan"]
        assert multi_plan["strategy"] == "worker-pool"
        assert multi_plan["scheduling"]["concurrent_task_capacity"] == min(
            9, available_cpus
        )
        assert result.returncode == 0, result.stderr or result.stdout
        document = json.loads(result.stdout)
        assert document["run"]["state"] == "SUCCEEDED"
        assert document["launch"]["values"]["source_root"] == str(project)
        assert document["launch"]["sources"]["seed"] == "generated"
        generated_seed = document["run"]["seed"]
        assert (
            project / f"retrieved/config/results/result-{generated_seed}.json"
        ).is_file()
        assert multi_task_result.returncode == 0, (
            multi_task_result.stderr or multi_task_result.stdout
        )
        multi_task_document = json.loads(multi_task_result.stdout)
        assert multi_task_document["run"]["state"] == "SUCCEEDED"
        assert multi_task_document["run"]["tasks"] == 9
        retrieved_seeds = {
            int(path.stem.removeprefix("result-"))
            for path in (project / "retrieved/multi-task").glob(
                "task_*/results/result-*.json"
            )
        }
        assert retrieved_seeds == set(range(4, 13))
        assert workspace.is_dir()
        assert not (project / ".rundra").exists()
    finally:
        for run_root in workspace.glob("runs/run_*"):
            _restore_writes(run_root / "source")
            _restore_writes(run_root / "input")


def test_zero_configuration_plan_reports_missing_adjacent_config(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    shutil.copy2(_EXAMPLE / "experiment.yaml", project / "experiment.yaml")
    home = tmp_path / "home"
    home.mkdir()

    result = subprocess.run(
        [_RUNDR, "plan", "experiment.yaml", "--json"],
        cwd=project,
        env={**os.environ, "HOME": str(home)},
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    document = json.loads(result.stdout)
    assert document["error"]["code"] == "CONFIG_NOT_FOUND"
    assert document["error"]["details"]["source"] == str(project / "config.yaml")


def _restore_writes(root: Path) -> None:
    if not root.exists():
        return
    for path in (root, *root.rglob("*")):
        if not path.is_symlink():
            os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)
