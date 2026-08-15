from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

from rundra.persistence import JsonRunStore

_EXAMPLE = Path(__file__).parents[2] / "examples/minimal"


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
version: 1
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
        ["rundr", "plan", "experiment.yaml", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    result = subprocess.run(
        ["rundr", "run", "experiment.yaml", "--json"],
        cwd=project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    try:
        assert planned.returncode == 0, planned.stderr or planned.stdout
        preview_seed = json.loads(planned.stdout)["plan"]["units"][0]["seed"]
        assert type(preview_seed) is int and 0 <= preview_seed < 2**63
        planned_launch = json.loads(planned.stdout)["launch"]
        assert planned_launch["profile"] == "local"
        assert planned_launch["sources"]["seed"] == "generated"
        assert planned_launch["sources"]["config"] == "project_profile:local"
        assert planned_launch["sources"]["targets_file"] == "user"
        assert result.returncode == 0, result.stderr or result.stdout
        document = json.loads(result.stdout)
        assert document["run"]["state"] == "SUCCEEDED"
        seed = document["run"]["seed"]
        assert type(seed) is int and 0 <= seed < 2**63
        assert document["launch"]["values"]["seed"] == seed
        assert document["launch"]["sources"]["seed"] == "generated"
        assert document["launch"]["sources"]["data_dir"] == "user"
        assert (project / "retrieved/results/result.json").is_file()
        result_document = json.loads(
            (project / "retrieved/results/result.json").read_text(encoding="utf-8")
        )
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
        assert (project / "retrieved/results/result.json").read_bytes() == (
            project / "replayed/results/result.json"
        ).read_bytes()
        stored = JsonRunStore(records).list()
        assert len(stored) == 2
        assert all(record.run.tasks[0].seed == seed for record in stored)
    finally:
        for run_root in (workspace / "runs").glob("run_*"):
            _restore_writes(run_root / "source")
            _restore_writes(run_root / "input")


def _restore_writes(root: Path) -> None:
    if not root.exists():
        return
    for path in (root, *root.rglob("*")):
        if not path.is_symlink():
            os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)
