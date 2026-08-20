from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from rundra.cli.main import main
from rundra.persistence import JsonRunStore


def test_one_command_local_prepared_run_persists_v2_provenance(
    tmp_path: Path,
    monkeypatch: object,
    capsys: object,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    experiment = project / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: locally-prepared}
command:
  argv:
    - python3
    - /workspace/source/main.py
    - "{config}"
    - "{seed}"
    - /workspace/output/result.json
container: {image: application.sif}
resources: {}
outputs: {include: [result.json]}
""",
        encoding="utf-8",
    )
    (project / "config.yaml").write_text("value: 4\n", encoding="utf-8")
    (project / "main.py").write_text(
        """\
import json
import pathlib
import sys

pathlib.Path(sys.argv[3]).write_text(
    json.dumps({"config": pathlib.Path(sys.argv[1]).read_text(), "seed": int(sys.argv[2])})
)
""",
        encoding="utf-8",
    )
    image = project / "application.sif"
    image.write_bytes(b"fake immutable sif")
    digest = hashlib.sha256(image.read_bytes()).hexdigest()
    (project / "rundra.yaml").write_text(
        f"""\
version: 2
defaults:
  config: config.yaml
  target: local
preparation:
  source:
    git:
      url: https://invalid.example.test/project.git
      revision: 0123456789abcdef0123456789abcdef01234567
  image:
    name: application.sif
    uri: library://invalid/application:v1
    sha256: {digest}
""",
        encoding="utf-8",
    )
    workspace = tmp_path / "workspace"
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""\
version: 1
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: apptainer}}
    workspace: {workspace}
""",
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_fake_apptainer(fake_bin / "apptainer")
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")  # type: ignore[attr-defined]
    destination = tmp_path / "retrieved"
    records = tmp_path / "records"

    exit_code = main(
        (
            "run",
            str(experiment),
            "--source-root",
            str(project),
            "--seed",
            "7",
            "--targets-file",
            str(targets),
            "--destination",
            str(destination),
            "--data-dir",
            str(records),
            "--offline",
            "--json",
        )
    )
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    document = json.loads(captured.out)

    assert exit_code == 0, document
    assert document["format_version"] == 2
    assert document["run"]["state"] == "SUCCEEDED"
    output = json.loads((destination / "result.json").read_text(encoding="utf-8"))
    assert output == {"config": "value: 4\n", "seed": 7}
    record = JsonRunStore(records).list()[0]
    assert record.format_version == 2
    assert record.container_digest == digest
    assert record.preparation is not None
    assert record.preparation.source_action == "snapshot_working_tree"
    assert record.preparation.image_path.is_absolute()
    assert record.experiment.container is not None
    assert record.experiment.container.image == record.preparation.image_path
    assert record.scheduler_metadata["container_runtime"] == "apptainer"
    assert (
        record.scheduler_metadata["container_runtime_version"]
        == "apptainer version test"
    )


def _write_fake_apptainer(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import os
import subprocess
import sys

args = sys.argv[1:]
if args == ["version"]:
    print("apptainer version test")
    raise SystemExit(0)
if args[0] != "exec":
    raise SystemExit(64)
mounts = {}
index = 1
while index < len(args):
    if args[index] == "--bind":
        source, destination, _mode = args[index + 1].split(":")
        mounts[destination] = source
        index += 2
    elif args[index] == "--cwd":
        cwd = args[index + 1]
        index += 2
    elif args[index].startswith("--"):
        index += 1
    else:
        index += 1
        break

def translate(value):
    for destination, source in mounts.items():
        if value == destination or value.startswith(destination + "/"):
            return source + value[len(destination):]
    return value

command = [translate(value) for value in args[index:]]
raise SystemExit(subprocess.run(command, cwd=translate(cwd), env=os.environ).returncode)
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
