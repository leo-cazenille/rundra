from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep
from typing import Any

import pytest
import yaml

from rundra.cli.operations import _record_ssh_transport, _record_workspace
from rundra.domain.models import Command, RunId
from rundra.persistence import JsonRunStore, SqliteTaskStore

pytestmark = pytest.mark.docker_slurm

# Use the ordinary CLI with small Task spaces. Record which journal bytes the
# real SSH transport returns without adding instrumentation to Rundra's API.
_CLI_DRIVER = """\
import json
import os
import sys
from dataclasses import replace
from pathlib import Path
from rundra.adapters.native import NativeRuntime
from rundra.adapters.ssh import SSHTransport
from rundra.cli import operations
from rundra.cli.main import main

trace, mode = sys.argv[1:3]
operations._COMPACT_EXECUTION_THRESHOLD = 2
SSHTransport.supports_incremental_journals = mode != "full"
run_ssh = SSHTransport.run

def traced(self, command):
    result = run_ssh(self, command)
    if command.argv[3:4] == ("rundra-journal-delta",) and result.exit_code == 0:
        rows = []
        for row in result.stdout.splitlines():
            name, offset, size, _ = row.split("\\t")
            rows.append([name, int(offset), int(size)])
        event = {"mode": "delta", "pid": os.getpid(), "rows": rows}
    elif command.argv[3:4] == ("rundra-compact-bundle-status",):
        event = {"mode": "full", "pid": os.getpid(), "rows": []}
    else:
        return result
    with Path(trace).open("a", encoding="utf-8") as log:
        log.write(json.dumps(event) + "\\n")
    return result

SSHTransport.run = traced

if os.environ.get("RUNDRA_DOCKER_COMPACT_NATIVE") == "1":
    # Opt-in for local Docker hosts that block nested user namespaces.
    # Slurm, workspace staging and all SSH operations are still real.
    class FixtureRuntime(NativeRuntime):
        def __init__(self, transport, executable):
            pass

        def build_command(self, request):
            command = super().build_command(replace(request, image=None))
            # The payload uses absolute staged paths. Keep a shell cd out of
            # the command that the worker passes to timeout.
            return replace(command, working_directory=None)

    operations.RemoteApptainerRuntime = FixtureRuntime

raise SystemExit(main(sys.argv[3:]))
"""

_PAYLOAD = """\
set -eu
config=$1
seed=$2
output=$3
runtime=$4
variant=$(awk '$1 == "variant:" { print $2 }' "$config")
if [ "$variant" = gated ]; then
    while [ ! -f "$runtime/release" ]; do sleep 0.2; done
fi
mkdir -p -- "$output/results"
printf '{"seed":%s,"variant":"%s"}\\n' "$seed" "$variant" > "$output/results/result.json"
if [ "$variant" = fail_one ] && [ "$seed" = 7 ]; then exit 23; fi
"""


@dataclass(frozen=True)
class _Harness:
    root: Path
    target: str
    targets: Path
    source: Path
    experiment: Path

    @property
    def records(self) -> Path:
        return self.root / "records"

    @property
    def trace(self) -> Path:
        return self.root / "journal-reads.jsonl"

    def cli(
        self,
        *arguments: str,
        expected: int = 0,
        records: Path | None = None,
        full: bool = False,
    ) -> dict[str, Any]:
        result = subprocess.run(
            (
                sys.executable,
                "-c",
                _CLI_DRIVER,
                str(self.trace),
                "full" if full else "delta",
                *arguments,
                "--data-dir",
                str(records or self.records),
                "--json",
            ),
            capture_output=True,
            check=False,
            text=True,
            timeout=160,
        )
        assert result.returncode == expected, result.stderr or result.stdout
        document = json.loads(result.stdout)
        assert isinstance(document, dict)
        return document

    def submit(self, config: Path, seeds: str) -> str:
        result = self.cli(
            "submit",
            str(self.experiment),
            "--target",
            self.target,
            "--targets-file",
            str(self.targets),
            "--source-root",
            str(self.source),
            "--config",
            str(config),
            "--destination",
            str(self.root / "retrieved"),
            "--seeds",
            seeds,
        )
        run_id = str(result["run"]["run_id"])
        assert JsonRunStore(self.records).load(RunId(run_id)).is_compact
        return run_id

    def await_run(
        self, run_id: str, *, records: Path | None = None, full: bool = False
    ) -> dict[str, Any]:
        result = self.cli(
            "await",
            run_id,
            "--timeout",
            "100",
            "--poll-interval",
            "0.5",
            records=records,
            full=full,
        )["await"]
        assert result["condition_met"], result
        return result["runs"][0]


@pytest.fixture
def compact_slurm(
    tmp_path: Path,
    docker_slurm_targets_source: Path,
    docker_slurm_target_name: str,
) -> _Harness:
    document = yaml.safe_load(docker_slurm_targets_source.read_text(encoding="utf-8"))
    target = document["targets"][docker_slurm_target_name]
    # The test driver signals gated Tasks through the shared runtime bind.
    target.pop("execution_storage", None)
    target["execution"]["output_shard_tasks"] = 2
    target["execution"]["worker_pool"].update(
        activation_threshold=2,
        default_workers=1,
        max_workers=1,
        infrastructure_retry_limit=0,
        requeue_limit=0,
    )
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        yaml.safe_dump(
            {
                "version": document["version"],
                "targets": {docker_slurm_target_name: target},
            }
        ),
        encoding="utf-8",
    )
    source = tmp_path / "source"
    source.mkdir()
    (source / "payload.sh").write_text(_PAYLOAD, encoding="utf-8")
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: docker-compact-validation}
command:
  argv:
    - /bin/sh
    - /workspace/source/payload.sh
    - "{config}"
    - "{seed}"
    - /workspace/output
    - /workspace/runtime
container: {image: /cluster/rundra-test.sif}
resources: {memory: 32MiB, walltime: "00:02:00"}
outputs: {include: ["results/**"]}
""",
        encoding="utf-8",
    )
    return _Harness(tmp_path, docker_slurm_target_name, targets, source, experiment)


def test_ssh_compact_retry_and_export_keep_exact_failed_coordinates(
    compact_slurm: _Harness,
) -> None:
    harness = compact_slurm
    config = harness.root / "sweep.yaml"
    config.write_text(
        "_rundr: {version: 1}\nvariant: {batch_options: [control, fail_one]}\n",
        encoding="utf-8",
    )
    run_id = harness.submit(config, "7:8")
    status = harness.await_run(run_id)
    assert status["state"] == "FAILED"
    tasks = harness.cli("tasks", run_id)["tasks"]["items"]
    assert [(item["seed"], item["state"]) for item in tasks] == [
        (7, "SUCCEEDED"),
        (8, "SUCCEEDED"),
        (7, "FAILED"),
        (8, "SUCCEEDED"),
    ]
    failed = tasks[2]
    assert failed["exit_code"] == 23

    record = JsonRunStore(harness.records).load(RunId(run_id))
    workspace = _record_workspace(record)
    transport = _record_ssh_transport(record)
    manifest = transport.run(
        Command(("cat", "--", str(workspace.metadata / "tasks.json")))
    )
    assert manifest.exit_code == 0
    parameter = json.loads(manifest.stdout)["parameter_sets"][1]
    retained_path = workspace.root / parameter["config"]
    original = transport.run(Command(("cat", "--", str(retained_path))))
    assert original.exit_code == 0
    retained = original.stdout.encode("utf-8")
    assert hashlib.sha256(retained).hexdigest() == parameter["config_sha256"]
    parameter_id = parameter["parameter_set"]["id"]

    retry = ("retry", run_id, "--failed", "--targets-file", str(harness.targets))
    preview = harness.cli(*retry)["retry"]
    assert preview["run_id"] is None
    assert preview["task_count"] == 1
    assert preview["tasks"] == [
        {
            "parent_task_id": failed["task_id"],
            "task_id": "task_000000",
            "seed": 7,
            "parameter_set": parameter_id,
        }
    ]
    # A changed client checkout and config must have no effect on the retry.
    (harness.source / "payload.sh").write_text("exit 99\n", encoding="utf-8")
    config.write_text("variant: changed\n", encoding="utf-8")
    retried = harness.cli(*retry, "--confirm-tasks", "1")["retry"]
    assert retried["state"] == "SUBMITTED"
    child = str(retried["run_id"])
    assert harness.await_run(child)["state"] == "FAILED"
    retry_record = harness.cli("inspect", child)["record"]
    assert retry_record["scheduler_metadata"]["retry_parent_run_id"] == run_id
    assert retry_record["task_exit_codes"] == {"task_000000": 23}
    assert [
        task["config"]["content"].encode("utf-8")
        for task in retry_record["run"]["tasks"]
    ] == [retained]
    lineage = json.loads(
        (harness.records / "retry-lineage" / f"{child}.json").read_text(
            encoding="utf-8"
        )
    )
    assert lineage["tasks"] == preview["tasks"]

    fetched = harness.cli("fetch", run_id, "--mode", "copy", "--extract")["fetch"]
    assert fetched["retrieval_state"] == "SUCCEEDED"
    destination = harness.root / "parent.tar.gz"
    archived = harness.cli(
        "export", run_id, "--destination", str(destination), "--include-results"
    )
    assert archived["export"]["result_count"] == 4
    with tarfile.open(destination, "r:gz") as bundle:
        catalog = json.load(bundle.extractfile("manifest.json"))
        names = catalog["files"]
        prefix = f"runs/{run_id}"
        assert (
            bundle.extractfile(
                f"{prefix}/configs/{parameter['config_sha256']}.yaml"
            ).read()
            == retained
        )
        results = []
        for name in names:
            if not name.startswith(f"{prefix}/results/"):
                continue
            contents = bundle.extractfile(name).read()
            assert hashlib.sha256(contents).hexdigest() == names[name]["sha256"]
            result = json.loads(contents)
            results.append((result["variant"], result["seed"]))
        assert sorted(results) == [
            ("control", 7),
            ("control", 8),
            ("fail_one", 7),
            ("fail_one", 8),
        ]

    # Inject corruption only into this disposable Run's retained config.
    updated = transport.run(
        Command(
            (
                "sh",
                "-c",
                'set -eu; chmod u+w -- "$1"; printf "\\nchanged: true\\n" >> "$1"; chmod a-w -- "$1"',
                "corrupt-test-config",
                str(retained_path),
            )
        )
    )
    assert updated.exit_code == 0
    before = {item.run.id for item in JsonRunStore(harness.records).list()}
    rejected = harness.cli(*retry, "--confirm-tasks", "1", expected=1)
    assert rejected["error"]["code"] == "RETRY_INPUTS_UNAVAILABLE"
    assert "has changed" in rejected["error"]["message"]
    assert {item.run.id for item in JsonRunStore(harness.records).list()} == before


def test_ssh_compact_polls_use_deltas_across_processes_and_read_aggregate(
    compact_slurm: _Harness,
) -> None:
    harness = compact_slurm
    config = harness.root / "gated.yaml"
    config.write_text("variant: gated\n", encoding="utf-8")
    run_id = harness.submit(config, "0:1")
    record = JsonRunStore(harness.records).load(RunId(run_id))
    workspace = _record_workspace(record)
    transport = _record_ssh_transport(record)

    def poll(expected: list[str]) -> None:
        deadline = monotonic() + 45
        while True:
            harness.cli("status", run_id, "--summary")
            items = harness.cli("tasks", run_id)["tasks"]["items"]
            states = [item["state"] for item in items]
            if states == expected:
                return
            assert monotonic() < deadline and "FAILED" not in states, states
            sleep(0.2)

    def release(task: str) -> None:
        ready = transport.run(
            Command(("touch", "--", str(workspace.runtime / task / "release")))
        )
        assert ready.exit_code == 0

    poll(["RUNNING", "QUEUED"])
    initial = SqliteTaskStore(harness.records).journal_cache(RunId(run_id))
    assert any(
        ".lane-" in name and b"START" in content for name, content in initial.items()
    )
    release("task_000000")
    poll(["SUCCEEDED", "RUNNING"])
    # Both readers start final reconciliation from the same nonterminal state.
    comparison = harness.root / "full-read-records"
    shutil.copytree(harness.records, comparison)
    release("task_000001")
    final = harness.await_run(run_id)
    full = harness.await_run(run_id, records=comparison, full=True)
    assert final["state"] == full["state"] == "SUCCEEDED"
    assert final["tasks"] == full["tasks"] == {"total": 2, "succeeded": 2}
    assert (
        harness.cli("tasks", run_id)["tasks"]["items"]
        == harness.cli("tasks", run_id, records=comparison)["tasks"]["items"]
    )

    observations = [
        json.loads(line)
        for line in harness.trace.read_text(encoding="utf-8").splitlines()
    ]
    deltas = [event for event in observations if event["mode"] == "delta"]
    assert len({event["pid"] for event in deltas}) >= 2
    assert any(
        ".lane-" in name and 0 < offset < size
        for event in deltas
        for name, offset, size in event["rows"]
    ), observations
    assert any(
        ".lane-" not in name and name.endswith(".attempt-0.tsv") and offset == 0 < size
        for event in deltas
        for name, offset, size in event["rows"]
    ), observations
    assert any(event["mode"] == "full" for event in observations)
    cached = SqliteTaskStore(harness.records).journal_cache(RunId(run_id))
    assert len(cached) == 1
    name, aggregate = next(iter(cached.items()))
    assert ".lane-" not in name and name.endswith(".attempt-0.tsv")
    assert aggregate.count(b"FINISH\t") == 2
