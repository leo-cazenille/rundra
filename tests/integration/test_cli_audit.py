from __future__ import annotations

import json
import os
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).parents[2]
_EXAMPLE = _ROOT / "examples/minimal"


@dataclass(frozen=True, slots=True)
class _CommandCase:
    operation: str
    arguments: tuple[str, ...]
    ok: bool
    error_code: str | None = None


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("rundr", *arguments),
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _cases(tmp_path: Path) -> tuple[_CommandCase, ...]:
    missing_experiment = str(tmp_path / "missing-experiment.yaml")
    missing_targets = str(tmp_path / "missing-targets.yaml")
    records = str(tmp_path / "records")
    shared_launch = (
        missing_experiment,
        "--config",
        str(_EXAMPLE / "config.yaml"),
        "--seed",
        "17",
        "--target",
        "local",
        "--targets-file",
        str(_EXAMPLE / "targets.yaml"),
    )
    return (
        _CommandCase(
            "validate", ("validate", missing_experiment), False, "CONFIG_NOT_FOUND"
        ),
        _CommandCase("plan", ("plan", *shared_launch), False, "CONFIG_NOT_FOUND"),
        _CommandCase(
            "targets",
            ("targets", "--targets-file", missing_targets),
            False,
            "CONFIG_NOT_FOUND",
        ),
        _CommandCase(
            "run",
            (
                "run",
                *shared_launch,
                "--source-root",
                str(_EXAMPLE),
                "--destination",
                str(tmp_path / "run-output"),
                "--data-dir",
                records,
            ),
            False,
            "CONFIG_NOT_FOUND",
        ),
        _CommandCase(
            "submit",
            (
                "submit",
                *shared_launch,
                "--source-root",
                str(_EXAMPLE),
                "--destination",
                str(tmp_path / "submit-output"),
                "--data-dir",
                records,
            ),
            False,
            "CONFIG_NOT_FOUND",
        ),
        _CommandCase(
            "status",
            ("status", "invalid", "--data-dir", records),
            False,
            "INVALID_RUN_ID",
        ),
        _CommandCase("list", ("list", "--data-dir", records), True),
        _CommandCase(
            "logs",
            ("logs", "invalid", "--data-dir", records),
            False,
            "INVALID_RUN_ID",
        ),
        _CommandCase(
            "fetch",
            (
                "fetch",
                "invalid",
                "--destination",
                str(tmp_path / "fetch-output"),
                "--data-dir",
                records,
            ),
            False,
            "INVALID_RUN_ID",
        ),
        _CommandCase(
            "inspect",
            ("inspect", "invalid", "--data-dir", records),
            False,
            "INVALID_RUN_ID",
        ),
        _CommandCase(
            "cancel",
            ("cancel", "invalid", "--data-dir", records),
            False,
            "INVALID_RUN_ID",
        ),
    )


def test_every_operation_has_deterministic_json_and_common_option_placement(
    tmp_path: Path,
) -> None:
    for case in _cases(tmp_path):
        global_json = _run("--json", *case.arguments)
        local_json = _run(*case.arguments, "--json")
        repeated = _run("--json", *case.arguments)

        expected_exit = 0 if case.ok else 1
        assert global_json.returncode == local_json.returncode == expected_exit
        assert global_json.stderr == local_json.stderr == repeated.stderr == ""
        assert global_json.stdout == local_json.stdout == repeated.stdout
        document = json.loads(global_json.stdout)
        assert document["format_version"] == 1
        assert document["operation"] == case.operation
        assert document["ok"] is case.ok
        if case.error_code is not None:
            assert document["error"]["code"] == case.error_code


def test_every_operation_failure_has_matching_human_error_code(tmp_path: Path) -> None:
    for case in _cases(tmp_path):
        if case.ok:
            continue
        result = _run(*case.arguments)

        assert result.returncode == 1
        assert result.stdout == ""
        assert case.error_code is not None
        assert f"Error [{case.error_code}]" in result.stderr


def test_successful_empty_list_has_human_and_json_views(tmp_path: Path) -> None:
    arguments = ("list", "--data-dir", str(tmp_path / "records"))
    human = _run(*arguments)
    machine = _run("--json", *arguments)

    assert human.returncode == machine.returncode == 0
    assert human.stderr == machine.stderr == ""
    assert human.stdout == "No Runs found.\n"
    assert json.loads(machine.stdout) == {
        "format_version": 1,
        "ok": True,
        "operation": "list",
        "runs": [],
    }


def test_exit_two_is_reserved_for_a_reconciled_failed_run(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.yaml").write_text("label: cli-audit\n", encoding="utf-8")
    (source / "run.sh").write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "mkdir -p ../output/results\n"
        "printf 'partial-before-exit\\n' > ../output/results/partial.txt\n"
        "exit 7\n",
        encoding="utf-8",
    )
    (source / "experiment.yaml").write_text(
        """\
version: 1
experiment:
  name: cli-audit-failure
command:
  argv: [/bin/sh, run.sh, --config, "{config}", --seed, "{seed}"]
resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 1
  gpus_per_task: 0
  memory: 1GiB
  walltime: "00:05:00"
outputs:
  include: [results/**]
""",
        encoding="utf-8",
    )
    workspace = tmp_path / "workspace"
    (source / "targets.yaml").write_text(
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

    result = _run(
        "--json",
        "run",
        str(source / "experiment.yaml"),
        "--config",
        str(source / "config.yaml"),
        "--seed",
        "17",
        "--target",
        "local",
        "--targets-file",
        str(source / "targets.yaml"),
        "--source-root",
        str(source),
        "--destination",
        str(tmp_path / "retrieved"),
        "--data-dir",
        str(tmp_path / "records"),
    )

    try:
        document = json.loads(result.stdout)
        assert result.returncode == 2
        assert result.stderr == ""
        assert document["ok"] is True
        assert document["operation"] == "run"
        assert document["run"]["state"] == "FAILED"
        assert document["run"]["task_exit_codes"] == {"task_000000": 7}
        assert (tmp_path / "retrieved/results/partial.txt").read_text(
            encoding="utf-8"
        ) == "partial-before-exit\n"
    finally:
        for immutable in (workspace / "runs").glob("run_*/source"):
            for path in (immutable, *immutable.rglob("*")):
                if not path.is_symlink():
                    os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)
