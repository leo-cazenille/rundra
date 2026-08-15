from __future__ import annotations

import subprocess

import pytest

from rundra.cli.main import build_parser


def test_console_entry_point_displays_help() -> None:
    result = subprocess.run(
        ["rundr", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout.startswith("usage: rundr")
    for command in ("run", "submit", "status", "list", "logs", "fetch", "inspect"):
        assert command in result.stdout


def test_run_parser_accepts_launch_resolution_without_repeated_arguments() -> None:
    arguments = build_parser().parse_args(["run", "experiment.yaml"])

    assert arguments.config is None
    assert arguments.seed is None
    assert arguments.target is None
    assert arguments.targets_file is None
    assert arguments.source_root is None
    assert arguments.destination is None
    assert arguments.data_dir is None
    assert arguments.random_seed is False

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["run", "experiment.yaml", "--seed", "1", "--random-seed"]
        )


def test_plan_parser_accepts_defaults_and_mutually_exclusive_seed_forms() -> None:
    arguments = build_parser().parse_args(["plan", "experiment.yaml"])

    assert arguments.config is None
    assert arguments.seed is None
    assert arguments.seeds is None
    assert arguments.random_seed is False
    assert arguments.target is None

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["plan", "experiment.yaml", "--seeds", "0:2", "--random-seed"]
        )
