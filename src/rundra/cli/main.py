from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from rundra.cli.operations import (
    plan_operation,
    targets_operation,
    validate_operation,
)
from rundra.cli.render import render_human, render_json
from rundra.results import OperationResult


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level command-line parser."""
    parser = argparse.ArgumentParser(
        prog="rundr",
        description="Portable experiment execution for scientific computing.",
    )
    subparsers = parser.add_subparsers(dest="command")

    validate = subparsers.add_parser("validate", help="validate an experiment")
    validate.add_argument("experiment", type=Path)
    _add_json_option(validate)

    plan = subparsers.add_parser("plan", help="inspect a plan without executing it")
    plan.add_argument("experiment", type=Path)
    plan.add_argument("--config", required=True, type=Path)
    seed_group = plan.add_mutually_exclusive_group(required=True)
    seed_group.add_argument("--seed", type=int)
    seed_group.add_argument("--seeds")
    plan.add_argument("--target", required=True)
    plan.add_argument("--targets-file", type=Path, default=_default_targets_file())
    _add_json_option(plan)

    targets = subparsers.add_parser("targets", help="list configured targets")
    targets.add_argument("--targets-file", type=Path, default=_default_targets_file())
    _add_json_option(targets)
    return parser


def _add_json_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="emit JSON output")


def _default_targets_file() -> Path:
    return Path("~/.config/rundra/targets.yaml").expanduser()


def main(argv: Sequence[str] | None = None) -> int:
    """Run the rundr command-line interface."""
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.command is None:
        parser.print_help()
        return 0
    result: OperationResult[Any]
    if arguments.command == "validate":
        result = validate_operation(arguments.experiment)
    elif arguments.command == "plan":
        result = plan_operation(
            arguments.experiment,
            arguments.config,
            arguments.targets_file,
            arguments.target,
            seed=arguments.seed,
            seeds=arguments.seeds,
        )
    else:
        result = targets_operation(arguments.targets_file)
    output = render_json(result) if arguments.json else render_human(result)
    stream = sys.stdout if arguments.json or result.ok else sys.stderr
    print(output, file=stream)
    return 0 if result.ok else 1
