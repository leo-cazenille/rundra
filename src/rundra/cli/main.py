from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from rundra.cli.operations import (
    RunValue,
    cancel_operation,
    fetch_operation,
    inspect_operation,
    list_runs_operation,
    logs_operation,
    plan_operation,
    resolve_plan_inputs_operation,
    resolve_run_inputs_operation,
    run_operation,
    status_operation,
    submit_operation,
    targets_operation,
    validate_operation,
)
from rundra.cli.render import render_human, render_json
from rundra.persistence import JsonRunStore
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
    plan.add_argument("--config", type=Path)
    seed_group = plan.add_mutually_exclusive_group()
    seed_group.add_argument("--seed", type=int)
    seed_group.add_argument("--seeds")
    seed_group.add_argument("--random-seed", action="store_true")
    plan.add_argument("--target")
    plan.add_argument("--targets-file", type=Path)
    plan.add_argument("--project-file", type=Path)
    plan.add_argument("--profile")
    _add_json_option(plan)

    targets = subparsers.add_parser("targets", help="list configured targets")
    targets.add_argument("--targets-file", type=Path, default=_default_targets_file())
    _add_json_option(targets)

    run = subparsers.add_parser("run", help="execute one Task synchronously")
    _add_execution_arguments(
        run, allow_many=False, required=False, allow_random_seed=True
    )
    run.add_argument("--source-root", type=Path)
    run.add_argument("--destination", type=Path)
    run.add_argument("--project-file", type=Path)
    run.add_argument("--profile")
    _add_store_option(run, use_default=False)
    _add_json_option(run)

    submit = subparsers.add_parser("submit", help="submit one Task asynchronously")
    _add_execution_arguments(
        submit, allow_many=False, required=False, allow_random_seed=True
    )
    submit.add_argument("--source-root", type=Path)
    submit.add_argument("--destination", type=Path)
    submit.add_argument("--project-file", type=Path)
    submit.add_argument("--profile")
    _add_store_option(submit, use_default=False)
    _add_json_option(submit)

    status = subparsers.add_parser("status", help="show persisted Run status")
    status.add_argument("run_id")
    _add_store_option(status)
    _add_json_option(status)

    list_runs = subparsers.add_parser("list", help="list persisted Runs")
    _add_store_option(list_runs)
    _add_json_option(list_runs)

    logs = subparsers.add_parser("logs", help="read framework-managed Task logs")
    logs.add_argument("run_id")
    logs.add_argument("--task")
    _add_store_option(logs)
    _add_json_option(logs)

    fetch = subparsers.add_parser("fetch", help="retrieve a Run's outputs")
    fetch.add_argument("run_id")
    fetch.add_argument("--destination", required=True, type=Path)
    _add_store_option(fetch)
    _add_json_option(fetch)

    inspect = subparsers.add_parser("inspect", help="inspect a persisted Run record")
    inspect.add_argument("run_id")
    _add_store_option(inspect)
    _add_json_option(inspect)

    cancel = subparsers.add_parser("cancel", help="cancel an active Slurm Run")
    cancel.add_argument("run_id")
    _add_store_option(cancel)
    _add_json_option(cancel)
    return parser


def _add_json_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="emit JSON output")


def _default_targets_file() -> Path:
    return Path("~/.config/rundra/targets.yaml").expanduser()


def _default_data_dir() -> Path:
    return Path("~/.local/share/rundra/runs").expanduser()


def _add_store_option(
    parser: argparse.ArgumentParser, *, use_default: bool = True
) -> None:
    parser.add_argument(
        "--data-dir", type=Path, default=_default_data_dir() if use_default else None
    )


def _add_execution_arguments(
    parser: argparse.ArgumentParser,
    *,
    allow_many: bool,
    required: bool,
    allow_random_seed: bool,
) -> None:
    parser.add_argument("experiment", type=Path)
    parser.add_argument("--config", required=required, type=Path)
    seed_group = parser.add_mutually_exclusive_group(required=required)
    seed_group.add_argument("--seed", type=int)
    if allow_many:
        seed_group.add_argument("--seeds")
    if allow_random_seed:
        seed_group.add_argument(
            "--random-seed",
            action="store_true",
            help="generate a new seed even when defaults specify one",
        )
    parser.add_argument("--target", required=required)
    parser.add_argument(
        "--targets-file",
        type=Path,
        default=_default_targets_file() if required else None,
    )


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
        resolved_plan = resolve_plan_inputs_operation(
            arguments.experiment,
            config=arguments.config,
            seed=arguments.seed,
            seeds=arguments.seeds,
            target=arguments.target,
            targets_file=arguments.targets_file,
            project_file=arguments.project_file,
            profile=arguments.profile,
            random_seed=arguments.random_seed,
        )
        if not resolved_plan.ok:
            result = resolved_plan
        else:
            assert resolved_plan.value is not None
            plan_inputs = resolved_plan.value
            result = plan_operation(
                arguments.experiment,
                plan_inputs.config,
                plan_inputs.targets_file,
                plan_inputs.target,
                seed=plan_inputs.seed,
                seeds=plan_inputs.seeds,
                launch=plan_inputs.launch,
            )
    elif arguments.command == "targets":
        result = targets_operation(arguments.targets_file)
    elif arguments.command == "run":
        resolved = resolve_run_inputs_operation(
            arguments.experiment,
            config=arguments.config,
            seed=arguments.seed,
            target=arguments.target,
            targets_file=arguments.targets_file,
            source_root=arguments.source_root,
            destination=arguments.destination,
            data_dir=arguments.data_dir,
            project_file=arguments.project_file,
            profile=arguments.profile,
            random_seed=arguments.random_seed,
        )
        if not resolved.ok:
            result = resolved
        else:
            assert resolved.value is not None
            run_inputs = resolved.value
            result = run_operation(
                arguments.experiment,
                run_inputs.config,
                run_inputs.targets_file,
                run_inputs.target,
                run_inputs.source_root,
                run_inputs.destination,
                JsonRunStore(run_inputs.data_dir),
                seed=run_inputs.seed,
                launch=run_inputs.launch,
            )
    elif arguments.command == "submit":
        resolved = resolve_run_inputs_operation(
            arguments.experiment,
            config=arguments.config,
            seed=arguments.seed,
            target=arguments.target,
            targets_file=arguments.targets_file,
            source_root=arguments.source_root,
            destination=arguments.destination,
            data_dir=arguments.data_dir,
            project_file=arguments.project_file,
            profile=arguments.profile,
            random_seed=arguments.random_seed,
            operation="submit",
        )
        if not resolved.ok:
            result = resolved
        else:
            assert resolved.value is not None
            submit_inputs = resolved.value
            result = submit_operation(
                arguments.experiment,
                submit_inputs.config,
                submit_inputs.targets_file,
                submit_inputs.target,
                submit_inputs.source_root,
                submit_inputs.destination,
                JsonRunStore(submit_inputs.data_dir),
                seed=submit_inputs.seed,
                launch=submit_inputs.launch,
            )
    elif arguments.command == "status":
        result = status_operation(arguments.run_id, JsonRunStore(arguments.data_dir))
    elif arguments.command == "list":
        result = list_runs_operation(JsonRunStore(arguments.data_dir))
    elif arguments.command == "logs":
        result = logs_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            task=arguments.task,
        )
    elif arguments.command == "fetch":
        result = fetch_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            arguments.destination,
        )
    elif arguments.command == "inspect":
        result = inspect_operation(arguments.run_id, JsonRunStore(arguments.data_dir))
    else:
        result = cancel_operation(arguments.run_id, JsonRunStore(arguments.data_dir))
    output = render_json(result) if arguments.json else render_human(result)
    stream = sys.stdout if arguments.json or result.ok else sys.stderr
    print(output, file=stream)
    if not result.ok:
        return 1
    if isinstance(result.value, RunValue):
        return result.value.exit_code
    return 0
