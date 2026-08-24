from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import IO, Any, Never, cast

from rundra.cli.agent_guide import (
    GUIDE_TOPICS,
    AgentGuideValue,
    agent_guide_operation,
)
from rundra.cli.capability_doctor import DoctorValue, doctor_operation
from rundra.cli.notification import write_await_notification, write_wait_notification
from rundra.cli.operations import (
    LAST_RUN_SELECTOR,
    AwaitRunsValue,
    RunValue,
    WaitValue,
    await_runs_operation,
    cancel_operation,
    fetch_operation,
    inspect_operation,
    list_runs_operation,
    logs_operation,
    plan_operation,
    purge_operation,
    resolve_plan_inputs_operation,
    resolve_run_inputs_operation,
    resolve_submission_operation,
    resume_operation,
    run_operation,
    status_operation,
    submit_operation,
    targets_operation,
    tasks_operation,
    validate_operation,
    wait_operation,
)
from rundra.cli.progress import (
    ProgressUnavailableError,
    close_progress_reporter,
    create_progress_reporter,
)
from rundra.cli.render import render_human, render_json
from rundra.persistence import (
    JsonRunStore,
    PurgeReceiptStore,
    SqliteTaskStore,
    SubmissionReceiptStore,
)
from rundra.results import OperationError, OperationResult

_COMMANDS = (
    "validate",
    "plan",
    "targets",
    "run",
    "submit",
    "resume",
    "resolve-submission",
    "wait",
    "await",
    "status",
    "tasks",
    "list",
    "logs",
    "fetch",
    "inspect",
    "cancel",
    "purge",
    "doctor",
    "agent-guide",
    "help",
    "version",
)


class CLIUsageError(ValueError):
    """An argparse failure that must pass through Rundra's result boundary."""


class RundraArgumentParser(argparse.ArgumentParser):
    """Raise usage failures so JSON callers receive a structured result."""

    def error(self, message: str) -> Never:
        raise CLIUsageError(message)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level command-line parser."""
    parser = RundraArgumentParser(
        prog="rundr",
        description="Portable experiment execution for scientific computing.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit JSON output (may also follow the command)",
    )
    parser.add_argument(
        "--version",
        dest="show_version",
        action="store_true",
        help="show the installed Rundra version and exit",
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
    plan.add_argument("--source-root", type=Path)
    plan.add_argument("--fetch-mode", choices=("auto", "copy", "reference", "archive"))
    _add_worker_scale_arguments(plan)
    plan.add_argument(
        "--execution-strategy",
        choices=("auto", "multi-array", "worker-pool"),
        default="auto",
    )
    plan.add_argument(
        "--retrieval",
        choices=("all", "manifest", "none"),
        default="manifest",
    )
    _add_preparation_arguments(plan)
    _add_json_option(plan)

    targets = subparsers.add_parser("targets", help="list configured targets")
    targets.add_argument("--targets-file", type=Path, default=_default_targets_file())
    _add_json_option(targets)

    doctor = subparsers.add_parser(
        "doctor", help="audit installation, sandbox, target, and scheduler access"
    )
    doctor.add_argument("experiment", nargs="?", type=Path)
    doctor.add_argument("--config", type=Path)
    doctor.add_argument("--target")
    doctor.add_argument("--targets-file", type=Path)
    doctor.add_argument("--source-root", type=Path)
    doctor.add_argument("--destination", type=Path)
    doctor.add_argument("--data-dir", type=Path)
    doctor.add_argument("--project-file", type=Path)
    doctor.add_argument("--profile")
    doctor.add_argument("--connect", action="store_true")
    doctor.add_argument("--scheduler-probe", action="store_true")
    doctor.add_argument("--probe-timeout", type=int, default=120)
    doctor.add_argument("--no-write-probe", action="store_true")
    doctor.add_argument("--local-target-access", action="store_true")
    doctor.add_argument(
        "--offline",
        action="store_true",
        help="verify that immutable preparation inputs are already cached",
    )
    doctor.add_argument(
        "--prepare-location",
        choices=("auto", "local", "target"),
        default="auto",
    )
    doctor.add_argument("--agent", choices=("generic", "codex"), default="generic")
    _add_json_option(doctor)

    run = subparsers.add_parser("run", help="execute one Run synchronously")
    _add_execution_arguments(
        run, allow_many=True, required=False, allow_random_seed=True
    )
    run.add_argument("--source-root", type=Path)
    run.add_argument("--destination", type=Path)
    run.add_argument("--project-file", type=Path)
    run.add_argument("--profile")
    run.add_argument("--fetch-mode", choices=("auto", "copy", "reference", "archive"))
    run.add_argument("--confirm-tasks", type=int)
    _add_worker_scale_arguments(run)
    _add_preparation_arguments(run)
    _add_feedback_arguments(run)
    _add_store_option(run, use_default=False)
    _add_json_option(run)

    submit = subparsers.add_parser("submit", help="submit one Run asynchronously")
    _add_execution_arguments(
        submit, allow_many=True, required=False, allow_random_seed=True
    )
    submit.add_argument("--source-root", type=Path)
    submit.add_argument("--destination", type=Path)
    submit.add_argument("--project-file", type=Path)
    submit.add_argument("--profile")
    submit.add_argument(
        "--fetch-mode", choices=("auto", "copy", "reference", "archive")
    )
    submit.add_argument("--confirm-tasks", type=int)
    _add_worker_scale_arguments(submit)
    _add_preparation_arguments(submit)
    _add_feedback_arguments(submit)
    _add_store_option(submit, use_default=False)
    _add_json_option(submit)

    resume = subparsers.add_parser(
        "resume", help="recover an interrupted scheduler submission"
    )
    _add_run_selector(resume)
    _add_store_option(resume)
    _add_json_option(resume)

    resolve_submission = subparsers.add_parser(
        "resolve-submission",
        help="close an uncertain submission after operator verification",
    )
    _add_run_selector(resolve_submission)
    resolve_submission.add_argument(
        "--not-submitted",
        action="store_true",
        required=True,
        help="assert that scheduler inspection found no submitted job",
    )
    resolve_submission.add_argument(
        "--confirm",
        required=True,
        metavar="RUN_ID",
        help="repeat the exact Run ID to confirm the resolution",
    )
    _add_store_option(resolve_submission)
    _add_json_option(resolve_submission)

    wait = subparsers.add_parser("wait", help="wait for a submitted Run")
    _add_run_selector(wait)
    wait.add_argument("--timeout", type=float)
    wait.add_argument("--poll-interval", type=float, default=2.0)
    wait.add_argument(
        "--notify",
        action="store_true",
        help="emit one terminal alert when the Run completes",
    )
    wait.add_argument(
        "--notify-file",
        type=Path,
        metavar="PATH",
        help="atomically write one terminal JSON notification",
    )
    _add_feedback_arguments(wait)
    _add_store_option(wait)
    _add_json_option(wait)

    await_runs = subparsers.add_parser(
        "await", help="wait silently for one or more Runs"
    )
    await_runs.add_argument("run_ids", nargs="+")
    await_runs.add_argument("--until", choices=("all", "any"), default="all")
    await_runs.add_argument("--timeout", type=float)
    await_runs.add_argument("--poll-interval", type=float, default=15.0)
    await_runs.add_argument("--fail-on-run-failure", action="store_true")
    await_runs.add_argument("--notify-file", type=Path, metavar="PATH")
    _add_store_option(await_runs)
    _add_json_option(await_runs)

    status = subparsers.add_parser("status", help="show persisted Run status")
    _add_run_selector(status)
    _add_store_option(status)
    _add_json_option(status)

    tasks = subparsers.add_parser("tasks", help="page through compact Run Tasks")
    _add_run_selector(tasks)
    tasks.add_argument("--offset", type=int, default=0)
    tasks.add_argument("--limit", type=int, default=100)
    _add_store_option(tasks)
    _add_json_option(tasks)

    list_runs = subparsers.add_parser("list", help="list persisted Runs")
    list_runs.add_argument("--offset", type=int, default=0)
    list_runs.add_argument("--limit", type=int, default=100)
    list_runs.add_argument(
        "--include-tasks",
        action="store_true",
        help="include per-Task details in each Run summary",
    )
    _add_store_option(list_runs)
    _add_json_option(list_runs)

    logs = subparsers.add_parser("logs", help="read framework-managed Task logs")
    _add_run_selector(logs)
    log_selection = logs.add_mutually_exclusive_group()
    log_selection.add_argument(
        "--task",
        metavar="TASK_ID_OR_INDEX",
        help="select one Task by stable ID or zero-based ordinal",
    )
    log_selection.add_argument(
        "--preparation",
        action="store_true",
        help="read logs from the framework-owned preparation job",
    )
    _add_store_option(logs)
    _add_json_option(logs)

    fetch = subparsers.add_parser("fetch", help="retrieve a Run's outputs")
    _add_run_selector(fetch)
    fetch.add_argument("--destination", type=Path)
    fetch.add_argument(
        "--mode",
        choices=("auto", "copy", "reference", "archive"),
        help="retrieval strategy; auto references shared targets and copies others",
    )
    fetch.add_argument(
        "--extract",
        action="store_true",
        help="verify and safely extract fetched result shards",
    )
    fetch.add_argument(
        "--task",
        action="append",
        metavar="TASK_ID_OR_INDEX",
        help="retrieve only this Task; repeat to select multiple Tasks",
    )
    _add_feedback_arguments(fetch)
    _add_store_option(fetch)
    _add_json_option(fetch)

    inspect = subparsers.add_parser("inspect", help="inspect a persisted Run record")
    _add_run_selector(inspect)
    _add_store_option(inspect)
    _add_json_option(inspect)

    cancel = subparsers.add_parser(
        "cancel", help="cancel active scheduler work for a Run"
    )
    _add_run_selector(cancel)
    _add_store_option(cancel)
    _add_json_option(cancel)

    purge = subparsers.add_parser("purge", help="delete terminal Run data safely")
    _add_run_selector(purge)
    purge.add_argument(
        "--workspace",
        action="store_true",
        help="delete the complete per-Run workspace instead of outputs only",
    )
    purge.add_argument("--confirm", metavar="RUN_ID")
    purge.add_argument("--dry-run", action="store_true")
    _add_store_option(purge)
    _add_json_option(purge)

    agent_guide = subparsers.add_parser(
        "agent-guide", help="print, install, or check agent instructions"
    )
    guide_action = agent_guide.add_mutually_exclusive_group()
    guide_action.add_argument("--write", type=Path, metavar="PATH")
    guide_action.add_argument("--check", type=Path, metavar="PATH")
    guide_action.add_argument("--topic", choices=tuple(GUIDE_TOPICS))
    guide_action.add_argument("--list-topics", action="store_true")
    _add_json_option(agent_guide)

    help_command = subparsers.add_parser(
        "help", help="show an overview or detailed command help"
    )
    help_command.add_argument(
        "topic",
        nargs="?",
        choices=tuple(subparsers.choices),
        metavar="COMMAND",
        help="command whose detailed help should be shown",
    )
    subparsers.add_parser("version", help="show the installed Rundra version")
    return parser


def _help_text(parser: argparse.ArgumentParser, topic: str | None) -> str:
    subcommands = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    if topic is not None:
        command_parser = cast(argparse.ArgumentParser, subcommands.choices[topic])
        return command_parser.format_help().rstrip()
    summaries = {action.dest: action.help for action in subcommands._choices_actions}
    width = max(len(name) for name in subcommands.choices)
    command_lines = tuple(
        f"  {name:<{width}}  {summaries.get(name) or ''}"
        for name in subcommands.choices
    )
    return "\n".join(
        (
            "Rundra executes reproducible scientific experiments locally or on "
            "remote schedulers.",
            "",
            "Usage:",
            "  rundr COMMAND [OPTIONS]",
            "  rundr help [COMMAND]",
            "",
            "Common workflow:",
            "  rundr doctor EXPERIMENT       Check target configuration and access",
            "  rundr plan EXPERIMENT         Inspect tasks, resources, and preparation",
            "  rundr submit EXPERIMENT       Submit without keeping the client attached",
            "  rundr wait RUN_ID             Wait until completion",
            "  rundr fetch RUN_ID            Retrieve outputs to the default destination",
            "",
            "Commands:",
            *command_lines,
            "",
            "Run 'rundr help COMMAND' for detailed arguments and options.",
        )
    )


def _add_json_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="emit JSON output",
    )


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


def _add_run_selector(parser: argparse.ArgumentParser) -> None:
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument(
        "run_id",
        nargs="?",
        default=argparse.SUPPRESS,
        help="stable Run ID",
    )
    selector.add_argument(
        "--last",
        dest="run_id",
        action="store_const",
        const=LAST_RUN_SELECTOR,
        default=argparse.SUPPRESS,
        help="use the most recently registered Run",
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


def _add_preparation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--prepare-location",
        choices=("auto", "local", "target"),
        default="auto",
        help="select where preparation may run",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="bypass only the compiled-output cache",
    )
    parser.add_argument(
        "--rebuild-image",
        action="store_true",
        help="bypass only the definition-image cache",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="prohibit Git fetches and image pulls",
    )


def _add_worker_scale_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workers",
        type=int,
        help="request worker allocations within the target policy",
    )
    parser.add_argument(
        "--task-slots-per-worker",
        type=int,
        help="request concurrent logical Task slots in each worker",
    )


def _add_feedback_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print detailed lifecycle transitions to stderr",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="show a TQDM lifecycle progress bar on stderr",
    )
    parser.add_argument(
        "--progress-interval",
        type=_positive_float,
        default=10.0,
        metavar="SECONDS",
        help="minimum interval between progress redraws (default: 10)",
    )


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _emit_wait_notification(value: WaitValue, stream: IO[str]) -> None:
    if not value.terminal:
        return
    print(
        f"\a[rundr] notification: run={value.status.run_id} "
        f"state={value.status.state.value}",
        file=stream,
        flush=True,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the rundr command-line interface."""
    raw_arguments = tuple(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    try:
        arguments = parser.parse_args(raw_arguments)
    except CLIUsageError as error:
        operation = _requested_operation(raw_arguments)
        usage_result: OperationResult[Any] = OperationResult.failure(
            operation,
            OperationError(
                "CLI_USAGE_ERROR",
                str(error),
                {"command": operation},
            ),
        )
        json_requested = "--json" in raw_arguments
        output = (
            render_json(usage_result) if json_requested else render_human(usage_result)
        )
        print(output, file=sys.stdout if json_requested else sys.stderr)
        return 1
    if arguments.show_version:
        print(_version_text())
        return 0
    if arguments.command is None:
        if arguments.json:
            missing_command_result: OperationResult[Any] = OperationResult.failure(
                "cli",
                OperationError(
                    "CLI_USAGE_ERROR",
                    "a command is required",
                    {"command": "cli"},
                ),
            )
            print(render_json(missing_command_result))
            return 1
        parser.print_help()
        return 0
    if arguments.command == "help":
        print(_help_text(parser, arguments.topic))
        return 0
    if arguments.command == "version":
        print(_version_text())
        return 0

    try:
        if (
            getattr(arguments, "json", False)
            and getattr(arguments, "progress", False)
            and not sys.stderr.isatty()
        ):
            print(
                "Warning: --json --progress on captured stderr may create a large "
                "transcript; agents should omit --progress.",
                file=sys.stderr,
                flush=True,
            )
        progress = create_progress_reporter(
            verbose=getattr(arguments, "verbose", False),
            progress=getattr(arguments, "progress", False),
            stream=sys.stderr,
            announce_run=arguments.command == "submit",
            progress_interval=getattr(arguments, "progress_interval", 10.0),
        )
    except ProgressUnavailableError as error:
        unavailable: OperationResult[Any] = OperationResult.failure(
            arguments.command,
            OperationError("PROGRESS_UNAVAILABLE", str(error)),
        )
        output = (
            render_json(unavailable) if arguments.json else render_human(unavailable)
        )
        print(output, file=sys.stdout if arguments.json else sys.stderr)
        return 1
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
            source_root=arguments.source_root,
            prepare_location=arguments.prepare_location,
            rebuild=arguments.rebuild,
            rebuild_image=arguments.rebuild_image,
            offline=arguments.offline,
            workers=arguments.workers,
            task_slots_per_worker=arguments.task_slots_per_worker,
            fetch_mode=arguments.fetch_mode,
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
                preparation=plan_inputs.preparation_plan,
                sweep=plan_inputs.sweep,
                execution_strategy=arguments.execution_strategy,
                retrieval_policy=arguments.retrieval,
                workers=plan_inputs.workers,
                task_slots_per_worker=plan_inputs.task_slots_per_worker,
            )
    elif arguments.command == "targets":
        result = targets_operation(arguments.targets_file)
    elif arguments.command == "doctor":
        if arguments.experiment is None:
            result = doctor_operation(
                arguments.targets_file or _default_targets_file(),
                arguments.target,
                connect=arguments.connect,
                scheduler_probe=arguments.scheduler_probe,
                probe_timeout=arguments.probe_timeout,
                write_probe=not arguments.no_write_probe,
                data_dir=arguments.data_dir,
                destination=arguments.destination,
                source_root=arguments.source_root,
                local_target_access=arguments.local_target_access,
                agent=arguments.agent,
                offline=arguments.offline,
            )
        else:
            resolved_doctor = resolve_run_inputs_operation(
                arguments.experiment,
                config=arguments.config,
                target=arguments.target,
                targets_file=arguments.targets_file,
                source_root=arguments.source_root,
                destination=arguments.destination,
                data_dir=arguments.data_dir,
                project_file=arguments.project_file,
                profile=arguments.profile,
                operation="doctor",
                offline=arguments.offline,
                prepare_location=arguments.prepare_location,
            )
            if not resolved_doctor.ok:
                result = resolved_doctor
            else:
                assert resolved_doctor.value is not None
                result = doctor_operation(
                    resolved_doctor.value.targets_file,
                    resolved_doctor.value.target,
                    connect=arguments.connect,
                    scheduler_probe=arguments.scheduler_probe,
                    probe_timeout=arguments.probe_timeout,
                    write_probe=not arguments.no_write_probe,
                    data_dir=resolved_doctor.value.data_dir,
                    destination=resolved_doctor.value.destination,
                    source_root=resolved_doctor.value.source_root,
                    experiment_source=arguments.experiment,
                    config_source=resolved_doctor.value.config,
                    cache_root=(
                        None
                        if resolved_doctor.value.preparation_storage.cache_root is None
                        else Path(
                            str(resolved_doctor.value.preparation_storage.cache_root)
                        )
                    ),
                    preparation=resolved_doctor.value.preparation_plan,
                    preparation_storage=resolved_doctor.value.preparation_storage,
                    offline=arguments.offline,
                    local_target_access=arguments.local_target_access,
                    agent=arguments.agent,
                )
    elif arguments.command == "run":
        resolved = resolve_run_inputs_operation(
            arguments.experiment,
            config=arguments.config,
            seed=arguments.seed,
            seeds=arguments.seeds,
            target=arguments.target,
            targets_file=arguments.targets_file,
            source_root=arguments.source_root,
            destination=arguments.destination,
            data_dir=arguments.data_dir,
            project_file=arguments.project_file,
            profile=arguments.profile,
            random_seed=arguments.random_seed,
            prepare_location=arguments.prepare_location,
            rebuild=arguments.rebuild,
            rebuild_image=arguments.rebuild_image,
            offline=arguments.offline,
            workers=arguments.workers,
            task_slots_per_worker=arguments.task_slots_per_worker,
            fetch_mode=arguments.fetch_mode,
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
                seeds=run_inputs.seeds if run_inputs.seed is None else None,
                launch=run_inputs.launch,
                preparation=run_inputs.preparation_plan,
                preparation_storage=run_inputs.preparation_storage,
                progress=progress,
                sweep=run_inputs.sweep,
                confirm_tasks=arguments.confirm_tasks,
                workers=run_inputs.workers,
                task_slots_per_worker=run_inputs.task_slots_per_worker,
                task_store=SqliteTaskStore(run_inputs.data_dir),
            )
    elif arguments.command == "submit":
        resolved = resolve_run_inputs_operation(
            arguments.experiment,
            config=arguments.config,
            seed=arguments.seed,
            seeds=arguments.seeds,
            target=arguments.target,
            targets_file=arguments.targets_file,
            source_root=arguments.source_root,
            destination=arguments.destination,
            data_dir=arguments.data_dir,
            project_file=arguments.project_file,
            profile=arguments.profile,
            random_seed=arguments.random_seed,
            operation="submit",
            prepare_location=arguments.prepare_location,
            rebuild=arguments.rebuild,
            rebuild_image=arguments.rebuild_image,
            offline=arguments.offline,
            workers=arguments.workers,
            task_slots_per_worker=arguments.task_slots_per_worker,
            fetch_mode=arguments.fetch_mode,
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
                seeds=submit_inputs.seeds if submit_inputs.seed is None else None,
                launch=submit_inputs.launch,
                preparation=submit_inputs.preparation_plan,
                preparation_storage=submit_inputs.preparation_storage,
                progress=progress,
                sweep=submit_inputs.sweep,
                confirm_tasks=arguments.confirm_tasks,
                workers=submit_inputs.workers,
                task_slots_per_worker=submit_inputs.task_slots_per_worker,
                submission_receipts=SubmissionReceiptStore(submit_inputs.data_dir),
                task_store=SqliteTaskStore(submit_inputs.data_dir),
            )
    elif arguments.command == "resume":
        result = resume_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            SubmissionReceiptStore(arguments.data_dir),
        )
    elif arguments.command == "resolve-submission":
        result = resolve_submission_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            SubmissionReceiptStore(arguments.data_dir),
            not_submitted=arguments.not_submitted,
            confirmation=arguments.confirm,
        )
    elif arguments.command == "wait":
        result = wait_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            timeout=arguments.timeout,
            poll_interval=arguments.poll_interval,
            task_store=SqliteTaskStore(arguments.data_dir),
            progress=progress,
        )
    elif arguments.command == "await":
        result = await_runs_operation(
            arguments.run_ids,
            JsonRunStore(arguments.data_dir),
            until=arguments.until,
            timeout=arguments.timeout,
            poll_interval=arguments.poll_interval,
            fail_on_run_failure=arguments.fail_on_run_failure,
            task_store=SqliteTaskStore(arguments.data_dir),
        )
    elif arguments.command == "status":
        result = status_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            task_store=SqliteTaskStore(arguments.data_dir),
        )
    elif arguments.command == "tasks":
        result = tasks_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            SqliteTaskStore(arguments.data_dir),
            offset=arguments.offset,
            limit=arguments.limit,
        )
    elif arguments.command == "list":
        result = list_runs_operation(
            JsonRunStore(arguments.data_dir),
            task_store=SqliteTaskStore(arguments.data_dir),
            offset=arguments.offset,
            limit=arguments.limit,
            include_tasks=arguments.include_tasks,
        )
    elif arguments.command == "logs":
        result = logs_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            task=arguments.task,
            preparation=arguments.preparation,
        )
    elif arguments.command == "fetch":
        result = fetch_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            arguments.destination,
            tasks=arguments.task,
            mode=arguments.mode,
            extract=arguments.extract,
            progress=progress,
            task_store=SqliteTaskStore(arguments.data_dir),
        )
    elif arguments.command == "inspect":
        result = inspect_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            receipts=PurgeReceiptStore(arguments.data_dir),
        )
    elif arguments.command == "cancel":
        result = cancel_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            task_store=SqliteTaskStore(arguments.data_dir),
        )
    elif arguments.command == "purge":
        result = purge_operation(
            arguments.run_id,
            JsonRunStore(arguments.data_dir),
            PurgeReceiptStore(arguments.data_dir),
            workspace=arguments.workspace,
            confirm=arguments.confirm,
            dry_run=arguments.dry_run,
        )
    elif arguments.command == "agent-guide":
        result = agent_guide_operation(
            write=arguments.write,
            check=arguments.check,
            topic=arguments.topic,
            list_topics=arguments.list_topics,
        )
    else:
        raise AssertionError(f"Unhandled CLI command: {arguments.command}")
    close_progress_reporter(progress)
    if (
        arguments.command == "wait"
        and arguments.notify
        and result.ok
        and isinstance(result.value, WaitValue)
    ):
        _emit_wait_notification(result.value, sys.stderr)
    if (
        arguments.command == "wait"
        and arguments.notify_file is not None
        and result.ok
        and isinstance(result.value, WaitValue)
        and result.value.terminal
    ):
        try:
            write_wait_notification(arguments.notify_file, result.value)
        except (OSError, ValueError) as error:
            result = OperationResult.failure(
                "wait",
                OperationError(
                    "NOTIFICATION_FAILED",
                    str(error),
                    {"run_id": str(result.value.status.run_id)},
                ),
            )
    if (
        arguments.command == "await"
        and arguments.notify_file is not None
        and result.ok
        and isinstance(result.value, AwaitRunsValue)
        and result.value.condition_met
    ):
        try:
            write_await_notification(arguments.notify_file, result.value)
        except (OSError, ValueError) as error:
            result = OperationResult.failure(
                "await",
                OperationError(
                    "NOTIFICATION_FAILED",
                    str(error),
                    {
                        "run_ids": tuple(
                            str(item.run_id) for item in result.value.statuses
                        )
                    },
                ),
            )
    output = render_json(result) if arguments.json else render_human(result)
    stream = sys.stdout if arguments.json or result.ok else sys.stderr
    print(output, file=stream)
    if not result.ok:
        return 1
    if isinstance(result.value, RunValue):
        return result.value.exit_code
    if isinstance(result.value, AwaitRunsValue):
        return result.value.exit_code
    if isinstance(result.value, DoctorValue) and not result.value.ready:
        return 1
    if isinstance(result.value, AgentGuideValue) and result.value.action == "outdated":
        return 1
    return 0


def _requested_operation(arguments: Sequence[str]) -> str:
    candidate = next((value for value in arguments if value != "--json"), None)
    return candidate if candidate in _COMMANDS else "cli"


def _version_text() -> str:
    return f"rundr version {distribution_version('rundra')}"
