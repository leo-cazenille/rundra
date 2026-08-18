from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.server import MCPServer

from rundra.cli.agent_guide import GUIDE
from rundra.cli.doctor import doctor_operation
from rundra.cli.operations import (
    cancel_operation,
    fetch_operation,
    inspect_operation,
    list_runs_operation,
    logs_operation,
    plan_operation,
    purge_operation,
    resolve_plan_inputs_operation,
    resolve_run_inputs_operation,
    run_operation,
    status_operation,
    submit_operation,
    targets_operation,
    tasks_operation,
    validate_operation,
    wait_operation,
)
from rundra.cli.render import result_document
from rundra.persistence import JsonRunStore, PurgeReceiptStore, SqliteTaskStore
from rundra.results import OperationError, OperationResult


@dataclass(frozen=True, slots=True)
class ServerSettings:
    root: Path
    data_dir: Path
    targets_file: Path
    allowed_roots: tuple[Path, ...]

    def path(self, value: str) -> Path:
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = self.root / candidate
        resolved = candidate.resolve(strict=False)
        if not any(
            resolved == root or resolved.is_relative_to(root)
            for root in self.allowed_roots
        ):
            raise ValueError(f"Path is outside the MCP allowed roots: {value}")
        return resolved


def build_server(settings: ServerSettings) -> MCPServer:
    server = MCPServer(
        "Rundra",
        instructions=(
            "Plan before submission. Use explicit seeds and preserve Run IDs. "
            "For long work use submit_experiment, wait_run, then fetch_results."
        ),
    )

    def store() -> JsonRunStore:
        return JsonRunStore(settings.data_dir)

    def task_store() -> SqliteTaskStore:
        return SqliteTaskStore(settings.data_dir)

    def document(result: OperationResult[Any]) -> dict[str, Any]:
        return result_document(result)

    def plan_result(
        experiment: str,
        config: str | None,
        seeds: str,
        target: str | None,
        profile: str | None,
        execution_strategy: str,
        retrieval: str,
    ) -> tuple[OperationResult[Any], str | None]:
        experiment_path = settings.path(experiment)
        resolved = resolve_plan_inputs_operation(
            experiment_path,
            config=None if config is None else settings.path(config),
            seeds=seeds,
            target=target,
            targets_file=settings.targets_file,
            profile=profile,
        )
        if not resolved.ok:
            return resolved, None
        assert resolved.value is not None
        inputs = resolved.value
        planned = plan_operation(
            experiment_path,
            inputs.config,
            inputs.targets_file,
            inputs.target,
            seed=inputs.seed,
            seeds=inputs.seeds,
            launch=inputs.launch,
            preparation=inputs.preparation_plan,
            sweep=inputs.sweep,
            execution_strategy=execution_strategy,
            retrieval_policy=retrieval,
        )
        if not planned.ok:
            return planned, None
        canonical = json.dumps(
            document(planned), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        return planned, hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @server.resource("rundra://guide/agent")
    def agent_guide() -> str:
        """Canonical portable instructions for agents using Rundra."""
        return GUIDE

    @server.tool()
    def validate_experiment(experiment: str) -> dict[str, Any]:
        """Validate an experiment and adjacent project configuration."""
        return document(validate_operation(settings.path(experiment)))

    @server.tool()
    def list_targets() -> dict[str, Any]:
        """List configured execution targets without contacting them."""
        return document(targets_operation(settings.targets_file))

    @server.tool()
    def doctor_target(target: str, connect: bool = False) -> dict[str, Any]:
        """Diagnose a target; connect=true performs a read-only live probe."""
        return document(
            doctor_operation(settings.targets_file, target, connect=connect)
        )

    @server.tool()
    def plan_experiment(
        experiment: str,
        seeds: str,
        config: str | None = None,
        target: str | None = None,
        profile: str | None = None,
        execution_strategy: str = "auto",
        retrieval: str = "manifest",
    ) -> dict[str, Any]:
        """Create an offline plan and a digest required for execution tools."""
        result, digest = plan_result(
            experiment, config, seeds, target, profile, execution_strategy, retrieval
        )
        rendered = document(result)
        if digest is not None:
            rendered["plan_digest"] = digest
        return rendered

    def execute(
        operation: str,
        experiment: str,
        seeds: str,
        plan_digest: str,
        config: str | None,
        target: str | None,
        profile: str | None,
        source_root: str | None,
        destination: str | None,
        confirm_tasks: int | None,
    ) -> dict[str, Any]:
        planned, current_digest = plan_result(
            experiment, config, seeds, target, profile, "auto", "manifest"
        )
        if not planned.ok:
            return document(planned)
        if current_digest != plan_digest:
            return document(
                OperationResult.failure(
                    operation,
                    OperationError(
                        "PLAN_DIGEST_MISMATCH",
                        "The approved plan does not match the current inputs",
                    ),
                )
            )
        experiment_path = settings.path(experiment)
        resolved = resolve_run_inputs_operation(
            experiment_path,
            config=None if config is None else settings.path(config),
            seeds=seeds,
            target=target,
            targets_file=settings.targets_file,
            source_root=None if source_root is None else settings.path(source_root),
            destination=(None if destination is None else settings.path(destination)),
            data_dir=settings.data_dir,
            profile=profile,
            operation=operation,
        )
        if not resolved.ok:
            return document(resolved)
        assert resolved.value is not None
        inputs = resolved.value
        function = run_operation if operation == "run" else submit_operation
        return document(
            function(
                experiment_path,
                inputs.config,
                inputs.targets_file,
                inputs.target,
                inputs.source_root,
                inputs.destination,
                store(),
                seed=inputs.seed,
                seeds=inputs.seeds if len(inputs.seeds) > 1 else None,
                launch=inputs.launch,
                preparation=inputs.preparation_plan,
                preparation_storage=inputs.preparation_storage,
                sweep=inputs.sweep,
                confirm_tasks=confirm_tasks,
            )
        )

    @server.tool()
    def submit_experiment(
        experiment: str,
        seeds: str,
        plan_digest: str,
        config: str | None = None,
        target: str | None = None,
        profile: str | None = None,
        source_root: str | None = None,
        destination: str | None = None,
        confirm_tasks: int | None = None,
    ) -> dict[str, Any]:
        """Submit an approved plan and return after durable scheduler submission."""
        return execute(
            "submit",
            experiment,
            seeds,
            plan_digest,
            config,
            target,
            profile,
            source_root,
            destination,
            confirm_tasks,
        )

    @server.tool()
    def run_experiment(
        experiment: str,
        seeds: str,
        plan_digest: str,
        config: str | None = None,
        target: str | None = None,
        profile: str | None = None,
        source_root: str | None = None,
        destination: str | None = None,
        confirm_tasks: int | None = None,
    ) -> dict[str, Any]:
        """Execute an approved short Run synchronously and retrieve results."""
        return execute(
            "run",
            experiment,
            seeds,
            plan_digest,
            config,
            target,
            profile,
            source_root,
            destination,
            confirm_tasks,
        )

    @server.tool()
    async def wait_run(
        run_id: str, timeout_seconds: float = 300, poll_interval: float = 2
    ) -> dict[str, Any]:
        """Wait up to five minutes for terminal state; renew after a timeout."""
        if not 1 <= timeout_seconds <= 3600:
            return document(
                OperationResult.failure(
                    "wait",
                    OperationError(
                        "INVALID_TIMEOUT", "timeout_seconds must be between 1 and 3600"
                    ),
                )
            )
        result = await asyncio.to_thread(
            wait_operation,
            run_id,
            store(),
            timeout=timeout_seconds,
            poll_interval=poll_interval,
            task_store=task_store(),
        )
        return document(result)

    @server.tool()
    def get_status(run_id: str) -> dict[str, Any]:
        """Refresh and return aggregate Run status."""
        return document(status_operation(run_id, store(), task_store=task_store()))

    @server.tool()
    def list_runs() -> dict[str, Any]:
        """List Runs in the configured client RunStore."""
        return document(list_runs_operation(store(), task_store=task_store()))

    @server.tool()
    def list_tasks(run_id: str, offset: int = 0, limit: int = 100) -> dict[str, Any]:
        """Return one bounded page of compact Task state."""
        return document(
            tasks_operation(run_id, store(), task_store(), offset=offset, limit=limit)
        )

    @server.tool()
    def get_logs(
        run_id: str, task: str | None = None, preparation: bool = False
    ) -> dict[str, Any]:
        """Read framework-managed Task or preparation logs."""
        return document(
            logs_operation(run_id, store(), task=task, preparation=preparation)
        )

    @server.tool()
    def fetch_results(
        run_id: str,
        destination: str,
        mode: str = "auto",
        extract: bool = False,
    ) -> dict[str, Any]:
        """Retrieve terminal or partial Run outputs into an allowed path."""
        return document(
            fetch_operation(
                run_id, store(), settings.path(destination), mode=mode, extract=extract
            )
        )

    @server.tool()
    def inspect_run(run_id: str) -> dict[str, Any]:
        """Return the complete persisted RunRecord and retention receipt."""
        return document(
            inspect_operation(
                run_id, store(), receipts=PurgeReceiptStore(settings.data_dir)
            )
        )

    @server.tool()
    def cancel_run(run_id: str) -> dict[str, Any]:
        """Cancel active scheduler work after reconciling current state."""
        return document(cancel_operation(run_id, store()))

    @server.tool()
    def purge_run(
        run_id: str,
        confirm_run_id: str,
        workspace: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Preview or perform guarded Run output/workspace deletion."""
        return document(
            purge_operation(
                run_id,
                store(),
                PurgeReceiptStore(settings.data_dir),
                workspace=workspace,
                confirm=confirm_run_id,
                dry_run=dry_run,
            )
        )

    return server


def main() -> None:
    parser = argparse.ArgumentParser(prog="rundr-mcp")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--data-dir", type=Path, default=Path("~/.local/share/rundra/runs")
    )
    parser.add_argument(
        "--targets-file", type=Path, default=Path("~/.config/rundra/targets.yaml")
    )
    parser.add_argument("--allow-path", action="append", type=Path, default=[])
    arguments = parser.parse_args()
    root = arguments.root.expanduser().resolve()
    allowed = tuple(
        dict.fromkeys(
            (root, *(path.expanduser().resolve() for path in arguments.allow_path))
        )
    )
    settings = ServerSettings(
        root,
        arguments.data_dir.expanduser().resolve(),
        arguments.targets_file.expanduser().resolve(),
        allowed,
    )
    build_server(settings).run(transport="stdio")


if __name__ == "__main__":
    main()
