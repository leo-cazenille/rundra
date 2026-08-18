from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from mcp.server import MCPServer
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import AnyHttpUrl

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


@dataclass(frozen=True, slots=True)
class HTTPSettings:
    host: str
    port: int
    path: str
    token_env: str
    allowed_hosts: tuple[str, ...]
    allowed_origins: tuple[str, ...]

    def transport_security(self) -> TransportSecuritySettings | None:
        if not self.allowed_hosts and self.host in _LOOPBACK_HOSTS:
            return None
        hosts = self.allowed_hosts
        origins = self.allowed_origins
        if self.host in _LOOPBACK_HOSTS:
            hosts = tuple(dict.fromkeys((*_LOOPBACK_ALLOWED_HOSTS, *hosts)))
            origins = tuple(dict.fromkeys((*_LOOPBACK_ALLOWED_ORIGINS, *origins)))
        return TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=list(hosts),
            allowed_origins=list(origins),
        )


class StaticBearerTokenVerifier:
    """Verify one process-local opaque bearer token without logging it."""

    def __init__(self, token: str) -> None:
        _validate_token(token)
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        if not secrets.compare_digest(token, self._token):
            return None
        return AccessToken(
            token=token,
            client_id="rundr-mcp-static",
            scopes=[],
            subject="rundr-mcp-client",
        )


_LOOPBACK_HOSTS = frozenset(("127.0.0.1", "localhost", "::1"))
_LOOPBACK_ALLOWED_HOSTS = (
    "127.0.0.1:*",
    "localhost:*",
    "[::1]:*",
)
_LOOPBACK_ALLOWED_ORIGINS = (
    "http://127.0.0.1:*",
    "http://localhost:*",
    "http://[::1]:*",
)
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_STATIC_AUTH_SETTINGS = AuthSettings(
    issuer_url=AnyHttpUrl("https://rundr-mcp.invalid"),
    resource_server_url=None,
    required_scopes=[],
)


def build_server(
    settings: ServerSettings,
    *,
    token_verifier: TokenVerifier | None = None,
) -> MCPServer:
    server = MCPServer(
        "Rundra",
        instructions=(
            "Plan before submission. Use explicit seeds and preserve Run IDs. "
            "For long work use submit_experiment, wait_run, then fetch_results."
        ),
        token_verifier=token_verifier,
        auth=_STATIC_AUTH_SETTINGS if token_verifier is not None else None,
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


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rundr-mcp")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--data-dir", type=Path, default=Path("~/.local/share/rundra/runs")
    )
    parser.add_argument(
        "--targets-file", type=Path, default=Path("~/.config/rundra/targets.yaml")
    )
    parser.add_argument("--allow-path", action="append", type=Path, default=[])
    parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default="stdio",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=_port, default=8000)
    parser.add_argument("--http-path", default="/mcp")
    parser.add_argument("--token-env", default="RUNDRA_MCP_TOKEN")
    parser.add_argument("--allowed-host", action="append", default=[])
    parser.add_argument("--allowed-origin", action="append", default=[])
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    parser = build_argument_parser()
    arguments = parser.parse_args(argv)
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
    transport: Literal["stdio", "streamable-http"] = arguments.transport
    if transport == "stdio":
        build_server(settings).run(transport="stdio")
        return
    try:
        http = _http_settings(arguments)
        token = _token_from_environment(http.token_env, environ or os.environ)
    except ValueError as error:
        parser.error(str(error))
    verifier = StaticBearerTokenVerifier(token)
    build_server(settings, token_verifier=verifier).run(
        transport="streamable-http",
        host=http.host,
        port=http.port,
        streamable_http_path=http.path,
        transport_security=http.transport_security(),
    )


def _http_settings(arguments: argparse.Namespace) -> HTTPSettings:
    host = _safe_nonblank(arguments.host, name="HTTP host")
    if "://" in host or "/" in host:
        raise ValueError("HTTP host must be a hostname or address, not a URL")
    path = _safe_nonblank(arguments.http_path, name="HTTP path")
    if (
        not path.startswith("/")
        or path == "/"
        or "?" in path
        or "#" in path
        or "//" in path
    ):
        raise ValueError("HTTP path must be one absolute path such as /mcp")
    token_env = _safe_nonblank(arguments.token_env, name="token environment name")
    if _ENVIRONMENT_NAME.fullmatch(token_env) is None:
        raise ValueError("token environment name is invalid")
    allowed_hosts = tuple(
        dict.fromkeys(_allowed_host(value) for value in arguments.allowed_host)
    )
    allowed_origins = tuple(
        dict.fromkeys(_allowed_origin(value) for value in arguments.allowed_origin)
    )
    if host not in _LOOPBACK_HOSTS and not allowed_hosts:
        raise ValueError("non-loopback HTTP requires at least one --allowed-host")
    return HTTPSettings(
        host,
        arguments.port,
        path,
        token_env,
        allowed_hosts,
        allowed_origins,
    )


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer") from error
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def _safe_nonblank(value: object, *, name: str) -> str:
    if type(value) is not str or not value.strip() or "\x00" in value:
        raise ValueError(f"{name} must be a safe nonblank string")
    if value != value.strip() or any(character.isspace() for character in value):
        raise ValueError(f"{name} must not contain whitespace")
    return value


def _allowed_host(value: object) -> str:
    host = _safe_nonblank(value, name="allowed host")
    if "://" in host or "/" in host:
        raise ValueError("allowed host must be a Host header pattern")
    return host


def _allowed_origin(value: object) -> str:
    origin = _safe_nonblank(value, name="allowed origin")
    if not origin.startswith(("http://", "https://")):
        raise ValueError("allowed origin must use http:// or https://")
    remainder = origin.split("://", 1)[1]
    if not remainder or "/" in remainder or "@" in remainder:
        raise ValueError("allowed origin must contain only scheme and authority")
    return origin


def _token_from_environment(name: str, environ: Mapping[str, str]) -> str:
    token = environ.get(name)
    if token is None:
        raise ValueError(f"Streamable HTTP requires token environment variable {name}")
    _validate_token(token)
    return token


def _validate_token(token: object) -> None:
    if type(token) is not str or len(token) < 32:
        raise ValueError(
            "Streamable HTTP bearer token must contain at least 32 characters"
        )
    if token != token.strip() or any(character in token for character in "\r\n\x00"):
        raise ValueError("Streamable HTTP bearer token contains unsafe whitespace")


if __name__ == "__main__":
    main()
