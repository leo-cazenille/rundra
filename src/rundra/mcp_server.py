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

from rundra.cli.agent_guide import GUIDE, GUIDE_TOPICS
from rundra.cli.campaign_operations import (
    CampaignAndRunListValue,
    CampaignPlanValue,
    campaign_cancel_operation,
    campaign_fetch_operation,
    campaign_inspect_operation,
    campaign_list_operation,
    campaign_logs_operation,
    campaign_plan_operation,
    campaign_purge_operation,
    campaign_resume_operation,
    campaign_run_operation,
    campaign_status_operation,
    campaign_submit_operation,
    campaign_tasks_operation,
    campaign_validate_operation,
    campaign_wait_operation,
)
from rundra.cli.doctor import doctor_operation
from rundra.cli.operations import (
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
from rundra.cli.render import result_document
from rundra.config.campaigns import is_campaign_source
from rundra.domain.campaigns import (
    CampaignId,
    CampaignRecord,
    CampaignSubmissionState,
)
from rundra.persistence import (
    JsonCampaignStore,
    JsonRunStore,
    PurgeReceiptStore,
    SqliteTaskStore,
    SubmissionReceiptStore,
)
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
            "For long work use submit_experiment, wait_run, then fetch_results. "
            "Use bounded waits without interactive progress, page list_runs and "
            "list_tasks, retain compact archives for large results, and call "
            "get_guidance for workflow-specific instructions. Use "
            "resume_submission after an interrupted submit."
        ),
        token_verifier=token_verifier,
        auth=_STATIC_AUTH_SETTINGS if token_verifier is not None else None,
    )

    def store() -> JsonRunStore:
        return JsonRunStore(settings.data_dir)

    def task_store() -> SqliteTaskStore:
        return SqliteTaskStore(settings.data_dir)

    def submission_receipts() -> SubmissionReceiptStore:
        return SubmissionReceiptStore(settings.data_dir)

    def campaign_requested(source: Path, campaign: str | None) -> bool:
        return campaign is not None or is_campaign_source(source)

    def campaign_id(value: str) -> CampaignId | None:
        try:
            return CampaignId(value)
        except (TypeError, ValueError):
            return None

    def document(result: OperationResult[Any]) -> dict[str, Any]:
        return result_document(result)

    def plan_result(
        experiment: str,
        config: str | None,
        seeds: str | None,
        target: str | None,
        profile: str | None,
        campaign: str | None,
        source_root: str | None,
        destination: str | None,
        execution_strategy: str,
        retrieval: str,
        prepare_location: str,
        rebuild: bool,
        rebuild_image: bool,
        offline: bool,
    ) -> tuple[OperationResult[Any], str | None]:
        experiment_path = settings.path(experiment)
        if campaign_requested(experiment_path, campaign):
            overrides = tuple(
                name
                for name, value in (
                    ("config", config),
                    ("seeds", seeds),
                    ("target", target),
                    ("profile", profile),
                )
                if value is not None
            )
            if overrides:
                return (
                    OperationResult.failure(
                        "plan",
                        OperationError(
                            "CAMPAIGN_OVERRIDE_UNSUPPORTED",
                            "Campaign Tasks must be assigned in the campaign definition",
                            {"fields": overrides},
                        ),
                    ),
                    None,
                )
            campaign_planned = campaign_plan_operation(
                experiment_path,
                campaign_name=campaign,
                targets_file=settings.targets_file,
                data_dir=settings.data_dir,
                source_root=(
                    None if source_root is None else settings.path(source_root)
                ),
                destination=(
                    None if destination is None else settings.path(destination)
                ),
                prepare_location=prepare_location,
                rebuild=rebuild,
                rebuild_image=rebuild_image,
                offline=offline,
                execution_strategy=execution_strategy,
                retrieval_policy=retrieval,
            )
            if not campaign_planned.ok:
                return campaign_planned, None
            canonical = json.dumps(
                document(campaign_planned),
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            return (
                campaign_planned,
                hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            )
        if seeds is None:
            return (
                OperationResult.failure(
                    "plan",
                    OperationError(
                        "SEEDS_REQUIRED",
                        "MCP experiment plans require explicit seeds",
                    ),
                ),
                None,
            )
        resolved = resolve_plan_inputs_operation(
            experiment_path,
            config=None if config is None else settings.path(config),
            seeds=seeds,
            target=target,
            targets_file=settings.targets_file,
            profile=profile,
            prepare_location=prepare_location,
            rebuild=rebuild,
            rebuild_image=rebuild_image,
            offline=offline,
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
    def get_guidance(topic: str) -> dict[str, Any]:
        """Return bounded agent instructions for one named workflow topic."""
        guidance = GUIDE_TOPICS.get(topic)
        if guidance is None:
            return {
                "ok": False,
                "error": {
                    "code": "UNKNOWN_GUIDE_TOPIC",
                    "message": f"Unknown guidance topic: {topic}",
                    "details": {"topics": list(GUIDE_TOPICS)},
                },
            }
        return {"ok": True, "topic": topic, "guidance": guidance}

    @server.tool()
    def validate_experiment(
        experiment: str, campaign: str | None = None
    ) -> dict[str, Any]:
        """Validate an experiment or campaign and adjacent project configuration."""
        source = settings.path(experiment)
        if campaign_requested(source, campaign):
            return document(campaign_validate_operation(source, campaign_name=campaign))
        return document(validate_operation(source))

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
        seeds: str | None = None,
        config: str | None = None,
        target: str | None = None,
        profile: str | None = None,
        campaign: str | None = None,
        source_root: str | None = None,
        destination: str | None = None,
        execution_strategy: str = "auto",
        retrieval: str = "manifest",
        prepare_location: str = "auto",
        rebuild: bool = False,
        rebuild_image: bool = False,
        offline: bool = False,
    ) -> dict[str, Any]:
        """Create an offline plan and a digest required for execution tools."""
        result, digest = plan_result(
            experiment,
            config,
            seeds,
            target,
            profile,
            campaign,
            source_root,
            destination,
            execution_strategy,
            retrieval,
            prepare_location,
            rebuild,
            rebuild_image,
            offline,
        )
        rendered = document(result)
        if digest is not None:
            rendered["plan_digest"] = digest
        return rendered

    def execute(
        operation: str,
        experiment: str,
        plan_digest: str,
        seeds: str | None,
        config: str | None,
        target: str | None,
        profile: str | None,
        campaign: str | None,
        source_root: str | None,
        destination: str | None,
        confirm_tasks: int | None,
        prepare_location: str,
        rebuild: bool,
        rebuild_image: bool,
        offline: bool,
    ) -> dict[str, Any]:
        planned, current_digest = plan_result(
            experiment,
            config,
            seeds,
            target,
            profile,
            campaign,
            source_root,
            destination,
            "auto",
            "manifest",
            prepare_location,
            rebuild,
            rebuild_image,
            offline,
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
        if isinstance(planned.value, CampaignPlanValue):
            campaign_result = (
                campaign_run_operation(planned.value, confirm_tasks=confirm_tasks)
                if operation == "run"
                else campaign_submit_operation(
                    planned.value, confirm_tasks=confirm_tasks
                )
            )
            return document(campaign_result)
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
            prepare_location=prepare_location,
            rebuild=rebuild,
            rebuild_image=rebuild_image,
            offline=offline,
        )
        if not resolved.ok:
            return document(resolved)
        assert resolved.value is not None
        inputs = resolved.value
        if operation == "run":
            result = run_operation(
                experiment_path,
                inputs.config,
                inputs.targets_file,
                inputs.target,
                inputs.source_root,
                inputs.destination,
                store(),
                seed=inputs.seed,
                seeds=inputs.seeds if inputs.seed_count > 1 else None,
                launch=inputs.launch,
                preparation=inputs.preparation_plan,
                preparation_storage=inputs.preparation_storage,
                sweep=inputs.sweep,
                confirm_tasks=confirm_tasks,
            )
        else:
            result = submit_operation(
                experiment_path,
                inputs.config,
                inputs.targets_file,
                inputs.target,
                inputs.source_root,
                inputs.destination,
                store(),
                seed=inputs.seed,
                seeds=inputs.seeds if inputs.seed_count > 1 else None,
                launch=inputs.launch,
                preparation=inputs.preparation_plan,
                preparation_storage=inputs.preparation_storage,
                sweep=inputs.sweep,
                confirm_tasks=confirm_tasks,
                submission_receipts=submission_receipts(),
            )
        return document(result)

    @server.tool()
    def submit_experiment(
        experiment: str,
        plan_digest: str,
        seeds: str | None = None,
        config: str | None = None,
        target: str | None = None,
        profile: str | None = None,
        campaign: str | None = None,
        source_root: str | None = None,
        destination: str | None = None,
        confirm_tasks: int | None = None,
        prepare_location: str = "auto",
        rebuild: bool = False,
        rebuild_image: bool = False,
        offline: bool = False,
    ) -> dict[str, Any]:
        """Submit an approved plan and return after durable scheduler submission."""
        return execute(
            "submit",
            experiment,
            plan_digest,
            seeds,
            config,
            target,
            profile,
            campaign,
            source_root,
            destination,
            confirm_tasks,
            prepare_location,
            rebuild,
            rebuild_image,
            offline,
        )

    @server.tool()
    def run_experiment(
        experiment: str,
        plan_digest: str,
        seeds: str | None = None,
        config: str | None = None,
        target: str | None = None,
        profile: str | None = None,
        campaign: str | None = None,
        source_root: str | None = None,
        destination: str | None = None,
        confirm_tasks: int | None = None,
        prepare_location: str = "auto",
        rebuild: bool = False,
        rebuild_image: bool = False,
        offline: bool = False,
    ) -> dict[str, Any]:
        """Execute an approved short Run synchronously and retrieve results."""
        return execute(
            "run",
            experiment,
            plan_digest,
            seeds,
            config,
            target,
            profile,
            campaign,
            source_root,
            destination,
            confirm_tasks,
            prepare_location,
            rebuild,
            rebuild_image,
            offline,
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
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            result = await asyncio.to_thread(
                campaign_wait_operation,
                selected_campaign,
                settings.data_dir,
                timeout=timeout_seconds,
                poll_interval=poll_interval,
            )
            return document(result)
        run_result = await asyncio.to_thread(
            wait_operation,
            run_id,
            store(),
            timeout=timeout_seconds,
            poll_interval=poll_interval,
            task_store=task_store(),
        )
        return document(run_result)

    @server.tool()
    async def await_runs(
        run_ids: list[str],
        until: str = "all",
        timeout_seconds: float | None = None,
        poll_interval: float = 15,
        fail_on_run_failure: bool = False,
    ) -> dict[str, Any]:
        """Wait silently until all or any of several Runs terminate."""
        expanded: list[str] = []
        for identifier in run_ids:
            selected_campaign = campaign_id(identifier)
            if selected_campaign is None:
                expanded.append(identifier)
                continue
            try:
                record = JsonCampaignStore(settings.data_dir).load(selected_campaign)
            except Exception as error:
                return document(
                    OperationResult.failure(
                        "await", OperationError("CAMPAIGN_STORE_ERROR", str(error))
                    )
                )
            expanded.extend(
                str(launch.run_id)
                for launch in record.launches
                if launch.submission_state
                not in {
                    CampaignSubmissionState.PENDING,
                    CampaignSubmissionState.NOT_ATTEMPTED,
                }
            )
        result = await asyncio.to_thread(
            await_runs_operation,
            expanded,
            store(),
            until=until,
            timeout=timeout_seconds,
            poll_interval=poll_interval,
            fail_on_run_failure=fail_on_run_failure,
            task_store=task_store(),
        )
        return document(result)

    @server.tool()
    def get_status(run_id: str) -> dict[str, Any]:
        """Refresh and return aggregate Run status."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            return document(
                campaign_status_operation(selected_campaign, settings.data_dir)
            )
        return document(status_operation(run_id, store(), task_store=task_store()))

    @server.tool()
    def resume_submission(run_id: str) -> dict[str, Any]:
        """Recover an interrupted submission or find its durable scheduler IDs."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            inspected = campaign_inspect_operation(selected_campaign, settings.data_dir)
            if not inspected.ok:
                return document(inspected)
            assert inspected.value is not None
            record = inspected.value.record
            planned = campaign_plan_for_record(record)
            if not planned.ok:
                return document(planned)
            assert planned.value is not None
            return document(campaign_resume_operation(planned.value, selected_campaign))
        return document(resume_operation(run_id, store(), submission_receipts()))

    @server.tool()
    def resolve_submission(
        run_id: str,
        confirm_run_id: str,
        not_submitted: bool,
    ) -> dict[str, Any]:
        """Close an uncertain submission after external scheduler verification."""
        if campaign_id(run_id) is not None:
            return document(
                OperationResult.failure(
                    "resolve-submission",
                    OperationError(
                        "CHILD_RUN_REQUIRED",
                        "resolve_submission requires the uncertain child Run ID",
                        {"campaign_id": run_id},
                    ),
                )
            )
        return document(
            resolve_submission_operation(
                run_id,
                store(),
                submission_receipts(),
                not_submitted=not_submitted,
                confirmation=confirm_run_id,
            )
        )

    @server.tool()
    def list_runs(
        offset: int = 0,
        limit: int = 100,
        include_tasks: bool = False,
        kind: Literal["run", "campaign", "all"] = "run",
    ) -> dict[str, Any]:
        """Return one bounded page of Run, campaign, or combined summaries."""
        campaigns = campaign_list_operation(
            settings.data_dir, offset=offset, limit=limit
        )
        if kind == "campaign":
            return document(campaigns)
        runs = list_runs_operation(
            store(),
            task_store=task_store(),
            offset=offset,
            limit=limit,
            include_tasks=include_tasks,
        )
        if kind == "run" or not runs.ok:
            return document(runs)
        if not campaigns.ok:
            return document(campaigns)
        assert runs.value is not None and campaigns.value is not None
        return document(
            OperationResult.success(
                "list", CampaignAndRunListValue(runs.value, campaigns.value)
            )
        )

    @server.tool()
    def list_tasks(run_id: str, offset: int = 0, limit: int = 100) -> dict[str, Any]:
        """Return one bounded page of compact Task state."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            return document(
                campaign_tasks_operation(
                    selected_campaign,
                    settings.data_dir,
                    offset=offset,
                    limit=limit,
                )
            )
        return document(
            tasks_operation(run_id, store(), task_store(), offset=offset, limit=limit)
        )

    @server.tool()
    def get_logs(
        run_id: str,
        task: str | None = None,
        preparation: bool = False,
        launch: str | None = None,
    ) -> dict[str, Any]:
        """Read framework-managed Task or preparation logs."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            return document(
                campaign_logs_operation(
                    selected_campaign,
                    settings.data_dir,
                    task=task,
                    preparation=preparation,
                    launch_name=launch,
                )
            )
        return document(
            logs_operation(run_id, store(), task=task, preparation=preparation)
        )

    @server.tool()
    def fetch_results(
        run_id: str,
        destination: str | None = None,
        mode: str = "auto",
        extract: bool = False,
    ) -> dict[str, Any]:
        """Retrieve terminal or partial Run outputs into an allowed path."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            return document(
                campaign_fetch_operation(
                    selected_campaign,
                    settings.data_dir,
                    settings.path(destination) if destination is not None else None,
                    mode=mode,
                    extract=extract,
                )
            )
        return document(
            fetch_operation(
                run_id,
                store(),
                settings.path(destination) if destination is not None else None,
                mode=mode,
                extract=extract,
            )
        )

    @server.tool()
    def inspect_run(run_id: str) -> dict[str, Any]:
        """Return the complete persisted RunRecord and retention receipt."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            return document(
                campaign_inspect_operation(selected_campaign, settings.data_dir)
            )
        return document(
            inspect_operation(
                run_id, store(), receipts=PurgeReceiptStore(settings.data_dir)
            )
        )

    @server.tool()
    def cancel_run(run_id: str) -> dict[str, Any]:
        """Cancel active scheduler work after reconciling current state."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            return document(
                campaign_cancel_operation(selected_campaign, settings.data_dir)
            )
        return document(cancel_operation(run_id, store()))

    @server.tool()
    def purge_run(
        run_id: str,
        confirm_run_id: str,
        workspace: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Preview or perform guarded Run output/workspace deletion."""
        selected_campaign = campaign_id(run_id)
        if selected_campaign is not None:
            return document(
                campaign_purge_operation(
                    selected_campaign,
                    settings.data_dir,
                    workspace=workspace,
                    confirm=confirm_run_id,
                    dry_run=dry_run,
                )
            )
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

    def campaign_plan_for_record(
        record: CampaignRecord,
    ) -> OperationResult[CampaignPlanValue]:
        source = settings.path(str(record.source))
        if is_campaign_source(source):
            return campaign_plan_operation(
                source,
                targets_file=settings.targets_file,
                data_dir=settings.data_dir,
            )
        return campaign_plan_operation(
            settings.path(str(record.experiment_source)),
            campaign_name=record.name,
            project_file=source,
            targets_file=settings.targets_file,
            data_dir=settings.data_dir,
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
