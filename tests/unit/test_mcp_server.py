import asyncio
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

import rundra.mcp_server as mcp_server
from rundra.mcp_server import (
    HTTPSettings,
    ServerSettings,
    StaticBearerTokenVerifier,
    build_argument_parser,
    build_server,
)


def test_mcp_settings_confine_tool_paths(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    settings = ServerSettings(
        root,
        tmp_path / "records",
        tmp_path / "targets.yaml",
        (root,),
    )

    assert settings.path("config.yaml") == root / "config.yaml"
    with pytest.raises(ValueError, match="outside"):
        settings.path("../secret")


def test_mcp_server_exposes_guarded_lifecycle_tools(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    settings = ServerSettings(
        root, tmp_path / "records", tmp_path / "targets.yaml", (root,)
    )

    server = build_server(settings)
    tools = {tool.name: tool for tool in asyncio.run(server.list_tools())}

    assert server.name == "Rundra"
    assert "resume_submission" in tools
    assert "resolve_submission" in tools
    list_schema = tools["list_runs"].input_schema
    assert {"offset", "limit", "include_tasks"} <= set(list_schema["properties"])


def test_static_bearer_verifier_accepts_only_the_configured_token() -> None:
    token = "a" * 64
    verifier = StaticBearerTokenVerifier(token)

    accepted = asyncio.run(verifier.verify_token(token))
    rejected = asyncio.run(verifier.verify_token("b" * 64))

    assert accepted is not None
    assert accepted.client_id == "rundr-mcp-static"
    assert rejected is None


def test_streamable_http_app_requires_bearer_and_allowed_host(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    settings = ServerSettings(
        root, tmp_path / "records", tmp_path / "targets.yaml", (root,)
    )
    token = "c" * 64
    security = HTTPSettings(
        "0.0.0.0",
        8000,
        "/mcp",
        "RUNDRA_MCP_TOKEN",
        ("testserver",),
        (),
    ).transport_security()
    app = build_server(
        settings, token_verifier=StaticBearerTokenVerifier(token)
    ).streamable_http_app(transport_security=security)
    initialize = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "1"},
        },
    }
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
    }

    with TestClient(app) as client:
        missing = client.post("/mcp", json=initialize, headers=headers)
        wrong = client.post(
            "/mcp",
            json=initialize,
            headers={**headers, "authorization": f"Bearer {'d' * 64}"},
        )
        accepted = client.post(
            "/mcp",
            json=initialize,
            headers={**headers, "authorization": f"Bearer {token}"},
        )
        forbidden_host = client.post(
            "http://invalid.example/mcp",
            json=initialize,
            headers={**headers, "authorization": f"Bearer {token}"},
        )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert accepted.status_code == 200
    assert forbidden_host.status_code in {403, 421}


def test_mcp_launcher_preserves_stdio_defaults() -> None:
    arguments = build_argument_parser().parse_args(())

    assert arguments.transport == "stdio"
    assert arguments.host == "127.0.0.1"
    assert arguments.port == 8000
    assert arguments.http_path == "/mcp"


def test_http_launcher_requires_token_and_non_loopback_allowed_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeServer:
        def run(self, transport: str, **kwargs: Any) -> None:
            calls.append((transport, kwargs))

    monkeypatch.setattr(
        mcp_server, "build_server", lambda *args, **kwargs: FakeServer()
    )
    base = (
        "--root",
        str(tmp_path),
        "--transport",
        "streamable-http",
        "--host",
        "0.0.0.0",
    )
    with pytest.raises(SystemExit):
        mcp_server.main(base, environ={"RUNDRA_MCP_TOKEN": "e" * 64})
    with pytest.raises(SystemExit):
        mcp_server.main((*base, "--allowed-host", "bigfish:8000"), environ={})

    mcp_server.main(
        (*base, "--allowed-host", "bigfish:8000"),
        environ={"RUNDRA_MCP_TOKEN": "e" * 64},
    )

    assert calls[0][0] == "streamable-http"
    assert calls[0][1]["host"] == "0.0.0.0"
    assert calls[0][1]["port"] == 8000
    assert calls[0][1]["streamable_http_path"] == "/mcp"
