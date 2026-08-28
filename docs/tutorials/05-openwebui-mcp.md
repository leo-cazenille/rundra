# Connect Open WebUI to Rundra MCP

`rundr-mcp` defaults to local stdio. To connect Open WebUI on another host,
start the authenticated Streamable HTTP transport on bigfish and terminate TLS
at a reverse proxy.

Generate and retain a token outside the repository:

```bash
export RUNDRA_MCP_TOKEN="$(openssl rand -hex 32)"
```

For a reverse proxy on bigfish forwarding `rundr-mcp.internal` to loopback:

```bash
uv run rundr-mcp \
  --root "$PWD" \
  --data-dir /tmp/rundra-m11-mcp-runs \
  --transport streamable-http \
  --host 127.0.0.1 \
  --port 8000 \
  --allowed-host rundr-mcp.internal
```

A minimal Caddy route is:

```caddyfile
rundr-mcp.internal {
    reverse_proxy 127.0.0.1:8000
}
```

The proxy must preserve the external `Host` header. Restrict direct access to
port 8000. If deployment on an isolated trusted network requires a non-loopback
listener, bind the bigfish address or `0.0.0.0` and pass every accepted Host
header using repeatable `--allowed-host` options.

In Open WebUI 0.6.31 or newer on fishvision:

1. Configure a persistent `WEBUI_SECRET_KEY`.
2. Open **Admin Settings -> Integrations** and add a server.
3. Select **MCP Streamable HTTP**.
4. Enter `https://rundr-mcp.internal/mcp`.
5. Select Bearer authentication and enter `RUNDRA_MCP_TOKEN`'s value.
6. Verify tool discovery, save, and restrict access to trusted users or groups.

Test `list_targets`, then `plan_experiment` before approving a small
`submit_experiment`. Use `await_runs` so the client harness, rather than the
model, waits for completion; use `get_status` for an intentional snapshot and
`fetch_results` after completion. If submission is interrupted, use `resume_submission`
with the retained Run ID rather than submitting a duplicate. If the outcome
remains unknown, inspect the scheduler outside MCP through an approved
read-only route. Only after proving that no job exists, call
`resolve_submission` with `not_submitted=true` and the exact same Run ID in
both Run-ID fields. `list_runs`
returns compact pages by default; pass `offset` and `limit` to advance, and use
`list_tasks` instead of expanded Run pages for large experiments. Rotate the
token by changing the environment value,
restarting `rundr-mcp`, and updating Open WebUI.

Never put the token in command arguments, project YAML, target configuration,
RunRecords, logs, or chat messages. Bearer authentication without TLS is
supported only on loopback or a trusted isolated network.
