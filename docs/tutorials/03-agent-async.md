# Tutorial: detached agent workflow

```bash
rundr plan experiment.yaml --seeds 0:99 --json
rundr submit experiment.yaml --seeds 0:99 --json
rundr wait RUN_ID --timeout 300 --json
rundr fetch RUN_ID --destination retrieved/config --json
```

A timed-out wait is successful and contains current state. Renew until terminal.
Fetching remains separately retryable. Install instructions with
`rundr agent-guide --write AGENTS.md`.
