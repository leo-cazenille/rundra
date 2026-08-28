# Tutorial: detached agent workflow

```bash
rundr plan experiment.yaml --seeds 0:99 --json
rundr submit experiment.yaml --seeds 0:99 --json
rundr await RUN_ID --json
rundr fetch RUN_ID --json
```

`await` keeps the harness blocked and emits one compact final document instead
of waking the model to poll. Add `--timeout` only when the harness imposes a
deadline; a timed-out result contains the current aggregate state. Fetching
reuses the destination persisted at submission and remains separately retryable.
Install or refresh instructions with `rundr agent-guide --write AGENTS.md`.
