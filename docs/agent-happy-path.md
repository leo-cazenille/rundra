# Agent happy path

Use one persistent Run store. Sandboxed agents should normally keep it inside
the permitted project workspace:

```bash
DATA_DIR="$PWD/.rundra-data"
```

Run the five lifecycle stages in this order:

```bash
rundr doctor experiment.yaml --connect --agent codex --data-dir "$DATA_DIR" --json
rundr plan experiment.yaml --json
rundr submit experiment.yaml --data-dir "$DATA_DIR" --json
rundr await RUN_ID --data-dir "$DATA_DIR" --json
rundr fetch RUN_ID --mode copy --extract --summary --data-dir "$DATA_DIR" --json
```

If doctor returns `run_store_durability.verification_argv`, execute that exact
argument vector as a separate command, then rerun doctor. A same-process write
probe cannot detect a sandbox overlay that disappears when the command exits.
Do not submit until the cross-command challenge reports `verified`.

Retain the Run ID and exact data directory from submit. `await` emits one final
bounded document. `--mode copy --extract` verifies worker-pool result shards and
materializes ordinary files for downstream tools. Use `status --summary --json`
and `inspect --summary --json` for compact diagnostics; page detail with `tasks`
and `artifacts`.

For a static multi-target campaign, substitute `experiment.yaml --campaign
NAME` or a standalone `campaign.yaml` in the first three commands. Submit
returns a `campaign_*` ID and child Run IDs. Pass the campaign ID to `await`,
`fetch`, `status`, `tasks`, and `inspect`; Task selectors use
`launch-name/task_NNNNNN`. Preserve all IDs. If submission becomes uncertain,
resume the campaign and resolve only the explicitly reported child Run.
