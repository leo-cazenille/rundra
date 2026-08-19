# Running and managing experiments

This guide shows the implemented human and agent workflows. Commands are fully
explicit where that makes the example isolated; a project `rundra.yaml` can
supply the repeated launch values described in the
[setup guide](getting-started.md).

## Safe inspection

Validate an experiment document, list a target file, and inspect a plan without
contacting the target:

```bash
uv run rundr validate examples/minimal/experiment.yaml
uv run rundr targets --targets-file examples/minimal/targets.yaml
uv run rundr plan examples/minimal/experiment.yaml \
  --config examples/minimal/config.yaml \
  --seed 17 \
  --target local \
  --targets-file examples/minimal/targets.yaml
```

`plan` does not accept `--destination`, `--source-root`, or `--data-dir` because
it creates no Run, workspace, transfer, or record. Those values matter only to
`run` and `submit`. A plan does report the selected staging strategy and its
workspace root.

For agents, request JSON and parse fields rather than human text:

```bash
uv run rundr plan examples/minimal/experiment.yaml \
  --config examples/minimal/config.yaml \
  --seeds 17:19 \
  --target local \
  --targets-file examples/minimal/targets.yaml \
  --json | python3 -m json.tool
```

Seed ranges are inclusive. A missing seed is generated before planning and
reported; pass the reported integer with `--seed` to replay it.

## Complete local lifecycle

Use a dedicated temporary record and retrieval root so the example does not
depend on user defaults:

```bash
M65_ROOT=/tmp/rundra-m65-local
mkdir -p "$M65_ROOT"

uv run rundr run examples/minimal/experiment.yaml \
  --config examples/minimal/config.yaml \
  --seed 17 \
  --target local \
  --targets-file examples/minimal/targets.yaml \
  --source-root examples/minimal \
  --destination "$M65_ROOT/retrieved" \
  --data-dir "$M65_ROOT/records" \
  --json > "$M65_ROOT/run.json"

RUN_ID=$(python3 -c \
  'import json, sys; print(json.load(sys.stdin)["run"]["run_id"])' \
  < "$M65_ROOT/run.json")
printf '%s\n' "$RUN_ID"
```

The Run is synchronous and its requested results have already been retrieved.
The remaining commands address durable state only by Run ID:

```bash
uv run rundr status "$RUN_ID" \
  --data-dir "$M65_ROOT/records" --json | python3 -m json.tool

uv run rundr list \
  --data-dir "$M65_ROOT/records" --json | python3 -m json.tool

uv run rundr logs "$RUN_ID" --task 0 \
  --data-dir "$M65_ROOT/records"

uv run rundr inspect "$RUN_ID" \
  --data-dir "$M65_ROOT/records" --json | python3 -m json.tool

uv run rundr fetch "$RUN_ID" \
  --destination "$M65_ROOT/refetched" \
  --task task_000000 \
  --data-dir "$M65_ROOT/records" \
  --json | python3 -m json.tool
```

`list --json` returns compact Run summaries and pagination metadata by default.
Use `--offset N --limit N` to advance through Runs. Prefer `rundr tasks RUN_ID`
for Task pages; `--include-tasks` is available only when an expanded Run list is
specifically required.

`--task` accepts a stable Task ID or zero-based ordinal and may be repeated.
Without it, `logs` requires a single-Task Run and `fetch` selects every Task.
Repeated fetches are safe and update the same selected destination files.

To check reproducibility, run again with the same source, effective config,
seed, Python/runtime, and a different destination, then compare the raw files:

```bash
uv run rundr run examples/minimal/experiment.yaml \
  --config examples/minimal/config.yaml \
  --seed 17 \
  --target local \
  --targets-file examples/minimal/targets.yaml \
  --source-root examples/minimal \
  --destination "$M65_ROOT/retrieved-again" \
  --data-dir "$M65_ROOT/records"

cmp "$M65_ROOT/retrieved/results/result.json" \
  "$M65_ROOT/retrieved-again/results/result.json"
```

## Remote scheduler lifecycle

Start from an edited target and experiment whose workspace and Apptainer image
are valid for the site. The checked Shoal templates contain placeholders and
must not be submitted unchanged.

First inspect the plan. Two or more homogeneous seeds report explicit
Task/seed/index mapping. Slurm uses the `slurm_array` strategy and OpenPBS uses
`scheduler_array`:

```bash
uv run rundr plan /path/to/experiment.yaml \
  --config /path/to/config.yaml \
  --seeds 40:42 \
  --target shoal \
  --targets-file /path/to/targets.yaml \
  --json | python3 -m json.tool
```

Submit asynchronously and retain the client-side RunRecord directory:

```bash
REMOTE_ROOT=/tmp/rundra-m65-remote
mkdir -p "$REMOTE_ROOT"

uv run rundr submit /path/to/experiment.yaml \
  --config /path/to/config.yaml \
  --seeds 40:42 \
  --target shoal \
  --targets-file /path/to/targets.yaml \
  --source-root /path/to/source \
  --destination "$REMOTE_ROOT/retrieved" \
  --data-dir "$REMOTE_ROOT/records" \
  --json > "$REMOTE_ROOT/submit.json"

RUN_ID=$(python3 -c \
  'import json, sys; print(json.load(sys.stdin)["run"]["run_id"])' \
  < "$REMOTE_ROOT/submit.json")
```

`submit` persists scheduler identities before it returns. A later process can
reconcile the Run; no daemon or original shell is required:

```bash
uv run rundr status "$RUN_ID" \
  --data-dir "$REMOTE_ROOT/records" --json | python3 -m json.tool

uv run rundr logs "$RUN_ID" --task 1 \
  --data-dir "$REMOTE_ROOT/records"

uv run rundr fetch "$RUN_ID" \
  --destination "$REMOTE_ROOT/retrieved" \
  --task 0 --task 2 \
  --data-dir "$REMOTE_ROOT/records" \
  --json | python3 -m json.tool
```

Poll `status` explicitly until the selected work is terminal before expecting
complete logs and outputs. Synchronous `run` performs that waiting and result
retrieval in one command. To cancel the still-active elements of a submitted
Run, use:

```bash
uv run rundr cancel "$RUN_ID" \
  --data-dir "$REMOTE_ROOT/records" --json | python3 -m json.tool
```

Cancellation reconciles first and does not cancel elements already known to be
terminal. Repeat cancellation is safe. Scheduler-native account, partition,
queue, QOS, project, constraint, and placement requests belong in the
experiment's explicit `resources.native.slurm` or `resources.native.pbs`
mapping and remain subject to site policy.

## Exit status

| Exit | Meaning |
|---|---|
| 0 | requested CLI operation succeeded |
| 1 | usage, configuration, capability, persistence, transport, scheduler, staging, or retrieval operation failed |
| 2 | synchronous `run` returned a durable failed or cancelled experiment Run |

A successful `status`, `logs`, `fetch`, or `inspect` operation can describe a
failed experiment and still exit 0. Read `ok` to determine operation success and
the nested Run/Task state to determine scientific execution success.

## Troubleshooting

Use `--json` first: the stable error code and details are more reliable than
matching prose.

| Error or symptom | Check |
|---|---|
| `CONFIG_NOT_FOUND`, `INVALID_YAML`, `UNKNOWN_FIELD`, `UNSUPPORTED_VERSION` | Validate the exact experiment, target, project, or user file; all public YAML documents require version 1. |
| `LAUNCH_VALUE_REQUIRED`, `PROFILE_NOT_FOUND`, `TARGET_NOT_FOUND` | Inspect adjacent `rundra.yaml`, selected profile, `~/.config/rundra/config.yaml`, target name, and target-file path. |
| `CONTAINER_REQUIRED`, `CONTAINER_CONFLICT`, `GPU_CONFIGURATION_MISMATCH` | Remote execution requires Apptainer; native local execution forbids a container/GPU request; scheduler GPU resources and `container.gpu` must agree. |
| `CAPABILITY_CHECK_FAILED` | Check local `ssh`, `rsync`, or `apptainer`; remotely check rsync, selected scheduler clients, Apptainer, image readability, and normal SSH authentication/host verification. |
| `STAGING_FAILED` | Verify the configured workspace's nearest existing ancestor is writable and shared, the source exists, rsync is available, and no destination component is a symlink. |
| `SCHEDULER_SUBMISSION_FAILED` | Re-run `plan`, then inspect account/partition/QOS/constraint, time/memory/GPU requests, and site policy. Rundra intentionally redacts scheduler stderr. |
| `SCHEDULER_QUERY_FAILED` or `ACCOUNTING_PENDING` | Retry `status`; verify the configured scheduler's query commands. For Slurm, `sacct` is optional and `squeue`/`scontrol` provide fallback paths; for OpenPBS, verify `qstat`. |
| `LOGS_UNAVAILABLE` | Select a Task for a multi-Task Run and wait until scheduler log paths or terminal artifacts exist. |
| `RESULT_RETRIEVAL_FAILED` | Preserve computation state, correct connectivity/path/permissions, and retry `fetch`; retrieval state is independent and retryable. |
| `RUN_STORE_CONFLICT` | Another process updated the same Run. Reload with `status` or `inspect` and retry the lifecycle operation. |
| `RUN_NOT_FOUND` | Use the same `--data-dir` used for `run`/`submit`; the default is client-local `~/.local/share/rundra/runs`. |
| Slurm works without `sacct` but completion cannot be found | Ensure `scontrol show job -o JOB_ID` retains the completed job long enough for reconciliation. |

Framework errors do not include external stderr or command/environment values.
Experiment stdout and stderr are separate Run artifacts intentionally returned
by `logs`. Never place credentials in Rundra YAML, argv, scientific config, or
Run data.

## Real-cluster tests

Normal `uv run pytest` never contacts Shoal. The concrete environment variables,
resource-specific switches, bounded requests, expected evidence, and cleanup
rules are documented in [Shoal system testing](shoal.md). Each submitting test
requires both the general opt-in and its own CPU, GPU, failure, or array opt-in.
