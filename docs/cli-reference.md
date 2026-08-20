# v0.1 CLI reference

The supported executable is `rundr`. `--json` may appear before or after every
command. `-h`/`--help` is human-oriented and its formatting is not stable.

## Command surface

| Command | Positional | Options | Semantics |
|---|---|---|---|
| `validate` | `EXPERIMENT` | `--json` | Validate experiment YAML without executing. |
| `plan` | `EXPERIMENT` | `--config`, `--seed`/`--seeds`/`--random-seed`, `--target`, `--targets-file`, `--project-file`, `--profile`, preparation options, `--execution-strategy`, `--retrieval`, `--workers`, `--task-slots-per-worker`, `--json` | Resolve and inspect execution without target contact or state changes. |
| `targets` | none | `--targets-file`, `--json` | Validate and list configured targets. |
| `doctor` | optional `EXPERIMENT` | launch path overrides, `--connect`, `--local-target-access`, `--scheduler-probe`, `--probe-timeout`, `--no-write-probe`, `--agent`, `--json` | Audit installation, sandbox paths, target access, reversible staging, and an optional bounded scheduler submission. |
| `run` | `EXPERIMENT` | plan options plus `--source-root`, `--destination`, `--data-dir`, `--workers`, `--task-slots-per-worker`, `--verbose`, `--progress`, `--progress-interval`, `--json` | Execute synchronously, persist, reconcile, and fetch requested outputs. |
| `wait` | `RUN_ID` or `--last` | `--timeout`, `--poll-interval`, `--notify`, `--data-dir`, `--verbose`, `--progress`, `--progress-interval`, `--json` | Reconcile until terminal or a renewable timeout; optionally emit one terminal alert. |
| `agent-guide` | none | `--write PATH`, `--check PATH`, `--json` | Print, install, or check portable agent instructions. |
| `help` | optional `COMMAND` | none | List commands and the common workflow, or show one command's detailed arguments and options. |
| `submit` | `EXPERIMENT` | same as `run` | Submit asynchronously when the selected scheduler supports it. |

`--verbose` prints lifecycle details and `--progress` displays a TQDM phase
bar. They may be combined. Both write only to stderr, preserving the final
human or JSON result on stdout.
Progress redraws are deduplicated and throttled to `--progress-interval`
seconds (10 by default), except for phase and terminal updates. Captured
`--json --progress` emits a warning because terminal redraws may inflate agent
transcripts. Agents should use blocking `wait --json`, or renew `wait --timeout
300 --json` when their tool-call deadline is bounded.
For synchronous arrays the bar total is six lifecycle units plus the number of
Tasks; scheduler updates show terminal/total, running, queued, failed, and
allocated-node counts.

Bare `doctor` performs reversible local write probes in the effective Run store
and preparation cache. With an experiment it also checks the source, config,
and retrieval destination. `--connect` creates and removes a private target
workspace and performs a one-token staging round trip. `--scheduler-probe`
implies connection, submits at most one 1-CPU no-op job, and cancels it on
timeout. `--no-write-probe` leaves write capabilities untested and cannot be
combined with `--scheduler-probe`. Doctor JSON version 2 distinguishes `ready`
from complete requested verification and can generate, but never apply, a
Codex permission profile.

`--local-target-access` requires a selected target and audits the target
workspace, target preparation cache, and configured target image-search paths
from the client. Use it for cluster system tests and when an SSH target's
filesystem is also mounted on the client. Shared staging implies this audit;
ordinary rsync targets do not, so remote-only laptops are not required to see
cluster paths locally. A failed required local probe makes `ready` false and
the generated Codex profile includes the missing paths.
| `status` | `RUN_ID` or `--last` | `--data-dir`, `--json` | Reconcile scheduler state and return portable Run/Task status. |
| `tasks` | `RUN_ID` or `--last` | `--offset`, `--limit`, `--data-dir`, `--json` | Return one bounded page from materialized Run tasks or a compact version-4 TaskSpace sidecar. |
| `list` | none | `--offset`, `--limit`, `--include-tasks`, `--data-dir`, `--json` | Page through compact persisted Run summaries; include per-Task details only when explicitly requested. |
| `logs` | `RUN_ID` or `--last` | `--task`, `--data-dir`, `--json` | Read one Task's framework-managed stdout/stderr. |
| `fetch` | `RUN_ID` or `--last` | optional `--destination`; repeatable `--task`; `--mode`; `--verbose`, `--progress`, `--data-dir`, `--json` | Idempotently retrieve all or selected Task artifacts. Auto mode uses a verified shared reference when the Run workspace is jointly visible, otherwise it copies normally. |
| `inspect` | `RUN_ID` or `--last` | `--data-dir`, `--json` | Return the complete persisted RunRecord. |
| `cancel` | `RUN_ID` or `--last` | `--data-dir`, `--json` | Reconcile and cancel only active scheduler work; repeat safely. |
| `purge` | `RUN_ID` or `--last` | `--workspace`, `--confirm RUN_ID`, `--dry-run`, `--data-dir`, `--json` | Preview or delete terminal Run outputs and workspaces with explicit confirmation. |

`--seeds START:STOP` is an inclusive integer range. Seed selectors are mutually
exclusive. If no seed is supplied, launch resolution uses a configured seed or
generates and persists one; `--random-seed` forces generation even when a fixed
default exists. `--task` accepts a stable `task_NNNNNN` ID or zero-based
ordinal. Without a selector, `fetch` addresses all Tasks and `logs` requires a
single-Task Run.

Lifecycle commands accepting a Run ID also accept `--last`, which resolves once
to the newest Run in the selected `--data-dir`. Scripts and agents should retain
explicit stable Run IDs; `--last` is intended for interactive use. `purge --last`
still requires `--confirm` with the resolved concrete Run ID.

A config with `_rundr: {version: 1}` may define deterministic YAML dimensions
using `batch_options`, `batch_options_range`, and
`batch_hierarchical_options`. `plan`, `run`, and `submit` expand parameter sets
automatically. `_rundr.seeds` supplies an integer or inclusive range unless a
CLI seed selector overrides it. Sweep plans and Runs use format version 3.

Target configuration version 3 enables constant-memory version-4 planning.
`--execution-strategy auto|multi-array|worker-pool` selects or previews the
target-bounded strategy, while `--retrieval all|manifest|none` records the
intended output policy. Version-4 plan JSON reports the exact TaskSpace count,
a maximum ten-Task preview, scheduler batch or worker counts, target limits,
and confirmation threshold. Planning remains network-free.

The optional target-v3 execution field `max_concurrent_jobs` defaults to 256.
It limits submitted scheduler jobs and array elements, not only simultaneously
running elements. On Slurm, materialized `run` and `submit` operations above
this limit may use a bounded worker array whose elements execute logical Tasks
sequentially, with isolated outputs, per-Task timeouts, and atomic exit
journals. OpenPBS uses bounded scheduler arrays and does not implement Rundra's
Slurm worker-pool strategy.

```yaml
execution:
  max_concurrent_jobs: 128
```

Prepared project-v2/v3 operations accept `--prepare-location auto|local|target`,
`--rebuild`, `--rebuild-image`, and `--offline`. `--rebuild` bypasses only the
compiled-application cache; `--rebuild-image` bypasses only the definition-image
cache. `plan` additionally accepts `--source-root` to
describe mutable-working-tree mode; it snapshots nothing and does not probe
caches. On `run` and `submit`, an explicit `--source-root` selects that mode,
while omission uses the recipe's pinned Git commit.

Project schema v3 definition recipes require targets schema v8
`preparation.definition_build` policy. `auto` builds on the client and
publishes by measured SHA-256 for remote execution. Forced `target` builds run
as bounded scheduler work, not on an SSH controller, and complete before
scientific submission because their SIF digest is not known in advance.

Launch values resolve in this order: explicit CLI, selected project profile,
project defaults, user defaults, then built-ins. Automatic locations are:

- targets: `~/.config/rundra/targets.yaml`;
- user launch defaults: `~/.config/rundra/config.yaml`;
- adjacent project launch defaults: `rundra.yaml`;
- client RunRecords: `~/.local/share/rundra/runs`.
- omitted destination: `<project-root>/retrieved/<config-stem>`, or the same
  path below the current working directory without project discovery.

See [agent target setup](agent-setup.md) for safe sandbox access to user
configuration, host trust, and SSH-agent authentication.
SSH targets may set an absolute `transport.config_file` and optional
`transport.executable`; these are used consistently by execution, retrieval,
and `doctor`.

`plan` deliberately has no `--destination` or `--data-dir` because it creates
no snapshot, retrieval, or RunRecord. Local `submit` returns
`ASYNC_UNAVAILABLE`; SSH/Slurm and SSH/OpenPBS submission persist scheduler
identities before returning so later processes can operate by Run ID.

## Machine output and exits

All programmatically useful commands support the checked versioned
[JSON contracts](schemas/README.md). Unprepared projects continue emitting
`format_version: 1`; prepared plans and RunRecords emit `format_version: 2`;
parameterized plans and Runs emit `format_version: 3`. Compact TaskSpace plans,
RunRecords, and `tasks` pages emit `format_version: 4`.
JSON goes to stdout with an empty stderr. Human errors go to stderr. Both
renderers consume the same operation result.

| Exit | Meaning |
|---|---|
| 0 | the requested operation completed successfully |
| 1 | CLI usage or an operation failed |
| 2 | synchronous `run` returned a durable failed or cancelled experiment |

Operation success is distinct from scientific success: a successful `status`,
`logs`, `fetch`, or `inspect` may describe a failed Run. Read `ok` first, then
the nested execution and retrieval states.

See [running and managing experiments](usage.md) for executable workflows and
[v0.1 interface stability](stability.md) for compatibility guarantees.

## Target-v6 worker scale

Target configuration version 6 separates conservative defaults from hard
site-owned ceilings:

```yaml
execution:
  max_active_tasks: 320
  max_concurrent_jobs: 8
  worker_pool:
    activation_threshold: 10000
    default_workers: 1
    max_workers: 8
    default_task_slots_per_worker: 1
    max_task_slots_per_worker: 40
    tasks_per_lease: 100
    infrastructure_retry_limit: 2
    requeue_limit: 8
```

Use `--workers N --task-slots-per-worker M` or equivalent project-profile
values to request scale. Requests above a target ceiling fail rather than being
silently clamped. Without a request, target defaults apply; permitting eight
workers therefore does not reserve eight workers by default.

## Target-v7 worker memory ceiling

Target configuration version 7 optionally limits the aggregate memory of each
worker allocation before scheduler contact:

```yaml
execution:
  max_memory_per_worker: 60GiB
```

Rundra multiplies declared logical Task memory by the effective Task slots per
worker. If the result exceeds this site-owned ceiling, `plan`, `run`, and
`submit` fail with `WORKER_MEMORY_LIMIT_EXCEEDED`. Reduce
`--task-slots-per-worker` or correct the experiment's per-Task memory request;
Rundra does not infer node memory or silently lower either value.

`rundr plan` remains offline and does not probe node topology. Target v6
returns plan format 6 with requested and effective scale, policy ceilings,
worker resources, and scheduler-controlled placement. Operators must configure
limits and defaults from known site policy; Rundra does not infer cores,
exclusive placement, or memory overcommit.
