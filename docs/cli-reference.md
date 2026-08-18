# v0.1 CLI reference

The supported executable is `rundr`. `--json` may appear before or after every
command. `-h`/`--help` is human-oriented and its formatting is not stable.

## Command surface

| Command | Positional | Options | Semantics |
|---|---|---|---|
| `validate` | `EXPERIMENT` | `--json` | Validate experiment YAML without executing. |
| `plan` | `EXPERIMENT` | `--config`, `--seed`/`--seeds`/`--random-seed`, `--target`, `--targets-file`, `--project-file`, `--profile`, preparation options, `--execution-strategy`, `--retrieval`, `--json` | Resolve and inspect execution without target contact or state changes. |
| `targets` | none | `--targets-file`, `--json` | Validate and list configured targets. |
| `doctor` | optional `EXPERIMENT` | `--target`, `--targets-file`, `--project-file`, `--profile`, `--connect`, `--json` | Diagnose static target setup and optionally perform a read-only live SSH probe. |
| `run` | `EXPERIMENT` | plan options plus `--source-root`, `--destination`, `--data-dir`, `--verbose`, `--progress`, `--json` | Execute synchronously, persist, reconcile, and fetch requested outputs. |
| `wait` | `RUN_ID` | `--timeout`, `--poll-interval`, `--data-dir`, `--verbose`, `--progress`, `--json` | Reconcile until terminal or a renewable timeout. |
| `agent-guide` | none | `--write PATH`, `--check PATH`, `--json` | Print, install, or check portable agent instructions. |
| `submit` | `EXPERIMENT` | same as `run` | Submit asynchronously when the selected scheduler supports it. |

`--verbose` prints lifecycle details and `--progress` displays a TQDM phase
bar. They may be combined. Both write only to stderr, preserving the final
human or JSON result on stdout.
For synchronous arrays the bar total is six lifecycle units plus the number of
Tasks; scheduler updates show terminal/total, running, queued, failed, and
allocated-node counts.
| `status` | `RUN_ID` | `--data-dir`, `--json` | Reconcile scheduler state and return portable Run/Task status. |
| `tasks` | `RUN_ID` | `--offset`, `--limit`, `--data-dir`, `--json` | Return one bounded page from a compact version-4 TaskSpace sidecar. |
| `list` | none | `--data-dir`, `--json` | List persisted Runs in deterministic order. |
| `logs` | `RUN_ID` | `--task`, `--data-dir`, `--json` | Read one Task's framework-managed stdout/stderr. |
| `fetch` | `RUN_ID` | required `--destination`; repeatable `--task`; `--data-dir`, `--json` | Idempotently retrieve all or selected Task artifacts. |
| `inspect` | `RUN_ID` | `--data-dir`, `--json` | Return the complete persisted RunRecord. |
| `cancel` | `RUN_ID` | `--data-dir`, `--json` | Reconcile and cancel only active scheduler work; repeat safely. |

`--seeds START:STOP` is an inclusive integer range. Seed selectors are mutually
exclusive. If no seed is supplied, launch resolution uses a configured seed or
generates and persists one; `--random-seed` forces generation even when a fixed
default exists. `--task` accepts a stable `task_NNNNNN` ID or zero-based
ordinal. Without a selector, `fetch` addresses all Tasks and `logs` requires a
single-Task Run.

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
It limits submitted Slurm jobs and array elements, not only simultaneously
running elements. Materialized `run` and `submit` operations above this limit
use a bounded worker array whose elements execute logical Tasks sequentially,
with isolated outputs, per-Task timeouts, and atomic exit journals. Compact
TaskSpace launch, requeue recovery, and shard retrieval remain future work.

```yaml
execution:
  max_concurrent_jobs: 128
```

Prepared project-v2 operations accept `--prepare-location auto|local|target`,
`--rebuild`, and `--offline`. `plan` additionally accepts `--source-root` to
describe mutable-working-tree mode; it snapshots nothing and does not probe
caches. On `run` and `submit`, an explicit `--source-root` selects that mode,
while omission uses the recipe's pinned Git commit.

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
`ASYNC_UNAVAILABLE`; SSH/Slurm submission persists scheduler identities before
returning so later processes can operate by Run ID.

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

## Target-v4 worker slots

For large single-threaded sweeps, target configuration version 4 can bound the
number of Slurm jobs independently from the number of processes running inside
each allocation:

```yaml
execution:
  max_active_tasks: 320
  max_concurrent_jobs: 8
  worker_pool:
    activation_threshold: 10000
    max_workers: 8
    task_slots_per_worker: 40
    tasks_per_lease: 100
    infrastructure_retry_limit: 2
    requeue_limit: 8
```

`rundr plan` remains offline and does not probe node topology. For target v4 it
returns plan format 5 with `worker_count`, `task_slots_per_worker`,
`concurrent_task_capacity`, `max_lane_depth`, and `worker_resources`. Operators
must configure slots from known site policy; Rundra does not infer cores or
oversubscribe an allocation.
