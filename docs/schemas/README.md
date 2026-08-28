# Versioned JSON contracts

These checked examples define Rundra's public versioned machine-readable
interfaces. They are concrete contract fixtures, not JSON Schema documents.
Contract tests construct or execute the corresponding operation and compare the
parsed result with each file.

M6.7 freezes these version-1 structures for the v0.1 release. The checked
[`cli-surface-v1.json`](cli-surface-v1.json) fixture separately freezes command,
positional, and option names; their semantics are in the
[CLI reference](../cli-reference.md).

Every CLI document has a `format_version`, an `operation` name, and an `ok`
flag. Unprepared operations retain version 1; prepared plans and Runs use
version 2; parameterized plans and Runs use version 3; compact TaskSpace
documents use version 4. A successful document contains an operation-specific value. A
failed document contains `error.code`, `error.message`, and `error.details`
instead. Use the fields, not object-key order or human output, as the interface.

## Contract inventory

| Operation or document | Checked example | Primary payload |
|---|---|---|
| CLI surface | [`cli-surface-v23.json`](cli-surface-v23.json) | current program, commands, positionals, options |
| MCP launcher surface | [`rundr-mcp-surface-v1.json`](rundr-mcp-surface-v1.json) | stdio and authenticated Streamable HTTP options |
| `validate` | [`validate-success-v1.json`](validate-success-v1.json) | `experiment` |
| `plan` | [`plan-success-v1.json`](plan-success-v1.json) | `plan`, plus launch resolution |
| parameterized `plan` | [`plan-success-v3.json`](plan-success-v3.json) | Task parameter sets and effective-config hashes |
| `targets` | [`targets-success-v2.json`](targets-success-v2.json) | `targets` |
| `run` | [`run-success-v1.json`](run-success-v1.json) | terminal `run`, plus launch resolution |
| `submit` | [`submit-success-v1.json`](submit-success-v1.json) | submitted `run` |
| `status` | [`status-success-v1.json`](status-success-v1.json) | aggregate and Task status |
| `wait` | composed and contract-tested | status, terminal/timeout flags, elapsed duration |
| `await` | composed and contract-tested | bounded aggregate state for one or several Runs |
| `tasks` | composed and contract-tested | bounded v4 Task-state page |
| `list` | [`list-success-v1.json`](list-success-v1.json) | ordered `runs` summaries |
| `logs` | [`logs-success-v1.json`](logs-success-v1.json) | one Task's stdout/stderr and paths |
| `fetch` | [`fetch-success-v1.json`](fetch-success-v1.json) | destination, selected Tasks, artifacts |
| `cancel` | [`cancel-success-v1.json`](cancel-success-v1.json) | reconciled cancellation status |
| `purge` | composed and contract-tested | scope, backend, outcome, paths, receipt |
| `agent-guide` | composed and contract-tested | action, path, canonical Markdown content |
| `inspect` | composed and contract-tested | `record` equal to the RunRecord below |
| operation failure | [`error-v1.json`](error-v1.json) | structured `error` |
| CLI usage failure | [`cli-usage-error-v1.json`](cli-usage-error-v1.json) | `CLI_USAGE_ERROR` |
| persisted state | [`run-record-v1.json`](run-record-v1.json) | one complete unprepared RunRecord |
| previous persisted state | [`run-record-v5.json`](run-record-v5.json) | canonical Run kind and retrieval destination |
| current persisted state | [`run-record-v6.json`](run-record-v6.json) | typed fetch mode and verified preparation identity |

Project-managed preparation uses format version 2. Version-2 plans and
RunRecords add preparation source, image, build, cache, output-hash, and log
metadata; version-1 documents do not gain optional fields. The CLI option
surface introduced for preparation is frozen in
[`cli-surface-v2.json`](cli-surface-v2.json), while the v1 fixture remains as
the historical contract.

The version-3 CLI surface added the original non-mutating `doctor` diagnostic.
Doctor JSON version 2 expands it into a bootstrap and sandbox capability audit
with typed requirements, verification coverage, reversible-probe results, and
generated remediation. It reports credential paths when OpenSSH requires them
but never reads credential contents into output.
The representative contract is [`doctor-success-v2.json`](doctor-success-v2.json).
The expanded command options are frozen in
[`cli-surface-v13.json`](cli-surface-v13.json); versions 11 and 12 remain historical.

Version-3 parameterized documents add a `parameter_set` object to each Task,
permit a seed to recur in different parameter sets, and stage a distinct
effective config per Task. Lifecycle envelopes retain version 3 when operating
on such a Run. Version-1 and version-2 documents do not silently gain these
fields.

`inspect` deliberately embeds `run-record-v1.json` unchanged beneath `record`.
Keeping one RunRecord fixture avoids two copies of the durable schema drifting.

Version-4 plans replace complete Task arrays with a compact arithmetic seed
range, parameter-set count, exact product, and bounded preview. Version-4
RunRecords identify the sparse SQLite task-state sidecar and record execution
and retrieval strategies. The `tasks` operation returns at most 1,000
individually identified states per request. Older documents do not gain
version-4 fields.

RunRecord version 5 introduced the canonical durable shape for all Runs.
It adds an explicit `run_kind`, mandatory absolute `retrieval_destination`,
nullable preparation, and nullable compact-state fields. Materialized tasks
always include a nullable `parameter_set`. This decouples durable schema
evolution from plan, preparation, sweep, and compact-execution versions.

Accepted Slurm worker-pool Runs at or above 1,000 Tasks now use this durable v4
form automatically. The Run store permits only a validated one-way conversion:
Run identity, target, experiment, source/provenance, Task IDs, seed order, and
current lifecycle state must remain identical. Scheduler manifest and recovery
receipt generation are still transiently materialized and are not part of the
v4 RunRecord.

Version-5 large-Run CLI envelopes bound serialized materialized state at the
public interface. They provide TaskSpace identities, aggregate counts, worker
activity, throughput, and ETA when measured. Existing v1-v4 durable RunRecords
remain unchanged; this envelope version does not claim that a historical
materialized record has been migrated to compact persistence.

RunRecord version 6 makes `fetch_mode` a typed durable field and separates a
definition image's deterministic recipe key from its measured SIF SHA-256.
Pending target definition builds leave `container_digest` and `image_sha256`
unavailable; successful preparation records the verified equal digest in both.
Readers continue accepting versions 1 through 5 without rewriting them.

For shell pipelines, use an available JSON parser rather than matching text:

```bash
uv run rundr plan examples/minimal/experiment.yaml --seed 17 --json \
  | python3 -m json.tool
```

`--json` may instead appear before the command. Machine-readable usage and
operation failures write JSON to stdout, leave stderr empty, and exit 1.
Successful operations exit 0. Only synchronous `run` uses exit 2 after it has
successfully returned a durable failed or cancelled experiment result.

The version-6 CLI surface adds `purge`. Purge results use operation format
version 1 and strict receipt version 1. An inspected Run with purge history uses
inspect format version 5 and adds a `retention` sibling without changing the
embedded RunRecord. Inspecting an unpurged Run remains unchanged.

## RunRecord and lifecycle semantics

[`run-record-v1.json`](run-record-v1.json) is a complete checked persisted
RunRecord. Unlike a CLI envelope, it is durable state rather than an operation
result. Optional unavailable provenance uses JSON `null` or an empty collection;
values are never inferred while loading. Computation state is stored in
`run.state`, while result-transfer state is stored independently in
`run.retrieval_state` and `task_retrieval_states`.

The example demonstrates its artifact manifest, scheduler reference,
timestamps, Task exit code, successful retrieval, and optional bounded Git
provenance. Immutable scalar `scheduler_metadata` can retain available
accounting source, allocated-node, accounting-delay, and normalized log paths.

The same version-1 fields represent ordered multi-Task Runs. Every `run.tasks`
entry carries its stable ordinal ID, explicit seed, effective config, resources,
and state. `task_exit_codes`, `task_scheduler_ids`, `task_native_states`,
`task_retrieval_states`, and Task-specific artifacts refer back to those IDs.
`task_array_mapping` records the explicit Task ID, seed, and contiguous
zero-based array index; native job IDs are stored separately and never inferred
at the planning boundary.

The `launch` sibling in checked `plan` and `run` results records the selected
profile, every launch value consumed by the operation, and each value's
resolution source. A generated seed is therefore concrete and replayable.

## Compatibility rules

The CLI JSON renderer and human renderer consume the same typed operation
result. Consumers should require the supported `format_version`, branch on
`ok`, and tolerate additive fields within version 1. Removing a documented
field, changing its meaning, or changing its type requires a new format
version.

Persisted RunRecords are stricter than CLI results: they reject unknown fields,
credential-bearing field names, and unsupported format versions rather than
silently reinterpreting them. Older version-1 records may omit documented
additive fields such as `task_array_mapping`; loading supplies the version-1
empty/default meaning described above.
The version-20 CLI surface adds the cache-only `--offline` audit to `doctor`.
Version 19 remains available as the preceding public surface contract.

The version-21 CLI surface adds `doctor --prepare-location`; plan v8, doctor
v3, and targets v2 expose derived scheduler capabilities. Version 22 adds the
aggregate `await` command, and version 23 adds read-only
`doctor --scheduler-inventory`.

Scheduler capability fields are added only to those new versions. Older plan,
doctor, and target-list documents retain their previous shapes.
