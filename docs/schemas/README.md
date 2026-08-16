# Versioned JSON contracts

These checked examples define Rundra's public version-1 machine-readable
interfaces. They are concrete contract fixtures, not JSON Schema documents.
Contract tests construct or execute the corresponding operation and compare the
parsed result with each file.

Every CLI document has `format_version: 1`, an `operation` name, and an `ok`
flag. A successful document contains an operation-specific value. A failed
document contains `error.code`, `error.message`, and `error.details` instead.
Use the fields, not object-key order or human output, as the interface.

## Contract inventory

| Operation or document | Checked example | Primary payload |
|---|---|---|
| `validate` | [`validate-success-v1.json`](validate-success-v1.json) | `experiment` |
| `plan` | [`plan-success-v1.json`](plan-success-v1.json) | `plan`, plus launch resolution |
| `targets` | [`targets-success-v1.json`](targets-success-v1.json) | `targets` |
| `run` | [`run-success-v1.json`](run-success-v1.json) | terminal `run`, plus launch resolution |
| `submit` | [`submit-success-v1.json`](submit-success-v1.json) | submitted `run` |
| `status` | [`status-success-v1.json`](status-success-v1.json) | aggregate and Task status |
| `list` | [`list-success-v1.json`](list-success-v1.json) | ordered `runs` summaries |
| `logs` | [`logs-success-v1.json`](logs-success-v1.json) | one Task's stdout/stderr and paths |
| `fetch` | [`fetch-success-v1.json`](fetch-success-v1.json) | destination, selected Tasks, artifacts |
| `cancel` | [`cancel-success-v1.json`](cancel-success-v1.json) | reconciled cancellation status |
| `inspect` | composed and contract-tested | `record` equal to the RunRecord below |
| operation failure | [`error-v1.json`](error-v1.json) | structured `error` |
| CLI usage failure | [`cli-usage-error-v1.json`](cli-usage-error-v1.json) | `CLI_USAGE_ERROR` |
| persisted state | [`run-record-v1.json`](run-record-v1.json) | one complete RunRecord |

`inspect` deliberately embeds `run-record-v1.json` unchanged beneath `record`.
Keeping one RunRecord fixture avoids two copies of the durable schema drifting.

For shell pipelines, use an available JSON parser rather than matching text:

```bash
uv run rundr plan examples/minimal/experiment.yaml --seed 17 --json \
  | python3 -m json.tool
```

`--json` may instead appear before the command. Machine-readable usage and
operation failures write JSON to stdout, leave stderr empty, and exit 1.
Successful operations exit 0. Only synchronous `run` uses exit 2 after it has
successfully returned a durable failed or cancelled experiment result.

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
