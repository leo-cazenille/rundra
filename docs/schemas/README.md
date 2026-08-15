# Versioned JSON contracts

The checked CLI examples in this directory define the public JSON envelope for
`validate`, `plan`, and `targets`. Every CLI document has `format_version: 1`,
an operation name, and an `ok` flag. Successful documents contain an
operation-specific value; failures contain a structured `error` with `code`,
`message`, and `details`.

[`run-record-v1.json`](run-record-v1.json) defines a complete checked example of
the persisted RunRecord format introduced in M1.1. Unlike CLI envelopes, it is
a durable state document rather than an operation result. Optional unavailable
provenance uses JSON `null` or an empty collection; values are never inferred or
fabricated while loading. Computation state is stored in `run.state`, while
result-transfer state is independently stored in `run.retrieval_state`.
The M1.4 example represents a completed local Run and demonstrates its artifact
manifest, scheduler reference, timestamps, Task exit code, and independent
successful retrieval state. It also demonstrates the optional M1.6 Git commit,
branch, dirty flag, and bounded dirty patch fields.
M3 adds immutable scalar `scheduler_metadata`, including available accounting
source, allocated-node, accounting-delay, and normalized log-path values.

M1.5 adds checked success envelopes for synchronous `run`, `status`, `list`,
`logs`, and `fetch`. The `inspect` contract embeds `run-record-v1.json` unchanged
under the standard envelope's `record` field. M3 adds checked successful
`submit` and `cancel` envelopes; status/list include the separately preserved
native state and scheduler job IDs. Local asynchronous submission still uses
the common `ASYNC_UNAVAILABLE` error.

M1E adds a sibling `launch` object to checked `plan` and `run` success
envelopes. It contains the selected profile, every launch value consumed by the
operation, and the resolution source for each value. Generated seeds therefore
remain explicit and replayable without changing the existing `plan` or `run`
payload shapes.

Fields may be added compatibly during v0.1 development. Removing a field,
changing its meaning, or changing its type requires a new `format_version`.
The CLI JSON renderer and human renderer consume the same typed operation
result. Persisted RunRecords reject unknown fields and unsupported format
versions instead of silently reinterpreting them.
