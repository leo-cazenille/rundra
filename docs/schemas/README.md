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
successful retrieval state.

Fields may be added compatibly during v0.1 development. Removing a field,
changing its meaning, or changing its type requires a new `format_version`.
The CLI JSON renderer and human renderer consume the same typed operation
result. Persisted RunRecords reject unknown fields and unsupported format
versions instead of silently reinterpreting them.
