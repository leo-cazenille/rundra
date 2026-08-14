# CLI JSON contracts

The checked examples in this directory define the public M0 JSON envelope for
`validate`, `plan`, and `targets`. Every document has `format_version: 1`, an
operation name, and an `ok` flag. Successful documents contain an
operation-specific value; failures contain a structured `error` with `code`,
`message`, and `details`.

Fields may be added compatibly during v0.1 development. Removing a field,
changing its meaning, or changing its type requires a new `format_version`.
The JSON renderer and human renderer consume the same typed operation result.
