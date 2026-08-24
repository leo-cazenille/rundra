# M30: Harness-efficient aggregate waiting

## Objective

Allow a human or agent harness to block on one or several Runs without repeatedly
invoking a model, emitting progress updates, or operating a persistent Rundra daemon.

## Public interface

`rundr await RUN_ID [RUN_ID ...]` waits in the foreground and emits exactly one final
human or JSON result. `--until all` is the default; `--until any` returns when the first
Run is terminal. Optional timeout, polling interval, failure-sensitive exit status, and
an atomic aggregate notification file are supported.

The MCP server exposes the same behavior as `await_runs`, allowing an MCP client to
keep one request pending and resume the model only after completion.

## Safety and semantics

- Every Run ID is explicit and unique; aggregate waiting does not support `--last`.
- Rundra uses scheduler adapters and persisted Run records rather than native output.
- Operational errors fail immediately. A timeout is a successful observation with
  `timed_out: true`.
- Scientific failure changes the process exit status only when
  `--fail-on-run-failure` is selected.
- Notification files are written only when the selected condition is met, using mode
  `0600`, temporary-file publication, and atomic replacement.
- The command is a foreground operation. It introduces no daemon, callback endpoint,
  credential store, or network listener.

## Verification

Unit coverage validates all/any conditions, timeout and failure exits, compact JSON,
notification safety, CLI parsing, and MCP discovery. CLI surface schema v22 records the
new command. Full tests, Ruff, mypy, and the local execution example remain release
gates.
