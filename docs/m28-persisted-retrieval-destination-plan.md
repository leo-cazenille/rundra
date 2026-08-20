# M28 persisted retrieval destination plan

## Goal

Make asynchronous `submit` followed by a later `fetch` honor the exact
destination resolved at launch, without storing client retrieval intent in
scheduler metadata.

## Contract

- Every newly created Run uses durable RunRecord version 5.
- Version 5 records a mandatory absolute `retrieval_destination` and explicit
  `run_kind` (`materialized` or `compact`). Preparation, parameter sets, and
  compact TaskSpace state are independent capabilities in one canonical shape.
- Relative CLI or project destinations are resolved before Run creation. The
  record exists before staging or scheduler contact, so interrupted submission
  recovery retains the same destination.
- `fetch --destination PATH` overrides the persisted destination. Without an
  override, `fetch` uses the persisted value and never derives a new path for a
  version-5 Run.
- MCP fetch accepts an omitted destination and follows the same precedence.
- Legacy v1-v4 records remain readable for development history and retain their
  old derived fallback; all new materialized and compact records use v5.

## Acceptance

- Local and remote submission records preserve the resolved absolute path.
- Materialized and compact v5 records round-trip strictly.
- Missing, relative, unsafe, or run-kind-inconsistent v5 fields are rejected.
- Cross-process fetch defaults to the submitted destination, while an explicit
  override wins.
