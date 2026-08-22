# M21 Offline Preparation Doctor

## Objective

Make `rundr doctor EXPERIMENT --offline` prove that preparation can start
without network acquisition, rather than reporting only filesystem readiness.

## Contract

- Normal `doctor` remains a permissions and connectivity audit.
- `doctor --offline` resolves the same project preparation recipe as `run`.
- For local preparation it checks the exact pinned Git commit and resolves the
  verified prebuilt or definition-built image using cache-only semantics.
- The audit never fetches Git, pulls or builds an image, compiles an
  application, or submits scheduler work.
- Missing source and image inputs produce failed checks and stable remediation
  action codes. Agents should warm the cache by rerunning without `--offline`.

## Validation

- Unit tests cover cold and warm pinned-Git/prebuilt-image caches.
- CLI tests cover propagation of `--offline` through project resolution.
- Agent and user documentation distinguish ordinary readiness from offline
  cache readiness.
- CLI surface v20 freezes the new public option while retaining v19.
