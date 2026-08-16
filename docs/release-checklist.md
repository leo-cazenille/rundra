# v0.1 release checklist

This checklist turns the validated `0.1.0.dev0` source tree into a v0.1 release.
Checking it does not itself authorize publishing, pushing, tagging, reserving a
domain, or changing external services.

## Scope and contracts

- [ ] Confirm the release contains only the documented v0.1 local and
  SSH/Slurm execution paths and the exclusions in the specification.
- [ ] Review [`CHANGELOG.md`](../CHANGELOG.md), move its Unreleased content to
  `0.1.0` with the release date, and leave a new empty Unreleased section.
- [ ] Review the [CLI reference](cli-reference.md),
  [stability policy](stability.md), and every checked
  [version-1 contract](schemas/README.md).
- [ ] Confirm any public behavior change updates its fixture, contract test,
  specification, and changelog together.
- [ ] Confirm the package still exposes no claimed stable Python API.

## Clean validation

Run from a clean checkout using Python 3.12:

```bash
uv sync --locked
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv build
```

- [ ] Default tests pass with every real-cluster test explicitly skipped.
- [ ] Lint, format, strict typing, and diff hygiene pass.
- [ ] Build produces both an sdist and wheel; inspect their expected project
  metadata, source contents, installed package, and `rundr` entry point.
- [ ] Install the wheel into a clean Python 3.12 environment; run `rundr
  --help`, one JSON `validate`, and one JSON `plan`.
- [ ] Run the minimal example twice with the same explicit seed and compare raw
  `result.json` files byte-for-byte.
- [ ] Parse every checked JSON fixture and verify every local Markdown link.

## Security and live evidence

- [ ] Inspect the final diff for credentials, private hosts/keys, command
  values, generated RunRecords, and temporary results.
- [ ] Confirm default tests cannot contact SSH, Slurm, rsync peers, or a remote
  Apptainer runtime.
- [ ] Review the dated [M6.6 Shoal evidence](shoal.md) or, when site state or
  execution code changed, rerun all authorized resource-gated tests exactly as
  documented there.
- [ ] Confirm CPU, GPU, failure, array, disconnected lifecycle, active
  cancellation, repeated fetch/cancel, and retrieval-state evidence remains
  accurate.
- [ ] Confirm no account, partition, QOS, host-verification, or authentication
  policy was bypassed.

## Version and publication

- [ ] Replace `0.1.0.dev0` with `0.1.0` in `pyproject.toml` and regenerate the
  lockfile through `uv`; inspect the resulting version-only diff.
- [ ] Rebuild from the exact intended commit and repeat the clean wheel smoke
  test against the final artifacts.
- [ ] Confirm the GitHub repository and PyPI project names are `rundra`, while
  the console command remains `rundr`.
- [ ] Commit the release metadata and changelog, obtain project-owner approval,
  then create/push the signed or annotated `v0.1.0` tag according to repository
  policy.
- [ ] Publish the exact validated sdist and wheel to PyPI only with explicit
  authorization; verify their hashes and installed `rundr` behavior.
- [ ] Treat `rundra.ai` registration and the `rundr.ai` redirect as separate
  external tasks; do not claim either domain until verified.

## After release

- [ ] Verify the GitHub release, PyPI metadata, installation instructions, and
  changelog point to the same tag/version.
- [ ] Restore a development version for subsequent work if development
  continues.
- [ ] Record any platform-specific release failure before changing a frozen
  v0.1 contract.
