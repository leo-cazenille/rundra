# Pogosim on Shoal: large-scale example plan

## Objective

Add a complete, reproducible example that uses Rundra to execute Pogosim's
`run_and_tumble` simulation on the Shoal Slurm cluster. Rundra owns the
experiment-level fan-out: each Rundra task is one Pogosim process with one
explicit seed. The example deliberately does not invoke `pogobatch` inside a
scheduled task, because nested fan-out would obscure resource ownership,
scheduler state, and per-seed provenance.

The example must work as a small three-seed system smoke test and as a manual
100-seed showcase without changing Rundra's public CLI or schemas.

## Fixed design decisions

- Workload: upstream Pogosim `examples/run_and_tumble`.
- Container: pull Pogosim's stable prebuilt `pogosim-full:v0.10.10` image.
- Source: a separate checkout of Pogosim pinned to an exact commit.
- Build: compile `run_and_tumble` once in that checkout with the same image
  used for execution.
- Fan-out: one Rundra seed per Slurm array element.
- Outputs: raw Pogosim Feather data and console output only; derived analysis
  remains outside the run directory.
- Scale: three seeds for the gated live test, 100 seeds for the operator-run
  showcase.
- Upstream scope: no Pogosim or `pogobatch` changes.

## Deliverables

### P1. Checked example

Create `examples/pogosim-shoal/` with:

- `experiment.yaml`: headless direct invocation, bounded resources, explicit
  output capture, and the shared-filesystem image path;
- `config.yaml`: 50 robots, disk arena, ten simulated seconds, no video, and
  Feather output under Rundra's existing `/workspace/output/` directory;
- `rundra.yaml.example`: reusable human-facing launch defaults;
- `README.md`: pinned checkout/image preparation and complete operator flow.

The Pogosim configuration intentionally contains no seed. Rundra substitutes
the task's explicit integer seed into the executable arguments and preserves it
in the run record.

### P2. Offline contract tests

Add default-CI tests that load the example through Rundra's public parsers and
verify:

- the experiment and config are schema-valid;
- the command calls `run_and_tumble` directly and contains `{config}` and
  `{seed}` exactly once;
- `-g`, `-q`, and `-nr` keep the workload suitable for unattended execution;
- GPU count, CPU count, memory, and wall time stay within the documented smoke
  bounds;
- the config enables Feather logging, disables video, contains no seed, and
  writes only beneath `/workspace/output/`;
- planning seeds `0:2` produces three task mappings suitable for a Slurm
  array.

These tests must not need SSH, Slurm, Apptainer, or a Pogosim checkout.

### P3. Gated Shoal system test

Add a `shoal_pogosim` pytest marker and an opt-in
`--run-shoal-pogosim-test` switch. The live test requires operator-supplied
target, pinned source checkout, and image values; it remains skipped by
default.

For seeds `0:2`, the test should preflight, submit, poll from a fresh CLI
process, inspect logs, and fetch outputs. It should assert that all three tasks
succeed, scheduler identifiers are preserved, and each fetched Feather file is
non-empty with the Arrow IPC `ARROW1` signature. Do not add PyArrow merely for
this integrity check.

### P4. Operator documentation

Document:

- exact upstream commit pinning, stable library image retrieval, and image
  SHA-256 recording;
- why the external checkout is the `source_root` (the baseline definition does
  not copy upstream examples into the image);
- three-seed validation and 100-seed launch commands;
- plan-before-submit, status, logs, selective/all fetch, cancellation, and
  replay/inspection;
- queue/account/QOS policy remains explicit in the target configuration;
- there is no implicit array throttle or automatic retry; partial results are
  fetchable and failures remain visible.

## Acceptance criteria

1. Default tests, lint, formatting, and type checking pass without cluster
   access.
2. The checked example can be planned for seeds `0:2` and `0:99`.
3. The normal test suite never submits work to Shoal.
4. With explicit opt-in and prepared prerequisites, the live smoke produces
   three successful, independently seeded Feather outputs.
5. No persistent Rundra directory is required on a Slurm controller node; the
   target's configured shared workspace is used.
6. The 100-seed launch remains a documented manual action and is never started
   automatically by tests.
