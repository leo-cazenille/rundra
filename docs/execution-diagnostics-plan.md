# Execution diagnostics and output contracts

1. Record Slurm scratch startup failures in bounded, versioned worker diagnostics
   separate from Task journals and RunRecords. Preserve existing Task reconciliation.
2. Add opt-in diagnostic reports to `inspect`, `status`, and `await`, using
   the same redacted, bounded data for agents and humans. Old Runs can lack
   worker diagnostics. Unavailable diagnostic storage is reported separately.
3. Extend the explicitly requested scheduler probe to run an existing target
   image in a bounded Slurm allocation. Use the experiment's selected partition
   when supplied and expose the tested node and failed stage.
4. Add experiment v2 with optional `outputs.required`. Verify exact relative
   regular-file paths in the Task's output directory, with `{seed}` expansion.
   Check after a successful command and before result publication. Preserve
   application failures and validate the declaration before scheduling.
5. Document the opt-in formats and verify legacy fixtures, new failure paths,
   and single-Task and worker-pool local execution. Use simulated allocation
   and transport tests for Slurm-specific behavior.

Existing YAML, command defaults, public JSON shapes, and saved Runs remain
supported. The diagnostics report contains framework-generated reason codes
and messages rather than excerpts from arbitrary process logs.
