# Failed Task retry, portable exports, and compact status

## Execution and compatibility

1. Add a preview-first `retry RUN_ID --failed` command. Select terminal failed
   Task coordinates in ordinal order and load their exact retained configs.
   Build a sparse materialized plan through the normal scheduler policy and
   submission path. Require an exact confirmation count and record parent to
   child Task lineage before submission. A campaign launch can select its
   child Run explicitly. Preserve unknown-submission recovery by child Run ID.
2. Add `export RUN_ID_OR_CAMPAIGN --destination ARCHIVE`. Bundle recorded
   experiment data, configs, Task identities, provenance, and a result index.
   Include regular files already materialized locally only with
   `--include-results`, and record checksums for bundled files. Publish the
   archive atomically without replacing existing files.
3. Read appended bytes of compact SSH journals through a private per-Run
   cache. Detect replacement, truncation, and changes to the checked prefix.
   Reconcile the same current journal view with the existing event parser.
   Keep a full read path for unsupported environments and cache failures.
   Measure transport payload savings with a synthetic compact journal.
4. Extend the opt-in Docker Slurm workflow with scratch startup, container
   launch, and required-output failures. Exercise the interrupted-submit
   recovery contract in the same workflow using the existing fault injection.

Add new CLI options and versioned documents while preserving existing
invocations, RunRecord fields, Task outcome semantics, and journal formats.
Document retention prerequisites and the supported target scope. Validate
local execution, export and retry contracts, journal reconciliation, and the
existing test suite. Run the Docker matrix only against the disposable harness.
