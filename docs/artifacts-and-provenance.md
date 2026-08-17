# Artifacts, provenance, and reproducibility

Rundra's durable RunRecord combines the immutable experiment definition with
observed lifecycle state and an artifact manifest. Inspect it with:

```bash
uv run rundr inspect RUN_ID --json | python3 -m json.tool
```

The checked field-level example is
[`schemas/run-record-v1.json`](schemas/run-record-v1.json).

## What is preserved

Every RunRecord stores:

- the framework and RunRecord format versions;
- the stable Run ID and ordered Task IDs;
- the normalized experiment, exact effective configuration text, and explicit
  seed for every Task;
- the target stack, workspace root, source root, and experiment source path;
- portable resources and explicit backend-native resources;
- portable execution/retrieval states and available native scheduler states;
- scheduler Run/Task identities, array mapping, exits, nodes, scalar metadata,
  and timestamps when observed;
- the artifact manifest; and
- optional Git and container-digest fields without fabricated values.

The Run definition is created before staging and is not reinterpreted later.
Lifecycle commands replace the complete versioned record atomically as new
scheduler, retrieval, and artifact observations become available.

## Effective configuration and source snapshot

Scientific configuration is opaque to Rundra. The YAML is syntax-checked, but
its exact UTF-8 text, newline style, and source path are preserved in each Task
and copied to the staged `input/config.yaml`. Rundra substitutes that staged path
and the concrete integer seed into the experiment argv; the application owns the
configuration's scientific meaning.

Staging snapshots the current filesystem tree, including uncommitted and
untracked files, without requiring a Git commit or push. Local staging
dereferences source symlinks; remote rsync staging copies their referents. The
staged source and input trees are sealed read-only for normal execution, while
runtime, output, log, and metadata locations remain writable. Configured and
default transient exclusions mean the snapshot is not intended as a full
repository backup.

Default exclusions are `.git`, `.hg`, `.svn`, `.venv`, `venv`, `__pycache__`,
`.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `.tox`, `.nox`, `.rundra`,
`.agents`, `retrieved`, `tmp`, `downloads`, `*.py[cod]`, `*.sif`, and `*.simg`.
Experiment `sync.exclude` entries extend this list. The same defaults apply to
working-tree preparation hashing and staging, preventing excluded large files
from being uploaded or invalidating prepared build caches.

The `source_snapshot` and `effective_config` artifacts identify these staged
inputs. Their paths may be local or remote workspace paths and are not a promise
that the workspace is retained forever.

## Git provenance

When the source root is a readable Git working tree, Rundra attempts to capture
before staging:

- `git_commit` when `HEAD` resolves;
- `git_branch` when attached to a symbolic branch;
- `git_dirty`, including the presence of untracked files; and
- a tracked dirty patch in `git_diff` when it is valid UTF-8, at most 1 MiB, and
  contains none of the common credential markers.

Untracked file contents are staged but never copied into `git_diff`. If Git is
missing, the source is not a repository, `HEAD` is unborn/detached, capture times
out, output is oversized or invalid, or a credential marker is found, only the
affected optional field remains `null`. Execution continues. Marker screening
is defense in depth and cannot determine whether arbitrary source, argv, or
scientific configuration contains a secret.

Git describes the working-tree context; it is not the transfer mechanism and is
not sufficient by itself to reconstruct untracked or omitted content. The
source snapshot and exact configuration are the execution inputs actually used.

## Container provenance

The normalized experiment stores the declared image path/reference and GPU
intent, and the target stores whether execution selected native or Apptainer.
The `container_digest` field is present in RunRecord version 1 but v0.1 does not
calculate an image digest or persist an Apptainer version, so ordinary CLI Runs
record it as `null`. Rundra never invents either value. If exact image identity
is required, use an immutable site-managed image reference and preserve it under
the site's own image policy.

## Artifact manifest

Each manifest entry has `kind`, `path`, optional `task_id`, and optional
`size_bytes`:

| Kind | Meaning in v0.1 |
|---|---|
| `source_snapshot` | staged source directory shared by the Run |
| `effective_config` | exact staged configuration shared by the Run |
| `stdout` | one Task's framework-managed standard output |
| `stderr` | one Task's framework-managed standard error |
| `raw_result` | regular file matching an experiment `outputs.include` pattern |
| `scheduler_metadata` | retrieved scheduler-owned metadata file, when present |
| `provenance_metadata` | reserved version-1 category; no separate file is emitted by the current Git provider |

Run-level inputs have `task_id: null`; Task-specific logs and results carry a
stable `task_NNNNNN` ID. `size_bytes` is recorded when Rundra measures a regular
file. Directories and remote paths that have not been fetched may have
`size_bytes: null`. The manifest is a locator and classification list, not a
content-addressed store: v0.1 does not checksum arbitrary artifacts.

Synchronous `run` fetches requested results after execution. `fetch` can later
retrieve all Tasks or repeated `--task ID_OR_INDEX` selections. Repeating a
fetch to the same destination atomically replaces matching files and does not
duplicate the same kind/Task/path manifest key. Fetching to a different
destination records that different path as another locator.

Computation and retrieval are independent. A Run may be `SUCCEEDED` while
retrieval is `FAILED`; correct the transfer and retry `fetch` without changing
the scientific execution state. Failed experiments can still have stdout,
stderr, and partial raw-result artifacts.

## Raw and derived outputs

Rundra manages raw execution products selected by `outputs.include`. It does not
interpret metrics, plots, reports, or other scientific meaning. A project may
select an analysis-produced file as a raw output for transfer, but v0.1 provides
no derived-analysis model or lineage claim. Keep post-processing outputs in a
separate project location when that distinction matters.

## Reproducibility boundary

The checked local criterion is byte identity of the raw result for the same
source snapshot, exact effective config, explicit seed, Python 3.12 environment,
and runtime. Rundra does not claim byte identity across different application
dependencies, container images, hardware, drivers, runtime versions, scheduler
placement, or external services. A generated seed is concrete and persisted so
it can be replayed, but the application must actually use the `{seed}` argument
for stochastic reproducibility.

## Security and retention

Never place credentials in experiment/target/launch YAML, argv, opaque
scientific configuration, source intended for capture, or Run data. RunStore
rejects credential-bearing field names, but semantic secret detection in
arbitrary values is impossible.

Remote Run directories and scheduler logs are not deleted automatically. Use
the RunRecord to resolve one exact terminal Run, then apply site-approved
retention or cleanup to only that Run's workspace and log paths. Do not
recursively delete a configured workspace root.
