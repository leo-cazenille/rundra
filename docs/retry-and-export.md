# Retry failed Tasks

Preview the failed Tasks of a terminal Run first. Rundra reads the saved Task
states, the exact effective configs, and the retained source snapshot. The
preview shows the original and proposed Task IDs, seeds, and parameter sets.

```sh
rundr retry RUN_ID --failed --json
rundr retry RUN_ID --failed --confirm-tasks 2 --json
```

Use the count from the preview. Confirmation creates a new Run with the
selected coordinates in their original order. A sweep can have several Tasks
with the same seed and different parameter sets, and only the failed
coordinates are selected. Tasks that succeeded or were cancelled keep their
original outcomes. The new Run records its parent Run ID and a complete Task
mapping. Results go to a new directory under the Run store, or to a new path
supplied with `--destination`.

Supply the original `--data-dir` and, for a custom target, its current
`--targets-file`. To select a different target, use `--target` in both commands.
Rundra copies the retained source locally or over rsync when needed. A remote
absolute image path requires a matching recorded SHA-256 to move to another
host. Use `--image /target/path/image.sif` to select a verified copy at a new
location. With an unpinned absolute image on the original host the retry reuses
the saved image path. The target's current resource policy still applies.

A compact Run needs its SQLite Task state and the staged parameter manifest.
Rundra checks each referenced config against the manifest SHA-256. A missing
or changed retained input stops retry before submission. Keep the Run
workspace until you have finished retrying its Tasks.

To retry one launch from a campaign, select it explicitly:

```sh
rundr retry CAMPAIGN_ID --launch shoal --failed --target isircluster --json
```

Local retries execute and retrieve results in the foreground. Detached
targets return a new Run ID for `await` and `fetch`. If the scheduler outcome
is uncertain, use `rundr resume CHILD_RUN_ID` with the child ID reported in the
error. Each confirmed retry starts a separate submission.

## Export recorded evidence

Give `export` a new local archive path. It accepts a Run ID or a campaign ID.
A campaign archive includes its recorded child assignments and the artifacts
belonging to each child.

```sh
rundr export RUN_ID --destination run-evidence.tar.gz --json
rundr fetch RUN_ID --mode copy --extract --json
rundr export RUN_ID --destination run-with-results.tar.gz --include-results --json
```

The archive contains `manifest.json`, a normalized experiment document,
deduplicated effective configs, Task identities, provenance, and a result
index. Compact Runs retain the arithmetic seed range and parameter sets.
Retry exports include the original-to-new Task mapping when its sidecar is
available. The manifest records the size and SHA-256 of every payload member.
The CLI reports the SHA-256 of the complete archive.

`--include-results` copies recorded regular files after retrieval into the
saved destination. For a reference manifest or result shards, fetch with
`--mode copy --extract` first. The export records source and image identities.
Keep the corresponding source checkout or snapshot and SIF separately if
another machine needs to execute the experiment. Review configs and selected
outputs before sharing an archive. Rundra opens the archive with owner-only
permissions and publishes it without overwriting an existing path.

## Polling large Runs

Compact SSH status refreshes cache journal payloads in the Run's Task database.
The target checks the SHA-256 of the saved prefix before sending an appended
suffix. A replaced or truncated journal is read in full. A published per-attempt
aggregate takes precedence over that attempt's lane journals. The same event
parser checks conflicts. Clients or targets without the delta path use a full
read. The cache consumes local SQLite space and the target still reads the
prefix to verify it.

The synthetic [journal test](../tests/unit/orchestration/test_journal_delta.py)
prints transferred bytes and elapsed time for a 1,000-event prefix plus one
appended event. The measurement runs against a local transport with the same
shell protocol. It measures payload reduction and local command overhead,
with cluster-dependent latency left to deployment measurements.
One validation run transferred 43,064 bytes on a full read and 89 bytes on
the append read. The corresponding local command times were approximately
5 ms and 29 ms.

The Docker Slurm workflow covers scratch staging failure, container launch
failure, and missing required outputs. It also runs the interrupted-submission
recovery fixture. To run just that matrix in the disposable cluster:

```sh
RUNDRA_DOCKER_SLURM_SUITE=failures tools/run_docker_slurm_system_tests.sh
```

The required-output case runs Apptainer inside a worker. The host needs
nested user namespaces, which the Docker Slurm GitHub Actions job configures
on its runner.

The same workflow checks compact retry and export across SSH, including a
rejected retry when a retained config has changed. It also starts a fresh CLI
process for each poll, checks that appended events travel as suffixes, and
compares the final state against an independent full read. To run just these
cases:

```sh
RUNDRA_DOCKER_SLURM_SUITE=compact tools/run_docker_slurm_system_tests.sh
```

On a development host that restricts nested user namespaces, set
`RUNDRA_DOCKER_COMPACT_NATIVE=1` for these tests. This uses a native shell
payload while still exercising Docker Slurm, SSH, and rsync. CI uses Apptainer.
