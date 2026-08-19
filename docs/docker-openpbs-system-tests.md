# Dockerized OpenPBS system tests

Rundra includes an opt-in two-MOM OpenPBS cluster that exercises the complete
SSH, rsync, OpenPBS, Apptainer, waiting, reconciliation, and retrieval path.
The suite covers a successful mapped array, partial array failure with retained
outputs, cancellation, and evidence that scientific commands ran on MOM nodes.

Run it on an amd64 Linux Docker host with Compose v2 and `/dev/fuse`:

```bash
tools/run_docker_pbs_system_tests.sh
```

The image builds the pinned official OpenPBS v23.06.06 source and verifies its
SHA-256. It layers on `RUNDRA_DOCKER_SLURM_IMAGE` when supplied, reusing the
test-only Apptainer runtime rather than building it twice. The OpenPBS image is
cached locally as `rundra-pbs-system:local`.

The test is excluded from ordinary pytest and push CI. GitHub Actions runs it
weekly and exposes the `docker-pbs-system` workflow for manual dispatch. The
workflow caches the shared Slurm/Apptainer base-image layers and uploads a
14-day diagnostic artifact after failures.

The harness creates temporary SSH credentials with strict host verification
and removes containers, volumes, and credentials after success. On failure it
preserves Docker logs, PBS server, node, and job state. Select a stable
diagnostic directory with:

The host harness creates the ephemeral SSH client key before Compose starts so
rootful containers never leave a root-owned key in the runner's bind-mounted
temporary directory.

```bash
RUNDRA_DOCKER_PBS_DIAGNOSTICS=/path/to/diagnostics \
    tools/run_docker_pbs_system_tests.sh
```

This is a functional compatibility environment, not production OpenPBS
deployment guidance or a scheduler performance benchmark.
