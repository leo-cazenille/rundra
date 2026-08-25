# Dockerized Slurm system tests

Rundra includes an opt-in two-node Slurm cluster for validating the complete
SSH, rsync, Slurm, Apptainer, worker-pool, sharded-output, and retrieval path.
It executes 1,000 deterministic logical Tasks while limiting scheduled workers
and Docker CPU/memory consumption.
The suite also covers bounded-array partial failure and cancellation. Compute
nodes expose private `/scratch` and `/gpu-scratch` tmpfs mounts through the
same scheduler-variable contract expected from a scratch-mandating site.

Run it on an amd64 Linux host with Docker Engine, Docker Compose v2,
`/dev/fuse`, and the kernel support required by unprivileged Apptainer:

```bash
tools/run_docker_slurm_system_tests.sh
```

By default, the harness builds `rundra-slurm-system:local` once and reuses
Docker's layer cache on later runs. All four Compose services use that single
image rather than exporting separate copies.

To skip the build, select a compatible prebuilt image. The harness uses an
existing local image or pulls it when absent. Pin registry images by digest for
reproducibility:

```bash
RUNDRA_DOCKER_SLURM_IMAGE='registry.example/rundra-slurm-system@sha256:DIGEST' \
    tools/run_docker_slurm_system_tests.sh
```

The prebuilt image must implement the `init`, `controller`, and `compute`
entrypoint modes defined by the checked-in Docker harness.

On failure, the harness captures Compose status and logs, Slurm queue/job/node
state, and the test workspace before deleting the cluster. By default it creates
a temporary directory and prints its path. Select a stable destination with:

```bash
RUNDRA_DOCKER_SLURM_DIAGNOSTICS=/path/to/diagnostics \
    tools/run_docker_slurm_system_tests.sh
```

The diagnostic bundle contains only Docker/Slurm state and the synthetic test
runs. The temporary SSH and Munge key state is not copied.

The default suite enables target schema v10 allocation scratch. Its bounded
scheduler doctor probe verifies scratch write/copy-back/cleanup, and all scale,
failure, and cancellation Runs stage source, configuration, and the SIF into
compute-local storage. The 1,000-Task worker-pool test requires copied-back
results to report the synthetic CPU scratch variable. A separate cold fixture
builds a definition-derived SIF and an application inside a scheduled scratch
preparation job, inspects the recorded preparation and storage provenance, then
executes and retrieves one result.

The harness never uses Docker privileged mode. It generates temporary SSH and
Munge keys, uses strict SSH host verification, prints Compose diagnostics on
failure, and deletes containers, volumes, and credentials on exit. Slurm cgroup
enforcement is intentionally disabled inside Docker; Compose limits protect the
host, so this is lifecycle and logical-scale validation rather than a scheduler
performance benchmark.

The host harness creates the ephemeral SSH client key before Compose starts.
Containers receive only the resulting bind-mounted test state. This preserves
runner ownership and mode control on both rootful CI and rootless local Docker.

The test is excluded from normal `pytest` runs. GitHub Actions runs it nightly
and also exposes a manual `docker-slurm-system` workflow. The workflow caches
the Docker build layers, loads one shared image, and uploads the diagnostic
bundle for failed runs with a 14-day retention period.

## Privileged cgroup-v2 memory test

The default harness deliberately disables Slurm cgroups so it can run with
rootless, non-privileged Docker. A separate test validates actual Slurm memory
enforcement:

```bash
tools/run_docker_slurm_cgroup_tests.sh
```

This variant requires Docker to provide a privileged private cgroup-v2
namespace with a writable `memory` controller. It enables Slurm
`task/cgroup`, launches a deliberately over-limit experiment through
`rundr run --json`, and requires both a durable failed Rundra Task and Slurm's
`OUT_OF_MEMORY` state for the recorded scheduler ID. It runs a successful
control job first so startup or scheduling failures cannot be mistaken for
enforcement.

The variant uses Slurm's `IgnoreSystemd=yes` development/testing mode. It is
not a production Slurm deployment example; production cgroup-v2 deployments
should run `slurmd` from systemd with delegation. The privileged variant is
manual-only and is intentionally excluded from default and scheduled CI.

Maintainers can run the same check on GitHub's rootful runner from **Actions >
Docker Slurm cgroup system > Run workflow**. This workflow has only a
`workflow_dispatch` trigger, checks for rootful cgroup v2 before cluster
startup, uses a dedicated Docker layer-cache scope, and uploads a 14-day
diagnostic artifact when the harness fails.

Set `RUNDRA_DOCKER_SLURM_CGROUP_DIAGNOSTICS` to preserve diagnostics at a
specific location when the test fails. The harness reuses
`RUNDRA_DOCKER_SLURM_IMAGE` when provided and layers the private-cgroup
bootstrap into the local `rundra-slurm-cgroup-system:local` image.
