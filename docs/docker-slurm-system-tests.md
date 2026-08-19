# Dockerized Slurm system tests

Rundra includes an opt-in two-node Slurm cluster for validating the complete
SSH, rsync, Slurm, Apptainer, worker-pool, sharded-output, and retrieval path.
It executes 1,000 deterministic logical Tasks while limiting scheduled workers
and Docker CPU/memory consumption.
The suite also covers bounded-array partial failure and cancellation.

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

The harness never uses Docker privileged mode. It generates temporary SSH and
Munge keys, uses strict SSH host verification, prints Compose diagnostics on
failure, and deletes containers, volumes, and credentials on exit. Slurm cgroup
enforcement is intentionally disabled inside Docker; Compose limits protect the
host, so this is lifecycle and logical-scale validation rather than a scheduler
performance benchmark.

The test is excluded from normal `pytest` runs. GitHub Actions runs it nightly
and also exposes a manual `docker-slurm-system` workflow.
