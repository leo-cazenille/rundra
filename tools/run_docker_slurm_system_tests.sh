#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/tests/system/docker_slurm/compose.yaml"
state=$(mktemp -d "${TMPDIR:-/tmp}/rundra-docker-slurm.XXXXXX")
export RUNDRA_DOCKER_STATE=$state
ssh-keygen -q -t ed25519 -N '' -f "$state/id_ed25519"

capture_diagnostics() {
    if [ -n "${RUNDRA_DOCKER_SLURM_DIAGNOSTICS:-}" ]; then
        destination=$RUNDRA_DOCKER_SLURM_DIAGNOSTICS
        mkdir -p "$destination"
    else
        destination=$(mktemp -d \
            "${TMPDIR:-/tmp}/rundra-docker-slurm-failure.XXXXXX")
    fi

    {
        date -u '+captured_at=%Y-%m-%dT%H:%M:%SZ'
        printf 'image=%s\n' "$RUNDRA_DOCKER_SLURM_IMAGE"
        docker version
        docker compose version
    } > "$destination/environment.txt" 2>&1 || true
    docker compose -f "$compose" ps --all \
        > "$destination/compose-ps.txt" 2>&1 || true
    docker compose -f "$compose" logs --no-color --timestamps \
        > "$destination/compose.log" 2>&1 || true
    docker compose -f "$compose" exec -T controller squeue --all \
        > "$destination/slurm-queue.txt" 2>&1 || true
    docker compose -f "$compose" exec -T controller scontrol show job \
        > "$destination/slurm-jobs.txt" 2>&1 || true
    docker compose -f "$compose" exec -T controller scontrol show nodes \
        > "$destination/slurm-nodes.txt" 2>&1 || true
    mkdir -p "$destination/workspace-runs"
    {
        docker compose -f "$compose" exec -T controller \
            tar -C /workspace -cf - runs .rundra-scheduler-logs \
            | tar -C "$destination/workspace-runs" -xf -
    } > "$destination/workspace-copy.log" 2>&1 || true

    printf 'Docker Slurm diagnostics: %s\n' "$destination" >&2
    tail -n 200 "$destination/compose.log" >&2 || true
}

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        capture_diagnostics || true
    fi
    docker compose -f "$compose" down --volumes --remove-orphans || true
    chmod -R u+w "$state" 2>/dev/null || true
    rm -rf "$state"
    exit "$status"
}
trap cleanup EXIT INT TERM

if [ -n "${RUNDRA_DOCKER_SLURM_IMAGE:-}" ]; then
    if ! docker image inspect "$RUNDRA_DOCKER_SLURM_IMAGE" >/dev/null 2>&1; then
        docker pull "$RUNDRA_DOCKER_SLURM_IMAGE"
    fi
else
    RUNDRA_DOCKER_SLURM_IMAGE=rundra-slurm-system:local
    export RUNDRA_DOCKER_SLURM_IMAGE
    docker build --tag "$RUNDRA_DOCKER_SLURM_IMAGE" \
        "$root/tests/system/docker_slurm"
fi

docker compose -f "$compose" up --detach --no-build

attempt=0
until ssh-keyscan -p 2222 127.0.0.1 > "$state/known_hosts" 2>/dev/null; do
    attempt=$((attempt + 1))
    test "$attempt" -lt 120 || { echo "controller SSH did not become ready" >&2; exit 1; }
    sleep 1
done

mkdir -p "$state/home/.ssh"
cat > "$state/home/.ssh/config" <<EOF
Host rundra-docker-slurm
    HostName 127.0.0.1
    Port 2222
    User rundra
    IdentityFile $state/id_ed25519
    UserKnownHostsFile $state/known_hosts
    StrictHostKeyChecking yes
EOF
chmod 600 "$state/home/.ssh/config" "$state/id_ed25519"

cat > "$state/targets.yaml" <<EOF
version: 11
targets:
  docker-slurm: &docker-slurm
    transport:
      type: ssh
      host: rundra-docker-slurm
      config_file: $state/home/.ssh/config
    scheduler:
      type: slurm
      partition_routes:
        - {name: cpu_short, partition: cpu-short, resource_class: cpu, max_walltime: "01:00:00"}
        - {name: cpu_long, partition: cpu-long, resource_class: cpu, max_walltime: "12:00:00"}
        - {name: gpu_short, partition: gpu-short, resource_class: gpu, max_walltime: "01:00:00"}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /workspace
    preparation:
      definition_build:
        allowed_locations: [target]
        mode: unprivileged
        max_resources:
          cpus_per_task: 2
          memory: 1GiB
          walltime: "00:10:00"
    execution_storage:
      type: slurm_scratch
      cpu_environment: SLURM_TMPDIR
      gpu_environment: SLURM_GPUTMPDIR
      stage_image: true
      copy_back: task
    execution:
      hard_task_limit: 2000
      confirmation_threshold: 500
      max_active_tasks: 4
      max_concurrent_jobs: 8
      max_array_size: 4
      output_shard_tasks: 100
      automatic_retrieval_threshold: 2000
      max_memory_per_worker: 256MiB
      worker_pool:
        activation_threshold: 100
        default_workers: 4
        max_workers: 4
        default_task_slots_per_worker: 1
        max_task_slots_per_worker: 1
        tasks_per_lease: 100
        infrastructure_retry_limit: 1
        requeue_limit: 2
  docker-campaign-a:
    <<: *docker-slurm
  docker-campaign-b:
    <<: *docker-slurm
EOF

case "${RUNDRA_DOCKER_SLURM_SUITE:-scale}" in
    scale)
        test_source=tests/system/test_docker_slurm_scale.py
        ;;
    campaign)
        test_source=tests/system/test_docker_campaign.py
        ;;
    *)
        echo "RUNDRA_DOCKER_SLURM_SUITE must be scale or campaign" >&2
        exit 64
        ;;
esac

HOME="$state/home" \
RUNDRA_DOCKER_SLURM_TARGETS_FILE="$state/targets.yaml" \
uv run pytest "$test_source" \
    --run-docker-slurm-system-tests
