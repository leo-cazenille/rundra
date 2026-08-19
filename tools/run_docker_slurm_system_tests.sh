#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/tests/system/docker_slurm/compose.yaml"
state=$(mktemp -d "${TMPDIR:-/tmp}/rundra-docker-slurm.XXXXXX")
export RUNDRA_DOCKER_STATE=$state

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

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        docker compose -f "$compose" ps || true
        docker compose -f "$compose" logs --no-color || true
    fi
    docker compose -f "$compose" down --volumes --remove-orphans || true
    rm -rf "$state"
    exit "$status"
}
trap cleanup EXIT INT TERM

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
version: 4
targets:
  docker-slurm:
    transport:
      type: ssh
      host: rundra-docker-slurm
      config_file: $state/home/.ssh/config
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /workspace
    execution:
      hard_task_limit: 2000
      confirmation_threshold: 500
      max_active_tasks: 4
      max_concurrent_jobs: 8
      max_array_size: 4
      output_shard_tasks: 100
      automatic_retrieval_threshold: 2000
      worker_pool:
        activation_threshold: 100
        max_workers: 4
        task_slots_per_worker: 1
        tasks_per_lease: 100
        infrastructure_retry_limit: 1
        requeue_limit: 2
EOF

HOME="$state/home" \
RUNDRA_DOCKER_SLURM_TARGETS_FILE="$state/targets.yaml" \
uv run pytest tests/system/test_docker_slurm_scale.py \
    --run-docker-slurm-system-tests
