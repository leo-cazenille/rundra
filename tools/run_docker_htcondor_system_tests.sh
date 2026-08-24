#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture="$root/tests/system/docker_htcondor"
temporary=$(mktemp -d "${TMPDIR:-/tmp}/rundra-htcondor-system.XXXXXX")
project="rundra-htcondor-${temporary##*.}"
project=${project,,}
compose=(docker compose --project-name "$project" --file "$fixture/compose.yaml")
started=false
cleanup() {
  local exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    "${compose[@]}" exec -T access sh -c \
      'for path in /workspace/.rundra-scheduler-logs/*.stderr; do [ -f "$path" ] || continue; printf "%s\n" "--- $path ---"; tail -n 40 "$path"; done' >&2 || true
    "${compose[@]}" ps --all >&2 || true
  fi
  if [[ $started == true ]]; then
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "$temporary"
  exit "$exit_code"
}
trap cleanup EXIT
export RUNDRA_DOCKER_HTCONDOR_STATE="$temporary/state"
mkdir -p "$RUNDRA_DOCKER_HTCONDOR_STATE"
ssh-keygen -q -t ed25519 -N '' -f "$RUNDRA_DOCKER_HTCONDOR_STATE/id_ed25519"
printf 'rundra-docker-htcondor-pool\n' >"$RUNDRA_DOCKER_HTCONDOR_STATE/pool_password"
chmod 0600 "$RUNDRA_DOCKER_HTCONDOR_STATE/pool_password"
"${compose[@]}" up --detach --build
started=true
ready=false
for _ in $(seq 1 120); do
  count=$("${compose[@]}" exec -T access condor_status -af Name 2>/dev/null | wc -l) || true
  if [[ $count -ge 2 ]]; then ready=true; break; fi
  sleep 1
done
[[ $ready == true ]] || { printf 'HTCondor pool did not expose two execute nodes.\n' >&2; exit 1; }
"${compose[@]}" exec -T access touch /workspace/test.sif
port=$("${compose[@]}" port access 22); port=${port##*:}
ssh-keyscan -p "$port" 127.0.0.1 >"$temporary/known_hosts" 2>/dev/null
cat >"$temporary/ssh_config" <<EOF
Host docker-htcondor
    HostName 127.0.0.1
    Port $port
    User rundra
    IdentityFile $RUNDRA_DOCKER_HTCONDOR_STATE/id_ed25519
    IdentitiesOnly yes
    BatchMode yes
    StrictHostKeyChecking yes
    UserKnownHostsFile $temporary/known_hosts
EOF
chmod 0600 "$temporary/ssh_config" "$RUNDRA_DOCKER_HTCONDOR_STATE/id_ed25519"
for attempt in $(seq 1 30); do
  if ssh -F "$temporary/ssh_config" docker-htcondor \
    'command -v apptainer >/dev/null 2>&1'; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "Docker HTCondor SSH/runtime probe did not become ready" >&2
    exit 1
  fi
  sleep 1
done
export RUNDRA_DOCKER_HTCONDOR_TARGETS_FILE="$temporary/targets.yaml"
cat >"$RUNDRA_DOCKER_HTCONDOR_TARGETS_FILE" <<EOF
version: 9
targets:
  docker-htcondor:
    transport: {type: ssh, host: docker-htcondor, executable: /usr/bin/ssh, config_file: $temporary/ssh_config}
    scheduler: {type: htcondor, shared_workspace: true}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /workspace
    execution:
      hard_task_limit: 1000
      confirmation_threshold: 1000
      max_active_tasks: 8
      max_concurrent_jobs: 8
      max_array_size: 1000
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 1000
      max_memory_per_worker: 1GiB
      worker_pool:
        activation_threshold: 1000
        max_workers: 8
        tasks_per_lease: 1
        infrastructure_retry_limit: 0
        requeue_limit: 0
        default_workers: 1
        default_task_slots_per_worker: 1
        max_task_slots_per_worker: 1
EOF
uv run pytest tests/system/test_docker_htcondor.py --run-docker-htcondor-system-tests
