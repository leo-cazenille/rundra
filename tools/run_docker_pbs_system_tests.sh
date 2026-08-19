#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture="$root/tests/system/docker_pbs"
compose=(docker compose --file "$fixture/compose.yaml")
temporary=$(mktemp -d "${TMPDIR:-/tmp}/rundra-pbs-system.XXXXXX")
diagnostics=${RUNDRA_DOCKER_PBS_DIAGNOSTICS:-$temporary/diagnostics}
started=false

if [[ -z ${DOCKER_HOST:-} && -S /run/user/"$(id -u)"/docker.sock ]]; then
  export DOCKER_HOST="unix:///run/user/$(id -u)/docker.sock"
fi

cleanup() {
  local exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    mkdir -p "$diagnostics"
    {
      docker info 2>&1 || true
      printf '\n--- compose ps ---\n'
      "${compose[@]}" ps --all 2>&1 || true
      printf '\n--- compose logs ---\n'
      "${compose[@]}" logs --no-color 2>&1 || true
      printf '\n--- PBS server ---\n'
      "${compose[@]}" exec -T controller qstat -Bf 2>&1 || true
      printf '\n--- PBS nodes ---\n'
      "${compose[@]}" exec -T controller pbsnodes -av 2>&1 || true
      printf '\n--- PBS jobs ---\n'
      "${compose[@]}" exec -T controller qstat -x -f -F json 2>&1 || true
      printf '\n--- PBS server logs ---\n'
      "${compose[@]}" exec -T controller sh -c \
        'cat /var/spool/pbs/server_logs/* 2>/dev/null' 2>&1 || true
      printf '\n--- compute1 MOM logs ---\n'
      "${compose[@]}" exec -T compute1 sh -c \
        'cat /var/spool/pbs/mom_logs/* 2>/dev/null' 2>&1 || true
      printf '\n--- compute2 MOM logs ---\n'
      "${compose[@]}" exec -T compute2 sh -c \
        'cat /var/spool/pbs/mom_logs/* 2>/dev/null' 2>&1 || true
      printf '\n--- Rundra scheduler logs ---\n'
      "${compose[@]}" exec -T controller sh -c \
        'for file in /workspace/.rundra-scheduler-logs/*; do printf "\n--- %s ---\n" "$file"; cat "$file"; done' \
        2>&1 || true
    } >"$diagnostics/docker-pbs.log"
    printf 'Docker OpenPBS diagnostics: %s\n' "$diagnostics" >&2
  fi
  if [[ $started == true ]]; then
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  if [[ $exit_code -eq 0 || $diagnostics != "$temporary/diagnostics" ]]; then
    rm -rf "$temporary"
  fi
  exit "$exit_code"
}
trap cleanup EXIT

base_image=${RUNDRA_DOCKER_SLURM_IMAGE:-rundra-slurm-system:local}
if ! docker image inspect "$base_image" >/dev/null 2>&1; then
  if [[ -n ${RUNDRA_DOCKER_SLURM_IMAGE:-} ]]; then
    docker pull "$base_image"
  else
    docker build --tag "$base_image" \
      --file "$root/tests/system/docker_slurm/Dockerfile" \
      "$root/tests/system/docker_slurm"
  fi
fi

export RUNDRA_DOCKER_PBS_IMAGE=${RUNDRA_DOCKER_PBS_IMAGE:-rundra-pbs-system:local}
docker build --build-arg "BASE_IMAGE=$base_image" \
  --tag "$RUNDRA_DOCKER_PBS_IMAGE" --file "$fixture/Dockerfile" "$fixture"

export RUNDRA_DOCKER_PBS_STATE="$temporary/state"
mkdir -p "$RUNDRA_DOCKER_PBS_STATE"
ssh-keygen -q -t ed25519 -N '' -f "$RUNDRA_DOCKER_PBS_STATE/id_ed25519"
"${compose[@]}" up --detach --no-build
started=true

ready=false
for _ in $(seq 1 120); do
  free_nodes=$("${compose[@]}" exec -T controller pbsnodes -aSj 2>/dev/null \
    | awk '$2 == "free" {count++} END {print count+0}') || true
  if [[ $free_nodes -ge 2 ]]; then
    ready=true
    break
  fi
  sleep 1
done
if [[ $ready != true ]]; then
  printf 'Docker OpenPBS cluster did not expose two free MOM nodes.\n' >&2
  exit 1
fi

port=$("${compose[@]}" port controller 22)
port=${port##*:}
for _ in $(seq 1 30); do
  if ssh-keyscan -p "$port" 127.0.0.1 >"$temporary/known_hosts" 2>/dev/null; then
    break
  fi
  sleep 1
done
cat >"$temporary/ssh_config" <<EOF
Host docker-pbs
    HostName 127.0.0.1
    Port $port
    User rundra
    IdentityFile $RUNDRA_DOCKER_PBS_STATE/id_ed25519
    IdentitiesOnly yes
    BatchMode yes
    StrictHostKeyChecking yes
    UserKnownHostsFile $temporary/known_hosts
EOF
chmod 0600 "$temporary/ssh_config" "$RUNDRA_DOCKER_PBS_STATE/id_ed25519"

export RUNDRA_DOCKER_PBS_TARGETS_FILE="$temporary/targets.yaml"
cat >"$RUNDRA_DOCKER_PBS_TARGETS_FILE" <<EOF
version: 1
targets:
  docker-pbs:
    transport:
      type: ssh
      host: docker-pbs
      executable: /usr/bin/ssh
      config_file: $temporary/ssh_config
    scheduler: {type: pbs}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /workspace
EOF

uv run pytest tests/system/test_docker_pbs.py --run-docker-pbs-system-tests
