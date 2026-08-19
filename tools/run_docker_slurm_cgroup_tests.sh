#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture="$root/tests/system/docker_slurm"
compose_file="$fixture/compose-cgroup.yaml"
temporary=$(mktemp -d "${TMPDIR:-/tmp}/rundra-slurm-cgroup.XXXXXX")
diagnostics=${RUNDRA_DOCKER_SLURM_CGROUP_DIAGNOSTICS:-$temporary/diagnostics}
compose=(docker compose --file "$compose_file")
started=false

if [[ -z ${DOCKER_HOST:-} && -S /run/user/"$(id -u)"/docker.sock ]]; then
  export DOCKER_HOST="unix:///run/user/$(id -u)/docker.sock"
fi

capture_diagnostics() {
  local exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    mkdir -p "$diagnostics"
    {
      docker info 2>&1 || true
      printf '\n--- compose ps ---\n'
      "${compose[@]}" ps --all 2>&1 || true
      printf '\n--- compose logs ---\n'
      "${compose[@]}" logs --no-color 2>&1 || true
      printf '\n--- nodes ---\n'
      "${compose[@]}" exec -T controller scontrol show nodes 2>&1 || true
      printf '\n--- jobs ---\n'
      "${compose[@]}" exec -T controller squeue --all 2>&1 || true
    } >"$diagnostics/docker-slurm-cgroup.log"
    printf 'Docker Slurm cgroup diagnostics: %s\n' "$diagnostics" >&2
  fi
  if [[ $started == true ]]; then
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  if [[ $exit_code -eq 0 || $diagnostics != "$temporary/diagnostics" ]]; then
    rm -rf "$temporary"
  fi
  exit "$exit_code"
}
trap capture_diagnostics EXIT

base_image=${RUNDRA_DOCKER_SLURM_IMAGE:-rundra-slurm-system:local}
if ! docker image inspect "$base_image" >/dev/null 2>&1; then
  if [[ -n ${RUNDRA_DOCKER_SLURM_IMAGE:-} ]]; then
    docker pull "$base_image"
  else
    docker build --tag "$base_image" --file "$fixture/Dockerfile" "$fixture"
  fi
fi

export RUNDRA_DOCKER_SLURM_CGROUP_IMAGE=${RUNDRA_DOCKER_SLURM_CGROUP_IMAGE:-rundra-slurm-cgroup-system:local}
docker build \
  --build-arg "BASE_IMAGE=$base_image" \
  --tag "$RUNDRA_DOCKER_SLURM_CGROUP_IMAGE" \
  --file "$fixture/Dockerfile.cgroup" \
  "$fixture"

capabilities=$(docker run --rm --privileged \
  --entrypoint /bin/sh "$RUNDRA_DOCKER_SLURM_CGROUP_IMAGE" -c '
    test "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs || exit 10
    grep -qw memory /sys/fs/cgroup/cgroup.controllers || exit 11
    mkdir /sys/fs/cgroup/rundra-cgroup-probe || exit 12
    rmdir /sys/fs/cgroup/rundra-cgroup-probe
    printf supported
  ') || {
    printf '%s\n' \
      'Docker must provide a privileged private cgroup-v2 namespace with a writable memory controller.' >&2
    exit 1
  }
[[ $capabilities == supported ]]

export RUNDRA_DOCKER_SLURM_CGROUP_SLURM_CONF="$temporary/slurm.conf"
export RUNDRA_DOCKER_STATE="$temporary/state"
mkdir -p "$RUNDRA_DOCKER_STATE"
sed \
  -e 's|^ProctrackType=.*|ProctrackType=proctrack/cgroup|' \
  -e 's|^TaskPlugin=.*|TaskPlugin=task/cgroup|' \
  -e 's|^JobAcctGatherType=.*|JobAcctGatherType=jobacct_gather/cgroup|' \
  -e 's|NodeName=compute\[1-2\]|NodeName=compute1|' \
  -e 's|^MinJobAge=.*|MinJobAge=300|' \
  "$fixture/slurm.conf" >"$RUNDRA_DOCKER_SLURM_CGROUP_SLURM_CONF"
if ! grep -q '^MinJobAge=' "$RUNDRA_DOCKER_SLURM_CGROUP_SLURM_CONF"; then
  printf 'MinJobAge=300\n' >>"$RUNDRA_DOCKER_SLURM_CGROUP_SLURM_CONF"
fi

"${compose[@]}" up --detach --no-build
started=true

ready=false
for _ in $(seq 1 60); do
  if "${compose[@]}" exec -T controller scontrol ping 2>/dev/null | grep -q 'UP' \
    && "${compose[@]}" exec -T controller scontrol show node compute1 2>/dev/null \
      | grep -Eq 'State=(IDLE|MIXED|ALLOCATED)'; then
    ready=true
    break
  fi
  sleep 1
done
if [[ $ready != true ]]; then
  printf 'Docker Slurm cgroup cluster did not become ready.\n' >&2
  exit 1
fi

"${compose[@]}" exec -T controller \
  sbatch --wait --parsable --nodes=1 --ntasks=1 --mem=128M --time=00:01:00 \
  --wrap=/bin/true >/dev/null

port=$("${compose[@]}" port controller 22)
port=${port##*:}
ssh_ready=false
for _ in $(seq 1 30); do
  if ssh-keyscan -p "$port" 127.0.0.1 >"$temporary/known_hosts" 2>/dev/null; then
    ssh_ready=true
    break
  fi
  sleep 1
done
if [[ $ssh_ready != true ]]; then
  printf 'Docker Slurm controller SSH service did not become ready.\n' >&2
  exit 1
fi

cat >"$temporary/ssh_config" <<EOF
Host docker-slurm-cgroup
    HostName 127.0.0.1
    Port $port
    User rundra
    IdentityFile $RUNDRA_DOCKER_STATE/id_ed25519
    IdentitiesOnly yes
    BatchMode yes
    StrictHostKeyChecking yes
    UserKnownHostsFile $temporary/known_hosts
EOF
chmod 0600 "$temporary/ssh_config" "$RUNDRA_DOCKER_STATE/id_ed25519"

cat >"$temporary/targets.yaml" <<EOF
version: 1
targets:
  docker-slurm-cgroup:
    transport:
      type: ssh
      host: docker-slurm-cgroup
      executable: /usr/bin/ssh
      config_file: $temporary/ssh_config
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /workspace
EOF

cp "$fixture/config.yaml" "$temporary/memory-config.yaml"
printf '\nmemory_pressure: true\n' >>"$temporary/memory-config.yaml"

run_json="$temporary/run.json"
set +e
uv run rundr run "$fixture/experiment.yaml" \
  --config "$temporary/memory-config.yaml" \
  --seed 0 \
  --target docker-slurm-cgroup \
  --targets-file "$temporary/targets.yaml" \
  --source-root "$fixture" \
  --destination "$temporary/retrieved" \
  --data-dir "$temporary/records" \
  --json >"$run_json"
run_exit=$?
set -e
if [[ $run_exit -ne 2 ]]; then
  printf 'Expected rundr run to exit 2 for an OOM Task, observed %s.\n' "$run_exit" >&2
  cat "$run_json" >&2
  exit 1
fi

read -r run_id job_id run_state task_exit_code < <(python3 - "$run_json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    payload = json.load(source)
run = payload["run"]
task_codes = run.get("task_exit_codes", {})
print(
    run["run_id"],
    run["scheduler_job_ids"][0],
    run["state"],
    next(iter(task_codes.values()), "missing"),
)
PY
)
if [[ $run_state != FAILED || $task_exit_code == missing ]]; then
  printf 'Unexpected Rundra OOM Run record: run=%s state=%s task_exit=%s.\n' \
    "$run_id" "$run_state" "$task_exit_code" >&2
  exit 1
fi

status_json="$temporary/status.json"
uv run rundr status "$run_id" \
  --data-dir "$temporary/records" --json >"$status_json"
read -r task_state native_state persisted_exit < <(python3 - "$status_json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    detail = json.load(source)["status"]["task_details"][0]
print(detail["state"], detail["native_state"], detail["exit_code"])
PY
)
if [[ $task_state != FAILED || $native_state != OUT_OF_MEMORY ]]; then
  printf 'Unexpected Rundra OOM Task record: state=%s native_state=%s exit=%s.\n' \
    "$task_state" "$native_state" "$persisted_exit" >&2
  exit 1
fi

state=
for _ in $(seq 1 90); do
  observation=$("${compose[@]}" exec -T controller \
    scontrol show job --oneliner "$job_id" 2>/dev/null) || true
  state=$(printf '%s\n' "$observation" \
    | sed -n 's/.*JobState=\([^ ]*\).*/\1/p')
  case "$state" in
    OUT_OF_MEMORY*|FAILED*|CANCELLED*|TIMEOUT*) break ;;
  esac
  sleep 1
done

if [[ $state != OUT_OF_MEMORY* ]]; then
  printf 'Expected Slurm job %s to be OUT_OF_MEMORY, observed %s.\n' "$job_id" "${state:-UNKNOWN}" >&2
  exit 1
fi
printf 'Docker Slurm cgroup test passed: control=SUCCEEDED run=%s task=%s native=%s task_exit=%s memory_job=%s state=%s\n' \
  "$run_id" "$task_state" "$native_state" "$persisted_exit" "$job_id" "$state"
