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

"${compose[@]}" exec -T controller sh -c \
  'cat > /workspace/memory-pressure.sh && chmod 0755 /workspace/memory-pressure.sh' \
  <"$fixture/memory-pressure.sh"

"${compose[@]}" exec -T controller \
  sbatch --wait --parsable --nodes=1 --ntasks=1 --mem=128M --time=00:01:00 \
  --wrap=/bin/true >/dev/null

job_id=$("${compose[@]}" exec -T controller \
  sbatch --parsable --nodes=1 --ntasks=1 --mem=64M --time=00:02:00 \
  --output=/workspace/memory-pressure.out \
  --error=/workspace/memory-pressure.err \
  /workspace/memory-pressure.sh)
job_id=${job_id%%;*}
job_id=${job_id//$'\r'/}

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
if "${compose[@]}" exec -T controller test -e /workspace/memory-limit-not-enforced; then
  printf 'Memory pressure workload completed despite its 64 MiB allocation.\n' >&2
  exit 1
fi

printf 'Docker Slurm cgroup test passed: control=SUCCEEDED memory_job=%s state=%s\n' \
  "$job_id" "$state"
