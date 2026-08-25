#!/bin/sh
set -eu

run_memory_pressure() {
    cgroup_path=$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)
    events="/sys/fs/cgroup${cgroup_path}/memory.events"
    test -r "$events"
    oom_before=$(awk '$1 == "oom_kill" { print $2 }' "$events")

    pids=
    index=0
    while [ "$index" -lt 96 ]; do
        awk 'BEGIN { value = sprintf("%2097152s", "x"); system("sleep 10") }' &
        pids="$pids $!"
        index=$((index + 1))
    done
    for pid in $pids; do
        wait "$pid" || true
    done

    oom_after=$(awk '$1 == "oom_kill" { print $2 }' "$events")
    if [ "$oom_after" -gt "$oom_before" ]; then
        exit 97
    fi
}

for argument in "$@"; do
    if [ -f "$argument" ] \
        && grep -q '^memory_pressure:[[:space:]]*true[[:space:]]*$' "$argument"; then
        run_memory_pressure
    fi
done

config=$2
seed=$4
failure_seed=$(awk '$1 == "failure_seed:" {print $2}' "$config")
sleep_seconds=$(awk '$1 == "sleep_seconds:" {print $2}' "$config")
test "$sleep_seconds" = 0 || sleep "$sleep_seconds"
mkdir -p /workspace/output/results
printf '{"host":"%s","seed":%s,"scratch":"%s"}\n' \
    "$(hostname)" "$seed" "${SLURM_TMPDIR:-}" \
    > /workspace/output/results/result.json
test "$seed" -ne "$failure_seed" || exit 23
