#!/bin/sh
set -eu

# The Slurm allocation requests 64 MiB. Retaining 512 MiB must trigger the
# task/cgroup memory controller. stress-ng may recover after its worker is
# killed, so independently compare the cgroup's OOM kill counter.
cgroup_path=$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)
events="/sys/fs/cgroup${cgroup_path}/memory.events"
oom_before=$(awk '$1 == "oom_kill" { print $2 }' "$events")
stress-ng --vm 1 --vm-bytes 512M --vm-keep --timeout 20s || true
oom_after=$(awk '$1 == "oom_kill" { print $2 }' "$events")
if [ "$oom_after" -gt "$oom_before" ]; then
    exit 97
fi
touch /workspace/memory-limit-not-enforced
