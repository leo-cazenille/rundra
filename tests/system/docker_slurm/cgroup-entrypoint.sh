#!/bin/sh
set -eu

if [ "${1:-}" = compute ]; then
    # A private Docker cgroup namespace starts with PID 1 in its root cgroup.
    # Move the daemon into a sibling scope before enabling controllers so
    # Slurm can own system.slice without modifying host cgroups.
    mkdir -p /sys/fs/cgroup/rundra-init.scope
    echo $$ > /sys/fs/cgroup/rundra-init.scope/cgroup.procs
    echo '+cpuset +cpu +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
    mkdir -p /sys/fs/cgroup/system.slice
fi

exec /opt/rundra-cluster/entrypoint.sh "$@"
