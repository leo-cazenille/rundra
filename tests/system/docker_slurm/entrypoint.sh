#!/bin/sh
set -eu

# The synthetic scheduler exports allocation-local storage locations. The
# compute services provide these paths as private tmpfs mounts.
export SLURM_TMPDIR=/scratch
export SLURM_GPUTMPDIR=/gpu-scratch

wait_for_bootstrap() {
    while [ ! -f /cluster/ready ]; do sleep 1; done
}

start_munge() {
    install -o munge -g munge -m 0400 /cluster/munge.key /etc/munge/munge.key
    install -d -o munge -g munge /run/munge /var/log/munge
    munged --force
}

case "${1:-}" in
    init)
        umask 077
        head -c 1024 /dev/urandom > /cluster/munge.key
        if [ ! -f /state/id_ed25519 ]; then
            ssh-keygen -q -t ed25519 -N '' -f /state/id_ed25519
        fi
        cp /state/id_ed25519.pub /cluster/authorized_keys
        install -d -o rundra -g rundra -m 0755 /workspace
        apptainer pull --disable-cache /cluster/rundra-test.sif docker://alpine:3.20
        sha256sum /cluster/rundra-test.sif > /cluster/image.sha256
        chmod 0444 /cluster/rundra-test.sif /cluster/image.sha256
        touch /cluster/ready
        ;;
    controller)
        wait_for_bootstrap
        start_munge
        install -d -o rundra -g rundra -m 0700 /home/rundra/.ssh
        install -o rundra -g rundra -m 0600 \
            /cluster/authorized_keys /home/rundra/.ssh/authorized_keys
        ssh-keygen -A
        /usr/sbin/sshd
        exec slurmctld -D
        ;;
    compute)
        wait_for_bootstrap
        start_munge
        install -d -m 1777 /scratch /gpu-scratch
        exec slurmd -D
        ;;
    *)
        echo "usage: entrypoint.sh init|controller|compute" >&2
        exit 64
        ;;
esac
