#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}

write_pbs_config() {
    local start_server=$1
    local start_mom=$2
    local start_sched=$3
    local start_comm=$4
    cat >/etc/pbs.conf <<EOF
PBS_EXEC=/opt/pbs
PBS_SERVER=controller
PBS_START_SERVER=$start_server
PBS_START_SCHED=$start_sched
PBS_START_COMM=$start_comm
PBS_START_MOM=$start_mom
PBS_HOME=/var/spool/pbs
PBS_CORE_LIMIT=unlimited
PBS_SCP=/usr/bin/scp
EOF
}

install_ssh_access() {
    install -d -o rundra -g rundra -m 0700 /home/rundra/.ssh
    install -o rundra -g rundra -m 0600 /cluster/authorized_keys \
        /home/rundra/.ssh/authorized_keys
    ssh-keygen -A
    mkdir -p /run/sshd
    /usr/sbin/sshd
}

case "$mode" in
    init)
        mkdir -p /cluster /state /workspace
        if [ ! -f /state/id_ed25519 ]; then
            ssh-keygen -q -t ed25519 -N '' -f /state/id_ed25519
        fi
        cp /state/id_ed25519.pub /cluster/authorized_keys
        if [ ! -f /cluster/rundra-test.sif ]; then
            apptainer pull /cluster/.rundra-test.sif.tmp docker://alpine:3.20
            mv /cluster/.rundra-test.sif.tmp /cluster/rundra-test.sif
        fi
        chmod 0444 /cluster/rundra-test.sif
        chown -R rundra:rundra /workspace
        ;;
    controller)
        install_ssh_access
        write_pbs_config 1 0 1 1
        /etc/init.d/pbs start
        ready=false
        for _ in $(seq 1 90); do
            if qstat -B >/dev/null 2>&1; then
                ready=true
                break
            fi
            sleep 1
        done
        [ "$ready" = true ]
        qmgr -c 'set server flatuid = True'
        qmgr -c 'set server job_history_enable = True'
        qmgr -c 'set server job_history_duration = 3600'
        qmgr -c 'create node compute1' 2>/dev/null || true
        qmgr -c 'create node compute2' 2>/dev/null || true
        exec tail -f /dev/null
        ;;
    compute)
        write_pbs_config 0 1 0 0
    {
      printf '%s\n' '$clienthost controller'
      printf '%s\n' '$usecp *:/workspace /workspace'
      printf '%s\n' '$usecp *:/home/rundra /home/rundra'
    } > /var/spool/pbs/mom_priv/config
        /etc/init.d/pbs start
        exec tail -f /dev/null
        ;;
    *)
        printf 'Usage: %s init|controller|compute\n' "$0" >&2
        exit 64
        ;;
esac
