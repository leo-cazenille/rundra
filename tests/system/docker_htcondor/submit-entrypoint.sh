#!/usr/bin/env bash
set -euo pipefail
install -m 0600 -o rundra -g rundra /state/id_ed25519.pub /home/rundra/.ssh/authorized_keys
chown rundra:rundra /workspace
rm -f /run/nologin /etc/nologin
/usr/sbin/sshd -D -e -o LogLevel=VERBOSE -o PerSourcePenalties=no -o UsePAM=no &
exec /bin/bash -x /start.sh
