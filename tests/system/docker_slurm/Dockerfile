FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
ARG APPTAINER_VERSION=1.5.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl fuse3 munge openssh-server rsync \
        slurm-wlm squashfs-tools \
    && curl --fail --location --silent --show-error \
        --output /tmp/apptainer.deb \
        "https://github.com/apptainer/apptainer/releases/download/v${APPTAINER_VERSION}/apptainer_${APPTAINER_VERSION}_amd64.deb" \
    && apt-get install -y --no-install-recommends /tmp/apptainer.deb \
    && rm -rf /var/lib/apt/lists/* /tmp/apptainer.deb \
    && useradd --create-home --uid 1000 --shell /bin/bash rundra \
    && install -d -m 0755 /run/sshd /var/spool/slurmctld /var/spool/slurmd \
    && chown slurm:slurm /var/spool/slurmctld /var/spool/slurmd

COPY slurm.conf /etc/slurm/slurm.conf
COPY entrypoint.sh /opt/rundra-cluster/entrypoint.sh

ENTRYPOINT ["/opt/rundra-cluster/entrypoint.sh"]
