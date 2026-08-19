FROM ubuntu:24.04@sha256:1428a953896eef9e62fc6ef60cad05bbf98769f6ea5f8c278e519b9dd168ab26

ARG DEBIAN_FRONTEND=noninteractive
ARG APPTAINER_VERSION=1.5.1
ARG APPTAINER_SHA256=c7f1247e42d5d27aec4d9b2d632b0d0bca0f9254cc27ab836e02b20511e09eb5

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl fuse3 munge openssh-server rsync \
        slurm-wlm squashfs-tools \
    && curl --fail --location --silent --show-error \
        --output /tmp/apptainer.deb \
        "https://github.com/apptainer/apptainer/releases/download/v${APPTAINER_VERSION}/apptainer_${APPTAINER_VERSION}_amd64.deb" \
    && echo "${APPTAINER_SHA256}  /tmp/apptainer.deb" | sha256sum --check - \
    && apt-get install -y --no-install-recommends /tmp/apptainer.deb \
    && rm -rf /var/lib/apt/lists/* /tmp/apptainer.deb \
    && useradd --create-home --uid 1100 --shell /bin/bash rundra \
    && install -d -m 0755 /run/sshd /var/spool/slurmctld /var/spool/slurmd \
    && chown slurm:slurm /var/spool/slurmctld /var/spool/slurmd

COPY slurm.conf /etc/slurm/slurm.conf
COPY entrypoint.sh /opt/rundra-cluster/entrypoint.sh

ENTRYPOINT ["/opt/rundra-cluster/entrypoint.sh"]
