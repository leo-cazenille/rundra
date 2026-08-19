FROM debian:13-slim@sha256:3a39a0592364683e6bab97937b72cad5a8fa6dcbbee90edb3bb48c7f8e94f258

ARG DEBIAN_FRONTEND=noninteractive
ARG APPTAINER_VERSION=1.5.1
ARG APPTAINER_SHA256=ae00a6a2f1949a8f245c082660fd2990d61a6543159c9a28eede7966d89efe62

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl fuse3 golang-go libfuse3-dev \
        libseccomp-dev munge openssh-server pkg-config rsync slurm-wlm \
        squashfs-tools squashfuse uidmap \
    && curl --fail --location --silent --show-error \
        --output /tmp/apptainer.tar.gz \
        "https://github.com/apptainer/apptainer/releases/download/v${APPTAINER_VERSION}/apptainer-${APPTAINER_VERSION}.tar.gz" \
    && echo "${APPTAINER_SHA256}  /tmp/apptainer.tar.gz" | sha256sum --check - \
    && tar -C /tmp -xzf /tmp/apptainer.tar.gz \
    && cd "/tmp/apptainer-${APPTAINER_VERSION}" \
    && ./mconfig --without-suid --prefix=/usr \
    && make -C builddir -j2 \
    && make -C builddir install \
    && sed -i 's/^mount home = yes$/mount home = no/' \
        /usr/etc/apptainer/apptainer.conf \
    && grep -q '^mount home = no$' /usr/etc/apptainer/apptainer.conf \
    && rm -rf /var/lib/apt/lists/* /tmp/apptainer.tar.gz \
        "/tmp/apptainer-${APPTAINER_VERSION}" \
    && useradd --create-home --uid 1100 --shell /bin/bash rundra \
    && install -d -m 0755 /run/sshd /var/spool/slurmctld /var/spool/slurmd \
    && chown slurm:slurm /var/spool/slurmctld /var/spool/slurmd

COPY slurm.conf /etc/slurm/slurm.conf
COPY cgroup.conf /etc/slurm/cgroup.conf
COPY entrypoint.sh /opt/rundra-cluster/entrypoint.sh

ENTRYPOINT ["/opt/rundra-cluster/entrypoint.sh"]
