ARG BASE_IMAGE=rundra-slurm-system:local
FROM ${BASE_IMAGE}

ARG OPENPBS_VERSION=23.06.06
ARG OPENPBS_SHA256=689df6caa101422237118d81f9cfb0e6cad432209c77a6b3b8021cf984a91a90

USER root
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        autoconf automake g++ gcc libedit-dev libexpat1-dev libhwloc-dev \
        libical-dev libncurses-dev libssl-dev libtool libx11-dev libxext-dev \
        libxft-dev libxt-dev make perl pkg-config postgresql postgresql-contrib \
        postgresql-server-dev-all python3-dev sendmail-bin sudo swig tcl-dev tk-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --fail --location --silent --show-error \
        "https://github.com/openpbs/openpbs/archive/refs/tags/v${OPENPBS_VERSION}.tar.gz" \
        --output /tmp/openpbs.tar.gz \
    && echo "${OPENPBS_SHA256}  /tmp/openpbs.tar.gz" | sha256sum --check --strict \
    && mkdir /tmp/openpbs \
    && tar --extract --gzip --file /tmp/openpbs.tar.gz --strip-components=1 \
        --directory /tmp/openpbs \
    && cd /tmp/openpbs \
    && ./autogen.sh \
    && ./configure --prefix=/opt/pbs

RUN cd /tmp/openpbs \
    && sed --in-place '/#include <eval.h>/d' src/lib/Libpython/pbs_python_external.c \
    && (make --jobs=1 > /tmp/openpbs-build.log 2>&1 \
        || { tail -n 120 /tmp/openpbs-build.log; exit 1; }) \
    && make install \
    && /opt/pbs/libexec/pbs_postinstall \
    && chmod 4755 /opt/pbs/sbin/pbs_iff /opt/pbs/sbin/pbs_rcp \
    && rm -rf /tmp/openpbs /tmp/openpbs.tar.gz

RUN ln --symbolic /opt/pbs/bin/qsub /usr/local/bin/qsub \
    && ln --symbolic /opt/pbs/bin/qstat /usr/local/bin/qstat \
    && ln --symbolic /opt/pbs/bin/qdel /usr/local/bin/qdel

ENV PATH="/opt/pbs/bin:/opt/pbs/sbin:${PATH}"
COPY entrypoint.sh /opt/rundra-pbs/entrypoint.sh
RUN chmod 0755 /opt/rundra-pbs/entrypoint.sh
ENTRYPOINT ["/opt/rundra-pbs/entrypoint.sh"]
