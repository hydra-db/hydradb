# Jepsen DB node: graph-node binary + sshd + fault-injection tooling.
#
# Expects the runtime image to already be built:
#   docker build --target runtime -t turbolay:jepsen .
FROM turbolay:jepsen AS bin

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      openssh-server \
      iptables \
      iproute2 \
      iputils-ping \
      procps \
      psmisc \
      net-tools \
      curl \
      gcc \
      libc6-dev \
      faketime \
      ntpsec-ntpdate \
      dnsutils \
      libcypher-parser-dev \
      libgraphblas-dev \
      lsof \
      sudo \
      tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --from=bin /usr/local/bin/graph-node /usr/local/bin/graph-node
COPY --from=bin /usr/local/bin/graph-indexer /usr/local/bin/graph-indexer

# Jepsen drives nodes over SSH as root. Key auth rather than password auth:
# jepsen.control.sshj builds an agent-proxy auth method even when a password
# is supplied, and that construction throws when no SSH agent is present.
COPY ssh/id_rsa.pub /root/.ssh/authorized_keys

RUN mkdir -p /run/sshd /var/log /var/cache/slatedb/data /etc/turbolay \
    && chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys \
    && echo 'root:root' | chpasswd \
    && sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config \
    && printf 'HostKeyAlgorithms +ssh-rsa\nPubkeyAcceptedAlgorithms +ssh-rsa\n' \
       >> /etc/ssh/sshd_config

# /run is a tmpfs at runtime, so the privilege-separation directory has to be
# recreated on every start rather than baked into the image.
RUN printf '#!/bin/sh\nset -e\nmkdir -p /run/sshd\nexec /usr/sbin/sshd -D -e\n' \
      > /usr/local/bin/entrypoint.sh \
    && chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 22 7687 8443 9090
CMD ["/usr/local/bin/entrypoint.sh"]
