# Jepsen control node: JVM + Leiningen + plotting tools.
#
# JDK 21 is required, not merely preferred: jepsen 0.3.5's dependency graph
# references java.util.SequencedCollection, which does not exist before 21.
FROM eclipse-temurin:21-jdk

ENV DEBIAN_FRONTEND=noninteractive \
    LEIN_ROOT=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      git \
      openssh-client \
      sshpass \
      gnuplot-nox \
      graphviz \
      iputils-ping \
      dnsutils \
      procps \
      wget \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/local/bin/lein \
      https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \
    && chmod +x /usr/local/bin/lein \
    && lein version

# Never prompt on first connection to the freshly-created DB containers.
RUN mkdir -p /root/.ssh \
    && printf 'Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n' \
       > /root/.ssh/config

WORKDIR /jepsen
CMD ["sleep", "infinity"]
