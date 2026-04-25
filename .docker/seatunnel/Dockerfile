FROM eclipse-temurin:11-jre-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget curl \
    && rm -rf /var/lib/apt/lists/*

ENV SEATUNNEL_VERSION=2.3.12
ENV SEATUNNEL_HOME=/opt/seatunnel

RUN wget -q https://dlcdn.apache.org/seatunnel/${SEATUNNEL_VERSION}/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz \
        -O /tmp/seatunnel.tar.gz \
    && tar -xzf /tmp/seatunnel.tar.gz -C /opt \
    && mv /opt/apache-seatunnel-${SEATUNNEL_VERSION} ${SEATUNNEL_HOME} \
    && rm /tmp/seatunnel.tar.gz

WORKDIR ${SEATUNNEL_HOME}

RUN sh bin/install-plugin.sh ${SEATUNNEL_VERSION}

COPY .docker/seatunnel/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5801

ENTRYPOINT ["/entrypoint.sh"]
