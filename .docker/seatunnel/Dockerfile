FROM eclipse-temurin:11-jre-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget curl \
    && rm -rf /var/lib/apt/lists/*

ENV SEATUNNEL_VERSION=2.3.12
ENV SEATUNNEL_HOME=/opt/seatunnel

# Copy custom dir so the local zip (if present) is available during build.
# custom/* is gitignored — place the tarball at:
#   custom/apache-seatunnel-<VERSION>-bin.tar.gz
# to skip the download on subsequent builds.
COPY custom/ /tmp/custom/

RUN LOCAL_ZIP="/tmp/custom/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz" \
    && if [ -f "$LOCAL_ZIP" ]; then \
        echo "Using local SeaTunnel archive: $LOCAL_ZIP"; \
        cp "$LOCAL_ZIP" /tmp/seatunnel.tar.gz; \
    else \
        echo "Downloading SeaTunnel ${SEATUNNEL_VERSION}..."; \
        wget -q "https://dlcdn.apache.org/seatunnel/${SEATUNNEL_VERSION}/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz" \
            -O /tmp/seatunnel.tar.gz; \
    fi \
    && tar -xzf /tmp/seatunnel.tar.gz -C /opt \
    && mv /opt/apache-seatunnel-${SEATUNNEL_VERSION} ${SEATUNNEL_HOME} \
    && rm -rf /tmp/seatunnel.tar.gz /tmp/custom

WORKDIR ${SEATUNNEL_HOME}

RUN sh bin/install-plugin.sh ${SEATUNNEL_VERSION}

COPY .docker/seatunnel/lib/ /tmp/extra-jars/
RUN find /tmp/extra-jars -name "*.jar" -exec cp {} ${SEATUNNEL_HOME}/lib/ \; && rm -rf /tmp/extra-jars

COPY .docker/seatunnel/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5801

ENTRYPOINT ["/entrypoint.sh"]
