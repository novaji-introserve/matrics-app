FROM eclipse-temurin:11-jre-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget curl \
    && rm -rf /var/lib/apt/lists/*

ENV SEATUNNEL_VERSION=2.3.12
ENV SEATUNNEL_HOME=/opt/seatunnel

# Optional: place the tarball at seatunnel/apache-seatunnel-2.3.12-bin.tar.gz
# to skip the download. If absent, it is downloaded from the Apache mirror.
COPY . /tmp/build/

RUN LOCAL_ZIP="/tmp/build/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz" \
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
    && rm -rf /tmp/seatunnel.tar.gz /tmp/build

WORKDIR ${SEATUNNEL_HOME}

RUN sh bin/install-plugin.sh ${SEATUNNEL_VERSION}

# The JDBC connector needs the actual database driver in lib/.
# install-plugin.sh only downloads SeaTunnel connectors — it does not
# fetch third-party drivers, so we pull the PostgreSQL driver explicitly.
RUN wget -q "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" \
    -O ${SEATUNNEL_HOME}/lib/postgresql-42.7.3.jar

# ETL config, job definitions, and runner — copied separately so that
# editing jobs or the script doesn't bust the SeaTunnel install layer.
COPY config/   ${SEATUNNEL_HOME}/config/
COPY jobs/     ${SEATUNNEL_HOME}/jobs/
COPY run_etl.sh ${SEATUNNEL_HOME}/run_etl.sh
RUN chmod +x ${SEATUNNEL_HOME}/run_etl.sh

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Hazelcast cluster port
EXPOSE 5801
# REST API v2
EXPOSE 9080

ENTRYPOINT ["/entrypoint.sh"]
