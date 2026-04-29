FROM eclipse-temurin:11-jre-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget curl \
    && rm -rf /var/lib/apt/lists/*

ENV SEATUNNEL_VERSION=2.3.12
ENV SEATUNNEL_HOME=/opt/seatunnel

# Optional: place apache-seatunnel-2.3.12-bin.tar.gz in this folder
# to skip the download on build. If absent it is fetched from Apache mirrors.
COPY . /tmp/build/

RUN LOCAL_ZIP="/tmp/build/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz" \
    && if [ -f "$LOCAL_ZIP" ]; then \
        echo "Using local SeaTunnel archive"; \
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

# Copy every JAR from lib/ into the SeaTunnel lib directory.
# To add or update a driver just drop the JAR in seatunnel/lib/ — no
# Dockerfile changes needed, just rebuild.
COPY lib/ /tmp/extra-jars/
RUN find /tmp/extra-jars -name "*.jar" -exec cp {} ${SEATUNNEL_HOME}/lib/ \; \
    && rm -rf /tmp/extra-jars

# ETL jobs, config, and runner script
COPY config/    ${SEATUNNEL_HOME}/config/
COPY jobs/      ${SEATUNNEL_HOME}/jobs/
COPY run_etl.sh ${SEATUNNEL_HOME}/run_etl.sh
RUN chmod +x ${SEATUNNEL_HOME}/run_etl.sh

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5801
EXPOSE 9080

ENTRYPOINT ["/entrypoint.sh"]
