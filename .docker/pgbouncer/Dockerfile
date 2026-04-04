FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends pgbouncer \
    && groupadd --system pgbouncer \
    && useradd --system --gid pgbouncer --home-dir /var/lib/pgbouncer --create-home pgbouncer \
    && mkdir -p /etc/pgbouncer \
    && chown -R pgbouncer:pgbouncer /etc/pgbouncer /var/lib/pgbouncer \
    && rm -rf /var/lib/apt/lists/*

COPY ./pgbouncer/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
COPY ./pgbouncer/userlist.txt /etc/pgbouncer/userlist.txt

RUN chown pgbouncer:pgbouncer /etc/pgbouncer/pgbouncer.ini /etc/pgbouncer/userlist.txt

USER pgbouncer

CMD ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
