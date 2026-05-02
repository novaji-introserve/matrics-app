FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends pgbouncer gettext-base \
    && groupadd --system pgbouncer \
    && useradd --system --gid pgbouncer --home-dir /var/lib/pgbouncer --create-home pgbouncer \
    && mkdir -p /etc/pgbouncer \
    && chown -R pgbouncer:pgbouncer /etc/pgbouncer /var/lib/pgbouncer \
    && rm -rf /var/lib/apt/lists/*

COPY ./.docker/pgbouncer/entrypoint.sh /entrypoint.sh
COPY ./pgbouncer/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini.template
COPY ./pgbouncer/userlist.txt /etc/pgbouncer/userlist.txt

RUN chmod 755 /entrypoint.sh \
    && chown pgbouncer:pgbouncer /etc/pgbouncer/pgbouncer.ini.template /etc/pgbouncer/userlist.txt

USER pgbouncer

ENTRYPOINT ["/entrypoint.sh"]
CMD ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
