# jobs-db: official Postgres with the pg_cron extension.
# The compose enables it with shared_preload_libraries=pg_cron and
# cron.database_name=jobs; init scripts in ./init create the databases, roles
# and the extension on the first start with an empty data volume.
FROM postgres:16.15-bookworm

# PGDG apt sources are already configured in the official image.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends postgresql-16-cron; \
    rm -rf /var/lib/apt/lists/*
