#!/bin/sh
# Creates the Lakekeeper catalog database and its role inside the shared
# Postgres instance. Runs once, on the first start with an empty data volume.
set -eu

psql -v ON_ERROR_STOP=1 \
     -v db="$CATALOG_DB_NAME" -v usr="$CATALOG_DB_USER" -v pw="$CATALOG_DB_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
CREATE ROLE :"usr" LOGIN PASSWORD :'pw';
CREATE DATABASE :"db" OWNER :"usr";
-- Only the catalog role (and superusers) may connect to the catalog database.
REVOKE CONNECT ON DATABASE :"db" FROM PUBLIC;
\connect :"db"
-- Extensions required by the Lakekeeper migrations.
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "btree_gist";
EOSQL
