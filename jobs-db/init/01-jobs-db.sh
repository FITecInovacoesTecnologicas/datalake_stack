#!/bin/sh
# Bootstraps the "jobs" database (POSTGRES_DB) for the scheduler and the worker:
# pg_cron extension, the lake_worker role, the jobs schema and the job queue.
# Runs once, on the first start with an empty data volume, after 00-catalog-db.sh.
set -eu

psql -v ON_ERROR_STOP=1 \
     -v worker_pw="$JOBS_WORKER_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
-- pg_cron must live in cron.database_name (jobs); the library is preloaded by the server command.
CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE ROLE lake_worker LOGIN PASSWORD :'worker_pw';

CREATE SCHEMA jobs AUTHORIZATION lake_worker;

-- Queue the worker polls with FOR UPDATE SKIP LOCKED. Semantics live in the worker, which
-- also adds max_attempts/heartbeat_at/claimed_by to volumes created before they existed.
CREATE TABLE jobs.job_queue (
    id           bigserial PRIMARY KEY,
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status       text        NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued', 'running', 'done', 'failed')),
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 3,
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_at   timestamptz,
    claimed_by   text,
    heartbeat_at timestamptz,
    finished_at  timestamptz,
    last_error   text
);
CREATE INDEX job_queue_status_available_idx ON jobs.job_queue (status, available_at);
ALTER TABLE jobs.job_queue OWNER TO lake_worker;

-- Scheduled jobs (pg_cron) may enqueue work; the worker needs to see the cron schema.
GRANT USAGE ON SCHEMA cron TO lake_worker;
EOSQL
