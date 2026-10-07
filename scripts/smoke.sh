#!/usr/bin/env bash
# smoke.sh — bring the whole stack up from a clean checkout and prove every component answers.
#
# Usage: bash scripts/smoke.sh            (SMOKE_TIMEOUT_SECS=600 by default; KEEP=1 leaves the stack up)
#
# - Runs under its own compose project (rust-datalake-smoke) with its own host ports, so a real
#   deployment on this host is never touched.
# - Generates a throwaway env file from env.example (empty secrets filled with `openssl rand -hex 24`)
#   under tests/.tmp/ and removes it on exit; nothing secret is written anywhere else.
# - Tears everything down, including volumes, on exit (unless KEEP=1).
# Prints SMOKE=OK on success; exits non-zero naming the failing step otherwise.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROJECT="rust-datalake-smoke"
TIMEOUT="${SMOKE_TIMEOUT_SECS:-600}"
RUSTFS_PORT=19000; CONSOLE_PORT=19001; PG_PORT=15433; LK_PORT=18181; QUERY_PORT=50052

mkdir -p tests/.tmp
ENV_FILE="$(mktemp tests/.tmp/smoke-env.XXXXXX)"
OVERRIDE="$(mktemp tests/.tmp/smoke-override.XXXXXX)"
step="setup"

compose() { docker compose -p "$PROJECT" --env-file "$ENV_FILE" -f docker-compose.yml -f "$OVERRIDE" --profile query "$@"; }
fail() { echo "SMOKE=FAIL step=$step: $*" >&2; exit 1; }
cleanup() {
  rc=$?
  set +e
  if [[ $rc -ne 0 ]]; then echo "--- compose ps ---"; compose ps -a 2>/dev/null; echo "--- recent logs ---"; compose logs --tail=40 2>/dev/null | tail -120; fi
  if [[ "${KEEP:-0}" != "1" ]]; then compose down -v --remove-orphans >/dev/null 2>&1; fi
  rm -f "$ENV_FILE" "$OVERRIDE"
  rmdir tests/.tmp 2>/dev/null || true
  exit $rc
}
trap cleanup EXIT

# 1. Throwaway env: copy env.example, fill every empty key with a random hex secret.
#    S3 credentials follow the S3/MinIO limits RustFS enforces: access key 3-20 chars,
#    secret key 8-40 chars ("invalid secret key length" otherwise). Everything else: 48 hex chars.
secret_for() {
  case "$1" in
    LAKE_S3_ACCESS_KEY) openssl rand -hex 10 ;;   # 20 chars
    LAKE_S3_SECRET_KEY) openssl rand -hex 20 ;;   # 40 chars
    *) openssl rand -hex 24 ;;                    # 48 chars
  esac
}
while IFS= read -r line; do
  if [[ "$line" =~ ^([A-Z0-9_]+)=$ ]]; then
    printf '%s=%s\n' "${BASH_REMATCH[1]}" "$(secret_for "${BASH_REMATCH[1]}")"
  else
    printf '%s\n' "$line"
  fi
done < env.example > "$ENV_FILE"
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

# 2. Host-port override so the smoke project never collides with a real deployment.
cat > "$OVERRIDE" <<YAML
services:
  rustfs:
    ports: !override
      - "127.0.0.1:${RUSTFS_PORT}:9000"
      - "127.0.0.1:${CONSOLE_PORT}:9001"
  postgres:
    ports: !override
      - "127.0.0.1:${PG_PORT}:5432"
  lakekeeper:
    ports: !override
      - "127.0.0.1:${LK_PORT}:8181"
  lake-query:
    ports: !override
      - "127.0.0.1:${QUERY_PORT}:50051"
YAML

for port in $RUSTFS_PORT $CONSOLE_PORT $PG_PORT $LK_PORT $QUERY_PORT; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then fail "host port $port is already in use"; fi
done

wait_for() { # name, seconds, command...
  local name="$1" secs="$2"; shift 2
  local start; start=$(date +%s)
  until "$@" >/dev/null 2>&1; do
    if (( $(date +%s) - start >= secs )); then fail "timed out waiting for $name"; fi
    sleep 3
  done
}
health() { [[ "$(docker inspect -f '{{.State.Health.Status}}' "$(compose ps -q "$1")" 2>/dev/null)" == "healthy" ]]; }
exited_ok() { local cid; cid="$(compose ps -a -q "$1")"; [[ -n "$cid" && "$(docker inspect -f '{{.State.Status}}:{{.State.ExitCode}}' "$cid")" == "exited:0" ]]; }
running() { [[ "$(docker inspect -f '{{.State.Status}}' "$(compose ps -q "$1")" 2>/dev/null)" == "running" ]]; }

step="up"
echo "smoke: starting project $PROJECT (timeout ${TIMEOUT}s)"
compose up -d --build >/dev/null

step="health"
wait_for "rustfs healthy" "$TIMEOUT" health rustfs
wait_for "postgres healthy" "$TIMEOUT" health postgres
wait_for "storage-init completed" "$TIMEOUT" exited_ok storage-init
wait_for "lakekeeper-migrate completed" "$TIMEOUT" exited_ok lakekeeper-migrate
wait_for "lakekeeper healthy" "$TIMEOUT" health lakekeeper
wait_for "lakekeeper-init completed" "$TIMEOUT" exited_ok lakekeeper-init
echo "smoke: bootstrap one-shots completed"

step="rustfs"
curl -fsS "http://127.0.0.1:${RUSTFS_PORT}/health" >/dev/null || fail "rustfs /health did not answer"
compose logs storage-init 2>/dev/null | grep -q "bucket ${LAKE_BUCKET} ready" || fail "storage-init did not report the bucket ready"
echo "smoke: rustfs healthy, bucket ${LAKE_BUCKET} ready"

step="lakekeeper"
curl -fsS "http://127.0.0.1:${LK_PORT}/management/v1/info" | grep -Eq '"bootstrapped" *: *true' || fail "lakekeeper is not bootstrapped"
curl -fsS "http://127.0.0.1:${LK_PORT}/management/v1/warehouse" | grep -Eq "\"name\" *: *\"${LAKE_WAREHOUSE}\"" || fail "warehouse ${LAKE_WAREHOUSE} not found"
echo "smoke: lakekeeper bootstrapped, warehouse ${LAKE_WAREHOUSE} present"

step="postgres"
compose exec -T postgres psql -U "$PG_ADMIN_USER" -d jobs -tAc "select count(*) from pg_extension where extname='pg_cron'" | grep -qx 1 || fail "pg_cron extension missing in jobs"
compose exec -T postgres psql -U "$PG_ADMIN_USER" -d catalog -tAc "select 1" | grep -qx 1 || fail "catalog database not reachable"
echo "smoke: postgres has catalog db and pg_cron in jobs"

step="worker"
wait_for "lake-worker running" "$TIMEOUT" running lake-worker
wait_for "lake-worker polling" 90 bash -c "docker compose -p '$PROJECT' --env-file '$ENV_FILE' -f docker-compose.yml -f '$OVERRIDE' --profile query logs lake-worker 2>/dev/null | grep -qi 'polling jobs.job_queue'"
compose exec -T postgres psql -U "$PG_ADMIN_USER" -d jobs -tAc "insert into jobs.job_queue(kind, payload) values ('smoke', '{\"from\":\"smoke.sh\"}') returning id" >/dev/null || fail "could not enqueue a job"
wait_for "smoke job done" 60 bash -c "docker compose -p '$PROJECT' --env-file '$ENV_FILE' -f docker-compose.yml -f '$OVERRIDE' --profile query exec -T postgres psql -U '$PG_ADMIN_USER' -d jobs -tAc \"select count(*) from jobs.job_queue where kind='smoke' and status='done'\" | grep -qx 1"
echo "smoke: lake-worker polled the queue and completed a job"

step="query"
wait_for "lake-query running" "$TIMEOUT" running lake-query
wait_for "flight sql port" 90 bash -c "exec 3<>/dev/tcp/127.0.0.1/$QUERY_PORT"
echo "smoke: lake-query listening on ${QUERY_PORT} (Flight SQL)"

step="done"
echo "SMOKE=OK"
