//! lake-worker: claims queued jobs from Postgres (`jobs.job_queue`) and runs
//! DataFusion transformations over Iceberg tables registered in the REST catalog.
//!
//! Queue semantics (at-least-once):
//! - a job is claimed with `FOR UPDATE SKIP LOCKED`, so replicas never share one;
//! - while it runs, the worker refreshes `heartbeat_at`; a job whose heartbeat is
//!   older than `JOB_LEASE_SECS` is reaped by any replica and queued again;
//! - a failed job is retried with exponential backoff until `max_attempts`;
//! - `attempts` is the fencing token: a worker that lost its lease cannot mark the
//!   job done or failed any more.
//! The jobs database connection is re-established whenever it drops.
//!
//! Job kinds: `noop` / `smoke` (no work), `sql` (runs `payload.sql` against the
//! `lake` catalog). Any other kind fails permanently. Real raw -> clean ->
//! curated logic is added as new kinds in `run_job`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

/// Columns added after the first release; `01-jobs-db.sh` creates them on new volumes,
/// this brings existing volumes up to date. `lake_worker` owns the table.
const MIGRATE_SQL: &str = "ALTER TABLE jobs.job_queue \
     ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 3, \
     ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz, \
     ADD COLUMN IF NOT EXISTS claimed_by   text";

/// Claim exactly one queued job; concurrent workers never pick the same row.
const CLAIM_SQL: &str = "UPDATE jobs.job_queue \
     SET status = 'running', claimed_at = now(), heartbeat_at = now(), claimed_by = $1, \
         attempts = attempts + 1, finished_at = NULL \
     WHERE id = (SELECT id FROM jobs.job_queue \
                 WHERE status = 'queued' AND available_at <= now() \
                 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) \
     RETURNING id, kind, payload, attempts, max_attempts";

/// Every update of a running job is fenced by (id, attempts): once the lease is
/// lost and the job is claimed again, the old worker matches no row.
const HEARTBEAT_SQL: &str =
    "UPDATE jobs.job_queue SET heartbeat_at = now() WHERE id = $1 AND attempts = $2 AND status = 'running'";
const DONE_SQL: &str = "UPDATE jobs.job_queue \
     SET status = 'done', finished_at = now(), last_error = NULL \
     WHERE id = $1 AND attempts = $2 AND status = 'running'";
/// $3 = retryable, $4 = base backoff in seconds (doubled per attempt, capped at 1 h), $5 = error.
const FAILED_SQL: &str = "UPDATE jobs.job_queue \
     SET status       = CASE WHEN $3::bool AND attempts < max_attempts THEN 'queued' ELSE 'failed' END, \
         available_at = CASE WHEN $3 AND attempts < max_attempts \
                             THEN now() + make_interval(secs => least($4::float8 * power(2, attempts - 1), 3600)) \
                             ELSE available_at END, \
         finished_at  = CASE WHEN $3 AND attempts < max_attempts THEN NULL ELSE now() END, \
         last_error   = $5 \
     WHERE id = $1 AND attempts = $2 AND status = 'running' \
     RETURNING status, available_at::text";
/// Requeue (or fail, when out of attempts) jobs whose worker stopped heartbeating.
const REAP_SQL: &str = "UPDATE jobs.job_queue \
     SET status       = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END, \
         available_at = now(), \
         finished_at  = CASE WHEN attempts < max_attempts THEN NULL ELSE now() END, \
         last_error   = 'lease expired: worker ' || coalesce(claimed_by, '?') || ' stopped heartbeating' \
     WHERE status = 'running' \
       AND coalesce(heartbeat_at, claimed_at) < now() - make_interval(secs => $1::float8) \
     RETURNING id, status";

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} must be set"))
}

fn env_secs(name: &str, default: &str) -> Result<u64> {
    env_or(name, default).parse::<u64>().with_context(|| format!("{name} must be an integer"))
}

/// Resolves to completion on SIGTERM or SIGINT (the compose uses init: true so SIGTERM arrives).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        sigterm.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Builds the Iceberg REST catalog client from the compose environment (x-lake-client-env).
async fn build_catalog(uri: &str, warehouse: &str) -> Result<Arc<dyn Catalog>> {
    let mut props: HashMap<String, String> = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), uri.to_string()),
        (REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.to_string()),
    ]);
    // Use the iceberg constants: a misspelt key is silently ignored by FileIO.
    for (env, key) in [
        ("LAKE_S3_ENDPOINT", S3_ENDPOINT),
        ("LAKE_S3_REGION", S3_REGION),
        ("LAKE_S3_ACCESS_KEY_ID", S3_ACCESS_KEY_ID),
        ("LAKE_S3_SECRET_ACCESS_KEY", S3_SECRET_ACCESS_KEY),
        ("LAKE_S3_PATH_STYLE", S3_PATH_STYLE_ACCESS),
    ] {
        if let Ok(value) = std::env::var(env) {
            props.insert(key.to_string(), value);
        }
    }
    // Lakekeeper honours this header; with "client-managed" it returns no credentials
    // and the worker signs with the bucket-scoped key above.
    if let Ok(mode) = std::env::var("LAKE_ACCESS_DELEGATION") {
        props.insert("header.X-Iceberg-Access-Delegation".to_string(), mode);
    }
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 { customized_credential_load: None }))
        .load("lake", props)
        .await
        .context("load Iceberg REST catalog")?;
    Ok(Arc::new(catalog))
}

/// The `lake` catalog as DataFusion sees it. `IcebergCatalogProvider` lists namespaces
/// and tables once, at construction; this wrapper rebuilds it on `refresh` so tables
/// created after startup become visible, and maps `CREATE SCHEMA lake.x` to an Iceberg
/// namespace. Table data itself is never stale: each scan loads the current snapshot.
#[derive(Debug)]
struct LakeCatalogProvider {
    catalog: Arc<dyn Catalog>,
    inner: RwLock<Arc<IcebergCatalogProvider>>,
}

impl LakeCatalogProvider {
    async fn try_new(catalog: Arc<dyn Catalog>) -> Result<Self> {
        let inner = IcebergCatalogProvider::try_new(catalog.clone()).await.context("list Iceberg namespaces")?;
        Ok(Self { catalog, inner: RwLock::new(Arc::new(inner)) })
    }

    async fn refresh(&self) -> Result<()> {
        let fresh = IcebergCatalogProvider::try_new(self.catalog.clone()).await.context("refresh Iceberg catalog")?;
        *self.inner.write().expect("catalog lock poisoned") = Arc::new(fresh);
        Ok(())
    }

    fn current(&self) -> Arc<IcebergCatalogProvider> {
        self.inner.read().expect("catalog lock poisoned").clone()
    }
}

impl CatalogProvider for LakeCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.current().schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.current().schema(name)
    }

    fn register_schema(&self, name: &str, _schema: Arc<dyn SchemaProvider>) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        // CatalogProvider is synchronous: run the REST round trips on a scoped thread that
        // blocks on the runtime handle, which is safe from any runtime context.
        let namespace = NamespaceIdent::new(name.to_string());
        let handle = tokio::runtime::Handle::current();
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    handle.block_on(async {
                        self.catalog
                            .create_namespace(&namespace, HashMap::new())
                            .await
                            .map_err(|err| DataFusionError::External(Box::new(err)))?;
                        self.refresh().await.map_err(|err| DataFusionError::External(err.into()))
                    })
                })
                .join()
                .map_err(|_| DataFusionError::Execution("create namespace thread panicked".to_string()))?
        })?;
        Ok(None)
    }
}

/// The jobs database connection, re-established on demand after it drops.
struct JobsDb {
    url: String,
    client: Option<Client>,
}

impl JobsDb {
    async fn client(&mut self) -> Result<&Client> {
        if self.client.as_ref().is_some_and(Client::is_closed) {
            warn!("jobs database connection lost; reconnecting");
            self.client = None;
        }
        if self.client.is_none() {
            let (client, connection) =
                tokio::time::timeout(Duration::from_secs(10), tokio_postgres::connect(&self.url, NoTls))
                    .await
                    .context("connect to jobs database: timed out")?
                    .context("connect to jobs database")?;
            tokio::spawn(async move {
                if let Err(err) = connection.await {
                    error!(error = %err, "jobs database connection closed");
                }
            });
            info!("connected to jobs database");
            self.client = Some(client);
        }
        Ok(self.client.as_ref().expect("client set above"))
    }
}

struct Job {
    id: i64,
    kind: String,
    payload: serde_json::Value,
    attempts: i32,
    max_attempts: i32,
}

/// An error that retrying cannot fix (bad payload, unknown kind): the job fails at once.
#[derive(Debug)]
struct Permanent(String);

impl std::fmt::Display for Permanent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Permanent {}

fn permanent(message: impl Into<String>) -> anyhow::Error {
    Permanent(message.into()).into()
}

/// Transformation hook: one arm per job kind. `Ok` marks the job done; an error marks it
/// failed (retried with backoff unless it is `Permanent`). Jobs may run more than once
/// (at-least-once delivery), so each kind must be safe to repeat.
async fn run_job(ctx: &SessionContext, lake: Option<&LakeCatalogProvider>, job: &Job) -> Result<()> {
    info!(id = job.id, kind = %job.kind, attempt = job.attempts, payload = %job.payload, "running job");
    match job.kind.as_str() {
        "noop" | "smoke" => Ok(()),
        "sql" => {
            let statements: Vec<&str> = match job.payload.get("sql") {
                Some(serde_json::Value::String(sql)) => vec![sql.as_str()],
                Some(serde_json::Value::Array(items)) => items
                    .iter()
                    .map(|item| item.as_str().ok_or_else(|| permanent("payload.sql items must be strings")))
                    .collect::<Result<_>>()?,
                _ => return Err(permanent("payload.sql must be a string or an array of strings")),
            };
            let Some(lake) = lake else { bail!("iceberg catalog not available") };
            // Pick up namespaces and tables created since the last job.
            lake.refresh().await?;
            for (index, sql) in statements.iter().enumerate() {
                let batches = ctx
                    .sql(sql)
                    .await
                    .with_context(|| format!("plan statement {index}"))?
                    .collect()
                    .await
                    .with_context(|| format!("execute statement {index}"))?;
                let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
                info!(id = job.id, statement = index, rows, "statement done");
            }
            Ok(())
        }
        other => Err(permanent(format!("unknown job kind {other:?}"))),
    }
}

/// Runs the job while refreshing its heartbeat. Returns `None` when the lease was lost
/// (another replica reaped the job); the job future is dropped in that case.
async fn run_with_heartbeat(
    client: &Client,
    ctx: &SessionContext,
    lake: Option<&LakeCatalogProvider>,
    job: &Job,
    every: Duration,
) -> Option<Result<()>> {
    let heartbeat = async {
        loop {
            tokio::time::sleep(every).await;
            match client.execute(HEARTBEAT_SQL, &[&job.id, &job.attempts]).await {
                Ok(0) => return,
                Ok(_) => {}
                // Database unreachable: keep working. If the lease expires meanwhile the
                // fenced DONE/FAILED update simply matches nothing.
                Err(err) => warn!(id = job.id, error = %err, "heartbeat failed"),
            }
        }
    };
    tokio::select! {
        result = run_job(ctx, lake, job) => Some(result),
        () = heartbeat => None,
    }
}

/// Applies the outcome, reconnecting once if the connection dropped while the job ran.
async fn finish_job(db: &mut JobsDb, job: &Job, result: Result<()>, backoff_secs: f64) -> Result<()> {
    let mut last_err = None;
    for _ in 0..2 {
        let client = match db.client().await {
            Ok(client) => client,
            Err(err) => {
                last_err = Some(err);
                continue;
            }
        };
        let applied = match &result {
            Ok(()) => client.execute(DONE_SQL, &[&job.id, &job.attempts]).await.map(|n| {
                if n == 1 {
                    info!(id = job.id, kind = %job.kind, "job done");
                }
                n == 1
            }),
            Err(err) => {
                let retryable = err.downcast_ref::<Permanent>().is_none();
                let message = format!("{err:#}");
                client
                    .query_opt(FAILED_SQL, &[&job.id, &job.attempts, &retryable, &backoff_secs, &message])
                    .await
                    .map(|row| {
                        if let Some(row) = &row {
                            let status: String = row.get(0);
                            let retry_at: String = row.get(1);
                            if status == "queued" {
                                warn!(id = job.id, kind = %job.kind, attempt = job.attempts,
                                      max_attempts = job.max_attempts, retry_at, error = %message, "job failed; will retry");
                            } else {
                                error!(id = job.id, kind = %job.kind, attempt = job.attempts, error = %message, "job failed");
                            }
                        }
                        row.is_some()
                    })
            }
        };
        match applied {
            Ok(true) => return Ok(()),
            Ok(false) => {
                warn!(id = job.id, "lease lost before the result was recorded; another attempt owns the job");
                return Ok(());
            }
            Err(err) => last_err = Some(err.into()),
        }
    }
    // Still unrecorded: the reaper requeues the job once its lease expires.
    Err(last_err.expect("loop ran").context(format!("record outcome of job {}", job.id)))
}

/// Registers the `lake` catalog in the session; `None` (logged) while Lakekeeper is unreachable.
async fn register_lake(ctx: &SessionContext, catalog: &Arc<dyn Catalog>) -> Option<Arc<LakeCatalogProvider>> {
    match LakeCatalogProvider::try_new(catalog.clone()).await {
        Ok(provider) => {
            let provider = Arc::new(provider);
            ctx.register_catalog("lake", provider.clone());
            info!(namespaces = provider.schema_names().len(), "iceberg catalog registered as `lake`");
            Some(provider)
        }
        Err(err) => {
            warn!(error = %format!("{err:#}"), "iceberg catalog not reachable yet; will retry at the next job");
            None
        }
    }
}

/// One polling step: reap expired leases (at most once per heartbeat period), then claim a job.
async fn poll_once(
    db: &mut JobsDb,
    worker_id: &str,
    lease_secs: u64,
    reap_every: Duration,
    last_reap: &mut Option<Instant>,
) -> Result<Option<Job>> {
    let client = db.client().await?;
    if last_reap.is_none_or(|at| at.elapsed() >= reap_every) {
        for row in client.query(REAP_SQL, &[&(lease_secs as f64)]).await.context("reap expired jobs")? {
            let id: i64 = row.get(0);
            let status: String = row.get(1);
            warn!(id, status, "reaped job whose worker stopped heartbeating");
        }
        *last_reap = Some(Instant::now());
    }
    let claimed = client.query_opt(CLAIM_SQL, &[&worker_id]).await.context("claim a job")?;
    Ok(claimed.map(|row| Job {
        id: row.get(0),
        kind: row.get(1),
        payload: row.get(2),
        attempts: row.get(3),
        max_attempts: row.get(4),
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let catalog_uri = env_required("LAKE_CATALOG_URI")?;
    let warehouse = env_required("LAKE_WAREHOUSE")?;
    let jobs_url = env_required("JOBS_DATABASE_URL")?;
    let poll = Duration::from_secs(env_secs("POLL_INTERVAL_SECS", "5")?);
    let lease_secs = env_secs("JOB_LEASE_SECS", "60")?.max(3);
    let heartbeat_every = Duration::from_secs(lease_secs / 3);
    let backoff_secs = env_secs("JOB_RETRY_BACKOFF_SECS", "30")? as f64;
    let worker_id = env_or("HOSTNAME", "lake-worker");
    let memory_mb: usize =
        env_or("LAKE_MEMORY_LIMIT_MB", "1400").parse().context("LAKE_MEMORY_LIMIT_MB must be an integer")?;
    let spill_dir = env_or("LAKE_SPILL_DIR", "/spill");
    std::fs::create_dir_all(&spill_dir).with_context(|| format!("create spill dir {spill_dir}"))?;

    // DataFusion: bounded memory pool, spill to the per-replica anonymous volume.
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_mb * 1024 * 1024, 1.0)
        .with_temp_file_path(&spill_dir)
        .build_arc()
        .context("build DataFusion runtime")?;
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    info!(memory_mb, spill_dir, "datafusion session ready");

    // Register the Iceberg catalog as `lake`. If Lakekeeper is down at startup, retry at
    // the next job instead of exiting.
    let catalog = build_catalog(&catalog_uri, &warehouse).await?;
    let mut lake = register_lake(&ctx, &catalog).await;

    let mut db = JobsDb { url: jobs_url, client: None };
    db.client().await?.batch_execute(MIGRATE_SQL).await.context("migrate jobs.job_queue")?;
    info!(poll_secs = poll.as_secs(), lease_secs, worker_id, "lake-worker started");

    let mut last_reap: Option<Instant> = None;
    let mut shutdown = std::pin::pin!(shutdown_signal());
    loop {
        let claimed = tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; exiting");
                break;
            }
            claimed = poll_once(&mut db, &worker_id, lease_secs, heartbeat_every, &mut last_reap) => claimed,
        };
        match claimed {
            Ok(Some(job)) => {
                // A claimed job always finishes (stop_grace_period gives it 2 min on SIGTERM).
                if lake.is_none() {
                    lake = register_lake(&ctx, &catalog).await;
                }
                let outcome = match db.client().await {
                    Ok(client) => run_with_heartbeat(client, &ctx, lake.as_deref(), &job, heartbeat_every).await,
                    Err(err) => Some(Err(err)),
                };
                match outcome {
                    Some(result) => {
                        if let Err(err) = finish_job(&mut db, &job, result, backoff_secs).await {
                            error!(id = job.id, error = %format!("{err:#}"), "could not record job outcome");
                        }
                    }
                    None => warn!(id = job.id, "lease lost while running; job abandoned to its new owner"),
                }
                continue;
            }
            Ok(None) => info!("polling jobs.job_queue: idle"),
            Err(err) => error!(error = %format!("{err:#}"), "polling the queue failed; retrying after poll interval"),
        }
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; exiting");
                break;
            }
            _ = tokio::time::sleep(poll) => {}
        }
    }
    Ok(())
}
