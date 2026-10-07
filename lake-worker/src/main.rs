//! lake-worker: claims queued jobs from Postgres (`jobs.job_queue`) and runs
//! DataFusion transformations over Iceberg tables registered in the REST catalog.
//!
//! Scaffold: the transformation hook (`run_job`) runs a trivial query. Real
//! raw -> clean -> curated logic goes there. Everything else (environment
//! contract, memory pool, spill dir, catalog connection, polling with
//! `FOR UPDATE SKIP LOCKED`, SIGTERM handling) is the production shape.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

/// Claim exactly one queued job; concurrent workers never pick the same row.
const CLAIM_SQL: &str = "UPDATE jobs.job_queue \
     SET status = 'running', claimed_at = now(), attempts = attempts + 1 \
     WHERE id = (SELECT id FROM jobs.job_queue \
                 WHERE status = 'queued' AND available_at <= now() \
                 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) \
     RETURNING id, kind, payload";
const DONE_SQL: &str = "UPDATE jobs.job_queue SET status = 'done', finished_at = now() WHERE id = $1";
const FAILED_SQL: &str =
    "UPDATE jobs.job_queue SET status = 'failed', finished_at = now(), last_error = $2 WHERE id = $1";

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} must be set"))
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
    for (env, key) in [
        ("LAKE_S3_ENDPOINT", "s3.endpoint"),
        ("LAKE_S3_REGION", "s3.region"),
        ("LAKE_S3_ACCESS_KEY_ID", "s3.access.key.id"),
        ("LAKE_S3_SECRET_ACCESS_KEY", "s3.secret.access.key"),
        ("LAKE_S3_PATH_STYLE", "s3.path.style.access"),
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
        .load("lake", props)
        .await
        .context("load Iceberg REST catalog")?;
    Ok(Arc::new(catalog))
}

/// Transformation hook. Replace the trivial query with the raw -> clean -> curated job for `kind`.
async fn run_job(ctx: &SessionContext, kind: &str, payload: &serde_json::Value) -> Result<()> {
    info!(kind, payload = %payload, "running job");
    let df = ctx.sql("SELECT 1 AS ok").await.context("plan hook query")?;
    df.collect().await.context("execute hook query")?;
    Ok(())
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
    let poll = Duration::from_secs(
        env_or("POLL_INTERVAL_SECS", "5").parse::<u64>().context("POLL_INTERVAL_SECS must be an integer")?,
    );
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

    let catalog = build_catalog(&catalog_uri, &warehouse).await?;
    match catalog.list_namespaces(None).await {
        Ok(namespaces) => info!(count = namespaces.len(), "iceberg catalog reachable"),
        Err(err) => warn!(error = %err, "iceberg catalog not reachable yet; continuing"),
    }

    let (client, connection) = tokio_postgres::connect(&jobs_url, NoTls).await.context("connect to jobs database")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            error!(error = %err, "jobs database connection closed");
        }
    });
    info!(poll_secs = poll.as_secs(), "lake-worker started");

    let mut shutdown = std::pin::pin!(shutdown_signal());
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; exiting");
                break;
            }
            claimed = client.query_opt(CLAIM_SQL, &[]) => match claimed {
                Ok(Some(row)) => {
                    let id: i64 = row.get(0);
                    let kind: String = row.get(1);
                    let payload: serde_json::Value = row.get(2);
                    match run_job(&ctx, &kind, &payload).await {
                        Ok(()) => {
                            client.execute(DONE_SQL, &[&id]).await.context("mark job done")?;
                            info!(id, kind, "job done");
                        }
                        Err(err) => {
                            let message = format!("{err:#}");
                            client.execute(FAILED_SQL, &[&id, &message]).await.context("mark job failed")?;
                            error!(id, kind, error = %message, "job failed");
                        }
                    }
                }
                Ok(None) => {
                    info!("polling jobs.job_queue: idle");
                    tokio::select! {
                        _ = &mut shutdown => {
                            info!("shutdown signal received; exiting");
                            break;
                        }
                        _ = tokio::time::sleep(poll) => {}
                    }
                }
                Err(err) => {
                    error!(error = %err, "claiming a job failed; retrying after poll interval");
                    tokio::time::sleep(poll).await;
                }
            },
        }
    }
    Ok(())
}
