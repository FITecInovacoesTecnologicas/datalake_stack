//! Job kinds: the contract every kind implements and the registry that routes
//! `jobs.job_queue.kind` to it. The queue runtime (claim, heartbeat, retry) lives in
//! `main.rs` and never changes when a kind is added.
//!
//! # Adding a kind (see docs/novos-jobs.md)
//!
//! 1. Create `jobs/<kind>.rs` with a unit struct that implements [`JobKind`]:
//!    a `KIND` name, a `Payload` struct (serde) and `async fn run`.
//! 2. Declare the module below and add one line to [`dispatch`] and to [`KINDS`].
//! 3. Make `run` safe to repeat: delivery is at-least-once.
//!
//! Inside `run`, return:
//! - `Ok(())` -> the job is marked `done`;
//! - `Err(permanent(..))` -> `failed` at once (bad input that retrying cannot fix);
//! - any other `Err` -> retried with backoff until `max_attempts`, then `failed`.

mod ingest_http;
mod noop;
mod sql;

use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::info;

use crate::http_source::HttpSource;
use crate::lake::LakeCatalogProvider;

pub use ingest_http::IngestHttp;
pub use noop::Noop;
pub use sql::Sql;

/// Every kind this worker runs (logged at startup). Keep in sync with [`dispatch`].
pub const KINDS: &[&str] = &[Noop::KIND, "smoke", Sql::KIND, IngestHttp::KIND];

/// Routes a claimed job to its kind. One line per kind.
pub async fn dispatch(cx: &JobContext<'_>, kind: &str, payload: &Value) -> Result<()> {
    info!(id = cx.id, kind, attempt = cx.attempt, payload = %payload, "running job");
    match kind {
        Noop::KIND | "smoke" => run::<Noop>(cx, payload).await,
        Sql::KIND => run::<Sql>(cx, payload).await,
        IngestHttp::KIND => run::<IngestHttp>(cx, payload).await,
        other => Err(permanent(format!("unknown job kind {other:?}; known kinds: {}", KINDS.join(", ")))),
    }
}

/// A job kind: the unit of business logic the worker can run.
pub trait JobKind {
    /// The value of `jobs.job_queue.kind` that selects this kind.
    const KIND: &'static str;

    /// The shape of `jobs.job_queue.payload`. Use `#[serde(deny_unknown_fields)]` so a typo
    /// in a field name fails the job instead of being silently ignored.
    type Payload: DeserializeOwned;

    /// Does the work. Must be idempotent: the same job can run more than once.
    async fn run(cx: &JobContext<'_>, payload: Self::Payload) -> Result<()>;
}

/// Parses the payload (a parse error is permanent) and runs the kind.
async fn run<K: JobKind>(cx: &JobContext<'_>, payload: &Value) -> Result<()> {
    let payload = serde_json::from_value::<K::Payload>(payload.clone())
        .map_err(|err| permanent(format!("invalid payload for kind {:?}: {err}", K::KIND)))?;
    K::run(cx, payload).await
}

/// Resources created once per worker process and shared by every job.
pub struct Shared {
    pub http: HttpSource,
}

/// What a job receives besides its payload.
pub struct JobContext<'a> {
    /// `jobs.job_queue.id`: stable across retries, useful as an idempotency key.
    pub id: i64,
    /// 1 on the first run, 2 on the first retry, and so on.
    pub attempt: i32,
    /// DataFusion session with the Iceberg catalog registered as `lake`.
    pub session: &'a SessionContext,
    pub shared: &'a Shared,
    lake: Option<&'a LakeCatalogProvider>,
}

impl<'a> JobContext<'a> {
    pub fn new(
        id: i64,
        attempt: i32,
        session: &'a SessionContext,
        lake: Option<&'a LakeCatalogProvider>,
        shared: &'a Shared,
    ) -> Self {
        Self { id, attempt, session, shared, lake }
    }

    /// The `lake` catalog, refreshed so namespaces and tables created by earlier jobs are
    /// visible. Fails (retryable) while Lakekeeper is unreachable.
    pub async fn lake(&self) -> Result<&'a LakeCatalogProvider> {
        let lake = self.lake.context("iceberg catalog not available")?;
        lake.refresh().await?;
        Ok(lake)
    }

    /// Runs one SQL statement against the session and returns its result.
    /// Call [`JobContext::lake`] first when the statement touches `lake` tables.
    /// For an `INSERT`, `lake::written_rows` gives the number of rows written.
    pub async fn sql(&self, statement: &str) -> Result<Vec<RecordBatch>> {
        self.session.sql(statement).await.context("plan")?.collect().await.context("execute")
    }
}

/// An error that retrying cannot fix (bad payload, unknown kind): the job fails at once.
#[derive(Debug)]
pub struct Permanent(String);

impl std::fmt::Display for Permanent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Permanent {}

pub fn permanent(message: impl Into<String>) -> anyhow::Error {
    Permanent(message.into()).into()
}

/// True when `err` (or anything in its chain) is [`Permanent`].
pub fn is_permanent(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| cause.is::<Permanent>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_are_unique() {
        let mut kinds = KINDS.to_vec();
        kinds.sort_unstable();
        kinds.dedup();
        assert_eq!(kinds.len(), KINDS.len());
    }

    #[test]
    fn permanent_survives_context() {
        let err = permanent("bad").context("outer");
        assert!(is_permanent(&err));
        assert!(!is_permanent(&anyhow::anyhow!("transient")));
    }
}
