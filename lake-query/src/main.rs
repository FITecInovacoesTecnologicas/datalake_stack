//! lake-query: Arrow Flight SQL endpoint over DataFusion.
//!
//! Scaffold: `get_flight_info_statement` and `do_get_statement` execute SQL
//! through DataFusion and stream Arrow record batches; every other Flight SQL
//! method keeps the default `unimplemented` answer. The Iceberg REST catalog
//! from the compose environment is registered as the `lake` catalog when it is
//! reachable at startup.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use iceberg_datafusion::IcebergCatalogProvider;
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Resolves on SIGTERM or SIGINT so tonic drains in-flight requests before exit.
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
    info!("shutdown signal received; draining");
}

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
    if let Ok(mode) = std::env::var("LAKE_ACCESS_DELEGATION") {
        props.insert("header.X-Iceberg-Access-Delegation".to_string(), mode);
    }
    let catalog = RestCatalogBuilder::default()
        .load("lake", props)
        .await
        .context("load Iceberg REST catalog")?;
    Ok(Arc::new(catalog))
}

#[derive(Clone)]
struct LakeQueryService {
    ctx: Arc<SessionContext>,
}

fn invalid(err: impl std::fmt::Display) -> Status {
    Status::invalid_argument(err.to_string())
}

#[tonic::async_trait]
impl FlightSqlService for LakeQueryService {
    type FlightService = LakeQueryService;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query;
        let df = self.ctx.sql(&sql).await.map_err(invalid)?;
        let schema = df.schema().as_arrow().clone();
        let ticket = TicketStatementQuery { statement_handle: sql.into_bytes().into() };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket: ticket.as_any().encode_to_vec().into() });
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|err| Status::internal(err.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec()).map_err(invalid)?;
        let df = self.ctx.sql(&sql).await.map_err(invalid)?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df
            .execute_stream()
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .map_err(|err| FlightError::ExternalError(Box::new(err)));
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches)
            .map_err(Status::from);
        let stream: <Self as FlightService>::DoGetStream = Box::pin(flight);
        Ok(Response::new(stream))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let listen = env_or("LAKE_QUERY_LISTEN", "0.0.0.0:50051");
    let memory_mb: usize =
        env_or("LAKE_MEMORY_LIMIT_MB", "2800").parse().context("LAKE_MEMORY_LIMIT_MB must be an integer")?;
    let spill_dir = env_or("LAKE_SPILL_DIR", "/spill");
    let metadata_ttl: u64 =
        env_or("LAKE_METADATA_CACHE_TTL_SECS", "30").parse().context("LAKE_METADATA_CACHE_TTL_SECS must be an integer")?;
    std::fs::create_dir_all(&spill_dir).with_context(|| format!("create spill dir {spill_dir}"))?;

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_mb * 1024 * 1024, 1.0)
        .with_temp_file_path(&spill_dir)
        .build_arc()
        .context("build DataFusion runtime")?;
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_information_schema(true))
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    let ctx = Arc::new(SessionContext::new_with_state(state));
    info!(memory_mb, spill_dir, metadata_ttl, "datafusion session ready");

    // Register the Iceberg catalog as `lake` when reachable; the endpoint still serves plain SQL otherwise.
    match (std::env::var("LAKE_CATALOG_URI"), std::env::var("LAKE_WAREHOUSE")) {
        (Ok(uri), Ok(warehouse)) => match build_catalog(&uri, &warehouse).await {
            Ok(catalog) => match IcebergCatalogProvider::try_new(catalog).await {
                Ok(provider) => {
                    ctx.register_catalog("lake", Arc::new(provider));
                    info!("iceberg catalog registered as `lake`");
                }
                Err(err) => warn!(error = %err, "iceberg catalog provider not available; serving without it"),
            },
            Err(err) => warn!(error = %err, "iceberg catalog not reachable; serving without it"),
        },
        _ => warn!("LAKE_CATALOG_URI / LAKE_WAREHOUSE not set; serving without the iceberg catalog"),
    }

    let addr = listen.parse().with_context(|| format!("parse LAKE_QUERY_LISTEN {listen}"))?;
    let service = LakeQueryService { ctx };
    info!(%addr, "lake-query serving Arrow Flight SQL");
    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("flight sql server")?;
    Ok(())
}
