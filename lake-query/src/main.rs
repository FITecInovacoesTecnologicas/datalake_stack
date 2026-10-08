//! lake-query: read-only Arrow Flight SQL endpoint over DataFusion.
//!
//! - Statements (`GetFlightInfo` + `DoGet`) run through DataFusion with DDL, DML and
//!   `SET`-style statements rejected: the lake is written only by lake-worker jobs.
//! - Metadata calls used by JDBC/ADBC tools (catalogs, schemas, tables, table types,
//!   SQL info) are answered from the DataFusion catalog list.
//! - Every call needs `LAKE_QUERY_TOKEN`, as `authorization: Bearer <token>` or as the
//!   password of a Basic handshake (any user name), which answers with the bearer token.
//! - The Iceberg REST catalog is registered as `lake`; its namespace/table listing is
//!   refreshed when older than `LAKE_METADATA_CACHE_TTL_SECS` (table data is always current).

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket};
use base64::prelude::{Engine, BASE64_STANDARD};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableType;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use futures::{Stream, TryStreamExt};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
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
/// created after startup become visible. Read-only: `register_schema` keeps the default
/// (not supported) answer.
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
}

/// Registration state of the `lake` catalog; refreshed lazily before each request.
struct LakeState {
    catalog: Option<Arc<dyn Catalog>>,
    provider: Option<Arc<LakeCatalogProvider>>,
    refreshed: Instant,
}

static SQL_INFO: LazyLock<SqlInfoData> = LazyLock::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "lake-query");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::FlightSqlServerSubstrait, false);
    builder.append(SqlInfo::FlightSqlServerTransaction, 0_i32);
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    builder.append(SqlInfo::SqlDdlCatalog, false);
    builder.append(SqlInfo::SqlDdlSchema, false);
    builder.append(SqlInfo::SqlDdlTable, false);
    builder.build().expect("static SQL info")
});

#[derive(Clone)]
struct LakeQueryService {
    ctx: Arc<SessionContext>,
    lake: Arc<tokio::sync::Mutex<LakeState>>,
    metadata_ttl: Duration,
    token: Arc<str>,
}

fn invalid(err: impl std::fmt::Display) -> Status {
    Status::invalid_argument(err.to_string())
}

fn internal(err: impl std::fmt::Display) -> Status {
    Status::internal(err.to_string())
}

/// The lake is written only by jobs: reject DDL, DML and statements such as SET.
fn read_only() -> SQLOptions {
    SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false)
}

/// Constant-time comparison so the token cannot be guessed byte by byte from timings.
fn same_secret(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Accepts `Bearer <token>` or `Basic base64(<any user>:<token>)`.
fn authorized(token: &str, header: Option<&str>) -> bool {
    match header {
        Some(value) if value.starts_with("Bearer ") => same_secret(value["Bearer ".len()..].as_bytes(), token.as_bytes()),
        Some(value) if value.starts_with("Basic ") => BASE64_STANDARD
            .decode(&value["Basic ".len()..])
            .ok()
            .and_then(|raw| String::from_utf8(raw).ok())
            .and_then(|pair| pair.split_once(':').map(|(_, password)| same_secret(password.as_bytes(), token.as_bytes())))
            .unwrap_or(false),
        _ => false,
    }
}

fn one_batch_stream(schema: SchemaRef, batch: Result<RecordBatch, FlightError>) -> <LakeQueryService as FlightService>::DoGetStream {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::once(async { batch }))
        .map_err(Status::from);
    Box::pin(stream)
}

fn flight_info(schema: &Schema, ticket: Vec<u8>, descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket));
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(internal)?
        .with_endpoint(endpoint)
        .with_descriptor(descriptor);
    Ok(Response::new(info))
}

impl LakeQueryService {
    /// Registers the `lake` catalog if it is missing and refreshes its listing once it is
    /// older than the metadata TTL. Failures are logged; the previous listing stays in use.
    async fn refresh_lake(&self) {
        let mut state = self.lake.lock().await;
        let Some(catalog) = state.catalog.clone() else { return };
        match state.provider.clone() {
            None => match LakeCatalogProvider::try_new(catalog).await {
                Ok(provider) => {
                    let provider = Arc::new(provider);
                    self.ctx.register_catalog("lake", provider.clone());
                    state.provider = Some(provider);
                    state.refreshed = Instant::now();
                    info!("iceberg catalog registered as `lake`");
                }
                Err(err) => warn!(error = %format!("{err:#}"), "iceberg catalog not reachable; serving without it"),
            },
            Some(provider) if state.refreshed.elapsed() >= self.metadata_ttl => match provider.refresh().await {
                Ok(()) => state.refreshed = Instant::now(),
                Err(err) => warn!(error = %format!("{err:#}"), "iceberg catalog refresh failed; keeping the previous listing"),
            },
            Some(_) => {}
        }
    }

    async fn plan(&self, sql: &str) -> Result<datafusion::dataframe::DataFrame, Status> {
        self.refresh_lake().await;
        match self.ctx.sql_with_options(sql, read_only()).await {
            // DataFusion reports an unknown table only through this plan error. The table
            // may be newer than the cached listing: refresh now and plan once more.
            Err(err) if err.to_string().contains("not found") && self.force_refresh_lake().await => {
                self.ctx.sql_with_options(sql, read_only()).await.map_err(invalid)
            }
            result => result.map_err(invalid),
        }
    }

    /// Refreshes the listing regardless of its age; true when it was refreshed.
    async fn force_refresh_lake(&self) -> bool {
        let mut state = self.lake.lock().await;
        let Some(provider) = state.provider.clone() else { return false };
        match provider.refresh().await {
            Ok(()) => {
                state.refreshed = Instant::now();
                true
            }
            Err(err) => {
                warn!(error = %format!("{err:#}"), "iceberg catalog refresh failed; keeping the previous listing");
                false
            }
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for LakeQueryService {
    type FlightService = LakeQueryService;

    /// Basic-auth handshake for JDBC/ADBC clients: the password is the token (checked by
    /// the interceptor); the reply carries it back as the bearer token for later calls.
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status> {
        let reply = HandshakeResponse { protocol_version: 0, payload: self.token.as_bytes().to_vec().into() };
        let stream: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(futures::stream::iter([Ok(reply)]));
        let mut response = Response::new(stream);
        let bearer = MetadataValue::try_from(format!("Bearer {}", self.token)).map_err(internal)?;
        response.metadata_mut().insert("authorization", bearer);
        Ok(response)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query;
        let df = self.plan(&sql).await?;
        let schema = df.schema().as_arrow().clone();
        let ticket = TicketStatementQuery { statement_handle: sql.into_bytes().into() };
        flight_info(&schema, ticket.as_any().encode_to_vec(), request.into_inner())
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec()).map_err(invalid)?;
        let df = self.plan(&sql).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df
            .execute_stream()
            .await
            .map_err(internal)?
            .map_err(|err| FlightError::ExternalError(Box::new(err)));
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches)
            .map_err(Status::from);
        let stream: <Self as FlightService>::DoGetStream = Box::pin(flight);
        Ok(Response::new(stream))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        flight_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.refresh_lake().await;
        let mut builder = query.into_builder();
        let mut names = self.ctx.catalog_names();
        names.sort();
        for name in names {
            builder.append(name);
        }
        let schema = builder.schema();
        Ok(Response::new(one_batch_stream(schema, builder.build().map_err(FlightError::from))))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        flight_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.refresh_lake().await;
        let mut builder = query.into_builder();
        let mut names = self.ctx.catalog_names();
        names.sort();
        for catalog_name in names {
            let Some(catalog) = self.ctx.catalog(&catalog_name) else { continue };
            let mut schemas = catalog.schema_names();
            schemas.sort();
            for schema_name in schemas {
                builder.append(&catalog_name, schema_name);
            }
        }
        let schema = builder.schema();
        Ok(Response::new(one_batch_stream(schema, builder.build().map_err(FlightError::from))))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        flight_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.refresh_lake().await;
        let mut builder = query.into_builder();
        let mut names = self.ctx.catalog_names();
        names.sort();
        for catalog_name in names {
            let Some(catalog) = self.ctx.catalog(&catalog_name) else { continue };
            let mut schemas = catalog.schema_names();
            schemas.sort();
            for schema_name in schemas {
                let Some(schema) = catalog.schema(&schema_name) else { continue };
                let mut tables = schema.table_names();
                tables.sort();
                // Iceberg metadata tables (`orders$snapshots`, ...) stay queryable but unlisted.
                for table_name in tables.into_iter().filter(|name| !name.contains('$')) {
                    let Some(table) = schema.table(&table_name).await.map_err(internal)? else { continue };
                    let table_type = match table.table_type() {
                        TableType::Base => "TABLE",
                        TableType::View => "VIEW",
                        TableType::Temporary => "LOCAL TEMPORARY",
                    };
                    builder
                        .append(&catalog_name, &schema_name, &table_name, table_type, table.schema().as_ref())
                        .map_err(internal)?;
                }
            }
        }
        let schema = builder.schema();
        Ok(Response::new(one_batch_stream(schema, builder.build().map_err(FlightError::from))))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        flight_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        for table_type in ["LOCAL TEMPORARY", "TABLE", "VIEW"] {
            builder.append(table_type);
        }
        let schema = builder.schema();
        Ok(Response::new(one_batch_stream(schema, builder.build().map_err(FlightError::from))))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        flight_info(query.into_builder(&SQL_INFO).schema().as_ref(), ticket, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let builder = query.into_builder(&SQL_INFO);
        let schema = builder.schema();
        Ok(Response::new(one_batch_stream(schema, builder.build().map_err(FlightError::from))))
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
    let token: Arc<str> = std::env::var("LAKE_QUERY_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
        .context("LAKE_QUERY_TOKEN must be set: every Flight SQL call is authenticated")?
        .into();
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

    // The endpoint serves plain SQL even while the catalog is unreachable; each request
    // retries the registration.
    let catalog = match (std::env::var("LAKE_CATALOG_URI"), std::env::var("LAKE_WAREHOUSE")) {
        (Ok(uri), Ok(warehouse)) => match build_catalog(&uri, &warehouse).await {
            Ok(catalog) => Some(catalog),
            Err(err) => {
                warn!(error = %format!("{err:#}"), "iceberg catalog client not available; serving without it");
                None
            }
        },
        _ => {
            warn!("LAKE_CATALOG_URI / LAKE_WAREHOUSE not set; serving without the iceberg catalog");
            None
        }
    };
    let service = LakeQueryService {
        ctx,
        lake: Arc::new(tokio::sync::Mutex::new(LakeState { catalog, provider: None, refreshed: Instant::now() })),
        metadata_ttl: Duration::from_secs(metadata_ttl),
        token: token.clone(),
    };
    service.refresh_lake().await;

    let addr = listen.parse().with_context(|| format!("parse LAKE_QUERY_LISTEN {listen}"))?;
    info!(%addr, "lake-query serving Arrow Flight SQL (read-only, token required)");
    let check_auth = move |request: Request<()>| {
        let header = request.metadata().get("authorization").and_then(|value| value.to_str().ok());
        if authorized(&token, header) {
            Ok(request)
        } else {
            Err(Status::unauthenticated("missing or invalid authorization"))
        }
    };
    Server::builder()
        .add_service(FlightServiceServer::with_interceptor(service, check_auth))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("flight sql server")?;
    Ok(())
}
