//! The `lake` catalog: the Iceberg REST client, the DataFusion view of it, and the table
//! helpers that job kinds share (validate a table name, create a table from an Arrow
//! schema, append a batch).

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::SessionContext;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tracing::info;

/// Builds the Iceberg REST catalog client from the compose environment (x-lake-client-env).
pub async fn build_catalog(uri: &str, warehouse: &str) -> Result<Arc<dyn Catalog>> {
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
pub struct LakeCatalogProvider {
    catalog: Arc<dyn Catalog>,
    inner: RwLock<Arc<IcebergCatalogProvider>>,
}

impl LakeCatalogProvider {
    pub async fn try_new(catalog: Arc<dyn Catalog>) -> Result<Self> {
        let inner = IcebergCatalogProvider::try_new(catalog.clone()).await.context("list Iceberg namespaces")?;
        Ok(Self { catalog, inner: RwLock::new(Arc::new(inner)) })
    }

    pub async fn refresh(&self) -> Result<()> {
        let fresh = IcebergCatalogProvider::try_new(self.catalog.clone()).await.context("refresh Iceberg catalog")?;
        *self.inner.write().expect("catalog lock poisoned") = Arc::new(fresh);
        Ok(())
    }

    fn current(&self) -> Arc<IcebergCatalogProvider> {
        self.inner.read().expect("catalog lock poisoned").clone()
    }

    /// Arrow schema of `table`, or `None` when the table (or its namespace) does not exist.
    pub async fn table_schema(&self, table: &TableName) -> Result<Option<SchemaRef>> {
        let namespace = table.namespace_ident();
        if !self.catalog.namespace_exists(&namespace).await.context("check namespace")? {
            return Ok(None);
        }
        let ident = table.ident();
        if !self.catalog.table_exists(&ident).await.with_context(|| format!("check table {table}"))? {
            return Ok(None);
        }
        let loaded = self.catalog.load_table(&ident).await.with_context(|| format!("load table {table}"))?;
        let schema = schema_to_arrow_schema(loaded.metadata().current_schema())
            .with_context(|| format!("convert schema of {table}"))?;
        Ok(Some(Arc::new(schema)))
    }

    /// Creates `table` (and its namespace, if needed) with the given Arrow schema, then
    /// refreshes the DataFusion view. A concurrent creator winning the race is not an error.
    pub async fn create_table(&self, table: &TableName, schema: &ArrowSchema) -> Result<()> {
        let namespace = table.namespace_ident();
        if !self.catalog.namespace_exists(&namespace).await.context("check namespace")? {
            if let Err(err) = self.catalog.create_namespace(&namespace, HashMap::new()).await {
                if !self.catalog.namespace_exists(&namespace).await.unwrap_or(false) {
                    return Err(err).with_context(|| format!("create namespace {}", table.namespace));
                }
            }
        }
        let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(schema)
            .with_context(|| format!("convert schema for {table}"))?;
        let creation = TableCreation::builder().name(table.table.clone()).schema(iceberg_schema).build();
        if let Err(err) = self.catalog.create_table(&namespace, creation).await {
            if !self.catalog.table_exists(&table.ident()).await.unwrap_or(false) {
                return Err(err).with_context(|| format!("create table {table}"));
            }
        }
        info!(table = %table, columns = schema.fields().len(), "created iceberg table");
        self.refresh().await
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

/// True for a lower-case SQL identifier: `[a-z_][a-z0-9_]*`, at most 63 characters.
/// Names that pass are safe to put, quoted, into generated SQL.
pub fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('a'..='z' | '_'))
        && name.len() <= 63
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
}

/// Quotes an identifier for DataFusion SQL.
pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A table of the `lake` catalog, written `<namespace>.<table>` in job payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    pub namespace: String,
    pub table: String,
}

impl TableName {
    /// Parses `<namespace>.<table>`; both parts must be lower-case identifiers.
    pub fn parse(text: &str) -> std::result::Result<Self, String> {
        match text.split('.').collect::<Vec<_>>().as_slice() {
            [namespace, table] if is_identifier(namespace) && is_identifier(table) => {
                Ok(Self { namespace: namespace.to_string(), table: table.to_string() })
            }
            _ => Err(format!(
                "table {text:?} must be <namespace>.<table>, each part matching [a-z_][a-z0-9_]* (e.g. raw.vendas)"
            )),
        }
    }

    fn namespace_ident(&self) -> NamespaceIdent {
        NamespaceIdent::new(self.namespace.clone())
    }

    fn ident(&self) -> TableIdent {
        TableIdent::new(self.namespace_ident(), self.table.clone())
    }

    /// Fully qualified, quoted reference for SQL: `"lake"."raw"."vendas"`.
    pub fn sql(&self) -> String {
        format!("{}.{}.{}", quote("lake"), quote(&self.namespace), quote(&self.table))
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lake.{}.{}", self.namespace, self.table)
    }
}

/// Appends the given columns of `batch` to `table` with `INSERT ... SELECT`, so DataFusion
/// casts each column to the table's type. Table columns not listed are written as NULL.
/// `staging` names the temporary in-memory table (unique per job). Returns rows written.
pub async fn append_batch(
    ctx: &SessionContext,
    table: &TableName,
    batch: RecordBatch,
    columns: &[String],
    staging: &str,
) -> Result<u64> {
    let provider = MemTable::try_new(batch.schema(), vec![vec![batch]]).context("build staging table")?;
    ctx.register_table(staging, Arc::new(provider)).context("register staging table")?;
    let list = columns.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO {} ({list}) SELECT {list} FROM {}", table.sql(), quote(staging));
    let result = async { ctx.sql(&sql).await?.collect().await }.await;
    ctx.deregister_table(staging).context("deregister staging table")?;
    let batches = result.with_context(|| format!("append to {table}"))?;
    Ok(written_rows(&batches).unwrap_or_default())
}

/// Rows written by a DML statement (`INSERT`): DataFusion answers with a single `count`
/// row. `None` for any other result (a `SELECT`, DDL).
pub fn written_rows(batches: &[RecordBatch]) -> Option<u64> {
    let schema = batches.first()?.schema();
    if schema.fields().len() != 1 || schema.field(0).name() != "count" {
        return None;
    }
    let mut total = 0;
    for batch in batches {
        let column = batch.column(0).as_any().downcast_ref::<UInt64Array>()?;
        total += column.iter().flatten().sum::<u64>();
    }
    Some(total)
}

/// Runs a query whose result is a single integer (e.g. `SELECT count(*) ...`).
pub async fn query_count(ctx: &SessionContext, sql: &str) -> Result<i64> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let batch = batches.iter().find(|b| b.num_rows() > 0).context("count query returned no rows")?;
    let column = batch.column(0);
    let value = datafusion::arrow::compute::cast(column, &datafusion::arrow::datatypes::DataType::Int64)?;
    let value = value
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .context("count is not an integer")?;
    Ok(if value.is_null(0) { 0 } else { value.value(0) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names() {
        assert_eq!(
            TableName::parse("raw.dolar_ptax").unwrap(),
            TableName { namespace: "raw".into(), table: "dolar_ptax".into() }
        );
        assert_eq!(TableName::parse("raw.vendas").unwrap().sql(), r#""lake"."raw"."vendas""#);
        for bad in ["vendas", "lake.raw.vendas", "Raw.vendas", "raw.1x", "raw.ven-das", "raw.", "raw.x\"y"] {
            assert!(TableName::parse(bad).is_err(), "{bad} should be rejected");
        }
    }
}
