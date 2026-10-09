//! `ingest_http`: reads JSON records from an external HTTPS API and appends them to a raw
//! Iceberg table.
//!
//! Payload:
//! ```json
//! {
//!   "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/20?formato=json",
//!   "table": "raw.dolar_ptax",
//!   "records_path": ""
//! }
//! ```
//! - `url`: `https://` on a host listed in `INGEST_HTTP_ALLOWED_HOSTS`.
//! - `table`: `<namespace>.<table>` in the `lake` catalog. Created on the first load with the
//!   types inferred from the data (see `records.rs`); an existing table keeps its schema and
//!   DataFusion casts each value to it.
//! - `records_path` (optional): JSON Pointer to the records inside the response, e.g.
//!   `/data/items`. Empty (default) means the response itself. The target must be an array of
//!   objects, or a single object (one row).
//!
//! Each row also gets three metadata columns: `_job_id` (BIGINT, the queue id),
//! `_ingested_at` (TIMESTAMPTZ) and `_source_url` (VARCHAR).
//!
//! Idempotency: an Iceberg append is atomic, so a job's rows are either all in the table or
//! none. Before fetching, the job checks for rows with its own `_job_id`; if there are any, an
//! earlier attempt already committed and the job ends as `done` without loading again. Each
//! new job (e.g. one per pg_cron run) appends a new, tagged load: the raw layer keeps every
//! load and the clean layer de-duplicates.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use serde::Deserialize;
use serde_json::{Map, Value};
use tracing::{info, warn};

use super::{permanent, JobContext, JobKind};
use crate::lake::{append_batch, query_count, quote, TableName};
use crate::records;

pub const JOB_ID: &str = "_job_id";
pub const INGESTED_AT: &str = "_ingested_at";
pub const SOURCE_URL: &str = "_source_url";

pub struct IngestHttp;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload {
    url: String,
    table: String,
    #[serde(default)]
    records_path: String,
}

impl JobKind for IngestHttp {
    const KIND: &'static str = "ingest_http";
    type Payload = Payload;

    async fn run(cx: &JobContext<'_>, payload: Payload) -> Result<()> {
        let table = TableName::parse(&payload.table).map_err(permanent)?;
        let lake = cx.lake().await?;

        // 1. Idempotency: did an earlier attempt of this job already commit?
        let existing = lake.table_schema(&table).await?;
        if let Some(schema) = &existing {
            if schema.field_with_name(JOB_ID).is_err() {
                return Err(permanent(format!("{table} has no {JOB_ID} BIGINT column; ingest_http needs it")));
            }
            let sql = format!("SELECT count(*) FROM {} WHERE {} = {}", table.sql(), quote(JOB_ID), cx.id);
            let already = query_count(cx.session, &sql).await?;
            if already > 0 {
                info!(id = cx.id, table = %table, rows = already, "already loaded by an earlier attempt; skipping");
                return Ok(());
            }
        }

        // 2. Fetch and convert.
        let body = cx.shared.http.get_json(&payload.url).await?;
        let records = extract_records(&body, &payload.records_path)?;
        if records.is_empty() {
            info!(id = cx.id, url = %payload.url, "source returned no records; nothing to load");
            return Ok(());
        }
        let data = records::to_batch(&records, &[JOB_ID, INGESTED_AT, SOURCE_URL])?;
        let batch = with_metadata(&data, cx.id, &payload.url, SystemTime::now())?;

        // 3. Create the table on the first load; otherwise keep its schema.
        let schema = match existing {
            Some(schema) => schema,
            None => {
                lake.create_table(&table, batch.schema().as_ref()).await?;
                batch.schema()
            }
        };
        let (columns, ignored): (Vec<String>, Vec<String>) = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .partition(|name| schema.field_with_name(name).is_ok());
        if !ignored.is_empty() {
            warn!(id = cx.id, table = %table, ?ignored, "source fields missing from the table were not loaded");
        }

        // 4. Append (one atomic Iceberg commit).
        let staging = format!("__ingest_{}", cx.id);
        let rows = append_batch(cx.session, &table, batch, &columns, &staging).await?;
        info!(id = cx.id, table = %table, rows, columns = columns.len(), url = %payload.url, "ingested records");
        Ok(())
    }
}

/// The records at `path` (a JSON Pointer; empty = the whole body).
fn extract_records(body: &Value, path: &str) -> Result<Vec<Map<String, Value>>> {
    if !path.is_empty() && !path.starts_with('/') {
        return Err(permanent(format!("records_path {path:?} must be a JSON Pointer such as /data/items")));
    }
    let node = body
        .pointer(path)
        .ok_or_else(|| permanent(format!("records_path {path:?} not found in the response")))?;
    match node {
        Value::Object(record) => Ok(vec![record.clone()]),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(i, item)| match item {
                Value::Object(record) => Ok(record.clone()),
                other => Err(permanent(format!(
                    "record {i} at records_path {path:?} is not a JSON object: {}",
                    truncate(&other.to_string())
                ))),
            })
            .collect(),
        other => Err(permanent(format!(
            "records_path {path:?} points to neither an array nor an object: {}",
            truncate(&other.to_string())
        ))),
    }
}

fn truncate(text: &str) -> String {
    match text.char_indices().nth(80) {
        Some((at, _)) => format!("{}...", &text[..at]),
        None => text.to_string(),
    }
}

/// Prepends `_job_id`, `_ingested_at` and `_source_url` to the data columns.
fn with_metadata(data: &RecordBatch, job_id: i64, url: &str, at: SystemTime) -> Result<RecordBatch> {
    let rows = data.num_rows();
    let micros = at.duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or_default();
    let mut fields = vec![
        Field::new(JOB_ID, DataType::Int64, true),
        Field::new(INGESTED_AT, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new(SOURCE_URL, DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![job_id; rows])),
        Arc::new(TimestampMicrosecondArray::from(vec![micros; rows]).with_timezone("UTC")),
        Arc::new(StringArray::from(vec![url; rows])),
    ];
    fields.extend(data.schema().fields().iter().map(|f| f.as_ref().clone()));
    columns.extend(data.columns().iter().cloned());
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::is_permanent;
    use serde_json::json;

    #[test]
    fn records_at_root_or_pointer() {
        let root = json!([{"data": "01/10/2026", "valor": "5.1"}, {"data": "02/10/2026", "valor": "5.2"}]);
        assert_eq!(extract_records(&root, "").unwrap().len(), 2);
        let nested = json!({"value": [{"a": 1}], "count": 1});
        assert_eq!(extract_records(&nested, "/value").unwrap().len(), 1);
        assert_eq!(extract_records(&json!({"a": 1}), "").unwrap().len(), 1);
        assert_eq!(extract_records(&json!([]), "").unwrap().len(), 0);
    }

    #[test]
    fn bad_records_are_permanent() {
        for (body, path) in [
            (json!({"value": []}), "/missing"),
            (json!({"value": []}), "value"),
            (json!([1, 2]), ""),
            (json!({"value": "x"}), "/value"),
        ] {
            let err = extract_records(&body, path).unwrap_err();
            assert!(is_permanent(&err), "{body} {path}: {err}");
        }
    }

    #[test]
    fn metadata_columns_come_first() {
        let data = records::to_batch(
            &[json!({"valor": "5.1"}).as_object().unwrap().clone()],
            &[JOB_ID, INGESTED_AT, SOURCE_URL],
        )
        .unwrap();
        let batch = with_metadata(&data, 42, "https://x.test/", SystemTime::now()).unwrap();
        let names: Vec<String> = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec![JOB_ID, INGESTED_AT, SOURCE_URL, "valor"]);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn payload_rejects_unknown_fields() {
        let ok: Payload = serde_json::from_value(json!({"url": "https://x.test/", "table": "raw.x"})).unwrap();
        assert_eq!(ok.records_path, "");
        assert!(serde_json::from_value::<Payload>(json!({"url": "https://x.test/", "tabel": "raw.x"})).is_err());
    }
}
