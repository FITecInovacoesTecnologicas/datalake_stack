//! `sql`: runs one or more SQL statements against the `lake` catalog, in order.
//!
//! Payload:
//! ```json
//! {"sql": "INSERT INTO lake.curated.x SELECT ..."}
//! {"sql": ["CREATE SCHEMA IF NOT EXISTS lake.raw", "CREATE TABLE IF NOT EXISTS lake.raw.x (...)"]}
//! ```
//!
//! Idempotency is the author's job: prefer `IF NOT EXISTS` and inserts that skip rows
//! already present (`WHERE NOT EXISTS` / anti-join).

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

use super::{JobContext, JobKind};
use crate::lake::written_rows;

pub struct Sql;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload {
    sql: Statements,
}

#[derive(Debug, Deserialize)]
#[serde(untagged, expecting = "a string or an array of strings")]
enum Statements {
    One(String),
    Many(Vec<String>),
}

impl JobKind for Sql {
    const KIND: &'static str = "sql";
    type Payload = Payload;

    async fn run(cx: &JobContext<'_>, payload: Payload) -> Result<()> {
        let statements = match payload.sql {
            Statements::One(sql) => vec![sql],
            Statements::Many(list) => list,
        };
        // Pick up namespaces and tables created since the last job.
        cx.lake().await?;
        for (index, sql) in statements.iter().enumerate() {
            let batches = cx.sql(sql).await.with_context(|| format!("statement {index}"))?;
            match written_rows(&batches) {
                Some(written) => info!(id = cx.id, statement = index, written, "statement done"),
                None => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    info!(id = cx.id, statement = index, rows, "statement done");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_shapes() {
        let one: Payload = serde_json::from_str(r#"{"sql": "select 1"}"#).unwrap();
        assert!(matches!(one.sql, Statements::One(_)));
        let many: Payload = serde_json::from_str(r#"{"sql": ["select 1", "select 2"]}"#).unwrap();
        assert!(matches!(many.sql, Statements::Many(ref v) if v.len() == 2));
        assert!(serde_json::from_str::<Payload>(r#"{"sql": 1}"#).is_err());
        assert!(serde_json::from_str::<Payload>(r#"{"sqll": "select 1"}"#).is_err());
        assert!(serde_json::from_str::<Payload>(r#"{}"#).is_err());
    }
}
