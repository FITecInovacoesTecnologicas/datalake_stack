//! JSON records -> Arrow `RecordBatch`, for ingestion job kinds.
//!
//! Each record is a JSON object; each top-level key becomes a column. Column names are
//! normalised to lower-case SQL identifiers (`Data Ref.` -> `data_ref_`). Column types are
//! inferred from the values of this load:
//!
//! | values seen                    | Arrow type |
//! |--------------------------------|------------|
//! | only `true` / `false`          | Boolean    |
//! | only integers                  | Int64      |
//! | integers and decimals          | Float64    |
//! | strings, or a mix of types     | Utf8       |
//! | objects or arrays              | Utf8 (the JSON text) |
//! | only `null` (or key missing)   | Utf8       |
//!
//! Nested objects stay as JSON text: the raw layer keeps what the source sent and the clean
//! layer decides how to read it.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};

/// Lower-cases `key` and replaces anything outside `[a-z0-9_]` with `_`, so the column can be
/// used in SQL without quoting. A leading digit gets a `c_` prefix; an empty result is `col`.
pub fn column_name(key: &str) -> String {
    let mut name: String = key
        .trim()
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' { c } else { '_' }
        })
        .collect();
    if name.is_empty() {
        name = "col".to_string();
    }
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        name.insert_str(0, "c_");
    }
    name.truncate(56);
    name
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Null,
    Bool,
    Int,
    Float,
    Text,
}

impl Kind {
    fn of(value: &Value) -> Self {
        match value {
            Value::Null => Kind::Null,
            Value::Bool(_) => Kind::Bool,
            Value::Number(n) if n.is_i64() => Kind::Int,
            Value::Number(_) => Kind::Float,
            _ => Kind::Text,
        }
    }

    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (a, b) if a == b => a,
            (Kind::Null, b) => b,
            (a, Kind::Null) => a,
            (Kind::Int, Kind::Float) | (Kind::Float, Kind::Int) => Kind::Float,
            _ => Kind::Text,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Kind::Bool => DataType::Boolean,
            Kind::Int => DataType::Int64,
            Kind::Float => DataType::Float64,
            Kind::Null | Kind::Text => DataType::Utf8,
        }
    }
}

fn as_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Builds one batch from `records`. Column order follows the first appearance of each key.
/// `reserved` names (e.g. metadata columns the caller adds) are never produced: a source key
/// that normalises to one of them, or to a name already taken, gets a `_2`, `_3`... suffix.
pub fn to_batch(records: &[Map<String, Value>], reserved: &[&str]) -> Result<RecordBatch> {
    if records.is_empty() {
        bail!("no records to convert");
    }
    // Source key -> column index, with unique normalised names.
    let mut index: HashMap<&str, usize> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    let mut kinds: Vec<Kind> = Vec::new();
    for record in records {
        for (key, value) in record {
            let column = match index.get(key.as_str()) {
                Some(&column) => column,
                None => {
                    let base = column_name(key);
                    let mut name = base.clone();
                    let mut n = 2;
                    while reserved.contains(&name.as_str()) || names.contains(&name) {
                        name = format!("{base}_{n}");
                        n += 1;
                    }
                    names.push(name);
                    kinds.push(Kind::Null);
                    index.insert(key.as_str(), names.len() - 1);
                    names.len() - 1
                }
            };
            kinds[column] = kinds[column].merge(Kind::of(value));
        }
    }

    let mut keys: Vec<&str> = vec![""; names.len()];
    for (key, &column) in &index {
        keys[column] = key;
    }
    let mut fields = Vec::with_capacity(names.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(names.len());
    for (column, name) in names.iter().enumerate() {
        let key = keys[column];
        let values = records.iter().map(|r| r.get(key).unwrap_or(&Value::Null));
        let kind = kinds[column];
        let array: ArrayRef = match kind {
            Kind::Bool => Arc::new(values.map(Value::as_bool).collect::<BooleanArray>()),
            Kind::Int => Arc::new(values.map(Value::as_i64).collect::<Int64Array>()),
            Kind::Float => Arc::new(values.map(Value::as_f64).collect::<Float64Array>()),
            Kind::Null | Kind::Text => Arc::new(values.map(as_text).collect::<StringArray>()),
        };
        fields.push(Field::new(name, kind.data_type(), true));
        arrays.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use serde_json::json;

    fn objects(value: Value) -> Vec<Map<String, Value>> {
        value.as_array().unwrap().iter().map(|v| v.as_object().unwrap().clone()).collect()
    }

    #[test]
    fn names_are_sql_identifiers() {
        assert_eq!(column_name("valor"), "valor");
        assert_eq!(column_name("Data Ref."), "data_ref_");
        assert_eq!(column_name("2024"), "c_2024");
        assert_eq!(column_name("situação"), "situa__o");
        assert_eq!(column_name("  "), "col");
        assert!(crate::lake::is_identifier(&column_name("Qualquer Coisa-123")));
    }

    #[test]
    fn infers_types_per_column() {
        let batch = to_batch(
            &objects(json!([
                {"id": 1, "preco": 2,   "ativo": true,  "nome": "a", "misto": 1},
                {"id": 2, "preco": 2.5, "ativo": false, "nome": "b", "misto": "x", "novo": null}
            ])),
            &[],
        )
        .unwrap();
        let schema = batch.schema();
        let types: Vec<(&str, &DataType)> =
            schema.fields().iter().map(|f| (f.name().as_str(), f.data_type())).collect();
        assert_eq!(
            types,
            vec![
                ("id", &DataType::Int64),
                ("preco", &DataType::Float64),
                ("ativo", &DataType::Boolean),
                ("nome", &DataType::Utf8),
                ("misto", &DataType::Utf8),
                ("novo", &DataType::Utf8),
            ]
        );
        let misto = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((misto.value(0), misto.value(1)), ("1", "x"));
        assert_eq!(batch.column(5).null_count(), 2);
    }

    #[test]
    fn nested_values_become_json_text_and_missing_keys_null() {
        let batch = to_batch(
            &objects(json!([
                {"sigla": "AC", "regiao": {"id": 1, "nome": "Norte"}},
                {"sigla": "AL"}
            ])),
            &[],
        )
        .unwrap();
        let regiao = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(regiao.value(0), r#"{"id":1,"nome":"Norte"}"#);
        assert!(regiao.is_null(1));
    }

    #[test]
    fn reserved_and_colliding_names_get_a_suffix() {
        let batch = to_batch(&objects(json!([{"_job_id": 9, "A": 1, "a": 2}])), &["_job_id"]).unwrap();
        let names: Vec<String> = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["_job_id_2", "a", "a_2"]);
    }
}
