use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Float64Builder, Int64Array, Int64Builder,
    StringArray, StringBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use bicdb_core::{BicDb, BicDbError, Record};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, AnalyticsError>;

pub const SIDECAR_DIR: &str = "analytics";
pub const SIDECAR_EXTENSION: &str = "arrow";

#[derive(Debug, Error)]
pub enum AnalyticsError {
    #[error(transparent)]
    BicDb(#[from] BicDbError),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    DataFusion(#[from] DataFusionError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("sidecar verification failed: {0}")]
    SidecarVerification(String),

    #[error("analytics cache lock was poisoned")]
    CachePoisoned,
}

#[derive(Clone, Debug)]
pub struct ArrowTable {
    pub collection: String,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SidecarRebuildReport {
    pub collection: String,
    pub rows: usize,
    pub sidecar_bytes: u64,
    pub path: PathBuf,
    pub checksum: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SidecarVerifyReport {
    pub collection: String,
    pub canonical_rows: usize,
    pub sidecar_rows: usize,
    pub record_ids_match: bool,
    pub timestamps_match: bool,
    pub checksum_match: bool,
    pub verified: bool,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct AnalyticsQueryResult {
    pub batches: Vec<RecordBatch>,
}

impl AnalyticsQueryResult {
    pub fn columns(&self) -> Vec<String> {
        self.batches
            .first()
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn rows_as_json(&self) -> Vec<Value> {
        let mut rows = Vec::new();
        for batch in &self.batches {
            let fields = batch.schema().fields().clone();
            for row_idx in 0..batch.num_rows() {
                let mut row = serde_json::Map::new();
                for (column_idx, field) in fields.iter().enumerate() {
                    row.insert(
                        field.name().clone(),
                        arrow_value_to_json(batch.column(column_idx), row_idx),
                    );
                }
                rows.push(Value::Object(row));
            }
        }
        rows
    }

    pub fn to_csv(&self) -> String {
        let Some(first) = self.batches.first() else {
            return String::new();
        };
        let columns = first
            .schema()
            .fields()
            .iter()
            .map(|field| csv_escape(field.name()))
            .collect::<Vec<_>>();
        let mut csv = columns.join(",");
        csv.push('\n');
        for row in self.rows_as_json() {
            let Value::Object(object) = row else {
                continue;
            };
            let values = first
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let rendered = object
                        .get(field.name())
                        .map(json_to_cell)
                        .unwrap_or_default();
                    csv_escape(&rendered)
                })
                .collect::<Vec<_>>();
            csv.push_str(&values.join(","));
            csv.push('\n');
        }
        csv
    }
}

pub trait BicDbAnalyticsExt {
    fn to_record_batch(&self, collection_name: &str) -> Result<RecordBatch>;

    fn time_range_to_record_batch(
        &self,
        collection_name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<RecordBatch>;

    fn collection_to_arrow_table(&self, collection_name: &str) -> Result<ArrowTable>;
}

impl BicDbAnalyticsExt for BicDb {
    fn to_record_batch(&self, collection_name: &str) -> Result<RecordBatch> {
        let records = self.scan_collection(collection_name)?;
        records_to_record_batch(records.as_slice())
    }

    fn time_range_to_record_batch(
        &self,
        collection_name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<RecordBatch> {
        let records = self.scan_time_range(collection_name, start_ts, end_ts)?;
        records_to_record_batch(records.as_slice())
    }

    fn collection_to_arrow_table(&self, collection_name: &str) -> Result<ArrowTable> {
        let batch = self.to_record_batch(collection_name)?;
        Ok(ArrowTable {
            collection: collection_name.to_string(),
            schema: batch.schema(),
            rows: batch.num_rows(),
            batches: vec![batch],
        })
    }
}

#[derive(Debug)]
pub struct BicDataFusionContext<'db> {
    db: &'db BicDb,
    sidecar_root: Option<PathBuf>,
    collection_cache: Mutex<Option<Vec<CachedCollectionBatch>>>,
}

impl<'db> BicDataFusionContext<'db> {
    pub fn new(db: &'db BicDb) -> Self {
        let sidecar_root = db
            .stats()
            .ok()
            .map(|stats| default_sidecar_dir(&stats.path));
        Self {
            db,
            sidecar_root,
            collection_cache: Mutex::new(None),
        }
    }

    pub fn with_sidecar_root(db: &'db BicDb, sidecar_root: impl Into<PathBuf>) -> Self {
        Self {
            db,
            sidecar_root: Some(sidecar_root.into()),
            collection_cache: Mutex::new(None),
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<AnalyticsQueryResult> {
        let ctx = SessionContext::new();
        self.register_collections(&ctx)?;
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;
        Ok(AnalyticsQueryResult { batches })
    }

    fn register_collections(&self, ctx: &SessionContext) -> Result<()> {
        for collection in self.collection_batches()? {
            let batch = collection.batch;
            let schema = batch.schema();
            let table = MemTable::try_new(schema, vec![vec![batch]])?;
            ctx.register_table(collection.name, Arc::new(table))?;
        }
        Ok(())
    }

    fn collection_batches(&self) -> Result<Vec<CachedCollectionBatch>> {
        let mut cache = self
            .collection_cache
            .lock()
            .map_err(|_| AnalyticsError::CachePoisoned)?;
        if let Some(cached) = cache.as_ref() {
            return Ok(cached.clone());
        }

        let mut batches = Vec::new();
        for collection in self.db.collections() {
            let batch = if let Some(root) = self.sidecar_root.as_ref() {
                match read_sidecar_batch(root, &collection.name) {
                    Ok(batch) if batch.schema().as_ref() == analytics_schema().as_ref() => batch,
                    Err(_) => self.db.to_record_batch(&collection.name)?,
                    Ok(_) => self.db.to_record_batch(&collection.name)?,
                }
            } else {
                self.db.to_record_batch(&collection.name)?
            };
            batches.push(CachedCollectionBatch {
                name: collection.name,
                batch,
            });
        }
        *cache = Some(batches.clone());
        Ok(batches)
    }
}

#[derive(Clone, Debug)]
struct CachedCollectionBatch {
    name: String,
    batch: RecordBatch,
}

pub fn rebuild_sidecar(
    db: &BicDb,
    db_path: impl AsRef<Path>,
    collection: &str,
) -> Result<SidecarRebuildReport> {
    let root = default_sidecar_dir(db_path.as_ref());
    fs::create_dir_all(&root)?;
    let path = sidecar_path(&root, collection);
    let batch = db.to_record_batch(collection)?;
    write_sidecar_batch(&path, &batch)?;
    let checksum = batch_checksum(&batch);
    let sidecar_bytes = fs::metadata(&path)?.len();
    Ok(SidecarRebuildReport {
        collection: collection.to_string(),
        rows: batch.num_rows(),
        sidecar_bytes,
        path,
        checksum,
    })
}

pub fn verify_sidecar(
    db: &BicDb,
    db_path: impl AsRef<Path>,
    collection: &str,
) -> Result<SidecarVerifyReport> {
    let root = default_sidecar_dir(db_path.as_ref());
    let path = sidecar_path(&root, collection);
    let canonical = db.to_record_batch(collection)?;
    let sidecar = read_sidecar_batch(&root, collection)?;

    let canonical_ids = string_values(&canonical, "record_id");
    let sidecar_ids = string_values(&sidecar, "record_id");
    let canonical_timestamps = int_values(&canonical, "timestamp");
    let sidecar_timestamps = int_values(&sidecar, "timestamp");
    let canonical_checksum = batch_checksum(&canonical);
    let sidecar_checksum = batch_checksum(&sidecar);

    let record_ids_match = canonical_ids == sidecar_ids;
    let timestamps_match = canonical_timestamps == sidecar_timestamps;
    let checksum_match = canonical_checksum == sidecar_checksum;
    let verified = canonical.num_rows() == sidecar.num_rows()
        && record_ids_match
        && timestamps_match
        && checksum_match;

    if !verified {
        return Err(AnalyticsError::SidecarVerification(format!(
            "collection {collection} sidecar does not match canonical records"
        )));
    }

    Ok(SidecarVerifyReport {
        collection: collection.to_string(),
        canonical_rows: canonical.num_rows(),
        sidecar_rows: sidecar.num_rows(),
        record_ids_match,
        timestamps_match,
        checksum_match,
        verified,
        path,
    })
}

pub fn default_sidecar_dir(db_path: &Path) -> PathBuf {
    db_path.join(SIDECAR_DIR)
}

pub fn sidecar_path(sidecar_root: &Path, collection: &str) -> PathBuf {
    sidecar_root.join(format!("{collection}.{SIDECAR_EXTENSION}"))
}

fn records_to_record_batch(records: &[Record]) -> Result<RecordBatch> {
    let schema = analytics_schema();
    if records.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut record_id = StringBuilder::with_capacity(records.len(), records.len() * 16);
    let mut timestamp = Int64Builder::with_capacity(records.len());
    let mut device_id = StringBuilder::with_capacity(records.len(), records.len() * 16);
    let mut user_id = StringBuilder::with_capacity(records.len(), records.len() * 16);
    let mut metric = StringBuilder::with_capacity(records.len(), records.len() * 8);
    let mut value = Float64Builder::with_capacity(records.len());
    let mut metadata_json = StringBuilder::with_capacity(records.len(), records.len() * 64);

    for record in records {
        record_id.append_value(&record.id);
        append_optional_i64(&mut timestamp, record_timestamp(record));
        append_optional_str(&mut device_id, metadata_str(record, "device_id"));
        append_optional_str(&mut user_id, metadata_str(record, "user_id"));
        append_optional_str(&mut metric, metadata_str(record, "metric"));
        append_optional_f64(&mut value, metadata_f64(record, "value"));
        metadata_json.append_value(record.metadata.to_string());
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(record_id.finish()) as ArrayRef,
            Arc::new(timestamp.finish()) as ArrayRef,
            Arc::new(device_id.finish()) as ArrayRef,
            Arc::new(user_id.finish()) as ArrayRef,
            Arc::new(metric.finish()) as ArrayRef,
            Arc::new(value.finish()) as ArrayRef,
            Arc::new(metadata_json.finish()) as ArrayRef,
        ],
    )
    .map_err(Into::into)
}

fn analytics_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("record_id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, true),
            Field::new("device_id", DataType::Utf8, true),
            Field::new("user_id", DataType::Utf8, true),
            Field::new("metric", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
            Field::new("metadata_json", DataType::Utf8, false),
        ],
        HashMap::from([
            (
                "bicdb.analytics_schema_version".to_string(),
                "2".to_string(),
            ),
            (
                "bicdb.metadata_encoding".to_string(),
                "bicdb-record-metadata-json-v1".to_string(),
            ),
        ]),
    ))
}

fn write_sidecar_batch(path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = FileWriter::try_new(file, batch.schema_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}

fn read_sidecar_batch(sidecar_root: &Path, collection: &str) -> Result<RecordBatch> {
    let path = sidecar_path(sidecar_root, collection);
    let file = File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let batches = reader.collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    Ok(concat_batches(batches))
}

fn concat_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    let Some(first) = batches.into_iter().next() else {
        return RecordBatch::new_empty(analytics_schema());
    };
    first
}

fn append_optional_str(builder: &mut StringBuilder, value: Option<&str>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

fn append_optional_i64(builder: &mut Int64Builder, value: Option<i64>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

fn append_optional_f64(builder: &mut Float64Builder, value: Option<f64>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

fn record_timestamp(record: &Record) -> Option<i64> {
    record.timestamp.or_else(|| {
        record
            .metadata
            .get("timestamp")
            .and_then(serde_json::Value::as_i64)
    })
}

fn metadata_str<'a>(record: &'a Record, key: &str) -> Option<&'a str> {
    record.metadata.get(key).and_then(serde_json::Value::as_str)
}

fn metadata_f64(record: &Record, key: &str) -> Option<f64> {
    record.metadata.get(key).and_then(serde_json::Value::as_f64)
}

fn batch_checksum(batch: &RecordBatch) -> String {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut row = BTreeMap::new();
        for (column_idx, field) in batch.schema().fields().iter().enumerate() {
            row.insert(
                field.name().clone(),
                arrow_value_to_json(batch.column(column_idx), row_idx),
            );
        }
        rows.push(row);
    }
    let bytes = serde_json::to_vec(&rows).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

fn string_values(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
    let Some(array) = batch
        .column_by_name(column)
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
    else {
        return Vec::new();
    };
    (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx).to_string())
            }
        })
        .collect()
}

fn int_values(batch: &RecordBatch, column: &str) -> Vec<Option<i64>> {
    let Some(array) = batch
        .column_by_name(column)
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
    else {
        return Vec::new();
    };
    (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect()
}

fn arrow_value_to_json(array: &ArrayRef, row_idx: usize) -> Value {
    if array.is_null(row_idx) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| json!(array.value(row_idx)))
            .unwrap_or(Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| json!(array.value(row_idx)))
            .unwrap_or(Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|array| json!(array.value(row_idx)))
            .unwrap_or(Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| json!(array.value(row_idx)))
            .unwrap_or(Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| json!(array.value(row_idx)))
            .unwrap_or(Value::Null),
        _ => Value::String(format!("{array:?}")),
    }
}

fn json_to_cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
