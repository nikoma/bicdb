use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use bicdb_analytics::{
    default_sidecar_dir, rebuild_sidecar, sidecar_path, verify_sidecar, BicDataFusionContext,
    BicDbAnalyticsExt,
};
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

fn open_temp() -> (tempfile::TempDir, BicDb) {
    let temp = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
    (temp, db)
}

#[test]
fn exports_wearable_records_as_arrow_record_batch() {
    let (_temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();
    db.batch_insert(
        "wearable",
        vec![
            wearable("r1", "band-1", "user-1", "hrv", 57.2, 100),
            wearable("r2", "band-2", "user-2", "steps", 10.0, 200),
        ],
    )
    .unwrap();

    let batch = db.to_record_batch("wearable").unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(0).name(), "record_id");
    assert_eq!(
        batch
            .column_by_name("metric")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "hrv"
    );
    assert_eq!(
        batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        57.2
    );
}

#[test]
fn exports_empty_collection_with_schema() {
    let (_temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();

    let batch = db.to_record_batch("wearable").unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 7);
    assert!(batch.column_by_name("record_id").is_some());
    assert!(batch.column_by_name("metadata_json").is_some());
}

#[tokio::test]
async fn arrow_ipc_sidecars_preserve_versioned_typed_metadata() {
    let (temp, mut db) = open_temp();
    db.create_collection("typed_values").unwrap();
    let metadata = json!({
        "amount": {
            "$bicdb_typed": {
                "version": 1,
                "pg_type": "numeric",
                "value": {
                    "kind": "finite",
                    "negative": false,
                    "coefficient": "90071992547409930100",
                    "display_scale": 4
                },
                "index_key": [1, 3, 128, 16, 57, 0]
            }
        }
    });
    db.insert(
        "typed_values",
        Record::new("typed-1").with_metadata(metadata.clone()),
    )
    .unwrap();

    let batch = db.to_record_batch("typed_values").unwrap();
    assert_eq!(
        batch.schema().metadata()["bicdb.analytics_schema_version"],
        "2"
    );
    assert_eq!(
        batch.schema().metadata()["bicdb.metadata_encoding"],
        "bicdb-record-metadata-json-v1"
    );
    let encoded = batch
        .column_by_name("metadata_json")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(encoded).unwrap(),
        metadata
    );

    rebuild_sidecar(&db, temp.path(), "typed_values").unwrap();
    let context = BicDataFusionContext::with_sidecar_root(&db, default_sidecar_dir(temp.path()));
    let rows = context
        .sql("SELECT metadata_json FROM typed_values")
        .await
        .unwrap()
        .rows_as_json();
    let restored = rows[0]["metadata_json"].as_str().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(restored).unwrap(),
        metadata
    );
}

#[test]
fn missing_and_mixed_metadata_values_become_nulls() {
    let (_temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();
    db.batch_insert(
        "wearable",
        vec![
            Record::new("missing").with_timestamp(10),
            Record::new("mixed").with_metadata(json!({
                "timestamp": "not-an-int",
                "device_id": 123,
                "user_id": false,
                "metric": ["hrv"],
                "value": "bad"
            })),
        ],
    )
    .unwrap();

    let batch = db.to_record_batch("wearable").unwrap();
    let device_ids = batch
        .column_by_name("device_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert!(device_ids.is_null(0));
    assert!(device_ids.is_null(1));
    assert!(values.is_null(0));
    assert!(values.is_null(1));
}

#[test]
fn time_range_export_filters_by_timestamp() {
    let (_temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();
    db.batch_insert(
        "wearable",
        vec![
            wearable("r1", "band-1", "user-1", "hrv", 57.2, 100),
            wearable("r2", "band-1", "user-1", "hrv", 58.2, 200),
            wearable("r3", "band-1", "user-1", "hrv", 59.2, 300),
        ],
    )
    .unwrap();

    let batch = db.time_range_to_record_batch("wearable", 150, 250).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        200
    );
}

#[tokio::test]
async fn datafusion_count_avg_group_by_and_timestamp_filter_work() {
    let (_temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();
    db.batch_insert(
        "wearable",
        vec![
            wearable("r1", "band-1", "user-1", "hrv", 50.0, 100),
            wearable("r2", "band-1", "user-1", "hrv", 70.0, 200),
            wearable("r3", "band-2", "user-2", "steps", 10.0, 300),
        ],
    )
    .unwrap();

    let ctx = BicDataFusionContext::new(&db);

    let count = ctx.sql("SELECT COUNT(*) FROM wearable").await.unwrap();
    assert_eq!(count.rows_as_json()[0]["count(*)"], 3);

    let avg = ctx
        .sql("SELECT AVG(value) FROM wearable WHERE metric = 'hrv'")
        .await
        .unwrap();
    assert_eq!(avg.rows_as_json()[0]["avg(wearable.value)"], 60.0);

    let minmax = ctx
        .sql("SELECT MIN(value), MAX(value) FROM wearable WHERE timestamp >= 200")
        .await
        .unwrap();
    assert_eq!(minmax.rows_as_json()[0]["min(wearable.value)"], 10.0);
    assert_eq!(minmax.rows_as_json()[0]["max(wearable.value)"], 70.0);

    let by_metric = ctx
        .sql("SELECT metric, AVG(value) FROM wearable GROUP BY metric")
        .await
        .unwrap()
        .rows_as_json();
    assert!(by_metric
        .iter()
        .any(|row| row["metric"] == "hrv" && row["avg(wearable.value)"] == 60.0));

    let by_device = ctx
        .sql("SELECT device_id, AVG(value) FROM wearable WHERE metric='hrv' GROUP BY device_id")
        .await
        .unwrap()
        .rows_as_json();
    assert_eq!(by_device[0]["device_id"], "band-1");
    assert_eq!(by_device[0]["avg(wearable.value)"], 60.0);
}

#[test]
fn sidecar_rebuild_and_verify_use_canonical_records() {
    let (temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();
    db.batch_insert(
        "wearable",
        vec![
            wearable("r1", "band-1", "user-1", "hrv", 50.0, 100),
            wearable("r2", "band-2", "user-2", "steps", 10.0, 200),
        ],
    )
    .unwrap();

    let rebuild = rebuild_sidecar(&db, temp.path(), "wearable").unwrap();
    assert_eq!(rebuild.rows, 2);
    assert!(rebuild.sidecar_bytes > 0);
    assert!(rebuild.path.exists());

    let verify = verify_sidecar(&db, temp.path(), "wearable").unwrap();
    assert_eq!(verify.canonical_rows, 2);
    assert_eq!(verify.sidecar_rows, 2);
    assert!(verify.verified);
}

#[tokio::test]
async fn datafusion_context_reuses_cached_sidecar_batches() {
    let (temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();
    db.batch_insert(
        "wearable",
        vec![
            wearable("r1", "band-1", "user-1", "hrv", 50.0, 100),
            wearable("r2", "band-2", "user-2", "steps", 10.0, 200),
        ],
    )
    .unwrap();
    rebuild_sidecar(&db, temp.path(), "wearable").unwrap();

    let sidecar_root = default_sidecar_dir(temp.path());
    let ctx = BicDataFusionContext::with_sidecar_root(&db, sidecar_root.clone());
    let first = ctx.sql("SELECT COUNT(*) FROM wearable").await.unwrap();
    assert_eq!(first.rows_as_json()[0]["count(*)"], 2);

    std::fs::remove_file(sidecar_path(&sidecar_root, "wearable")).unwrap();
    let second = ctx.sql("SELECT COUNT(*) FROM wearable").await.unwrap();
    assert_eq!(second.rows_as_json()[0]["count(*)"], 2);
}

fn wearable(
    id: &str,
    device_id: &str,
    user_id: &str,
    metric: &str,
    value: f64,
    timestamp: i64,
) -> Record {
    Record::new(id)
        .with_timestamp(timestamp)
        .with_metadata(json!({
            "device_id": device_id,
            "user_id": user_id,
            "metric": metric,
            "value": value,
        }))
}
