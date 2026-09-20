//! End-to-end TID-hint A/B through the real read-through path:
//! `lookup_index` (durable entry scan) followed by `get_records_by_pks`.
//!
//!   BICDB_PAGED_TID_HINTS=1 cargo run --release -p bicdb-core --example tid_hint_probe
//!   BICDB_PAGED_TID_HINTS=0 cargo run --release -p bicdb-core --example tid_hint_probe

use bicdb_core::{
    paged_tid_hint_stats, BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue,
    Record, StorageMode,
};
use serde_json::json;
use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let lookups: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(50_000);
    let rows_per_key = 10usize;
    let keys = rows / rows_per_key;

    let dir = std::env::temp_dir().join(format!("tid-hint-probe-{rows}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged);

    {
        let mut db = BicDb::open_with_config(&dir, config.clone()).unwrap();
        db.create_collection("orders").unwrap();
        db.create_index(IndexDefinition {
            name: "orders_bucket".to_string(),
            collection: "orders".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["bucket".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        let started = Instant::now();
        let batch: Vec<Record> = (0..rows)
            .map(|row| {
                Record::new(format!("o{row:09}")).with_metadata(json!({
                    "bucket": (row % keys) as i64,
                    "amount": row as i64,
                    "note": "0123456789012345678901234567890123456789",
                }))
            })
            .collect();
        db.batch_insert("orders", batch).unwrap();
        db.close().unwrap();
        eprintln!("loaded {rows} rows in {:.1?}", started.elapsed());
    }

    let db = BicDb::open_with_config(&dir, config).unwrap();
    // Warm one pass so both arms start with comparable pool state.
    for key in 0..(keys.min(2_000)) {
        let pks = db
            .lookup_index("orders_bucket", &[IndexValue::from(key as i64)])
            .unwrap();
        std::hint::black_box(db.get_records_by_pks("orders", &pks).unwrap());
    }

    let started = Instant::now();
    let mut fetched = 0usize;
    for probe in 0..lookups {
        let key = (probe * 7919) % keys;
        let pks = db
            .lookup_index("orders_bucket", &[IndexValue::from(key as i64)])
            .unwrap();
        let records = db.get_records_by_pks("orders", &pks).unwrap();
        fetched += records.len();
        std::hint::black_box(records);
    }
    let elapsed = started.elapsed();
    let (hits, fallbacks) = paged_tid_hint_stats();
    println!(
        "hints={} lookups={lookups} rows_fetched={fetched} elapsed={elapsed:.2?} ns_per_row={} hint_hits={hits} hint_fallbacks={fallbacks}",
        std::env::var("BICDB_PAGED_TID_HINTS").unwrap_or_else(|_| "1".into()),
        elapsed.as_nanos() as u64 / fetched.max(1) as u64,
    );

    let _ = std::fs::remove_dir_all(&dir);
}
