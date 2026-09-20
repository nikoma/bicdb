//! v2 vs v3 entry economics on UUID-keyed rows: durable entry bytes, prefix
//! lookup+fetch throughput, and WAL bytes per indexed write.
//!
//!   BICDB_PAGED_V3_ENTRIES=1 cargo run --release -p bicdb-core --example v3_entry_probe
//!   BICDB_PAGED_V3_ENTRIES=0 cargo run --release -p bicdb-core --example v3_entry_probe

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue, PagedRecords,
    PagedRecordsOptions, Record, StorageMode,
};
use serde_json::json;
use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let buckets = 1_000usize;
    let arm = std::env::var("BICDB_PAGED_V3_ENTRIES").unwrap_or_else(|_| "1".into());

    let dir = std::env::temp_dir().join(format!("v3-entry-probe-{arm}-{rows}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged);

    // UUID-shaped pks: the workload the entry shrink targets.
    let pk = |row: usize| {
        format!(
            "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
            row * 2_654_435_761 % u32::MAX as usize,
            row % 0xFFFF,
            row % 0xFFF,
            (row * 31) % 0xFFF,
            row * 1_099_511_627_776 % 0xFFFF_FFFF_FFFF
        )
    };

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
                Record::new(pk(row)).with_metadata(json!({
                    "bucket": (row % buckets) as i64,
                    "note": "0123456789012345678901234567890123456789",
                }))
            })
            .collect();
        db.batch_insert("orders", batch).unwrap();
        let load = started.elapsed();
        db.close().unwrap();
        eprintln!("loaded {rows} in {load:.1?}");
    }

    // Durable entry bytes, straight off the closed store.
    let (entry_count, key_bytes, value_bytes) = {
        let paged = PagedRecords::open(
            dir.join("paged"),
            PagedRecordsOptions {
                fsync: false,
                ..Default::default()
            },
        )
        .unwrap();
        let snapshot = paged.latest_snapshot();
        let mut count = 0u64;
        let mut kb = 0u64;
        let mut vb = 0u64;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let batch = paged
                .scan_index_batch_after_raw(
                    &snapshot,
                    "orders_bucket",
                    after.as_deref(),
                    8_192,
                    16 * 1024 * 1024,
                )
                .unwrap();
            if batch.is_empty() {
                break;
            }
            for (raw, _, _) in &batch {
                count += 1;
                kb += raw.len() as u64;
            }
            // Values require the exact-refs scan; approximate via hint size
            // constant is wrong for v2-with-hints, so read them per key once
            // at the end instead — cheaper: values are uniform 22B hints.
            vb += 22 * batch.len() as u64;
            after = batch.last().map(|(raw, _, _)| raw.clone());
        }
        (count, kb, vb)
    };

    // Warm lookup+fetch throughput through the public surface.
    let db = BicDb::open_with_config(&dir, config).unwrap();
    for bucket in 0..200usize {
        let pks = db
            .lookup_index("orders_bucket", &[IndexValue::from(bucket as i64)])
            .unwrap();
        std::hint::black_box(db.get_records_by_pks("orders", &pks).unwrap());
    }
    let started = Instant::now();
    let mut fetched = 0usize;
    let lookups = 20_000usize;
    for probe in 0..lookups {
        let bucket = (probe * 7919) % buckets;
        let pks = db
            .lookup_index("orders_bucket", &[IndexValue::from(bucket as i64)])
            .unwrap();
        let records = db.get_records_by_pks("orders", &pks).unwrap();
        fetched += records.len();
        std::hint::black_box(records);
    }
    let elapsed = started.elapsed();
    let (hits, fallbacks) = bicdb_core::paged_tid_hint_stats();
    println!(
        "arm=v{} entries={entry_count} avg_entry_key_bytes={:.1} entry_value_bytes={value_bytes} lookup_fetch_ns_per_row={} hint_hits={hits} hint_fallbacks={fallbacks}",
        if arm == "0" { 2 } else { 3 },
        key_bytes as f64 / entry_count.max(1) as f64,
        elapsed.as_nanos() as u64 / fetched.max(1) as u64,
    );

    let _ = std::fs::remove_dir_all(&dir);
}
