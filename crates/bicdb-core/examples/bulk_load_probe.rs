//! Bulk-load A/B probe: the live `batch_insert` path vs the bulk-load path,
//! against a fresh server_paged database with Overture-shaped records.
//!
//! Mirrors the live Overture import: random-UUID primary keys, ~700-byte JSON
//! metadata, fsync on, 1 GiB WAL trigger, 2 GiB buffer pool. One "shard" of
//! the real import is ~4.6M records; default here is smaller for iteration —
//! scale with ROWS.
//!
//! ```text
//! ROWS=500000 BATCH=2000 MODE=batch cargo run --release -p bicdb-core --example bulk_load_probe
//! ROWS=500000 MODE=bulk cargo run --release -p bicdb-core --example bulk_load_probe
//! ```
//!
//! Prints rows/s, WAL peak, store.pages growth, and wall time. Data dir is
//! created fresh under DIR (default /dev/shm on dev boxes is WRONG for this —
//! use a real disk path to include fsync cost; default ./bulk-probe-data).

use std::time::Instant;

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// xorshift64* — deterministic pseudo-UUIDs without a rand dependency.
fn next_id(state: &mut u64) -> String {
    let mut advance = || {
        *state ^= *state >> 12;
        *state ^= *state << 25;
        *state ^= *state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };
    let hi = advance();
    let lo = advance();
    format!("{hi:016x}{lo:016x}")
}

fn record(id: String, seq: u64) -> Record {
    let metadata = serde_json::json!({
        "overture_id": id,
        "names": {"primary": format!("Place {seq}"), "common": null},
        "categories": {"primary": "health_and_medical", "alternate": ["pharmacy"]},
        "confidence": 0.77,
        "websites": [format!("https://example-{seq}.com")],
        "emails": [],
        "socials": [],
        "phones": [format!("+1-555-{:07}", seq % 10_000_000)],
        "addresses": [{"freeform": format!("{seq} Main St"), "locality": "Springfield",
                        "region": "IL", "country": "US", "postcode": "62701"}],
        "operating_status": "open",
        "latitude": 39.78 + (seq % 1000) as f64 * 1e-4,
        "longitude": -89.65 + (seq % 1000) as f64 * 1e-4,
        "source_release": "2026-07-22.0",
        "padding": "x".repeat(200),
    });
    Record::new(id)
        .with_metadata(metadata)
        .with_timestamp(1_754_500_000)
}

fn main() {
    let rows = env_u64("ROWS", 200_000);
    let batch = usize::try_from(env_u64("BATCH", 2_000)).unwrap();
    let mode = std::env::var("MODE").unwrap_or_else(|_| "batch".into());
    let dir = std::env::var("DIR").unwrap_or_else(|_| "./bulk-probe-data".into());
    let seed = env_u64("SEED", 0x9E37_79B9_7F4A_7C15);

    let path = std::path::Path::new(&dir);
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }
    std::fs::create_dir_all(path).unwrap();

    let config = || {
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(env_u64("FSYNC", 1) == 1)
            .with_sync_outbox(false)
            .with_paged_buffer_pool_bytes(env_u64("POOL", 2 * 1024 * 1024 * 1024))
            .with_paged_wal_max_bytes(env_u64("WAL_MAX", 1024 * 1024 * 1024))
    };
    // Create, close, reopen: the live importer loads into a collection that
    // already existed at open, which is what makes it lazy/paged-primary.
    // A collection created this session is resident-primary and would make
    // bulk_load_insert fall back to batch_insert.
    let mut db = BicDb::open_with_config(path, config()).unwrap();
    db.create_collection("places").unwrap();
    db.close().unwrap();
    let mut db = BicDb::open_with_config(path, config()).unwrap();

    // GLOBAL_SORT=1 models a source that emits the whole shard in id order
    // (DuckDB ORDER BY id): ids ascend across the entire run, so consecutive
    // batches sweep disjoint B-tree ranges. Random ids model the live import.
    let global_sort = env_u64("GLOBAL_SORT", 0) == 1;
    let mut id_state = seed;
    let started = Instant::now();
    let mut inserted = 0u64;
    let mut wal_peak = 0u64;

    while inserted < rows {
        let count = batch.min(usize::try_from(rows - inserted).unwrap());
        let records = (0..count)
            .map(|offset| {
                let seq = inserted + offset as u64;
                let id = if global_sort {
                    format!("{seq:016x}{}", next_id(&mut id_state))
                } else {
                    next_id(&mut id_state)
                };
                record(id, seq)
            })
            .collect::<Vec<_>>();
        match mode.as_str() {
            "batch" => db.batch_insert("places", records).unwrap(),
            "batch_sorted" => {
                let mut records = records;
                records.sort_unstable_by(|a, b| a.id.cmp(&b.id));
                db.batch_insert("places", records).unwrap();
            }
            "bulk" => db.bulk_load_insert("places", records).unwrap(),
            other => panic!("unknown MODE {other} (expected: batch, batch_sorted, bulk)"),
        }
        inserted += count as u64;
        if let Ok(Some(snapshot)) = db.paged_storage_snapshot() {
            wal_peak = wal_peak.max(snapshot.wal_bytes);
        }
        if inserted % 100_000 < batch as u64 {
            eprintln!(
                "{inserted}/{rows} rows, {:.0} rows/s",
                inserted as f64 / started.elapsed().as_secs_f64()
            );
        }
    }

    let elapsed = started.elapsed();
    let snapshot = db.paged_storage_snapshot().unwrap().unwrap();
    println!(
        "mode={mode} rows={rows} batch={batch} elapsed={:.1}s rate={:.0} rows/s wal_peak={} MiB pages={} MiB",
        elapsed.as_secs_f64(),
        rows as f64 / elapsed.as_secs_f64(),
        wal_peak / (1024 * 1024),
        snapshot.page_file_bytes / (1024 * 1024),
    );
    db.close().unwrap();
}
