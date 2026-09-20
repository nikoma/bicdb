use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_page_size(4 * 1024)
        .with_paged_buffer_pool_bytes(64 * 4 * 1024)
        .with_paged_read_ahead_queue_pages(32)
        .with_paged_wal_max_bytes(128 * 4 * 1024)
}

// The materialized variant skips the per-row page-store presence probe, so it
// must stamp exactly what the probing variant stamps for rows that do exist.
// Reopening puts the collection in lazy paged mode with untouched rows — the
// branch where the probe (and therefore the divergence risk) lives.
#[test]
fn materialized_system_metadata_matches_the_probing_variant_on_reopened_paged_rows() {
    let root = tempfile::tempdir().unwrap();
    let pks: Vec<String> = (0..64).map(|index| format!("row-{index:03}")).collect();
    {
        let mut db = BicDb::open_with_config(root.path(), paged_config()).unwrap();
        db.create_collection("events").unwrap();
        for (index, pk) in pks.iter().enumerate() {
            db.insert(
                "events",
                Record::new(pk.clone()).with_metadata(json!({ "sequence": index })),
            )
            .unwrap();
        }
        db.flush().unwrap();
    }

    let db = BicDb::open_with_config(root.path(), paged_config()).unwrap();
    let materialized = db
        .record_system_metadata_batch_for_materialized("events", &pks)
        .unwrap();
    assert_eq!(materialized.len(), pks.len());
    for (pk, fast) in pks.iter().zip(materialized) {
        let probed = db.record_system_metadata("events", pk).unwrap();
        assert!(probed.is_some(), "probing variant lost row {pk}");
        assert_eq!(probed, fast, "system metadata diverged for {pk}");
    }
}
