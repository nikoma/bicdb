use std::sync::Arc;

use bicdb_core::{
    doctor_bundle_json, BicDb, DbConfig, DoctorReport, OperationalMetrics, Record, StorageMode,
    MAX_WAL_RECORD_BYTES, PAGED_STORE_SNAPSHOT_FORMAT_VERSION,
};
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

#[test]
fn embedded_metrics_do_not_invent_paged_storage() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap();

    assert!(db.paged_storage_snapshot().unwrap().is_none());
    let metrics = OperationalMetrics::from_db(&db).unwrap();
    assert!(!metrics
        .storage
        .keys()
        .any(|name| name.starts_with("paged_")));
    assert!(!metrics.memory.keys().any(|name| name.starts_with("paged_")));
    assert!(!metrics.to_prometheus().contains("bicdb_storage_paged_"));
    let doctor = DoctorReport::collect(&db).unwrap();
    assert!(doctor.paged_storage.is_none());
}

#[test]
fn paged_snapshot_metrics_doctor_and_reopen_share_one_bounded_contract() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), paged_config()).unwrap();
    db.create_collection("observations").unwrap();
    for index in 0..500 {
        db.insert(
            "observations",
            Record::new(format!("row-{index:04}")).with_metadata(json!({
                "sequence": index,
                "payload": "x".repeat(2 * 1024),
            })),
        )
        .unwrap();
    }

    let snapshot = db.paged_storage_snapshot().unwrap().unwrap();
    assert_eq!(snapshot.format_version, PAGED_STORE_SNAPSHOT_FORMAT_VERSION);
    assert_eq!(snapshot.page_size, 4 * 1024);
    assert_eq!(snapshot.buffer_pool.budget_bytes, 64 * 4 * 1024);
    assert_eq!(snapshot.buffer_pool.read_ahead_queue_capacity, 32);
    assert_eq!(snapshot.wal_max_bytes, 128 * 4 * 1024);
    assert!(snapshot.buffer_pool.resident_bytes <= snapshot.buffer_pool.budget_bytes);
    assert!(snapshot.page_count > 1);
    assert!(snapshot.used_data_pages > 0);
    assert!(snapshot.page_io.writes > 0);
    assert!(snapshot.page_io.page_types.btree_leaf.writes > 0);
    assert!(snapshot.page_io.write_latency.count > 0);
    assert!(snapshot.page_io.read_latency.is_consistent());
    assert!(snapshot.page_io.write_latency.is_consistent());
    assert!(snapshot.page_io.sync_latency.is_consistent());
    assert!(snapshot.wal.syncs > 0);
    assert_eq!(snapshot.wal.sync_latency.count, snapshot.wal.syncs);
    assert_eq!(
        snapshot.wal.sync_latency.cumulative_buckets[8],
        snapshot.wal.syncs
    );
    assert!(snapshot.wal.sync_latency.is_consistent());
    assert!(snapshot.wal.sync_records_total > 0);
    assert!(snapshot.wal.max_sync_records >= snapshot.wal.last_sync_records);
    assert!(snapshot.recovery.duration_nanos > 0);
    assert_eq!(snapshot.recovery.scan_passes, 2);
    assert!(snapshot.recovery.peak_record_bytes <= MAX_WAL_RECORD_BYTES as u64);
    assert!(snapshot.transaction_next_xid > 0);
    assert!(snapshot.transaction_frozen_xid <= snapshot.transaction_next_xid);
    assert!(snapshot.abort_exception_capacity > 0);
    assert!(snapshot.abort_exceptions <= snapshot.abort_exception_capacity);
    assert!(snapshot.recovery.abort_exceptions_at_open <= snapshot.abort_exception_capacity);
    assert!(
        snapshot.status_spill_entries >= snapshot.recovery.status_spill_entries_at_open
            || snapshot.status_spill_entries == 0
    );

    let metrics = OperationalMetrics::from_db(&db).unwrap();
    assert_eq!(
        metrics.storage["paged_page_size_bytes"],
        i64::from(snapshot.page_size)
    );
    assert_eq!(
        metrics.memory["paged_buffer_pool_budget_bytes"],
        snapshot.buffer_pool.budget_bytes as i64
    );
    assert_eq!(
        metrics.transaction["paged_wal_max_bytes"],
        snapshot.wal_max_bytes as i64
    );
    assert!(
        metrics.storage["paged_page_btree_leaf_writes_total"]
            >= snapshot.page_io.page_types.btree_leaf.writes as i64
    );
    assert!(
        metrics.storage["paged_page_read_latency_inf_total"]
            >= snapshot.page_io.read_latency.count as i64
    );
    assert!(
        metrics.storage["paged_page_write_latency_nanos_total"]
            >= snapshot.page_io.write_latency.sum_nanos as i64
    );
    assert_eq!(
        metrics.transaction["paged_wal_sync_records_total"],
        snapshot.wal.sync_records_total as i64
    );
    assert_eq!(
        metrics.transaction["paged_wal_sync_latency_inf_total"],
        snapshot.wal.sync_latency.count as i64
    );
    assert_eq!(
        metrics.transaction["paged_recovery_duration_nanos"],
        snapshot.recovery.duration_nanos as i64
    );
    assert_eq!(
        metrics.transaction["paged_recovery_scan_passes"],
        snapshot.recovery.scan_passes as i64
    );
    assert_eq!(
        metrics.transaction["paged_recovery_peak_record_bytes"],
        snapshot.recovery.peak_record_bytes as i64
    );
    assert_eq!(
        metrics.transaction["paged_transaction_frozen_xid"],
        snapshot.transaction_frozen_xid as i64
    );
    assert_eq!(
        metrics.transaction["paged_abort_exceptions"],
        snapshot.abort_exceptions as i64
    );
    assert_eq!(
        metrics.transaction["paged_abort_exception_capacity"],
        snapshot.abort_exception_capacity as i64
    );
    assert_eq!(
        metrics.transaction["paged_recovery_frozen_outcomes_at_open"],
        snapshot.recovery.frozen_outcomes_at_open as i64
    );
    assert_eq!(
        metrics.transaction["paged_status_spill_entries"],
        snapshot.status_spill_entries as i64
    );
    let prometheus = metrics.to_prometheus();
    assert!(prometheus.contains("bicdb_storage_paged_page_count "));
    assert!(prometheus.contains("bicdb_storage_paged_page_btree_leaf_writes_total "));
    assert!(prometheus.contains("bicdb_storage_paged_page_unclassified_reads_total "));
    assert!(prometheus.contains("bicdb_storage_paged_page_read_latency_le_1us_total "));
    assert!(prometheus.contains("bicdb_storage_paged_page_read_latency_inf_total "));
    assert!(prometheus.contains("bicdb_storage_paged_page_write_latency_nanos_total "));
    assert!(prometheus.contains("bicdb_storage_paged_page_sync_latency_max_nanos "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_hits_total "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_writeback_steps_total "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_read_ahead_queue_depth "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_read_ahead_pages_used_total "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_shard_lock_waits_total "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_page_latch_waits_total "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_oldest_dirty_page_age_millis "));
    assert!(prometheus.contains("bicdb_memory_paged_buffer_pool_writeback_lag_bytes "));
    assert!(prometheus.contains("bicdb_transaction_paged_wal_bytes "));
    assert!(prometheus.contains("bicdb_transaction_paged_wal_sync_records_total "));
    assert!(prometheus.contains("bicdb_transaction_paged_wal_sync_latency_inf_total "));
    assert!(prometheus.contains("bicdb_transaction_paged_wal_checkpoint_age_millis "));
    assert!(prometheus.contains("bicdb_transaction_paged_recovery_duration_nanos "));
    assert!(prometheus.contains("bicdb_transaction_paged_recovery_scan_passes "));
    assert!(prometheus.contains("bicdb_transaction_paged_recovery_peak_record_bytes "));
    assert!(prometheus.contains("bicdb_transaction_paged_recovery_transaction_outcomes "));
    assert!(prometheus.contains("bicdb_transaction_paged_recovery_frozen_outcomes_at_open "));
    assert!(prometheus.contains("bicdb_transaction_paged_recovery_abort_exceptions_at_open "));
    assert!(prometheus.contains("bicdb_transaction_paged_abort_exceptions "));
    assert!(prometheus.contains("bicdb_transaction_paged_abort_exception_capacity "));
    assert!(prometheus.contains("bicdb_transaction_paged_status_spill_entries "));
    assert!(prometheus.contains("bicdb_transaction_paged_status_spill_pages "));
    assert!(prometheus.contains("bicdb_transaction_paged_status_spill_lookup_failures_total "));
    assert!(
        !prometheus.contains('{'),
        "dynamic labels entered the exporter"
    );
    assert_eq!(
        prometheus
            .lines()
            .filter(|line| line.starts_with("bicdb_storage_paged_page_count "))
            .count(),
        1
    );
    assert_eq!(
        serde_json::from_str::<OperationalMetrics>(&serde_json::to_string(&metrics).unwrap())
            .unwrap(),
        metrics
    );

    let doctor = DoctorReport::collect(&db).unwrap();
    let doctor_snapshot = doctor.paged_storage.unwrap();
    assert_eq!(doctor_snapshot.format_version, snapshot.format_version);
    assert_eq!(doctor_snapshot.page_size, snapshot.page_size);
    assert_eq!(doctor_snapshot.page_count, snapshot.page_count);
    assert_eq!(
        doctor_snapshot.logical_page_bytes,
        snapshot.logical_page_bytes
    );
    assert_eq!(doctor_snapshot.page_file_bytes, snapshot.page_file_bytes);
    assert_eq!(doctor_snapshot.wal_max_bytes, snapshot.wal_max_bytes);
    assert!(
        doctor_snapshot.page_io.page_types.btree_leaf.reads
            >= snapshot.page_io.page_types.btree_leaf.reads
    );
    assert!(
        doctor_snapshot.page_io.page_types.btree_leaf.writes
            >= snapshot.page_io.page_types.btree_leaf.writes
    );
    assert!(doctor_snapshot.page_io.write_latency.count >= snapshot.page_io.write_latency.count);
    assert!(doctor_snapshot.buffer_pool.resident_bytes <= doctor_snapshot.buffer_pool.budget_bytes);
    let bundle = doctor_bundle_json(&doctor).unwrap();
    assert_eq!(
        bundle["paged_storage"]["buffer_pool"]["budget_bytes"],
        json!(64 * 4 * 1024)
    );
    assert_eq!(
        bundle["paged_storage"]["page_io"]["page_types"]["btree_leaf"]["writes"],
        json!(doctor_snapshot.page_io.page_types.btree_leaf.writes)
    );
    assert_eq!(
        bundle["paged_storage"]["page_io"]["write_latency"]["cumulative_buckets"],
        json!(doctor_snapshot.page_io.write_latency.cumulative_buckets)
    );
    assert_eq!(
        bundle["paged_storage"]["wal"]["sync_latency"]["cumulative_buckets"],
        json!(doctor_snapshot.wal.sync_latency.cumulative_buckets)
    );
    assert_eq!(
        bundle["paged_storage"]["recovery"]["duration_nanos"],
        json!(doctor_snapshot.recovery.duration_nanos)
    );
    assert_eq!(
        bundle["paged_storage"]["abort_exception_capacity"],
        json!(doctor_snapshot.abort_exception_capacity)
    );
    assert_eq!(
        bundle["paged_storage"]["recovery"]["frozen_outcomes_at_open"],
        json!(doctor_snapshot.recovery.frozen_outcomes_at_open)
    );

    // The pressure findings consume the same typed snapshot rather than
    // re-scanning pages or accepting caller-controlled metric names.
    let mut pressure = snapshot;
    pressure.buffer_pool.admission_failures = 7;
    pressure.buffer_pool.dirty_pages = pressure.buffer_pool.total_frames;
    pressure.wal_bytes = pressure.wal_max_bytes;
    pressure.page_io.tail_reclaim_deferrals = 3;
    let mut pressure_metrics = metrics.clone();
    pressure_metrics.apply_paged_storage(&pressure);
    assert_eq!(
        pressure_metrics.memory["paged_buffer_pool_admission_failures_total"],
        7
    );
    assert_eq!(
        pressure_metrics.storage["paged_tail_reclaim_deferrals_total"],
        3
    );

    let durable = (
        snapshot.page_size,
        snapshot.page_count,
        snapshot.logical_page_bytes,
        snapshot.page_file_bytes,
        snapshot.free_pages,
        snapshot.wal_max_bytes,
    );
    drop(db);
    let reopened = BicDb::open_with_config(root.path(), paged_config()).unwrap();
    let reopened = reopened.paged_storage_snapshot().unwrap().unwrap();
    assert_eq!(reopened.recovery.wal_bytes_scanned, snapshot.wal_bytes);
    assert!(reopened.recovery.duration_nanos > 0);
    assert_eq!(reopened.recovery.scan_passes, 2);
    assert!(reopened.recovery.peak_record_bytes <= MAX_WAL_RECORD_BYTES as u64);
    assert_eq!(
        (
            reopened.page_size,
            reopened.page_count,
            reopened.logical_page_bytes,
            reopened.page_file_bytes,
            reopened.free_pages,
            reopened.wal_max_bytes,
        ),
        durable
    );
}

#[test]
fn concurrent_scrapes_are_read_only_and_fixed_cardinality() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), paged_config()).unwrap();
    db.create_collection("events").unwrap();
    db.insert("events", Record::new("one")).unwrap();
    let db = Arc::new(db);

    let workers = (0..16)
        .map(|_| {
            let db = db.clone();
            std::thread::spawn(move || {
                for _ in 0..50 {
                    let before = db.paged_storage_snapshot().unwrap().unwrap();
                    let text = OperationalMetrics::from_db(&db).unwrap().to_prometheus();
                    let after = db.paged_storage_snapshot().unwrap().unwrap();
                    assert_eq!(before.page_count, after.page_count);
                    assert_eq!(before.free_pages, after.free_pages);
                    assert!(!text.contains('{'));
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn configured_page_size_is_durable_and_a_mismatch_is_non_mutating() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), paged_config()).unwrap();
    assert_eq!(
        db.paged_storage_snapshot().unwrap().unwrap().page_size,
        4 * 1024
    );
    drop(db);

    let page_file = root.path().join("paged").join("store.pages");
    let before = std::fs::read(&page_file).unwrap();
    let error = BicDb::open_with_config(root.path(), paged_config().with_paged_page_size(8 * 1024))
        .unwrap_err();
    assert!(error.to_string().contains("page size"));
    assert_eq!(std::fs::read(page_file).unwrap(), before);
}
