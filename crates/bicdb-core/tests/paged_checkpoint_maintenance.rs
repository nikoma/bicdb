use bicdb_core::{
    BicDb, DbConfig, OperationalMetrics, PagedCheckpointScheduleAdvance,
    PagedCheckpointScheduleLimits, Record, ResourceDemand, ResourceGovernor,
    ResourceGovernorConfig, ResourceLane, StorageMode, TailReclaimLimits, WritebackLimits,
};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}

fn limits() -> PagedCheckpointScheduleLimits {
    let mut limits = PagedCheckpointScheduleLimits::default();
    limits.checkpoint.writeback = WritebackLimits {
        max_candidates: 1,
        max_io_bytes: 16 * 1024,
        max_duration_millis: 10_000,
    };
    limits.checkpoint.max_freeze_xids = 1;
    limits.checkpoint.tail_reclaim = TailReclaimLimits {
        max_page_visits: 64,
        max_io_bytes: 64 * 8_192,
    };
    limits.step_interval_ms = 1;
    limits.saturation_retry_ms = 10;
    limits.demand = ResourceDemand {
        memory_bytes: 128 * 1024,
        io_bytes: limits
            .checkpoint
            .writeback
            .max_io_bytes
            .saturating_add(limits.checkpoint.tail_reclaim.max_io_bytes),
        cpu_slots: 1,
        io_charge_bytes: limits
            .checkpoint
            .writeback
            .max_io_bytes
            .saturating_add(limits.checkpoint.tail_reclaim.max_io_bytes),
    };
    limits
}

fn dirty_database(db: &mut BicDb) {
    db.create_collection("events").unwrap();
    for batch in 0..8 {
        let payload = "x".repeat(2 * 1024);
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .batch_insert(
                "events",
                (0..8).map(|index| {
                    Record::new(format!("event-{batch:02}-{index:02}"))
                        .with_metadata(json!({ "payload": payload }))
                }),
            )
            .unwrap();
        transaction.commit().unwrap();
    }
}

#[test]
fn durable_supervisor_resumes_after_reopen_and_completes() {
    let root = tempfile::tempdir().unwrap();
    let operation_id = {
        let mut db =
            BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        dirty_database(&mut db);
        let schedule = db
            .start_paged_checkpoint_maintenance(100, limits())
            .unwrap();
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let first = db
            .tick_paged_checkpoint_maintenance(schedule.operation_id, &governor, 100)
            .unwrap();
        assert!(matches!(
            first,
            PagedCheckpointScheduleAdvance::Progress { .. }
        ));
        let durable = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
        assert_ne!(durable.cursor, Default::default());
        schedule.operation_id
    };

    let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    let resumed = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
    assert_eq!(resumed.operation_id, operation_id);
    assert!(!resumed.completed);
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let mut final_totals = None;
    for tick in 1..10_000_u64 {
        match db
            .tick_paged_checkpoint_maintenance(operation_id, &governor, 100 + tick)
            .unwrap()
        {
            PagedCheckpointScheduleAdvance::Complete { totals, report, .. } => {
                assert!(report.complete);
                assert!(report.checkpoint.is_some());
                final_totals = Some(totals);
                break;
            }
            PagedCheckpointScheduleAdvance::Progress { report, .. } => {
                if let Some(writeback) = report.writeback {
                    assert!(writeback.pages_written <= 1);
                    assert!(writeback.candidates_examined <= 1);
                }
            }
            PagedCheckpointScheduleAdvance::NotDue { .. } => {}
            other => panic!("unexpected checkpoint outcome: {other:?}"),
        }
    }
    let totals = final_totals.expect("checkpoint did not complete");
    assert_eq!(totals.checkpoints_completed, 1);
    assert!(totals.pages_written > 1);
    assert!(totals.active_duration_nanos > 0);
    assert!(totals.wal_bytes_checkpointed > 0);
    let status = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
    assert!(status.completed);
    assert_eq!(status.totals, totals);
    assert_eq!(db.paged_storage_snapshot().unwrap().unwrap().wal_bytes, 0);
    let metrics = OperationalMetrics::from_db(&db).unwrap();
    assert_eq!(
        metrics.compaction["paged_checkpoint_active_duration_nanos_total"],
        totals.active_duration_nanos as i64
    );
    assert_eq!(
        metrics.compaction["paged_checkpoint_wall_duration_millis"],
        status.updated_at_ms.saturating_sub(status.started_at_ms) as i64
    );
    assert_eq!(
        metrics.compaction["paged_checkpoint_wal_bytes_checkpointed_total"],
        totals.wal_bytes_checkpointed as i64
    );
    let prometheus = metrics.to_prometheus();
    assert!(prometheus.contains("bicdb_compaction_paged_checkpoint_completed 1\n"));
    assert!(prometheus.contains("bicdb_compaction_paged_checkpoint_steps_total "));
    assert!(prometheus.contains("bicdb_compaction_paged_checkpoint_active_duration_nanos_total "));
    assert!(prometheus.contains("bicdb_compaction_paged_checkpoint_wall_duration_millis "));
    assert!(prometheus.contains("bicdb_compaction_paged_checkpoint_wal_bytes_checkpointed_total "));
}

#[test]
fn resource_saturation_defers_before_page_work() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    dirty_database(&mut db);
    let schedule = db
        .start_paged_checkpoint_maintenance(100, limits())
        .unwrap();
    let before = db.paged_storage_snapshot().unwrap().unwrap();
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let held = governor
        .try_admit(
            ResourceLane::Compaction,
            ResourceDemand {
                memory_bytes: 4 * 1024 * 1024 * 1024,
                io_bytes: 1024 * 1024 * 1024,
                cpu_slots: 1,
                io_charge_bytes: 1,
            },
            100,
        )
        .unwrap();
    let outcome = db
        .tick_paged_checkpoint_maintenance(schedule.operation_id, &governor, 100)
        .unwrap();
    assert!(matches!(
        outcome,
        PagedCheckpointScheduleAdvance::ResourceDeferred { retry_at_ms: 110 }
    ));
    drop(held);
    let after = db.paged_storage_snapshot().unwrap().unwrap();
    assert_eq!(after.buffer_pool.writebacks, before.buffer_pool.writebacks);
    assert_eq!(
        after.buffer_pool.dirty_pages,
        before.buffer_pool.dirty_pages
    );
    let durable = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
    assert_eq!(durable.totals.resource_deferrals, 1);
    assert_eq!(durable.totals.active_duration_nanos, 0);
}

#[test]
fn pause_resume_stale_worker_and_active_replacement_are_fenced() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    let schedule = db
        .start_paged_checkpoint_maintenance(100, limits())
        .unwrap();
    assert!(db
        .start_paged_checkpoint_maintenance(101, limits())
        .unwrap_err()
        .to_string()
        .contains("still active"));
    let paused = db
        .pause_paged_checkpoint_maintenance(schedule.operation_id, "operator window", 102)
        .unwrap();
    assert_eq!(paused.paused_reason.as_deref(), Some("operator window"));
    assert!(db
        .resume_paged_checkpoint_maintenance(uuid::Uuid::new_v4(), 103, 103)
        .is_err());
    let resumed = db
        .resume_paged_checkpoint_maintenance(schedule.operation_id, 104, 103)
        .unwrap();
    assert!(resumed.paused_reason.is_none());
    assert_eq!(resumed.next_attempt_at_ms, Some(104));

    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    assert!(db
        .tick_paged_checkpoint_maintenance(uuid::Uuid::new_v4(), &governor, 104)
        .is_err());
}

#[test]
fn corrupt_or_symlinked_schedule_fails_closed_and_embedded_mode_rejects() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    db.start_paged_checkpoint_maintenance(100, limits())
        .unwrap();
    let schedule_path = root.path().join("maintenance/paged/checkpoint.json");
    let mut bytes = std::fs::read(&schedule_path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&schedule_path, bytes).unwrap();
    assert!(db.paged_checkpoint_maintenance_status().is_err());
    #[cfg(unix)]
    {
        let external = root.path().join("untrusted-checkpoint.json");
        std::fs::write(&external, b"{}").unwrap();
        std::fs::remove_file(&schedule_path).unwrap();
        std::os::unix::fs::symlink(&external, &schedule_path).unwrap();
        assert!(db.paged_checkpoint_maintenance_status().is_err());
    }

    let embedded_root = tempfile::tempdir().unwrap();
    let embedded =
        BicDb::open_with_config(embedded_root.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    assert!(embedded
        .start_paged_checkpoint_maintenance(100, limits())
        .unwrap_err()
        .to_string()
        .contains("storage_mode = server_paged"));
}
