use bicdb_core::{
    BicDb, DbConfig, Record, ResourceDemand, ResourceGovernor, ResourceGovernorConfig,
    ResourceLane, StorageMode, WritebackCursor, WritebackLimits, WritebackStopReason,
};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}

fn one_page_limits() -> WritebackLimits {
    WritebackLimits {
        max_candidates: 1,
        max_io_bytes: 16 * 1024,
        max_duration_millis: 10_000,
    }
}

fn demand(limits: WritebackLimits) -> ResourceDemand {
    ResourceDemand {
        memory_bytes: 128 * 1024,
        io_bytes: limits.max_io_bytes,
        cpu_slots: 1,
        io_charge_bytes: limits.max_io_bytes,
    }
}

fn dirty_database(db: &mut BicDb) {
    db.create_collection("events").unwrap();
    let payload = "x".repeat(2 * 1024);
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .batch_insert(
            "events",
            (0..40).map(|index| {
                Record::new(format!("event-{index:03}"))
                    .with_metadata(json!({ "payload": payload }))
            }),
        )
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn public_governed_writeback_drains_in_bounded_cursor_steps() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    dirty_database(&mut db);
    let before = db.paged_storage_snapshot().unwrap().unwrap();
    assert!(before.buffer_pool.dirty_pages > 1);

    let limits = one_page_limits();
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let mut cursor = WritebackCursor::default();
    let mut pages_written = 0_u64;
    let mut steps = 0_u64;
    for step in 0..10_000_u64 {
        let report = db
            .paged_writeback_step_governed(
                cursor,
                limits,
                &governor,
                demand(limits),
                100 + step * 1_000,
            )
            .unwrap();
        assert!(report.candidates_examined <= 1);
        assert!(report.pages_written <= 1);
        assert!(report.logical_io_bytes <= limits.max_io_bytes);
        assert_eq!(governor.snapshot().background.active, 0);
        pages_written += report.pages_written;
        steps += 1;
        cursor = report.next_cursor;
        if report.complete {
            assert_eq!(report.stop_reason, WritebackStopReason::Complete);
            assert_eq!(report.dirty_pages_remaining, 0);
            break;
        }
        assert!(matches!(
            report.stop_reason,
            WritebackStopReason::CandidateLimit | WritebackStopReason::IoLimit
        ));
        if step == 9_999 {
            panic!("bounded writeback did not complete after 10,000 steps");
        }
    }

    let after = db.paged_storage_snapshot().unwrap().unwrap();
    assert_eq!(after.buffer_pool.dirty_pages, 0);
    assert_eq!(
        after.buffer_pool.writebacks,
        before.buffer_pool.writebacks + pages_written
    );
    assert_eq!(after.buffer_pool.writeback_steps, steps);
    assert_eq!(after.buffer_pool.background_writebacks, pages_written);
    assert!(after.buffer_pool.writeback_candidate_limit_stops > 0);
    assert!(pages_written > 1);
}

#[test]
fn demand_validation_and_lane_saturation_fail_before_page_work() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    dirty_database(&mut db);
    let limits = one_page_limits();
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let before = db.paged_storage_snapshot().unwrap().unwrap();

    let mut too_small = demand(limits);
    too_small.io_bytes -= 1;
    assert!(db
        .paged_writeback_step_governed(
            WritebackCursor::default(),
            limits,
            &governor,
            too_small,
            100,
        )
        .unwrap_err()
        .to_string()
        .contains("must reserve"));
    let unchanged = db.paged_storage_snapshot().unwrap().unwrap();
    assert_eq!(
        unchanged.buffer_pool.dirty_pages,
        before.buffer_pool.dirty_pages
    );
    assert_eq!(
        unchanged.buffer_pool.writebacks,
        before.buffer_pool.writebacks
    );

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
    assert!(db
        .paged_writeback_step_governed(
            WritebackCursor::default(),
            limits,
            &governor,
            demand(limits),
            100,
        )
        .is_err());
    drop(held);
    let unchanged = db.paged_storage_snapshot().unwrap().unwrap();
    assert_eq!(
        unchanged.buffer_pool.dirty_pages,
        before.buffer_pool.dirty_pages
    );
    assert_eq!(
        unchanged.buffer_pool.writebacks,
        before.buffer_pool.writebacks
    );
}

#[test]
fn embedded_mode_rejects_paged_writeback() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    let limits = one_page_limits();
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    assert!(db
        .paged_writeback_step_governed(
            WritebackCursor::default(),
            limits,
            &governor,
            demand(limits),
            100,
        )
        .unwrap_err()
        .to_string()
        .contains("storage_mode = server_paged"));
}
