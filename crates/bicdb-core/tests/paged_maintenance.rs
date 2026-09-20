use bicdb_core::{
    BicDb, DbConfig, PagedVacuumScheduleAdvance, PagedVacuumScheduleLimits, Record,
    ResourceGovernor, ResourceGovernorConfig, StorageMode, VacuumLimits,
    DEFAULT_PAGED_MAINTENANCE_DIR,
};
use serde_json::json;
use std::sync::{Arc, Barrier};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}

fn small_steps() -> PagedVacuumScheduleLimits {
    let vacuum = VacuumLimits {
        max_pages: 1,
        max_bytes: 2 * 1024 * 1024,
        max_duration_millis: 1_000,
    };
    PagedVacuumScheduleLimits {
        vacuum,
        step_interval_ms: 1,
        saturation_retry_ms: 1,
        failure_retry_base_ms: 1,
        failure_retry_max_ms: 8,
        max_consecutive_failures: 4,
        max_schedule_state_bytes: 64 * 1024,
        demand: bicdb_core::ResourceDemand {
            memory_bytes: 4 * 1024 * 1024,
            io_bytes: vacuum.max_bytes,
            cpu_slots: 1,
            io_charge_bytes: vacuum.max_bytes,
        },
    }
}

#[test]
fn public_supervisor_restarts_every_step_and_publishes_completion_atomically() {
    let root = tempfile::tempdir().unwrap();
    let cfg = config(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
    db.create_collection("events").unwrap();
    let payload = "x".repeat(4 * 1024);
    let ids = (0..600)
        .map(|index| format!("row-{index:04}"))
        .collect::<Vec<_>>();
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.batch_insert(
            "events",
            ids.iter().map(|id| {
                Record::new(id.clone()).with_metadata(json!({
                    "payload": payload.clone(),
                }))
            }),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.delete_many("events", &ids).unwrap();
        tx.commit().unwrap();
    }
    db.flush().unwrap();

    let schedule = db
        .start_paged_vacuum_maintenance(100, small_steps())
        .unwrap();
    let operation_id = schedule.operation_id;
    assert_eq!(schedule.next_attempt_at_ms, Some(100));
    let active_error = db
        .start_paged_vacuum_maintenance(100, small_steps())
        .unwrap_err();
    assert!(active_error.to_string().contains("still active"));

    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let mut now = 100;
    for step in 0..2_000 {
        let outcome = db
            .tick_paged_vacuum_maintenance(operation_id, &governor, now)
            .unwrap();
        assert_eq!(governor.snapshot().background.active, 0);
        let durable = db.paged_vacuum_maintenance_status().unwrap().unwrap();
        assert_eq!(durable.operation_id, operation_id);
        assert_eq!(durable.state_sequence as usize, step + 1);
        if matches!(outcome, PagedVacuumScheduleAdvance::Complete { .. }) {
            assert!(durable.completed);
            assert!(durable.totals.successful_steps > 1);
            assert!(durable.totals.versions_reclaimed >= ids.len() as u64);
            break;
        }

        now = durable.next_attempt_at_ms.unwrap();
        // The directory lock and all in-memory state disappear every tick. The
        // next process reconstructs its authority solely from store pages plus
        // the atomically published supervisor checkpoint.
        drop(db);
        db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        if step == 1_999 {
            panic!("bounded vacuum did not complete after 2,000 restart-resumed steps");
        }
    }

    let completed = db.paged_vacuum_maintenance_status().unwrap().unwrap();
    let replacement = db
        .start_paged_vacuum_maintenance(now.saturating_add(1), small_steps())
        .unwrap();
    assert_ne!(replacement.operation_id, completed.operation_id);
    let stale = db
        .tick_paged_vacuum_maintenance(completed.operation_id, &governor, now.saturating_add(1))
        .unwrap_err();
    assert!(stale.to_string().contains("stale supervisor"));
    let paused = db
        .pause_paged_vacuum_maintenance(
            replacement.operation_id,
            "foreground latency protection",
            now.saturating_add(1),
        )
        .unwrap();
    assert_eq!(
        paused.paused_reason.as_deref(),
        Some("foreground latency protection")
    );
    let resumed = db
        .resume_paged_vacuum_maintenance(
            replacement.operation_id,
            now.saturating_add(2),
            now.saturating_add(2),
        )
        .unwrap();
    assert_eq!(resumed.next_attempt_at_ms, Some(now.saturating_add(2)));
}

#[test]
fn embedded_mode_rejects_supervisor_without_creating_state() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    let error = db
        .start_paged_vacuum_maintenance(100, PagedVacuumScheduleLimits::default())
        .unwrap_err();
    assert!(error.to_string().contains("storage_mode = server_paged"));
    assert!(!root.path().join(DEFAULT_PAGED_MAINTENANCE_DIR).exists());
}

#[test]
fn concurrent_ticks_are_serialized_and_only_one_step_is_admitted() {
    let root = tempfile::tempdir().unwrap();
    let db =
        Arc::new(BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap());
    let schedule = db
        .start_paged_vacuum_maintenance(100, PagedVacuumScheduleLimits::default())
        .unwrap();
    let governor = Arc::new(ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap());
    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..workers {
            let db = Arc::clone(&db);
            let governor = Arc::clone(&governor);
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.tick_paged_vacuum_maintenance(schedule.operation_id, &governor, 100)
            }));
        }
        for handle in handles {
            assert!(matches!(
                handle.join().unwrap().unwrap(),
                PagedVacuumScheduleAdvance::Complete { .. }
            ));
        }
    });

    let state = db.paged_vacuum_maintenance_status().unwrap().unwrap();
    assert_eq!(state.totals.successful_steps, 1);
    let compaction = governor
        .snapshot()
        .lanes
        .into_iter()
        .find(|lane| lane.lane == bicdb_core::ResourceLane::Compaction)
        .unwrap();
    assert_eq!(compaction.admitted, 1);
    assert_eq!(compaction.usage.active, 0);
}
