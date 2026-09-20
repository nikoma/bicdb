use bicdb_core::{
    BTreeVerifyLimits, BicDb, DbConfig, PagedIntegrityPhase, PagedIntegrityScheduleAdvance,
    PagedIntegrityScheduleLimits, PagedVacuumScheduleLimits, Record, ResourceDemand,
    ResourceGovernor, ResourceGovernorConfig, StorageMode, VersionChainVerifyLimits,
    DEFAULT_PAGED_INTEGRITY_SCHEDULE,
};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}

fn small_steps() -> PagedIntegrityScheduleLimits {
    let btree = BTreeVerifyLimits {
        max_entries: 3,
        max_leaf_pages: 1,
        max_page_bytes: 128 * 1024 * 1024,
        max_key_bytes: 64 * 1024,
        max_duration_millis: 10_000,
        max_cursor_key_bytes: 64 * 1024,
        max_height: 64,
    };
    let version_chains = VersionChainVerifyLimits {
        max_keys: 2,
        max_versions: 32,
        max_versions_per_chain: 4,
        max_bytes: 32 * bicdb_core::VERSION_HEADER_BYTES as u64,
        max_duration_millis: 10_000,
        max_fault_samples: 0,
        max_fault_sample_bytes: 0,
        max_cursor_key_bytes: 64 * 1024,
    };
    let io_bytes = btree.max_page_bytes.max(version_chains.max_bytes);
    PagedIntegrityScheduleLimits {
        btree,
        version_chains,
        step_interval_ms: 1,
        saturation_retry_ms: 1_000,
        failure_retry_base_ms: 1,
        failure_retry_max_ms: 8,
        max_consecutive_failures: 4,
        max_schedule_state_bytes: 256 * 1024,
        demand: ResourceDemand {
            memory_bytes: 16 * 1024 * 1024,
            io_bytes,
            cpu_slots: 1,
            io_charge_bytes: io_bytes,
        },
    }
}

#[test]
fn public_integrity_supervisor_resumes_every_phase_after_database_reopen() {
    let root = tempfile::tempdir().unwrap();
    let cfg = config(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
    db.create_collection("patients").unwrap();
    {
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .batch_insert(
                "patients",
                (0..25).map(|index| Record::new(format!("patient-{index:03}"))),
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    db.flush().unwrap();

    let schedule = db
        .start_paged_integrity_maintenance(100, small_steps())
        .unwrap();
    let operation_id = schedule.operation_id;
    assert_eq!(schedule.phase, PagedIntegrityPhase::Structural);
    assert_eq!(schedule.next_attempt_at_ms, Some(100));
    assert!(db
        .start_paged_integrity_maintenance(100, small_steps())
        .unwrap_err()
        .to_string()
        .contains("still active"));

    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let mut now = 100_u64;
    let mut saw_phase_boundary = false;
    for step in 0..1_000 {
        let outcome = db
            .tick_paged_integrity_maintenance(operation_id, &governor, now)
            .unwrap();
        assert_eq!(governor.snapshot().background.active, 0);
        let durable = db.paged_integrity_maintenance_status().unwrap().unwrap();
        assert_eq!(durable.operation_id, operation_id);
        assert_eq!(durable.state_sequence as usize, step + 1);
        if matches!(
            outcome,
            PagedIntegrityScheduleAdvance::PhaseAdvanced {
                phase: PagedIntegrityPhase::VersionChains,
                ..
            }
        ) {
            saw_phase_boundary = true;
            assert_eq!(durable.totals.version_chain_steps, 0);
            assert!(durable
                .last_btree_report
                .as_ref()
                .is_some_and(|report| report.complete && report.valid));
        }
        if matches!(
            outcome,
            PagedIntegrityScheduleAdvance::Complete { valid: true, .. }
        ) {
            assert!(durable.completed);
            assert!(durable.valid);
            assert_eq!(durable.phase, PagedIntegrityPhase::Complete);
            assert_eq!(durable.totals.structural_entries, 25);
            assert_eq!(durable.totals.keys_examined, 25);
            assert_eq!(durable.totals.versions_examined, 25);
            break;
        }

        now = durable.next_attempt_at_ms.unwrap();
        drop(db);
        db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        if step == 999 {
            panic!("integrity supervisor did not complete after 1,000 restart-resumed steps");
        }
    }
    assert!(saw_phase_boundary);

    let completed = db.paged_integrity_maintenance_status().unwrap().unwrap();
    assert!(completed.completed && completed.valid);
    let vacuum = db
        .start_paged_vacuum_maintenance(now.saturating_add(1), PagedVacuumScheduleLimits::default())
        .unwrap();
    assert_eq!(vacuum.store_id, completed.store_id);
    let replacement = db
        .start_paged_integrity_maintenance(now.saturating_add(1), small_steps())
        .unwrap();
    assert_ne!(replacement.operation_id, completed.operation_id);
    let paused = db
        .pause_paged_integrity_maintenance(
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
        .resume_paged_integrity_maintenance(
            replacement.operation_id,
            now.saturating_add(2),
            now.saturating_add(2),
        )
        .unwrap();
    assert_eq!(resumed.next_attempt_at_ms, Some(now.saturating_add(2)));
    assert!(db
        .tick_paged_integrity_maintenance(completed.operation_id, &governor, now.saturating_add(2))
        .unwrap_err()
        .to_string()
        .contains("stale supervisor"));
}

#[test]
fn embedded_mode_rejects_integrity_supervision_without_creating_state() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    assert!(db
        .start_paged_integrity_maintenance(100, PagedIntegrityScheduleLimits::default())
        .unwrap_err()
        .to_string()
        .contains("storage_mode = server_paged"));
    assert!(!root.path().join(DEFAULT_PAGED_INTEGRITY_SCHEDULE).exists());
    assert!(db.paged_integrity_maintenance_status().is_err());
}

#[test]
fn incompatible_first_step_limits_fail_before_schedule_creation() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    let mut limits = small_steps();
    limits.btree.max_page_bytes = 8 * 1024;
    let error = db
        .start_paged_integrity_maintenance(100, limits)
        .unwrap_err();
    assert!(error.to_string().contains("height guard"));
    assert!(!root.path().join(DEFAULT_PAGED_INTEGRITY_SCHEDULE).exists());
}
