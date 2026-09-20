//! L1: autovacuum. The SQL `VACUUM` statement drives the bounded reclaimer
//! to completion; the maintenance handle (what the pgwire worker clones)
//! runs the same supervised schedule tick by tick.

use bicdb_core::{
    BicDb, CancellationToken, DbConfig, PagedVacuumScheduleAdvance, ResourceGovernor,
    ResourceGovernorConfig, StorageMode,
};
use bicdb_sql::{SqlSession, SqlValue};

fn open(dir: &tempfile::TempDir) -> BicDb {
    BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap()
}

fn churn(sql: &mut SqlSession) {
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    for round in 0..4 {
        let values: Vec<String> = (0..300)
            .map(|index| format!("('k{index:04}', 'round {round} payload {index} xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')"))
            .collect();
        if round == 0 {
            sql.execute(&format!("INSERT INTO docs VALUES {}", values.join(", ")))
                .unwrap();
        } else {
            sql.execute(&format!(
                "UPDATE docs SET body = 'round {round} yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'"
            ))
            .unwrap();
        }
    }
    sql.execute("DELETE FROM docs WHERE id < 'k0150'").unwrap();
}

#[test]
fn sql_vacuum_reclaims_dead_versions_and_reports() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(&dir);
    let mut sql = SqlSession::new(&mut db);
    churn(&mut sql);

    let result = sql.execute("VACUUM").unwrap();
    assert_eq!(result.columns[0], "pages_scanned");
    let value = |index: usize| match result.rows[0][index] {
        SqlValue::Int(value) => value,
        ref other => panic!("expected int, got {other:?}"),
    };
    assert!(value(0) > 0, "pages scanned");
    assert!(
        value(2) > 0,
        "update/delete churn must leave reclaimable versions"
    );

    // A second pass right after finds (almost) nothing new.
    let again = sql.execute("VACUUM").unwrap();
    let SqlValue::Int(reclaimed_again) = again.rows[0][2] else {
        panic!("expected int");
    };
    assert!(
        reclaimed_again <= value(2) / 10,
        "second vacuum should be nearly idle, reclaimed {reclaimed_again}"
    );

    // Guardrails: no options, no transactions.
    assert!(sql
        .execute("VACUUM FULL")
        .unwrap_err()
        .to_string()
        .contains("store-wide"));
    sql.execute("BEGIN").unwrap();
    assert!(sql
        .execute("VACUUM")
        .unwrap_err()
        .to_string()
        .contains("transaction"));
    sql.execute("ROLLBACK").unwrap();
}

#[test]
fn maintenance_handle_drives_a_supervised_pass_to_completion() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(&dir);
    {
        let mut sql = SqlSession::new(&mut db);
        churn(&mut sql);
    }
    let handle = db.paged_vacuum_maintenance_handle().expect("server_paged");
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();

    assert!(handle
        .vacuum_maintenance_status_optional()
        .unwrap()
        .is_none());
    let schedule = handle
        .start_vacuum_maintenance(1_000, Default::default())
        .unwrap();

    let mut now_ms = 1_000_u64;
    let mut reclaimed = 0_u64;
    for _ in 0..10_000 {
        now_ms += 50;
        match handle
            .tick_vacuum_maintenance(schedule.operation_id, &governor, now_ms)
            .unwrap()
        {
            PagedVacuumScheduleAdvance::Complete { totals, .. } => {
                reclaimed = totals.versions_reclaimed;
                break;
            }
            PagedVacuumScheduleAdvance::NotDue { next_attempt_at_ms }
            | PagedVacuumScheduleAdvance::Progress {
                next_attempt_at_ms, ..
            } => {
                now_ms = now_ms.max(next_attempt_at_ms);
            }
            PagedVacuumScheduleAdvance::ResourceDeferred { retry_at_ms } => {
                now_ms = now_ms.max(retry_at_ms);
            }
            other => panic!("unexpected advance {other:?}"),
        }
    }
    assert!(reclaimed > 0, "supervised pass must reclaim the churn");
    // Completed schedules are visible as completed, and cancellation in the
    // SQL path still works after (fresh store state).
    let status = handle.vacuum_maintenance_status_optional().unwrap();
    assert!(status.is_none_or(|schedule| schedule.completed));

    let cancelled = CancellationToken::new(
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        None,
    );
    assert!(db.run_vacuum_to_completion(&cancelled).is_err());
}
