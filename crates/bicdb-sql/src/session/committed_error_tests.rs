//! A genuine post-commit finalization error through public SQL/model APIs.
//! This is not the timed visibility-Unknown control and uses no Core test hook.
use super::*;
use bicdb_core::{DbConfig, MemoryIndexMode, MemoryJobStatus, ModelRegistryEntry, StorageMode};

fn primary_core_error(error: &SqlError) -> Option<&bicdb_core::BicDbError> {
    match error {
        SqlError::BicDb(error) => Some(error),
        SqlError::WithNotices { error, .. } => primary_core_error(error),
        _ => None,
    }
}

#[test]
fn actual_committed_sync_dimension_error_preserves_sql_ddl_row_guc_and_original_error() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(true),
    )
    .unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE source_notes(id TEXT PRIMARY KEY, body TEXT)")
            .unwrap();
        sql.execute("INSERT INTO source_notes(id, body) VALUES ('seed', NULL)")
            .unwrap();
        assert_eq!(
            sql.execute("SELECT id, body FROM source_notes")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("seed".into()), SqlValue::Null]]
        );
    }
    // Preserve the real SQL row's exact encoded ID and metadata. Only its
    // native vector changes, through the public transaction update API.
    let rows = db.scan_collection("source_notes").unwrap();
    assert_eq!(rows.len(), 1);
    let sql_seed = rows[0].clone();
    let seed_with_vector = sql_seed.clone().with_vector(vec![1.0, 0.0, 0.0]);
    let mut update = db.begin_transaction().unwrap();
    update
        .update("source_notes", seed_with_vector.clone())
        .unwrap();
    update.commit().unwrap();
    assert_eq!(
        db.get("source_notes", &sql_seed.id).unwrap().as_deref(),
        Some(&seed_with_vector)
    );
    db.register_embedding_model(ModelRegistryEntry::local_test("valid_model_dim16", 16))
        .unwrap();
    db.create_memory_index(
        "source_notes_body_sync",
        "source_notes",
        "body",
        "valid_model_dim16",
        MemoryIndexMode::Sync,
    )
    .unwrap();
    // SQL NULL is not model input. The valid deterministic model's output
    // conflicts only when a later real row supplies text after source commit.
    assert!(db.memory_index_jobs().is_empty());
    let baseline_floor = db.current_visibility_watermark();
    let expected_dimension_error = bicdb_core::BicDbError::DimensionMismatch {
        collection: "source_notes".into(),
        expected: 3,
        actual: 16,
    }
    .to_string();
    {
        let mut sql = SqlSession::new(&mut db)
            .with_deferred_commit()
            .with_snapshot_floor(baseline_floor);
        sql.execute("SET application_name = 'before_committed_error'")
            .unwrap();
        sql.execute("BEGIN").unwrap();
        sql.execute("CREATE TABLE committed_ddl_witness(id INTEGER PRIMARY KEY)")
            .unwrap();
        sql.execute("INSERT INTO committed_ddl_witness(id) VALUES (7)")
            .unwrap();
        sql.execute("SET application_name = 'after_committed_error'")
            .unwrap();
        sql.execute("INSERT INTO source_notes(id, body) VALUES ('committed', 'real indexed text')")
            .unwrap();
        let successful_floor_before_commit = sql.snapshot_floor;
        let result = sql.execute("COMMIT");
        eprintln!(
            "ISSUE205_SQL_COMMITTED_DIMENSION_ERROR {}",
            serde_json::json!({
                "actual_result": format!("{result:?}"),
                "baseline_success_floor": baseline_floor,
            "actual_success_floor_before_commit": successful_floor_before_commit,
                "success_floor_after_error": sql.snapshot_floor,
                "actual_transaction_owner_removed": sql.tx.is_none(),
                "committed_undo_entries_remaining": sql.ddl_undo.len(),
                "actual_global_prefix": sql.db_ref().current_visibility_watermark(),
                "scope": "generic actual post-commit dimension finalization error; NOT timed Unknown or a Core cfg(test) injection"
            })
        );
        let error = result.unwrap_err();
        match primary_core_error(&error) {
            Some(bicdb_core::BicDbError::Memory(detail)) => assert_eq!(
                detail,
                &format!("sync memory indexing failed for 1 job(s): {expected_dimension_error}")
            ),
            other => panic!("must retain original real post-commit model error: {other:?}"),
        }
        assert_eq!(sql.snapshot_floor, successful_floor_before_commit);
        assert!(sql.tx.is_none());
        assert!(sql.ddl_undo.is_empty());
        assert_eq!(
            sql.execute("SHOW application_name").unwrap().rows,
            vec![vec![SqlValue::String("after_committed_error".into())]]
        );
        assert_eq!(
            sql.execute("SELECT id FROM committed_ddl_witness")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(7)]]
        );
        assert_eq!(
            sql.execute("SELECT id, body FROM source_notes WHERE id = 'committed'")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("committed".into()),
                SqlValue::String("real indexed text".into())
            ]]
        );
        // A later cleanup request cannot resurrect the detached committed
        // DDL/GUC undo log after the caller already received the original error.
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            sql.execute("SELECT id FROM committed_ddl_witness")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(7)]]
        );
        assert_eq!(
            sql.execute("SHOW application_name").unwrap().rows,
            vec![vec![SqlValue::String("after_committed_error".into())]]
        );
    }
    db.check_recovery_state().unwrap();
    let jobs = db.memory_index_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status, MemoryJobStatus::Failed);
    assert_eq!(
        jobs[0].error.as_deref(),
        Some(expected_dimension_error.as_str())
    );
    assert!(jobs[0].unresolved_derived_writes.is_empty());
    let mut fresh = SqlSession::new(&mut db);
    assert_eq!(
        fresh
            .execute("SELECT id FROM committed_ddl_witness")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(7)]]
    );
    assert_eq!(
        fresh
            .execute("SELECT body FROM source_notes WHERE id = 'committed'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("real indexed text".into())]]
    );
}
