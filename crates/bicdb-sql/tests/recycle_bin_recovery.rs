//! Startup recovery for the observed interrupted pre-entry DROP rename.
//! These phase cuts preserve data and native indexes; no generic orphan prune.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{
    ensure_primary_key_indexes, recover_interrupted_recycle_bin_renames, SqlSession, SqlValue,
};
fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}
fn seed(db: &mut BicDb) {
    let mut session = SqlSession::new(db);
    for sql in [
        "CREATE TABLE source(k int PRIMARY KEY,v text)",
        "CREATE INDEX source_v_idx ON source(v)",
        "INSERT INTO source VALUES(1,'acknowledged')",
    ] {
        session.execute(sql).unwrap();
    }
}
fn assert_row(db: &mut BicDb) {
    let mut session = SqlSession::new(db);
    assert_eq!(
        session.execute("SELECT k,v FROM source").unwrap().rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::String("acknowledged".into())
        ]]
    );
    assert_eq!(
        session
            .execute("INSERT INTO source VALUES(1,'duplicate')")
            .unwrap_err()
            .sqlstate(),
        "23505"
    );
}
#[test]
fn interrupted_collection_rename_rolls_back_before_primary_key_backfill_and_repeated_open() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let bin = format!("__bicdb_bin_{}", uuid::Uuid::new_v4().simple());
        {
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            seed(&mut db);
            // Core rename has completed, while SQL schema/index names remain
            // at the point immediately before the rest of DROP executes.
            db.rename_collection("source", &bin).unwrap();
        }
        {
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            assert_eq!(recover_interrupted_recycle_bin_renames(&mut db).unwrap(), 1);
            ensure_primary_key_indexes(&mut db).unwrap();
            assert_row(&mut db);
            assert!(db
                .index_definitions()
                .iter()
                .filter(|i| i.name.starts_with("source"))
                .all(|i| i.collection == "source"));
            assert_eq!(recover_interrupted_recycle_bin_renames(&mut db).unwrap(), 0);
        }
        let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
        ensure_primary_key_indexes(&mut db).unwrap();
        assert_row(&mut db);
    }
}
#[test]
fn completed_acknowledged_drop_is_not_resurrected() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        seed(&mut db);
        SqlSession::new(&mut db)
            .execute("DROP TABLE source")
            .unwrap();
    }
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    assert_eq!(recover_interrupted_recycle_bin_renames(&mut db).unwrap(), 0);
    ensure_primary_key_indexes(&mut db).unwrap();
    assert!(SqlSession::new(&mut db)
        .execute("SELECT * FROM source")
        .is_err());
    SqlSession::new(&mut db)
        .execute("FLASHBACK TABLE source TO BEFORE DROP")
        .unwrap();
    assert_row(&mut db);
}
#[test]
fn ambiguous_reused_original_name_is_refused_without_removing_either_backing() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    seed(&mut db);
    let bin = format!("__bicdb_bin_{}", uuid::Uuid::new_v4().simple());
    db.rename_collection("source", &bin).unwrap();
    db.create_collection("source").unwrap();
    db.insert(
        "source",
        Record::new("new").with_metadata(serde_json::json!({"other":"untouched"})),
    )
    .unwrap();
    let before = db.index_definitions();
    assert!(recover_interrupted_recycle_bin_renames(&mut db)
        .unwrap_err()
        .to_string()
        .contains("ambiguous"));
    assert_eq!(db.index_definitions(), before);
    assert!(db.get("source", "new").unwrap().is_some());
    assert_eq!(db.scan_collection(&bin).unwrap().len(), 1);
}
#[test]
fn normal_transaction_and_savepoint_rollback_keep_recycle_rows_visible() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    seed(&mut db);
    let mut s = SqlSession::new(&mut db);
    s.execute("BEGIN").unwrap();
    s.execute("SAVEPOINT before_drop").unwrap();
    s.execute("DROP TABLE source").unwrap();
    s.execute("ROLLBACK TO before_drop").unwrap();
    assert_eq!(
        s.execute("SELECT v FROM source").unwrap().rows,
        vec![vec![SqlValue::String("acknowledged".into())]]
    );
    s.execute("COMMIT").unwrap();
    drop(s);
    assert_row(&mut db);
}

#[test]
fn recovery_fence_wins_even_when_no_repair_is_needed() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        assert_eq!(recover_interrupted_recycle_bin_renames(&mut db).unwrap(), 0);
        db.durability_health_for_test()
            .record_failure("test.sql_catalog_recovery", &"injected");
        assert!(matches!(
            recover_interrupted_recycle_bin_renames(&mut db),
            Err(bicdb_sql::SqlError::BicDb(
                bicdb_core::BicDbError::WriteFailed { .. }
            ))
        ));
    }
}

#[test]
fn transactional_rename_requires_own_pending_transaction_and_checks_noop_health() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut own =
            BicDb::open_with_config(root.path().join("own"), config(mode.clone())).unwrap();
        let mut other = BicDb::open_with_config(root.path().join("other"), config(mode)).unwrap();
        own.create_collection("before").unwrap();
        other.create_collection("before").unwrap();
        let mut tx = own.begin_transaction().unwrap();
        assert!(other
            .rename_collection_preserving_pending_writes(&mut tx, "before", "after")
            .is_err());
        assert!(other.collections().iter().any(|meta| meta.name == "before"));
        own.rename_collection_preserving_pending_writes(&mut tx, "before", "before")
            .unwrap();
        own.durability_health_for_test()
            .record_failure("test.tx_rename", &"injected");
        for (old, new) in [("before", "before"), ("before", "after")] {
            assert!(matches!(
                own.rename_collection_preserving_pending_writes(&mut tx, old, new),
                Err(bicdb_core::BicDbError::WriteFailed { .. })
            ));
        }
        drop(tx);
        let mut finished = other.begin_transaction().unwrap();
        finished.insert("before", Record::new("pending")).unwrap();
        other
            .rename_collection_preserving_pending_writes(&mut finished, "before", "after")
            .unwrap();
        finished.commit().unwrap();
        assert!(other.get("after", "pending").unwrap().is_some());
    }
}

#[test]
fn transactional_rename_preserves_other_transaction_writes_and_read_locks() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for read_lock_only in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            db.create_collection("before").unwrap();
            db.insert("before", Record::new("existing")).unwrap();
            let mut other = db.begin_transaction().unwrap();
            if read_lock_only {
                assert!(other.try_lock_visible_record(None, "before", "existing").unwrap());
                assert_eq!(other.write_len(), 0);
            } else {
                other.insert("before", Record::new("other-pending")).unwrap();
            }
            let mut owner = db.begin_transaction().unwrap();
            owner.insert("before", Record::new("owner-pending")).unwrap();
            assert!(db
                .rename_collection_preserving_pending_writes(&mut owner, "before", "after")
                .is_err(), "rename admitted another transaction's state: {mode:?}, read_lock_only={read_lock_only}");
            assert!(db.collections().iter().any(|meta| meta.name == "before"));
            assert!(!db.collections().iter().any(|meta| meta.name == "after"));
            assert!(owner.get("before", "owner-pending").unwrap().is_some());
            if read_lock_only {
                assert!(!owner.try_lock_visible_record(None, "before", "existing").unwrap());
            } else {
                assert!(other.get("before", "other-pending").unwrap().is_some());
            }
            other.commit().unwrap();
            db.rename_collection_preserving_pending_writes(&mut owner, "before", "after")
                .unwrap();
            owner.commit().unwrap();
            assert!(db.get("after", "existing").unwrap().is_some());
            assert!(db.get("after", "owner-pending").unwrap().is_some());
            if !read_lock_only {
                assert!(db.get("after", "other-pending").unwrap().is_some());
            }
            let next = db.begin_transaction().unwrap();
            assert!(next.try_lock_visible_record(None, "after", "existing").unwrap());
        }
    }
}

#[test]
fn crash_recovery_never_reverses_a_live_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    seed(&mut db);
    let mut session = SqlSession::new(&mut db);
    session.execute("BEGIN").unwrap();
    session.execute("DROP TABLE source").unwrap();
    let pending = session.take_pending_transaction().unwrap();
    drop(session);
    assert!(db.check_catalog_recovery_quiescent().is_err());
    assert!(recover_interrupted_recycle_bin_renames(&mut db).is_err());
    assert!(db
        .collections()
        .iter()
        .any(|meta| meta.name.starts_with("__bicdb_bin_")));
    assert!(!db.collections().iter().any(|meta| meta.name == "source"));
    drop(pending);
    db.check_catalog_recovery_quiescent().unwrap();
    recover_interrupted_recycle_bin_renames(&mut db).unwrap();
    assert_row(&mut db);
}

#[test]
fn catalog_intent_feature_is_sticky_after_completed_ddl_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        seed(&mut db);
        let path = root.path().join("format.json");
        let before: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(!before["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "sql_catalog_intents_v1"));
        let mut sql = SqlSession::new(&mut db);
        sql.execute("DROP TABLE source").unwrap();
        sql.execute("FLASHBACK TABLE source TO BEFORE DROP")
            .unwrap();
        sql.execute("DROP TABLE source").unwrap();
        sql.execute("PURGE RECYCLEBIN").unwrap();
    }
    let mut db = BicDb::open(root.path()).unwrap();
    recover_interrupted_recycle_bin_renames(&mut db).unwrap();
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("format.json")).unwrap()).unwrap();
    assert!(metadata["feature_flags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|flag| flag == "sql_catalog_intents_v1"));
}
