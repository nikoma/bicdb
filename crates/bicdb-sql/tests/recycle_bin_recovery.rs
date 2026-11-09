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
            let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
            seed(&mut db);
            // Core rename has completed, while SQL schema/index names remain
            // at the point immediately before the rest of DROP executes.
            db.rename_collection("source", &bin).unwrap();
        }
        {
            let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
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
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
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
