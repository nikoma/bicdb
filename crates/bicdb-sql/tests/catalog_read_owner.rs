//! SQL sessions must carry their native owner's read identity per statement.
use bicdb_core::{BicDb, BicDbError, CatalogScopeToken, DbConfig, StorageMode, Transaction};
use bicdb_sql::{infer_query_result_columns_for_describe, SqlSession, SqlValue};

fn database(path: &std::path::Path, mode: StorageMode) -> BicDb {
    let mut db = BicDb::open_with_config(
        path,
        DbConfig::default().with_fsync(true).with_storage_mode(mode),
    )
    .unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); CREATE TABLE unrelated(id INT PRIMARY KEY); INSERT INTO source VALUES(1,'kept'); INSERT INTO unrelated VALUES(2)").unwrap();
    assert!(db.get("__bicdb_pg_schema", "source").unwrap().is_some());
    db
}

fn reserve(owner: &Transaction) -> CatalogScopeToken {
    owner
        .reserve_catalog_resources(
            &["source".into(), "renamed".into()],
            &[
                ("__bicdb_pg_schema".into(), "source".into()),
                ("__bicdb_pg_schema".into(), "renamed".into()),
            ],
        )
        .unwrap()
}

fn unrelated_data_continues(warm: bool) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path(), mode);
        if warm {
            SqlSession::new(&mut db)
                .execute("SELECT id FROM unrelated")
                .unwrap();
            infer_query_result_columns_for_describe(&db, "SELECT id FROM unrelated").unwrap();
        }
        let owner = db.begin_transaction().unwrap();
        let token = reserve(&owner);
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT id FROM unrelated")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)]]
        );
        assert!(
            infer_query_result_columns_for_describe(&db, "SELECT id FROM unrelated")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT id FROM source")
                .unwrap_err()
                .sqlstate(),
            "40001"
        );
        assert_eq!(
            infer_query_result_columns_for_describe(&db, "SELECT id FROM source")
                .unwrap_err()
                .sqlstate(),
            "40001"
        );
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}

#[test]
fn cold_unrelated_sql_and_describe_continue_under_an_exact_catalog_reservation() {
    unrelated_data_continues(false);
}

#[test]
fn warm_unrelated_sql_and_describe_continue_under_an_exact_catalog_reservation() {
    unrelated_data_continues(true);
}

#[test]
fn a_sql_session_reads_its_own_reserved_catalog_without_mutation_delegation() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let owner = db.begin_transaction().unwrap();
        let token = reserve(&owner);
        let mut sql = SqlSession::new_shared(&db).with_pending_transaction(owner);
        assert_eq!(
            sql.execute("SELECT id FROM source").unwrap().rows,
            vec![vec![SqlValue::Int(1)]]
        );
        let owner = sql.take_pending_transaction().unwrap();
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}

#[test]
fn owner_read_context_changes_across_commit_inside_one_sql_batch() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let owner = db.begin_transaction().unwrap();
        let _token = reserve(&owner);
        let mut sql = SqlSession::new_shared(&db).with_pending_transaction(owner);
        assert_eq!(
            sql.execute("SELECT id FROM source; COMMIT; SELECT id FROM source")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
        assert!(sql.take_pending_transaction().is_none());
        db.check_catalog_read_access("__bicdb_pg_schema", Some("source"))
            .unwrap();
    }
}

#[test]
fn a_sql_catalog_observation_fences_later_metadata_changes_until_transaction_end() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let mut sql = SqlSession::new_shared(&db);
        sql.execute("BEGIN; SELECT relname FROM pg_class WHERE relname='source'")
            .unwrap();
        let reader = sql.take_pending_transaction().unwrap();
        let owner = db.begin_transaction().unwrap();
        assert!(
            matches!(
                owner.reserve_catalog_resources(
                    &[],
                    &[("__bicdb_pg_schema".into(), "source".into())]
                ),
                Err(BicDbError::TransactionConflict(_))
            ),
            "SQL catalog reads must remain observations of their native transaction"
        );
        reader.rollback().unwrap();
        let token = owner
            .reserve_catalog_resources(&[], &[("__bicdb_pg_schema".into(), "source".into())])
            .unwrap();
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}
