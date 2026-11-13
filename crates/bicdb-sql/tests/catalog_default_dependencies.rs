//! A deferred write retains catalog dependencies of readonly default helpers.
use bicdb_core::{BicDb, BicDbError, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn readonly_default_dependency(warm: bool, explicit: bool) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'kept'); CREATE TABLE sink(id INT PRIMARY KEY,payload TEXT); CREATE FUNCTION default_lookup() RETURNS TEXT LANGUAGE sql AS 'SELECT payload FROM source WHERE id=1'; CREATE PROCEDURE buffered_lookup(v TEXT DEFAULT default_lookup()) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO sink VALUES(3,v); END; $$")
            .unwrap();
        if warm {
            assert_eq!(
                SqlSession::new_shared(&db)
                    .execute("SELECT default_lookup()")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::String("kept".into())]]
            );
        }
        let mut sql = SqlSession::new_shared(&db).with_deferred_commit();
        if explicit {
            sql.execute("BEGIN").unwrap();
        }
        sql.execute("CALL buffered_lookup()").unwrap();
        let reader = sql.take_pending_transaction().unwrap();
        assert!(db.scan_collection("sink").unwrap().is_empty());
        assert_eq!(reader.scan_collection("sink").unwrap().len(), 1);
        let owner = db.begin_transaction().unwrap();
        assert!(
            matches!(
                owner.reserve_catalog_resources(
                    &[],
                    &[("__bicdb_pg_schema".into(), "source".into())]
                ),
                Err(BicDbError::TransactionConflict(_))
            ),
            "the readonly helper's metadata dependency belongs to the buffered write owner"
        );
        reader.rollback().unwrap();
        let scope = owner
            .reserve_catalog_resources(&[], &[("__bicdb_pg_schema".into(), "source".into())])
            .unwrap();
        owner.release_catalog_scope(&scope).unwrap();
        owner.rollback().unwrap();
    }
}

#[test]
fn cold_default_helper_metadata_survives_the_auto_transaction_boundary() {
    readonly_default_dependency(false, false);
}

#[test]
fn warm_default_helper_metadata_survives_the_auto_transaction_boundary() {
    readonly_default_dependency(true, false);
}

#[test]
fn a_default_helper_keeps_the_existing_explicit_owner() {
    readonly_default_dependency(true, true);
}
