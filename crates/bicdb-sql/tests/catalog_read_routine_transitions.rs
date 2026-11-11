//! Repeated procedural expressions must use the owner after an internal COMMIT.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn loop_condition_after_commit(condition: &str) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'kept'); CREATE TABLE sink(id INT PRIMARY KEY)")
            .unwrap();
        SqlSession::new(&mut db)
            .execute(&format!("CREATE PROCEDURE loop_read() LANGUAGE plpgsql AS $$ DECLARE i INT := 0; BEGIN WHILE {condition} AND i < 2 LOOP INSERT INTO sink VALUES(i); COMMIT; i := i + 1; END LOOP; END; $$"))
            .unwrap();
        let mut sql = SqlSession::new_shared(&db);
        sql.execute("CALL loop_read()").unwrap();
        assert!(sql.take_pending_transaction().is_none());
        db.check_catalog_recovery_quiescent().unwrap();
        assert_eq!(
            sql.execute("SELECT id FROM sink ORDER BY id").unwrap().rows,
            vec![vec![SqlValue::Int(0)], vec![SqlValue::Int(1)]]
        );
    }
}

#[test]
fn a_repeated_row_type_condition_does_not_restore_an_ended_owner() {
    loop_condition_after_commit("(ROW(i,'kept')::source).payload = 'kept'");
}

#[test]
fn a_repeated_scalar_subquery_does_not_restore_an_ended_owner() {
    loop_condition_after_commit("(SELECT count(*) FROM source WHERE id=1) = 1");
}
