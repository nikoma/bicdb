//! Trigger execution and guards follow the same relation through a rename.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn setup(sql: &mut SqlSession<'_>, indexed: bool) {
    let key = if indexed { "PRIMARY KEY" } else { "" };
    sql.execute(&format!(
        "CREATE TABLE trigger_source(rowkey INT {key},payload INT)"
    ))
    .unwrap();
    sql.execute("CREATE FUNCTION rename_row_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.rowkey < 0 THEN RAISE EXCEPTION 'negative row rejected' USING ERRCODE='42501'; END IF; NEW.payload := NEW.payload + 1; RETURN NEW; END $$").unwrap();
    sql.execute("CREATE TRIGGER retained_guard BEFORE INSERT OR UPDATE ON trigger_source FOR EACH ROW EXECUTE FUNCTION rename_row_guard()").unwrap();
    sql.execute("INSERT INTO trigger_source VALUES(1,10)")
        .unwrap();
}

fn rows(sql: &mut SqlSession<'_>, table: &str) -> Vec<Vec<SqlValue>> {
    sql.execute(&format!(
        "SELECT rowkey,payload FROM {table} ORDER BY rowkey"
    ))
    .unwrap()
    .rows
}

#[test]
fn renamed_trigger_keeps_firing_and_rejecting_rows_after_restart() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            setup(&mut sql, true);
            sql.execute("ALTER TABLE trigger_source RENAME TO trigger_target")
                .unwrap();
            sql.execute("INSERT INTO trigger_target VALUES(2,20)")
                .unwrap();
            sql.execute("UPDATE trigger_target SET payload=30 WHERE rowkey=1")
                .unwrap();
            assert_eq!(
                rows(&mut sql, "trigger_target"),
                vec![
                    vec![SqlValue::Int(1), SqlValue::Int(31)],
                    vec![SqlValue::Int(2), SqlValue::Int(21)]
                ]
            );
            assert_eq!(
                sql.execute("INSERT INTO trigger_target VALUES(-1,99)")
                    .unwrap_err()
                    .sqlstate(),
                "42501"
            );
        }
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("INSERT INTO trigger_target VALUES(3,40)")
            .unwrap();
        assert_eq!(
            rows(&mut sql, "trigger_target")[2],
            vec![SqlValue::Int(3), SqlValue::Int(41)]
        );
        assert_eq!(
            sql.execute("INSERT INTO trigger_target VALUES(-2,99)")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
    }
}

#[test]
fn trigger_binding_returns_to_old_name_at_savepoint_and_full_rollback() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        let mut sql = SqlSession::new(&mut db);
        // An unindexed relation keeps this regression independent of the
        // separate transactional offline-index-builder failure.
        setup(&mut sql, false);
        sql.execute("BEGIN; SAVEPOINT before_rename; ALTER TABLE trigger_source RENAME TO trigger_target; INSERT INTO trigger_target VALUES(2,20)").unwrap();
        assert_eq!(
            rows(&mut sql, "trigger_target")[1],
            vec![SqlValue::Int(2), SqlValue::Int(21)]
        );
        sql.execute("ROLLBACK TO before_rename; INSERT INTO trigger_source VALUES(3,30)")
            .unwrap();
        assert_eq!(
            rows(&mut sql, "trigger_source")[1],
            vec![SqlValue::Int(3), SqlValue::Int(31)]
        );
        sql.execute("ROLLBACK; INSERT INTO trigger_source VALUES(4,40)")
            .unwrap();
        assert_eq!(
            rows(&mut sql, "trigger_source"),
            vec![
                vec![SqlValue::Int(1), SqlValue::Int(11)],
                vec![SqlValue::Int(4), SqlValue::Int(41)]
            ]
        );
        sql.execute("BEGIN; ALTER TABLE trigger_source RENAME TO trigger_committed; COMMIT; INSERT INTO trigger_committed VALUES(5,50)").unwrap();
        assert_eq!(
            rows(&mut sql, "trigger_committed")[2],
            vec![SqlValue::Int(5), SqlValue::Int(51)]
        );
        assert_eq!(
            sql.execute("INSERT INTO trigger_committed VALUES(-1,99)")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
    }
}
