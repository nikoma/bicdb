//! Routine signatures retain composite type identity across relation renames.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const SIGNATURES: &str = "SELECT proargtypes,prorettype,pg_get_function_arguments(oid),pg_get_function_result(oid) FROM pg_proc WHERE proname IN ('hold_record','hold_array') ORDER BY proname";

fn call_routines(sql: &mut SqlSession<'_>, row_type: &str) {
    let query = format!("SELECT (hold_record(ROW(1,'record-value')::{row_type})).payload,((hold_array(ARRAY[ROW(1,'array-value')::{row_type}]))[1]).payload");
    assert_eq!(
        sql.execute(&query).unwrap().rows,
        vec![vec![
            SqlValue::String("record-value".into()),
            SqlValue::String("array-value".into())
        ]]
    );
}

#[test]
fn table_rename_preserves_routine_row_and_array_type_identity() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for commit in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let cfg = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            let expected;
            {
                let mut sql = SqlSession::new(&mut db);
                sql.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); CREATE FUNCTION hold_record(arg source) RETURNS source LANGUAGE sql AS 'SELECT arg'; CREATE FUNCTION hold_array(arg source[]) RETURNS source[] LANGUAGE sql AS 'SELECT arg'").unwrap();
                let original = sql.execute(SIGNATURES).unwrap().rows;
                assert_eq!(original.len(), 2);
                call_routines(&mut sql, "source");
                let mut renamed = original.clone();
                for row in &mut renamed {
                    // Numeric argument/return OIDs must remain identical.
                    for value in &mut row[2..] {
                        let SqlValue::String(name) = value else {
                            panic!("routine type declaration is not text: {value:?}");
                        };
                        assert!(name.contains("source"), "{name}");
                        *name = name.replace("source", "renamed");
                    }
                }
                sql.execute("BEGIN; ALTER TABLE source RENAME TO renamed")
                    .unwrap();
                assert_eq!(
                    sql.execute(SIGNATURES).unwrap().rows,
                    renamed,
                    "{mode} commit={commit}: routine declarations follow the original row type"
                );
                call_routines(&mut sql, "renamed");
                sql.execute(if commit { "COMMIT" } else { "ROLLBACK" })
                    .unwrap();
                expected = if commit { renamed } else { original };
                assert_eq!(sql.execute(SIGNATURES).unwrap().rows, expected);
                call_routines(&mut sql, if commit { "renamed" } else { "source" });
                if commit {
                    // Reusing the spelling cannot rebind an existing signature
                    // to the replacement relation's distinct row type.
                    sql.execute("CREATE TABLE source(other TEXT)").unwrap();
                    assert_eq!(sql.execute(SIGNATURES).unwrap().rows, expected);
                    call_routines(&mut sql, "renamed");
                }
            }
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                assert_eq!(
                    sql.execute(SIGNATURES).unwrap().rows,
                    expected,
                    "{mode} commit={commit}: routine identity after reopen"
                );
                call_routines(&mut sql, if commit { "renamed" } else { "source" });
            }
            db.close().unwrap();
        }
    }
}
