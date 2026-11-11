//! Table composite types participate in routine input and result coercion.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn check(array: bool) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        let pg_type = if array { "typed_row[]" } else { "typed_row" };
        let value = if array {
            "ARRAY[ROW(1,'kept')::typed_row]"
        } else {
            "ROW(1,'kept')::typed_row"
        };
        let payload = if array {
            format!("((typed_identity({value}))[1]).payload")
        } else {
            format!("(typed_identity({value})).payload")
        };
        let query = format!("SELECT {payload},pg_typeof(typed_identity({value}))::text");
        let expected = vec![vec![
            SqlValue::String("kept".into()),
            SqlValue::String(pg_type.into()),
        ]];
        let signature;
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE typed_row(id INT PRIMARY KEY,payload TEXT)")
                .unwrap();
            sql.execute(&format!("CREATE FUNCTION typed_identity(arg {pg_type}) RETURNS {pg_type} LANGUAGE sql AS 'SELECT arg'"))
                .unwrap();
            sql.execute(&format!(
                "CREATE PROCEDURE typed_procedure(arg {pg_type}) LANGUAGE sql AS 'SELECT 1'"
            ))
            .unwrap();
            assert_eq!(sql.execute(&query).unwrap().rows, expected, "{mode}");
            sql.execute(&format!("CALL typed_procedure({value})"))
                .unwrap();
            signature = sql
                .execute(
                    "SELECT proargtypes,prorettype FROM pg_proc WHERE proname='typed_identity'",
                )
                .unwrap()
                .rows;
            assert_eq!(signature.len(), 1);
            let type_oid = sql
                .execute(&format!("SELECT '{pg_type}'::regtype::oid"))
                .unwrap()
                .rows[0][0]
                .clone();
            assert_eq!(signature[0][1], type_oid);
            assert_eq!(signature[0][0].to_cell(), type_oid.to_cell());
        }
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(sql.execute(&query).unwrap().rows, expected, "{mode} reopen");
            sql.execute(&format!("CALL typed_procedure({value})"))
                .unwrap();
            assert_eq!(
                sql.execute(
                    "SELECT proargtypes,prorettype FROM pg_proc WHERE proname='typed_identity'"
                )
                .unwrap()
                .rows,
                signature
            );
        }
        db.close().unwrap();
    }
}

#[test]
fn scalar_table_row_routine_preserves_value_and_type_after_reopen() {
    check(false);
}

#[test]
fn array_table_row_routine_preserves_value_and_type_after_reopen() {
    check(true);
}
