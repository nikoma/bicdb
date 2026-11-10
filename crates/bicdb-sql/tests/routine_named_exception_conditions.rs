use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
#[test]
fn named_missing_object_handlers_match_native_sqlstates_without_catching_other_errors() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        for (name, state) in [
            ("undefined_object", "42704"),
            ("undefined_table", "42P01"),
            ("undefined_column", "42703"),
            ("undefined_function", "42883"),
            ("invalid_schema_name", "3F000"),
            ("invalid_catalog_name", "3D000"),
        ] {
            sql.execute(&format!("CREATE FUNCTION caught_{name}() RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE {name}; EXCEPTION WHEN {name} THEN RETURN SQLSTATE; END $$")).unwrap();
            assert_eq!(
                sql.execute(&format!("SELECT caught_{name}()"))
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::String(state.into())]]
            );
        }
        for (name, clause, expected) in [
            ("numeric_default", "ERRCODE='42703'", "42703:42703"),
            (
                "named_default",
                "ERRCODE='undefined_column'",
                "42703:undefined_column",
            ),
            ("detail_default", "DETAIL='detail only'", "P0001:P0001"),
        ] {
            sql.execute(&format!("CREATE FUNCTION {name}() RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING {clause}; EXCEPTION WHEN OTHERS THEN RETURN SQLSTATE||':'||SQLERRM; END $$")).unwrap();
            assert_eq!(
                sql.execute(&format!("SELECT {name}()")).unwrap().rows,
                vec![vec![SqlValue::String(expected.into())]]
            );
        }
        sql.execute("CREATE FUNCTION unrelated_missing_column() RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='42703'; EXCEPTION WHEN undefined_object THEN RETURN 'incorrectly caught'; END $$").unwrap();
        assert_eq!(
            sql.execute("SELECT unrelated_missing_column()")
                .unwrap_err()
                .sqlstate(),
            "42703"
        );
    }
}
#[test]
fn deleting_attachment_rows_unlinks_transactionally_and_handles_already_missing_objects() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("SELECT lo_from_bytea(52001,'abc'::bytea)")
            .unwrap();
        sql.execute("CREATE TABLE lo_attachment(id integer PRIMARY KEY,loid oid);INSERT INTO lo_attachment VALUES(1,52001),(2,52001);CREATE FUNCTION attachment_unlink() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM lo_unlink(OLD.loid); RETURN OLD; EXCEPTION WHEN undefined_object THEN RETURN OLD; END $$;CREATE TRIGGER attachment_unlink BEFORE DELETE ON lo_attachment FOR EACH ROW EXECUTE FUNCTION attachment_unlink()").unwrap();
        sql.execute("BEGIN;DELETE FROM lo_attachment").unwrap();
        assert_eq!(
            sql.execute("SELECT lo_get(52001)").unwrap_err().sqlstate(),
            "42704"
        );
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            sql.execute("SELECT count(*) FROM lo_attachment")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)]]
        );
        assert_eq!(
            sql.execute("SELECT encode(lo_get(52001),'hex')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("616263".into())]]
        );
        sql.execute("DELETE FROM lo_attachment WHERE id=1").unwrap();
        assert_eq!(
            sql.execute("SELECT lo_get(52001)").unwrap_err().sqlstate(),
            "42704"
        );
        sql.execute("DELETE FROM lo_attachment WHERE id=2").unwrap();
        assert_eq!(
            sql.execute("SELECT count(*) FROM lo_attachment")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(0)]]
        );
    }
}
