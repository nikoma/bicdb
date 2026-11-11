//! Type references in routine defaults and output columns are dependencies.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn defaults(sql: &mut SqlSession<'_>) {
    assert_eq!(
        sql.execute("SELECT default_header_fn()").unwrap().rows,
        vec![vec![SqlValue::String("kept".into())]]
    );
    sql.execute("CALL default_header_proc()").unwrap();
}

#[test]
fn routine_default_casts_follow_a_renamed_table_type() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for commit in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let cfg = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                sql.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); CREATE FUNCTION default_header_fn(arg TEXT DEFAULT (ROW(1,'kept')::source).payload) RETURNS TEXT LANGUAGE sql AS 'SELECT arg'; CREATE PROCEDURE default_header_proc(arg TEXT DEFAULT (ROW(1,'kept')::source).payload) LANGUAGE sql AS 'SELECT 1'").unwrap();
                defaults(&mut sql);
                sql.execute("BEGIN; ALTER TABLE source RENAME TO renamed")
                    .unwrap();
                defaults(&mut sql);
                sql.execute(if commit { "COMMIT" } else { "ROLLBACK" })
                    .unwrap();
                if commit {
                    sql.execute("CREATE TABLE source(other TEXT)").unwrap();
                }
                defaults(&mut sql);
            }
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
            defaults(&mut SqlSession::new(&mut db));
            db.close().unwrap();
        }
    }
}

fn table_output(sql: &mut SqlSession<'_>, type_name: &str) {
    let metadata = sql
        .execute("SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname='header_rows'")
        .unwrap();
    let declaration = metadata.rows[0][0].to_cell();
    assert!(
        declaration.contains(type_name),
        "output declaration {declaration:?} must bind to {type_name}"
    );
    assert!(sql
        .execute("SELECT * FROM header_rows()")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn table_returning_routine_columns_follow_a_renamed_table_type() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for commit in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let cfg = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                sql.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); CREATE FUNCTION header_rows() RETURNS TABLE(item source) LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$").unwrap();
                table_output(&mut sql, "source");
                sql.execute("BEGIN; ALTER TABLE source RENAME TO renamed")
                    .unwrap();
                table_output(&mut sql, "renamed");
                sql.execute(if commit { "COMMIT" } else { "ROLLBACK" })
                    .unwrap();
                if commit {
                    sql.execute("CREATE TABLE source(other TEXT)").unwrap();
                }
                table_output(&mut sql, if commit { "renamed" } else { "source" });
            }
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
            table_output(
                &mut SqlSession::new(&mut db),
                if commit { "renamed" } else { "source" },
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn routine_defaults_keep_the_callers_privileges_before_definer_entry() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE ROLE header_guest; CREATE FUNCTION header_secret() RETURNS TEXT LANGUAGE sql AS 'SELECT ''secret'''; REVOKE ALL ON FUNCTION header_secret() FROM PUBLIC; CREATE FUNCTION header_definer(arg TEXT DEFAULT header_secret()) RETURNS TEXT LANGUAGE sql SECURITY DEFINER AS 'SELECT arg'; CREATE PROCEDURE header_definer_proc(arg TEXT DEFAULT header_secret()) LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'; CREATE FUNCTION header_definer_rows(arg TEXT DEFAULT header_secret()) RETURNS TABLE(item TEXT) LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN; END $$; SET ROLE header_guest").unwrap();
        for statement in [
            "SELECT header_definer()",
            "CALL header_definer_proc()",
            "SELECT * FROM header_definer_rows()",
        ] {
            let error = sql.execute(statement).unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "{statement}: {error}");
        }
        assert_eq!(
            sql.execute("SELECT header_definer('explicit')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("explicit".into())]]
        );
        sql.execute("CALL header_definer_proc('explicit')").unwrap();
        assert!(sql
            .execute("SELECT * FROM header_definer_rows('explicit')")
            .unwrap()
            .rows
            .is_empty());
        sql.execute("RESET ROLE; GRANT EXECUTE ON FUNCTION header_secret() TO header_guest; SET ROLE header_guest").unwrap();
        assert_eq!(
            sql.execute("SELECT header_definer()").unwrap().rows,
            vec![vec![SqlValue::String("secret".into())]]
        );
        sql.execute("CALL header_definer_proc()").unwrap();
        assert!(sql
            .execute("SELECT * FROM header_definer_rows()")
            .unwrap()
            .rows
            .is_empty());
    }
}
