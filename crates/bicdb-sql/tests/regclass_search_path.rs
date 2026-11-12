//! Regclass literals and metadata predicates resolve through the live session path.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn truth(sql: &mut SqlSession<'_>, query: &str) {
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![vec![SqlValue::Bool(true)]],
        "{query}"
    );
}

#[test]
fn regclass_uses_path_order_for_tables_sequences_views_indexes_and_quoted_names() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE SCHEMA app_a; CREATE SCHEMA app_b; CREATE TABLE app_a.target(id INT PRIMARY KEY); CREATE TABLE app_b.target(id INT PRIMARY KEY); CREATE SEQUENCE app_a.counter; CREATE VIEW app_a.target_view AS SELECT id FROM app_a.target; CREATE INDEX target_idx ON app_a.target(id); CREATE TABLE app_a.\"A.B\"(id INT); CREATE TABLE public.outside_path(id INT)").unwrap();
        sql.execute("SET search_path=app_a,app_b").unwrap();
        for query in [
            "SELECT 'target'::regclass::oid='app_a.target'::regclass::oid",
            "SELECT 'counter'::regclass::oid='app_a.counter'::regclass::oid",
            "SELECT 'target_view'::regclass::oid='app_a.target_view'::regclass::oid",
            "SELECT 'target_idx'::regclass::oid='app_a.target_idx'::regclass::oid",
            "SELECT 'target_pkey'::regclass::oid='app_a.target_pkey'::regclass::oid",
            "SELECT '\"A.B\"'::regclass::oid='app_a.\"A.B\"'::regclass::oid",
            "SELECT to_regclass('outside_path') IS NULL",
            "SELECT to_regclass('public.outside_path') IS NOT NULL",
            "SELECT to_regclass('missing_relation') IS NULL",
            "SELECT 'pg_class'::regclass::oid='pg_catalog.pg_class'::regclass::oid",
        ] {
            truth(&mut sql, query);
        }
        sql.execute("SET search_path=app_b,app_a").unwrap();
        truth(
            &mut sql,
            "SELECT 'target'::regclass::oid='app_b.target'::regclass::oid",
        );
        // A qualified reference ignores the path and cannot leak a public
        // namesake when its explicit namespace has no such relation.
        assert_eq!(
            sql.execute("SELECT 'public.target'::regclass")
                .unwrap_err()
                .sqlstate(),
            "42P01"
        );
        for path in ["missing_schema", "''"] {
            sql.execute(&format!("SET search_path={path}")).unwrap();
            truth(&mut sql, "SELECT to_regclass('outside_path') IS NULL");
            truth(&mut sql, "SELECT current_schema() IS NULL");
            truth(
                &mut sql,
                "SELECT to_regclass('public.outside_path') IS NOT NULL",
            );
        }
    }
}

#[test]
fn regclass_metadata_predicates_see_new_parent_identity_and_local_path_changes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE SCHEMA app_a; CREATE SCHEMA app_b; CREATE TABLE app_a.target(id INT PRIMARY KEY); CREATE TABLE app_b.target(id INT PRIMARY KEY)").unwrap();
        sql.execute("SET search_path=app_a,app_b").unwrap();
        let count = |sql: &mut SqlSession<'_>| {
            // Isolate identity resolution across catalog versions: BicDB's
            // PostgreSQL18 catalog has NOT NULL rows absent from the PG16
            // application reference. Primary-key cardinality agrees in both.
            sql.execute(
                "SELECT COUNT(*) FROM pg_constraint c WHERE conrelid=LOWER('TARGET')::regclass AND contype='p'",
            )
            .unwrap()
            .rows[0][0]
                .clone()
        };
        assert_eq!(count(&mut sql), SqlValue::Int(1));
        sql.execute("BEGIN").unwrap();
        sql.execute("ALTER TABLE target RENAME TO target_default_partition")
            .unwrap();
        sql.execute("CREATE TABLE target(id INT,tenant_id INT) PARTITION BY LIST(tenant_id)")
            .unwrap();
        assert_eq!(count(&mut sql), SqlValue::Int(0));
        sql.execute("SET LOCAL search_path=app_b,app_a").unwrap();
        assert_eq!(count(&mut sql), SqlValue::Int(1));
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(count(&mut sql), SqlValue::Int(1));
        truth(
            &mut sql,
            "SELECT 'target'::regclass::oid='app_a.target'::regclass::oid",
        );
    }
}

#[test]
fn regclass_exact_lookup_keeps_unrelated_reservations_available_and_affected_reads_fenced() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE source(id INT); CREATE TABLE unrelated(id INT)")
            .unwrap();
        // Warm the old directory path too: cache hits must not authorize an
        // affected lookup after a foreign owner has reserved its backing.
        SqlSession::new(&mut db)
            .execute("SELECT 'source'::regclass::oid")
            .unwrap();
        let owner = db.begin_transaction().unwrap();
        let token = owner
            .reserve_catalog_resources(
                &["source".into(), "renamed".into()],
                &[
                    ("__bicdb_pg_schema".into(), "source".into()),
                    ("__bicdb_pg_schema".into(), "renamed".into()),
                ],
            )
            .unwrap();
        let mut sql = SqlSession::new(&mut db);
        truth(&mut sql, "SELECT to_regclass('unrelated') IS NOT NULL");
        for query in [
            "SELECT 'source'::regclass::oid",
            "SELECT to_regclass('source')",
            "SELECT 'renamed'::regclass::oid",
            "SELECT to_regclass('renamed')",
        ] {
            assert_eq!(
                sql.execute(query).unwrap_err().sqlstate(),
                "40001",
                "{query}"
            );
        }
        drop(sql);
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
        truth(
            &mut SqlSession::new(&mut db),
            "SELECT to_regclass('source') IS NOT NULL",
        );
    }
}
