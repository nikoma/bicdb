//! PostgreSQL16 references retained in public-schema-pg16-reference.json.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};
fn config() -> DbConfig {
    DbConfig::default().with_fsync(false)
}
fn assert_absent(session: &mut SqlSession<'_>) {
    let values = session
        .execute("SELECT current_schema(),to_regnamespace('public')")
        .unwrap()
        .rows;
    assert_eq!(values, vec![vec![SqlValue::Null, SqlValue::Null]]);
    assert!(session
        .execute("SELECT nspname FROM pg_namespace WHERE oid=2200")
        .unwrap()
        .rows
        .is_empty());
    assert!(session
        .execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name='public'")
        .unwrap()
        .rows
        .is_empty());
    for sql in [
        "CREATE TABLE broken(id int)",
        "CREATE TABLE public.broken(id int)",
        "CREATE VIEW broken_view AS SELECT 1 AS v",
        "CREATE SEQUENCE broken_seq",
        "CREATE TYPE broken_type AS ENUM ('x')",
        "CREATE FUNCTION broken_fn() RETURNS int LANGUAGE SQL AS $$SELECT 1$$",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "3F000", "{sql}: {error}");
    }
}
#[test]
fn dropping_public_is_persistent_and_creation_follows_the_remaining_search_path() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE TABLE t(id int)").unwrap();
        s.execute("INSERT INTO t VALUES(1)").unwrap();
        assert_eq!(
            s.execute("DROP SCHEMA public RESTRICT")
                .unwrap_err()
                .sqlstate(),
            "2BP01"
        );
        s.execute("DROP SCHEMA public CASCADE").unwrap();
        assert_absent(&mut s);
    }
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    let mut s = SqlSession::new(&mut db);
    assert_absent(&mut s);
    s.execute("CREATE SCHEMA alternate").unwrap();
    s.execute("SET search_path=public,alternate").unwrap();
    s.execute("CREATE TABLE restored(id int)").unwrap();
    assert_eq!(
        s.execute("SELECT table_schema FROM information_schema.tables WHERE table_name='restored'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("alternate".into())]]
    );
    s.execute("CREATE SCHEMA public").unwrap();
    s.execute("SET search_path=public").unwrap();
    s.execute("CREATE TABLE fresh(id int)").unwrap();
    assert_eq!(
        s.execute("SELECT current_schema()").unwrap().rows,
        vec![vec![SqlValue::String("public".into())]]
    );
}
#[test]
fn namespace_tombstones_follow_transaction_and_savepoint_rollback() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE t(id int)").unwrap();
    s.execute("INSERT INTO t VALUES(1)").unwrap();
    s.execute("BEGIN").unwrap();
    s.execute("SAVEPOINT before_drop").unwrap();
    s.execute("DROP SCHEMA public CASCADE").unwrap();
    assert_eq!(
        s.execute("SELECT current_schema()").unwrap().rows,
        vec![vec![SqlValue::Null]]
    );
    s.execute("ROLLBACK TO before_drop").unwrap();
    assert_eq!(
        s.execute("SELECT id FROM public.t").unwrap().rows[0][0].to_cell(),
        "1"
    );
    s.execute("COMMIT").unwrap();
    s.execute("DROP SCHEMA public CASCADE").unwrap();
    s.execute("BEGIN").unwrap();
    s.execute("CREATE SCHEMA public").unwrap();
    s.execute("ROLLBACK").unwrap();
    assert_absent(&mut s);
}
