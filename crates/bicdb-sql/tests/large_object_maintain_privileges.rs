//! Large-object GRANT/REVOKE and has_largeobject_privilege, the MAINTAIN
//! table privilege and GRANTED BY, as PostgreSQL 17+ applies them. Every
//! newly allowed path is paired with the access it must still refuse.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn open() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();
    (root, db)
}

fn one(sql: &mut SqlSession<'_>, query: &str) -> SqlValue {
    sql.execute(query).unwrap().rows[0][0].clone()
}

#[test]
fn large_object_grants_allow_exactly_the_granted_access() {
    let (_root, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE ROLE lo_reader",
        "CREATE ROLE lo_other",
        "SELECT lo_create(44001)",
        "SELECT lo_put(44001, 0, '\\x0102')",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        one(
            &mut sql,
            "SELECT has_largeobject_privilege('lo_reader', 44001, 'SELECT')"
        ),
        SqlValue::Bool(false)
    );
    sql.execute("GRANT SELECT ON LARGE OBJECT 44001 TO lo_reader")
        .unwrap();
    assert_eq!(
        one(
            &mut sql,
            "SELECT has_largeobject_privilege('lo_reader', 44001, 'SELECT')"
        ),
        SqlValue::Bool(true)
    );
    assert_eq!(
        one(
            &mut sql,
            "SELECT has_largeobject_privilege('lo_reader', 44001, 'UPDATE')"
        ),
        SqlValue::Bool(false)
    );
    assert_eq!(
        one(
            &mut sql,
            "SELECT has_largeobject_privilege('lo_reader', 999999, 'SELECT')"
        ),
        SqlValue::Null
    );
    assert_eq!(
        one(
            &mut sql,
            "SELECT lomacl::text FROM pg_largeobject_metadata WHERE oid = 44001"
        ),
        SqlValue::String("{bicdb=rw/bicdb,lo_reader=r/bicdb}".into())
    );
    sql.execute("SET ROLE lo_reader").unwrap();
    assert_eq!(
        one(&mut sql, "SELECT length(lo_get(44001))"),
        SqlValue::Int(2)
    );
    // Canaries: SELECT does not allow writing, and a non-owner cannot grant.
    assert_eq!(
        sql.execute("SELECT lo_put(44001, 0, '\\x01')")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    assert_eq!(
        sql.execute("GRANT SELECT ON LARGE OBJECT 44001 TO lo_other")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    sql.execute("RESET ROLE").unwrap();
    sql.execute("REVOKE SELECT ON LARGE OBJECT 44001 FROM lo_reader")
        .unwrap();
    sql.execute("SET ROLE lo_reader").unwrap();
    assert_eq!(
        sql.execute("SELECT lo_get(44001)").unwrap_err().sqlstate(),
        "42501"
    );
    sql.execute("RESET ROLE").unwrap();
    assert_eq!(
        sql.execute("GRANT INSERT ON LARGE OBJECT 44001 TO lo_reader")
            .unwrap_err()
            .sqlstate(),
        "0LP01"
    );
    assert_eq!(
        sql.execute("GRANT SELECT ON LARGE OBJECT 123456 TO lo_reader")
            .unwrap_err()
            .sqlstate(),
        "42704"
    );
}

#[test]
fn maintain_privilege_allows_analyze_but_not_reads() {
    let (_root, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE ROLE mt",
        "CREATE TABLE mt_t (a int, b int)",
        "INSERT INTO mt_t SELECT g, g FROM generate_series(1, 20) g",
        "GRANT MAINTAIN ON mt_t TO mt",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        one(
            &mut sql,
            "SELECT has_table_privilege('mt', 'mt_t', 'MAINTAIN')"
        ),
        SqlValue::Bool(true)
    );
    sql.execute("SET ROLE mt").unwrap();
    sql.execute("ANALYZE mt_t").unwrap();
    // Canaries: MAINTAIN reads nothing, and its statistics stay hidden.
    assert_eq!(
        sql.execute("SELECT * FROM mt_t").unwrap_err().sqlstate(),
        "42501"
    );
    assert_eq!(
        one(
            &mut sql,
            "SELECT count(*) FROM pg_stats WHERE tablename = 'mt_t'"
        ),
        SqlValue::Int(0)
    );
    sql.execute("RESET ROLE").unwrap();
    sql.execute("REVOKE MAINTAIN ON mt_t FROM mt").unwrap();
    sql.execute("SET ROLE mt").unwrap();
    assert_eq!(sql.execute("ANALYZE mt_t").unwrap_err().sqlstate(), "42501");
    sql.execute("RESET ROLE").unwrap();
    // A column grant shows that column's statistics only.
    sql.execute("GRANT SELECT (a) ON mt_t TO mt").unwrap();
    sql.execute("ANALYZE mt_t").unwrap();
    sql.execute("SET ROLE mt").unwrap();
    assert_eq!(
        sql.execute("SELECT attname FROM pg_stats WHERE tablename = 'mt_t' ORDER BY 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("a".into())]]
    );
}

#[test]
fn granted_by_accepts_only_the_current_role() {
    let (_root, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE ROLE gb1",
        "CREATE ROLE gb2",
        "CREATE TABLE gb_t (a int)",
    ] {
        sql.execute(statement).unwrap();
    }
    sql.execute("GRANT SELECT ON gb_t TO gb1 GRANTED BY CURRENT_USER")
        .unwrap();
    assert_eq!(
        one(
            &mut sql,
            "SELECT has_table_privilege('gb1', 'gb_t', 'SELECT')"
        ),
        SqlValue::Bool(true)
    );
    let error = sql
        .execute("GRANT SELECT ON gb_t TO gb2 GRANTED BY gb1")
        .unwrap_err();
    assert_eq!(
        (error.sqlstate(), error.postgres_message().as_str()),
        ("0A000", "grantor must be current user")
    );
    assert_eq!(
        one(
            &mut sql,
            "SELECT has_table_privilege('gb2', 'gb_t', 'SELECT')"
        ),
        SqlValue::Bool(false)
    );
}
