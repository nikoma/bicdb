use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn int(value: i64) -> SqlValue {
    SqlValue::Int(value)
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session.execute(sql).unwrap().rows
}

#[test]
fn materialized_view_keeps_its_snapshot_until_refresh() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE src(a int); INSERT INTO src VALUES (1), (2); \
                 CREATE MATERIALIZED VIEW totals AS SELECT count(*) AS n, sum(a) AS s FROM src",
            )
            .unwrap();
        session.execute("INSERT INTO src VALUES (3)").unwrap();
        assert_eq!(
            rows(&mut session, "SELECT n, s FROM totals"),
            vec![vec![int(2), int(3)]]
        );
        let refreshed = session.execute("REFRESH MATERIALIZED VIEW totals").unwrap();
        assert_eq!(
            refreshed.command_tag.as_deref(),
            Some("REFRESH MATERIALIZED VIEW")
        );
        assert_eq!(
            rows(&mut session, "SELECT n, s FROM totals"),
            vec![vec![int(3), int(6)]]
        );
        session.execute("INSERT INTO src VALUES (4)").unwrap();
    }
    // The snapshot is durable.
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(&mut session, "SELECT n FROM totals"),
        vec![vec![int(3)]]
    );
}

#[test]
fn unpopulated_materialized_view_cannot_be_read_or_changed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE src(a int); INSERT INTO src VALUES (1); \
             CREATE MATERIALIZED VIEW later AS SELECT a FROM src WITH NO DATA",
        )
        .unwrap();
    let error = session.execute("SELECT * FROM later").unwrap_err();
    assert_eq!(error.sqlstate(), "55000");
    session
        .execute("REFRESH MATERIALIZED VIEW later WITH DATA")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM later"),
        vec![vec![int(1)]]
    );
    session
        .execute("REFRESH MATERIALIZED VIEW later WITH NO DATA")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT * FROM later")
            .unwrap_err()
            .sqlstate(),
        "55000"
    );
    for sql in [
        "INSERT INTO later VALUES (2)",
        "UPDATE later SET a = 2",
        "DELETE FROM later",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "42809",
            "{sql}"
        );
    }
    let error = session
        .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY later")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    let error = session
        .execute("REFRESH MATERIALIZED VIEW src")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42809");
}

#[test]
fn refresh_requires_ownership_and_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE reader; CREATE TABLE src(a int); INSERT INTO src VALUES (1); \
             CREATE MATERIALIZED VIEW mv AS SELECT a FROM src; GRANT SELECT ON mv TO reader",
        )
        .unwrap();
    session.execute("INSERT INTO src VALUES (2)").unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("REFRESH MATERIALIZED VIEW mv").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM mv"),
        vec![vec![int(2)]]
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM mv"),
        vec![vec![int(1)]]
    );
    session.execute("SET ROLE reader").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM mv"),
        vec![vec![int(1)]]
    );
    let error = session.execute("REFRESH MATERIALIZED VIEW mv").unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
}
