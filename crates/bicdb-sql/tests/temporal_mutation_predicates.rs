use bicdb_core::BicDb;
use bicdb_sql::{oracle, SqlSession, SqlValue};

fn mutation_predicates(profile: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let lower = |db: &BicDb, sql: &str| {
        if oracle::is_oracle_profile(profile) {
            oracle::lower_session_sql(db, sql, profile, None, "bicdb").unwrap()
        } else {
            sql.to_owned()
        }
    };
    for sql in [
        "CREATE TABLE temporal_rows(id INTEGER PRIMARY KEY,stamp TIMESTAMP,state VARCHAR(12))",
        "INSERT INTO temporal_rows VALUES(1,TIMESTAMP '2026-11-04 12:34:56.123456','original'),(2,NULL,'null')",
    ] {
        let sql = lower(&db, sql);
        SqlSession::new(&mut db).execute(&sql).unwrap();
    }
    // A text-format PostgreSQL timestamp bind can include an offset even
    // when its resolved type is timestamp without time zone. Exercise the
    // lowered expression shared by SELECT, UPDATE and DELETE evaluation.
    let queries = [
        "SELECT id FROM temporal_rows WHERE id=1 AND stamp='2026-11-04 12:34:56.123456+00'",
        "UPDATE temporal_rows SET state='fresh',stamp=TIMESTAMP '2026-11-04 12:34:57.123456' WHERE id=1 AND stamp='2026-11-04 12:34:56.123456+00'",
        "UPDATE temporal_rows SET state='stale' WHERE id=1 AND stamp='2026-11-04 12:34:56.123456+00'",
        "DELETE FROM temporal_rows WHERE id=1 AND stamp='2026-11-04 12:34:56.123456+00'",
        "SELECT state FROM temporal_rows WHERE id=1",
        "DELETE FROM temporal_rows WHERE id=1 AND stamp='2026-11-04 12:34:57.123456+00'",
        "UPDATE temporal_rows SET state='wrong' WHERE id=2 AND stamp='2026-11-04 12:34:56.123456+00'",
    ].map(|sql| lower(&db, sql));
    let original = lower(&db, "SELECT state FROM temporal_rows WHERE id=1");
    let mut session = SqlSession::new(&mut db);
    session.execute("BEGIN").unwrap();
    assert_eq!(
        session.execute(&queries[0]).unwrap().rows[0][0].to_cell(),
        "1"
    );
    for (index, tag) in [(1, "UPDATE 1"), (2, "UPDATE 0"), (3, "DELETE 0")] {
        assert_eq!(
            session
                .execute(&queries[index])
                .unwrap()
                .command_tag
                .as_deref(),
            Some(tag),
            "{profile}: {}",
            queries[index]
        );
    }
    assert_eq!(
        session.execute(&queries[4]).unwrap().rows,
        vec![vec![SqlValue::String("fresh".into())]]
    );
    for (index, tag) in [(5, "DELETE 1"), (6, "UPDATE 0")] {
        assert_eq!(
            session
                .execute(&queries[index])
                .unwrap()
                .command_tag
                .as_deref(),
            Some(tag),
            "{profile}: {}",
            queries[index]
        );
    }
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute(&original).unwrap().rows,
        vec![vec![SqlValue::String("original".into())]]
    );
}

#[test]
fn postgres_temporal_mutation_predicates_keep_column_semantics() {
    mutation_predicates(oracle::POSTGRES_PROFILE);
}

#[test]
fn oracle_transport_temporal_mutation_predicates_keep_column_semantics() {
    mutation_predicates(oracle::ORACLE_PREVIEW_PROFILE);
}

fn temporal_projection_types(profile: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE clock_rows(id INTEGER PRIMARY KEY,stamp TIMESTAMP(6))")
        .unwrap();
    session
        .execute("INSERT INTO clock_rows VALUES(1,TIMESTAMP '2000-01-01 01:00:00.123456')")
        .unwrap();
    session.execute("SET TIME ZONE 'Europe/Berlin'").unwrap();
    let instant = "CAST('2000-01-01 00:00:00.123456 +00:00' AS TIMESTAMP WITH TIME ZONE)";
    for operand in ["stamp", "c.stamp", "TIMESTAMP '2000-01-01 01:00:00.123456'"] {
        for comparison in [
            format!("{operand}={instant}"),
            format!("{instant}={operand}"),
        ] {
            assert_eq!(
                session
                    .execute(&format!("SELECT {comparison} FROM clock_rows c"))
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Bool(true)]],
                "{profile}: projected {comparison}"
            );
            assert_eq!(
                session
                    .execute(&format!("SELECT id FROM clock_rows c WHERE {comparison}"))
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1)]],
                "{profile}: filtered {comparison}"
            );
        }
    }
}

#[test]
fn postgres_temporal_projection_preserves_resolved_column_types() {
    temporal_projection_types(oracle::POSTGRES_PROFILE);
}

#[test]
fn oracle_transport_temporal_projection_preserves_resolved_column_types() {
    temporal_projection_types(oracle::ORACLE_PREVIEW_PROFILE);
}
