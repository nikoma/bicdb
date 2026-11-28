use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn record_quantifiers_use_composite_null_and_type_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for (query, expected) in [
        (
            "SELECT ROW(1, NULL::int) = ANY(ARRAY[ROW(1, NULL::int)])",
            SqlValue::Bool(true),
        ),
        (
            "SELECT ROW(1, NULL::int) = ROW(1, NULL::int)",
            SqlValue::Null,
        ),
        (
            "SELECT ROW(1, 2) < ANY(ARRAY[ROW(1, NULL::int)])",
            SqlValue::Bool(true),
        ),
        (
            "SELECT ROW(1, 2) = ALL(ARRAY[ROW(1,2), ROW(1,2)])",
            SqlValue::Bool(true),
        ),
        ("SELECT ROW(1) = ANY(ARRAY[NULL::record])", SqlValue::Null),
        (
            "SELECT ROW(1, 'a'::text) = ANY(ARRAY[ROW(2, 'a'::text)])",
            SqlValue::Bool(false),
        ),
        (
            "SELECT ROW(1, ROW(NULL::int)) = ANY(ARRAY[ROW(1, ROW(NULL::int))])",
            SqlValue::Bool(true),
        ),
    ] {
        assert_eq!(
            session.execute(query).unwrap().rows,
            vec![vec![expected]],
            "{query}"
        );
    }

    for (query, state) in [
        ("SELECT ROW(1) = ANY(ARRAY[ROW(1::bigint)])", "42804"),
        ("SELECT ROW(1) = ANY(ARRAY[ROW(1, 2)])", "42804"),
        ("SELECT ROW(1, 'x') = ANY(ARRAY[ROW(1, 'x')])", "42883"),
    ] {
        assert_eq!(
            session.execute(query).unwrap_err().sqlstate(),
            state,
            "{query}"
        );
    }
}

#[test]
fn anonymous_record_arrays_keep_their_type_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SELECT (ARRAY[ROW(1)] || ARRAY[ROW(2)])::text")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("{(1),(2)}".into())]]
    );
    let result = session.execute(
        "SELECT q.a::text FROM (VALUES (ARRAY[ROW(10)]), (ARRAY[ROW(2)]), (ARRAY[ROW(NULL::int)])) q(a) ORDER BY q.a",
    ).unwrap();
    assert_eq!(
        result.rows,
        ["{(2)}", "{(10)}", "{()}"].map(|text| vec![SqlValue::String(text.into())])
    );
    let result = session.execute(
        "WITH RECURSIVE walk(n, path) AS (SELECT 1, ARRAY[ROW(1)] UNION ALL SELECT n + 1, path || ARRAY[ROW(n + 1)] FROM walk WHERE n < 3) SELECT path::text FROM walk ORDER BY path",
    ).unwrap();
    assert_eq!(
        result.rows,
        ["{(1)}", "{(1),(2)}", "{(1),(2),(3)}"].map(|text| vec![SqlValue::String(text.into())])
    );
}
