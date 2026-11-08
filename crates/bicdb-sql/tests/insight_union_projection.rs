use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn unaliased_literals_keep_postgresql_names_through_ctes_and_union_wildcards() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let query = "WITH meds AS (SELECT 'medication'::text AS kind,'PatientMedication.medication_name',NULL,7 AS value FROM (SELECT 1 AS n) x) SELECT * FROM (SELECT * FROM meds UNION ALL SELECT 'other','Other.field',NULL,8) all_changes ORDER BY value";
    for _ in 0..3 {
        let result = sql.execute(query).unwrap();
        assert_eq!(
            result.columns,
            vec!["kind", "?column?", "?column?", "value"]
        );
        assert_eq!(
            result.rows,
            vec![
                vec![
                    SqlValue::String("medication".into()),
                    SqlValue::String("PatientMedication.medication_name".into()),
                    SqlValue::Null,
                    SqlValue::Int(7)
                ],
                vec![
                    SqlValue::String("other".into()),
                    SqlValue::String("Other.field".into()),
                    SqlValue::Null,
                    SqlValue::Int(8)
                ],
            ]
        );
    }
    let empty=sql.execute("WITH meds AS (SELECT 'PatientMedication.medication_name' FROM (SELECT 1 AS n) x WHERE false) SELECT * FROM (SELECT * FROM meds UNION ALL SELECT 'Other.field' WHERE false) all_changes").unwrap();
    assert_eq!(empty.columns, vec!["?column?"]);
    assert!(empty.rows.is_empty());
}

#[test]
fn insight_table_function_preserves_literal_values_and_declared_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE chart_events(id int PRIMARY KEY);INSERT INTO chart_events VALUES(1);CREATE FUNCTION chart_changes() RETURNS TABLE(kind text,label_field text) LANGUAGE sql STABLE AS $$ WITH meds AS (SELECT 'medication'::text AS kind,'PatientMedication.medication_name' FROM chart_events) SELECT * FROM (SELECT * FROM meds UNION ALL SELECT 'other','Other.field' WHERE false) all_changes $$").unwrap();
    let result = sql
        .execute("SELECT c.kind,c.label_field FROM chart_changes() c")
        .unwrap();
    assert_eq!(result.columns, vec!["kind", "label_field"]);
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("medication".into()),
            SqlValue::String("PatientMedication.medication_name".into())
        ]]
    );
}

#[test]
fn array_text_cast_preserves_null_in_record_join_and_aggregate_paths() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE array_refs(id int PRIMARY KEY,tags text[]);INSERT INTO array_refs VALUES(1,ARRAY['one','a,b']),(2,NULL)").unwrap();
    let result = sql
        .execute("SELECT tags::text FROM array_refs ORDER BY id")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("{one,\"a,b\"}".into())],
            vec![SqlValue::Null]
        ]
    );
    let joined=sql.execute("SELECT a.tags::text FROM array_refs a LEFT JOIN array_refs b ON b.id=a.id WHERE a.id=2").unwrap();
    assert_eq!(joined.rows, vec![vec![SqlValue::Null]]);
    let aggregate = sql
        .execute("SELECT array_agg(id) FILTER (WHERE false)::text FROM array_refs")
        .unwrap();
    assert_eq!(aggregate.rows, vec![vec![SqlValue::Null]]);
}

#[test]
fn array_text_cast_keeps_nondefault_subscripts_for_rollback_copy() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("SELECT '[0:1]={one,two}'::text[]::text AS value")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("[0:1]={one,two}".into())]]
    );
    let result = sql
        .execute("SELECT '[-2:-1][3:4]={{one,two},{three,NULL}}'::text[]::text AS value")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String(
            "[-2:-1][3:4]={{one,two},{three,NULL}}".into()
        )]]
    );
}

/// ORDER BY and LIMIT/OFFSET on set operations, checked against PostgreSQL
/// 16: positions and result column names sort; expressions are rejected.
#[test]
fn set_operation_order_by_positions_names_and_limits_follow_postgres() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE st (a int, b text)").unwrap();
    session
        .execute("INSERT INTO st VALUES (3, 'c'), (1, 'a'), (2, 'b')")
        .unwrap();
    let int = SqlValue::Int;
    let text = |value: &str| SqlValue::String(value.to_string());
    let rows = |session: &mut SqlSession, sql: &str| {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .rows
    };
    for (sql, expected) in [
        (
            "SELECT 7 UNION SELECT 1 UNION SELECT 4 ORDER BY 1",
            vec![vec![int(1)], vec![int(4)], vec![int(7)]],
        ),
        (
            "SELECT 7 AS x UNION SELECT 1 ORDER BY x DESC",
            vec![vec![int(7)], vec![int(1)]],
        ),
        (
            "SELECT 7 AS x, 'q' AS y UNION ALL SELECT 1, 'z' ORDER BY 2 DESC, 1 LIMIT 1",
            vec![vec![int(1), text("z")]],
        ),
        (
            "SELECT 7 UNION SELECT 1 UNION SELECT 4 ORDER BY 1 LIMIT 2 OFFSET 1",
            vec![vec![int(4)], vec![int(7)]],
        ),
        (
            "SELECT a FROM st UNION SELECT 10 ORDER BY a DESC LIMIT 2",
            vec![vec![int(10)], vec![int(3)]],
        ),
        (
            "SELECT a, b FROM st UNION ALL SELECT 0, 'z' ORDER BY b",
            vec![
                vec![int(1), text("a")],
                vec![int(2), text("b")],
                vec![int(3), text("c")],
                vec![int(0), text("z")],
            ],
        ),
        (
            "SELECT a FROM st EXCEPT SELECT 2 ORDER BY 1 DESC",
            vec![vec![int(3)], vec![int(1)]],
        ),
        ("SELECT a FROM st INTERSECT SELECT 2 ORDER BY 1", vec![vec![int(2)]]),
        (
            "(SELECT a FROM st ORDER BY a DESC LIMIT 1) UNION ALL (SELECT 0) ORDER BY 1",
            vec![vec![int(0)], vec![int(3)]],
        ),
        ("SELECT 2 UNION SELECT 1 ORDER BY 1 OFFSET 1", vec![vec![int(2)]]),
    ] {
        assert_eq!(rows(&mut session, sql), expected, "{sql}");
    }
    for (sql, sqlstate) in [
        ("SELECT a FROM st UNION SELECT 10 ORDER BY a + 1", "0A000"),
        ("SELECT a FROM st UNION SELECT 10 ORDER BY 3", "42P10"),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
}
