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
