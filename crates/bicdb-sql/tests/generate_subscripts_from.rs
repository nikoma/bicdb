use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn subscripts_table_function_matches_array_bounds_and_ordinality() {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for (query, wanted) in [
        ("SELECT s FROM generate_subscripts(ARRAY[10,20,30], 1) AS s", vec![vec![1],vec![2],vec![3]]),
        ("SELECT s,n FROM pg_catalog.generate_subscripts('[0:2]={4,5,6}'::int[], 1, true) WITH ORDINALITY AS t(s,n)", vec![vec![2,1],vec![1,2],vec![0,3]]),
        ("SELECT * FROM generate_subscripts(ARRAY[[1,2],[3,4]], 2)", vec![vec![1],vec![2]]),
        ("SELECT * FROM generate_subscripts(ARRAY[]::int[], 1)", vec![]),
        ("SELECT * FROM generate_subscripts(ARRAY[1,2], 2)", vec![]),
        ("SELECT * FROM generate_subscripts(NULL::int[], 1)", vec![]),
        ("SELECT * FROM generate_subscripts(ARRAY[1], NULL)", vec![]),
        ("SELECT * FROM generate_subscripts(ARRAY[1], 1, NULL)", vec![]),
    ] {
        assert_eq!(sql.execute(query).unwrap().rows,
            wanted.into_iter().map(|r| r.into_iter().map(SqlValue::Int).collect::<Vec<_>>()).collect::<Vec<_>>(), "{query}");
    }
    assert!(sql
        .execute("SELECT * FROM generate_subscripts(ARRAY[1])")
        .is_err());
}

#[test]
fn subscripts_correlate_in_scalar_subqueries_and_lateral_joins() {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE samples(id INT PRIMARY KEY, vals INT[]); INSERT INTO samples VALUES (1, ARRAY[8,9]), (2, ARRAY[7]), (3, ARRAY[]::int[])").unwrap();
    let query = "SELECT id, (SELECT SUM(vals[s]) FROM (SELECT s FROM generate_subscripts(samples.vals,1) AS s) positions) FROM samples ORDER BY id";
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(17)],
            vec![SqlValue::Int(2), SqlValue::Int(7)],
            vec![SqlValue::Int(3), SqlValue::Null]
        ]
    );
    let query = "SELECT id,s,vals[s] FROM samples CROSS JOIN LATERAL generate_subscripts(vals,1) AS p(s) ORDER BY id,s";
    assert_eq!(
        sql.execute(query).unwrap().rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(1), SqlValue::Int(8)],
            vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(9)],
            vec![SqlValue::Int(2), SqlValue::Int(1), SqlValue::Int(7)]
        ]
    );
    let query = "SELECT id, generate_subscripts FROM samples, generate_subscripts(vals,1) ORDER BY id,generate_subscripts";
    assert_eq!(sql.execute(query).unwrap().rows.len(), 3);
    let query = "SELECT id,s FROM samples LEFT JOIN LATERAL generate_subscripts(vals,1) AS p(s) ON true ORDER BY id,s";
    assert_eq!(
        sql.execute(query).unwrap().rows.last().unwrap(),
        &vec![SqlValue::Int(3), SqlValue::Null]
    );
}

#[test]
fn constraint_arrays_expose_one_based_composite_foreign_key_columns() {
    use serde_json::json;
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE parent_pairs(a INT,b INT,PRIMARY KEY(a,b)); CREATE TABLE child_pairs(id INT PRIMARY KEY,x INT,y INT,CONSTRAINT pair_fk FOREIGN KEY(x,y) REFERENCES parent_pairs(a,b))").unwrap();
    let result = sql.execute("SELECT conkey,confkey,array_lower(conkey,1) FROM pg_constraint WHERE conname='pair_fk'").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Json(json!([2, 3])),
            SqlValue::Json(json!([1, 2])),
            SqlValue::Int(1)
        ]]
    );
    let result = sql.execute("SELECT (SELECT array_agg(a.attname ORDER BY idx) FROM (SELECT idx,c.conkey[idx] AS element FROM generate_subscripts(c.conkey,1) AS idx) keys JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=keys.element) FROM pg_constraint c WHERE c.conname='pair_fk'").unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Json(json!(["x", "y"]))]]);
    let result = sql.execute("SELECT column_name FROM information_schema.key_column_usage WHERE constraint_name='pair_fk' ORDER BY ordinal_position").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("x".into())],
            vec![SqlValue::String("y".into())]
        ]
    );
}

#[test]
fn subscript_relation_description_has_one_integer_column_plus_optional_ordinality() {
    use bicdb_sql::infer_query_result_columns;
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();
    for (query, expected) in [
        (
            "SELECT * FROM generate_subscripts(ARRAY[1,2],1)",
            vec![("generate_subscripts".to_string(), Some("int4".to_string()))],
        ),
        (
            "SELECT * FROM generate_subscripts(ARRAY[1,2],1) WITH ORDINALITY AS p(s,n)",
            vec![
                ("s".to_string(), Some("int4".to_string())),
                ("n".to_string(), Some("int8".to_string())),
            ],
        ),
    ] {
        assert_eq!(
            infer_query_result_columns(&db, query).unwrap().unwrap(),
            expected
        );
    }
}
