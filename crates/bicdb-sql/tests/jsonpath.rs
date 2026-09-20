use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn jsonpath_scalar_functions_support_modes_filters_variables_and_methods() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            r#"SELECT
                jsonb_path_exists('{"a":[1,2,3]}'::jsonb, '$.a[*] ? (@ >= 2)'::jsonpath),
                jsonb_path_match('{"a":1}'::jsonb, '$.a == 1'::jsonpath),
                jsonb_path_query_array(
                    '{"a":[1,2,3]}'::jsonb,
                    '$.a[*] ? (@ >= $min)'::jsonpath,
                    '{"min":2}'::jsonb
                ),
                jsonb_path_query_first('{"a":[10,20]}'::jsonb, '$.a[last]'::jsonpath),
                jsonb_path_query_array('{"a":[1,2]}'::jsonb, '$.a.size()'::jsonpath),
                jsonb_path_query_array('{}'::jsonb, 'strict $.missing'::jsonpath, '{}'::jsonb, true)"#,
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Json(json!([2, 3])),
            SqlValue::Json(json!(20)),
            SqlValue::Json(json!([2])),
            SqlValue::Json(json!([])),
        ]]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("bool".to_string()),
            Some("bool".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
        ]
    );
}

#[test]
fn jsonpath_operators_work_for_constants_and_stored_rows_without_breaking_fts_dispatch() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE documents (id int PRIMARY KEY, payload jsonb)")
        .unwrap();
    session
        .execute(
            r#"INSERT INTO documents VALUES
               (1, '{"tags":["urgent","records"],"score":9}'::jsonb),
               (2, '{"tags":["other"],"score":2}'::jsonb)"#,
        )
        .unwrap();

    let constants = session
        .execute(
            r#"SELECT
                '{"a":[1,2]}'::jsonb @? '$.a[*] ? (@ == 2)'::jsonpath,
                '{"a":1}'::jsonb @@ '$.a == 1'::jsonpath"#,
        )
        .unwrap();
    assert_eq!(
        constants.rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]]
    );

    let stored = session
        .execute(
            r#"SELECT id FROM documents
               WHERE payload @? '$.tags[*] ? (@ == "urgent")'::jsonpath
                 AND payload @@ '$.score >= 5'::jsonpath
               ORDER BY id"#,
        )
        .unwrap();
    assert_eq!(stored.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn jsonb_path_query_is_set_returning_in_projection_and_from() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let projection = session
        .execute(
            r#"SELECT jsonb_path_query(
                   '{"a":[1,2,3]}'::jsonb,
                   '$.a[*] ? (@ > $min)'::jsonpath,
                   '{"min":1}'::jsonb
               ) AS value"#,
        )
        .unwrap();
    assert_eq!(
        projection.rows,
        vec![
            vec![SqlValue::Json(json!(2))],
            vec![SqlValue::Json(json!(3))]
        ]
    );

    let from = session
        .execute(
            r#"SELECT value, ordinality
               FROM jsonb_path_query(
                   '{"a":[10,20]}'::jsonb,
                   '$.a[*]'::jsonpath
               ) WITH ORDINALITY AS q(value, ordinality)"#,
        )
        .unwrap();
    assert_eq!(
        from.rows,
        vec![
            vec![SqlValue::Json(json!(10)), SqlValue::Int(1)],
            vec![SqlValue::Json(json!(20)), SqlValue::Int(2)],
        ]
    );
}

#[test]
fn jsonpath_input_is_validated() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let error = session.execute("SELECT '$.a['::jsonpath").unwrap_err();
    assert_eq!(error.sqlstate(), "22P02");
}

#[test]
fn jsonpath_extended_predicates_conversions_subscripts_and_canonical_text_match_postgres() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT
                'strict $.a[*] ? (@ > $min)'::jsonpath,
                jsonb_path_query_array(
                    '{"s":["Abc","other"]}'::jsonb,
                    '$.s[*] ? (@ starts with "Ab")'::jsonpath
                ),
                jsonb_path_query_array(
                    '{"s":["Abc","other"]}'::jsonb,
                    '$.s[*] ? (@ like_regex "^a" flag "i")'::jsonpath
                ),
                jsonb_path_query_array(
                    '{"n":"12.345"}'::jsonb,
                    '$.n.decimal(5,2)'::jsonpath
                ),
                jsonb_path_query_array(
                    '{"a":[10,20,30]}'::jsonb,
                    '$.a[last - $offset]'::jsonpath,
                    '{"offset":1}'::jsonb
                ),
                jsonb_path_exists_tz(
                    '{"d":"2024-01-02 12:00:00+02"}'::jsonb,
                    '$.d.datetime() < "2024-01-02 11:00:00+00".datetime()'::jsonpath
                )"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String(r#"strict $."a"[*]?(@ > $"min")"#.to_string()),
            SqlValue::Json(json!(["Abc"])),
            SqlValue::Json(json!(["Abc"])),
            SqlValue::Json(json!([12.35])),
            SqlValue::Json(json!([20])),
            SqlValue::Bool(true),
        ]]
    );
}

#[test]
fn jsonpath_predicates_use_jsonb_gin_candidates_with_mandatory_recheck() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE TABLE path_documents (id bigint PRIMARY KEY, payload jsonb NOT NULL);
               INSERT INTO path_documents VALUES
                 (1, '{"patient":{"status":"active"}}'),
                 (2, '{"patient":{"status":"inactive"}}'),
                 (3, '{"status":"active"}');
               CREATE INDEX idx_path_documents_payload
                 ON path_documents USING GIN (payload)"#,
        )
        .unwrap();

    let explain = session
        .execute(
            r#"EXPLAIN SELECT id FROM path_documents
               WHERE payload @? '$.patient.status ? (@ == "active")'::jsonpath"#,
        )
        .unwrap();
    assert!(explain.rows.iter().any(|row| row[0]
        .to_cell()
        .contains("JsonbIndexScan idx_path_documents_payload")));

    let rows = session
        .execute(
            r#"SELECT id FROM path_documents
               WHERE payload @? '$.patient.status ? (@ == "active")'::jsonpath
               ORDER BY id"#,
        )
        .unwrap();
    assert_eq!(rows.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn jsonpath_storage_copy_arrays_and_restart_preserve_canonical_values() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE stored_paths (
                    id text PRIMARY KEY,
                    path jsonpath NOT NULL,
                    paths jsonpath[]
                )",
            )
            .unwrap();
        session
            .execute(
                r#"INSERT INTO stored_paths VALUES (
                    'inserted',
                    'strict $.a[*] ? (@ > $minimum)',
                    ARRAY['$.a'::jsonpath, '$.b[last]'::jsonpath]
                )"#,
            )
            .unwrap();
        assert_eq!(
            session
                .copy_insert_rows(
                    "stored_paths",
                    &["id".into(), "path".into()],
                    vec![vec![Some("copied".into()), Some("$.payload.value".into())]],
                )
                .unwrap(),
            1
        );
        let invalid = session.copy_insert_rows(
            "stored_paths",
            &["id".into(), "path".into()],
            vec![vec![Some("invalid".into()), Some("$.broken[".into())]],
        );
        assert_eq!(invalid.unwrap_err().sqlstate(), "22P02");
    }

    let mut reopened = BicDb::open(dir.path()).unwrap();
    let result = SqlSession::new(&mut reopened)
        .execute("SELECT id, path, paths FROM stored_paths ORDER BY id")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("copied".into()),
                SqlValue::String(r#"$."payload"."value""#.into()),
                SqlValue::Null,
            ],
            vec![
                SqlValue::String("inserted".into()),
                SqlValue::String(r#"strict $."a"[*]?(@ > $"minimum")"#.into()),
                SqlValue::Json(json!([r#"$."a""#, r#"$."b"[last]"#])),
            ],
        ]
    );
    assert_eq!(result.column_types[1], Some("jsonpath".to_string()));
    assert_eq!(result.column_types[2], Some("jsonpath[]".to_string()));
}
