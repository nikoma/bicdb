use bicdb_core::BicDb;
use bicdb_sql::{PgJsonText, SqlSession, SqlValue};

const RAW: &str = r#"{  "z" : 1, "a" : [ true, null ], "z" : 2 }"#;
const UPDATED: &str = r#"[ 1,  2, { "same" : 1, "same" : 2 } ]"#;

#[test]
fn json_preserves_input_text_across_all_write_paths_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(&format!(
            "CREATE TABLE json_values (
                id TEXT PRIMARY KEY,
                payload JSON NOT NULL DEFAULT '{RAW}',
                canonical JSONB NOT NULL DEFAULT '{RAW}'
            )"
        ))
        .unwrap();
    session
        .execute(&format!(
            "INSERT INTO json_values (id, payload, canonical) VALUES ('inserted', '{RAW}', '{RAW}')"
        ))
        .unwrap();
    session
        .execute("INSERT INTO json_values (id) VALUES ('defaulted')")
        .unwrap();
    session
        .copy_insert_rows(
            "json_values",
            &["id".to_string(), "payload".to_string()],
            vec![vec![Some("copied".to_string()), Some(RAW.to_string())]],
        )
        .unwrap();

    let selected = session
        .execute("SELECT id, payload, canonical FROM json_values ORDER BY id")
        .unwrap();
    assert_eq!(selected.column_types[1], Some("json".to_string()));
    assert_eq!(selected.column_types[2], Some("jsonb".to_string()));
    for row in &selected.rows {
        assert_eq!(row[1].to_cell(), RAW);
        assert_eq!(row[2].to_cell(), r#"{"a":[true,null],"z":2}"#);
    }
    let casted = session
        .execute("SELECT payload::text, payload::jsonb FROM json_values WHERE id = 'inserted'")
        .unwrap();
    assert_eq!(casted.rows[0][0].to_cell(), RAW);
    assert_eq!(casted.rows[0][1].to_cell(), r#"{"a":[true,null],"z":2}"#);
    let ordered_casts = session
        .execute("SELECT payload::text FROM json_values ORDER BY id")
        .unwrap();
    assert!(ordered_casts.rows.iter().all(|row| row[0].to_cell() == RAW));

    session
        .execute(&format!(
            "UPDATE json_values SET payload = '{UPDATED}' WHERE id = 'inserted'"
        ))
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT payload FROM json_values WHERE id = 'inserted'")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        UPDATED
    );
    let error = session
        .execute("UPDATE json_values SET payload = '{broken' WHERE id = 'inserted'")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22P02");

    drop(session);
    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut reopened_session = SqlSession::new(&mut reopened);
    assert_eq!(
        reopened_session
            .execute("SELECT id, payload FROM json_values ORDER BY id")
            .unwrap()
            .rows
            .into_iter()
            .map(|row| (row[0].to_cell(), row[1].to_cell()))
            .collect::<Vec<_>>(),
        vec![
            ("copied".to_string(), RAW.to_string()),
            ("defaulted".to_string(), RAW.to_string()),
            ("inserted".to_string(), UPDATED.to_string()),
        ]
    );
}

#[test]
fn json_cast_preserves_text_while_jsonb_canonicalizes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(&format!("SELECT '{RAW}'::json, '{RAW}'::jsonb"))
        .unwrap();
    assert_eq!(result.rows[0][0].to_cell(), RAW);
    assert_eq!(result.rows[0][1].to_cell(), r#"{"a":[true,null],"z":2}"#);
    assert!(matches!(result.rows[0][0], SqlValue::JsonText(_)));
    assert!(matches!(result.rows[0][1], SqlValue::Json(_)));

    let allowed = session
        .execute(
            "SELECT '1'::text::json, '1'::varchar::json,
                    '1'::jsonb::json, '1'::json::jsonb,
                    '1'::json::text, '1'::json::varchar",
        )
        .unwrap();
    assert_eq!(
        allowed.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec!["1", "1", "1", "1", "1", "1"]
    );

    for sql in [
        "SELECT 1::json",
        "SELECT true::json",
        "SELECT ARRAY[1,2]::json",
        "SELECT '1'::json::integer",
        "SELECT 'true'::json::boolean",
        "SELECT '1'::json::numeric",
        "SELECT '1'::json::bytea",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42846", "{sql}: {error}");
    }
}

#[test]
fn json_scalar_functions_and_operators_accept_raw_json_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT json_array_length('[1, null, {"a": 2}]'::json),
                      json_typeof('{"a": 2}'::json),
                      ('{"a":[1,{"b":2}]}'::json)->'a'->1->>'b',
                      ('[1,2,3]'::json)->> -1,
                      ('{"a":[1,{"b":2}]}'::json)#>>'{a,1,b}',
                      json_strip_nulls('{"a":null,"b":[null,1]}'::json, true)"#,
        )
        .unwrap();
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec!["3", "object", "2", "3", "2", r#"{"b":[1]}"#]
    );
    assert_eq!(result.column_types[0], Some("int4".to_string()));
    assert_eq!(result.column_types[1], Some("text".to_string()));
    assert_eq!(result.column_types[5], Some("json".to_string()));
    assert_eq!(
        session
            .execute(
                r#"SELECT json_strip_nulls(
                    '{ "a" : 1, "a" : null, "a" : 2, "b" : [ null, {"c":null,"d":3} ] }'::json
                ), json_strip_nulls(
                    '{ "a" : 1, "a" : null, "a" : 2, "b" : [ null, {"c":null,"d":3} ] }'::json,
                    true
                )"#,
            )
            .unwrap()
            .rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            r#"{"a":1,"a":2,"b":[null,{"d":3}]}"#,
            r#"{"a":1,"a":2,"b":[{"d":3}]}"#,
        ]
    );
}

#[test]
fn json_extraction_preserves_nested_source_text_and_catalog_function_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let source = r#"{ "a" : { "x" : 1, "x" : 2 }, "a" : [ 3,  4 ], "n" : 1.2300e+02 }"#;

    let result = session
        .execute(&format!(
            r#"SELECT
                '{source}'::json->'a',
                '[ 1, {{ "x" : 1, "x" : 2 }} ]'::json->1,
                '{source}'::json->>'n',
                json_extract_path('{{"a": {{ "x" : 1, "x" : 2 }}}}'::json, 'a'),
                json_array_element('[ 10,  20 ]'::json, -1),
                json_array_element_text('[10,null]'::json, 1),
                json_object_field('{{"a": {{ "b" : 1 }}}}'::json, 'a'),
                json_object_field_text('{{"a":null}}'::json, 'a'),
                json_send(' {{"a":1}} '::json)"#,
        ))
        .unwrap();
    assert_eq!(
        result.rows[0],
        vec![
            SqlValue::JsonText(PgJsonText::parse("[ 3,  4 ]".to_string()).unwrap()),
            SqlValue::JsonText(PgJsonText::parse(r#"{ "x" : 1, "x" : 2 }"#.to_string()).unwrap(),),
            SqlValue::String("1.2300e+02".to_string()),
            SqlValue::JsonText(PgJsonText::parse(r#"{ "x" : 1, "x" : 2 }"#.to_string()).unwrap(),),
            SqlValue::JsonText(PgJsonText::parse("20".to_string()).unwrap()),
            SqlValue::Null,
            SqlValue::JsonText(PgJsonText::parse(r#"{ "b" : 1 }"#.to_string()).unwrap()),
            SqlValue::Null,
            SqlValue::String("\\x207b2261223a317d20".to_string()),
        ]
    );
    assert_eq!(result.column_types[8], Some("bytea".to_string()));
}

#[test]
fn json_set_returning_functions_preserve_raw_elements_and_duplicate_key_order() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let elements = session
        .execute(r#"SELECT json_array_elements('[1, { "a" : 2 }, null]'::json)"#)
        .unwrap();
    assert_eq!(elements.column_types, vec![Some("json".to_string())]);
    assert_eq!(
        elements.rows,
        vec![
            vec![SqlValue::JsonText(
                PgJsonText::parse("1".to_string()).unwrap()
            )],
            vec![SqlValue::JsonText(
                PgJsonText::parse(r#"{ "a" : 2 }"#.to_string()).unwrap()
            )],
            vec![SqlValue::JsonText(
                PgJsonText::parse("null".to_string()).unwrap()
            )],
        ]
    );

    let keys = session
        .execute(r#"SELECT * FROM json_object_keys('{"z":1,"a":2,"z":3}'::json)"#)
        .unwrap();
    assert_eq!(
        keys.rows
            .iter()
            .map(|row| row[0].to_cell())
            .collect::<Vec<_>>(),
        vec!["z", "a", "z"]
    );

    let text = session
        .execute(r#"SELECT * FROM json_array_elements_text('[null,"x", { "a" : 2 }]'::json)"#)
        .unwrap();
    assert_eq!(
        text.rows,
        vec![
            vec![SqlValue::Null],
            vec![SqlValue::String("x".to_string())],
            vec![SqlValue::String(r#"{ "a" : 2 }"#.to_string())],
        ]
    );
}

#[test]
fn json_array_and_object_constructors_match_postgres_text_and_errors() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT array_to_json(ARRAY[1,NULL,3]),
                    array_to_json(ARRAY[[1,2],[3,4]], true),
                    json_object(ARRAY['a','1','b',NULL]),
                    json_object(ARRAY['a','b'], ARRAY['1',NULL]),
                    json_object(ARRAY[['a','1'],['a','2']])",
        )
        .unwrap();
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            "[1,null,3]",
            "[[1,2],\n [3,4]]",
            r#"{"a" : "1", "b" : null}"#,
            r#"{"a" : "1", "b" : null}"#,
            r#"{"a" : "1", "a" : "2"}"#,
        ]
    );
    assert!(result
        .column_types
        .iter()
        .all(|pg_type| pg_type.as_deref() == Some("json")));

    for sql in [
        "SELECT json_object(ARRAY['a','1','b'])",
        "SELECT json_object(ARRAY['a',NULL], ARRAY['1','2'])",
        "SELECT json_object(ARRAY['a'], ARRAY['1','2'])",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "22023",
            "{sql}"
        );
    }
}

#[test]
fn sql_standard_json_constructors_honor_null_and_returning_clauses() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            r#"SELECT json_array(1, NULL, 'x'),
                      json_array(1, NULL, 'x' NULL ON NULL),
                      json_array(1, NULL, 'x' ABSENT ON NULL RETURNING text),
                      json_array(1 RETURNING jsonb),
                      json_object('a': 1, 'b': NULL),
                      json_object('a': 1, 'b': NULL ABSENT ON NULL),
                      json_object('a': 1 RETURNING jsonb),
                      json(' { "a" : 1 } '),
                      json_scalar('x'),
                      json_serialize(' { "a" : 1 } '::json RETURNING text),
                      json_array(1 RETURNING bytea)"#,
        )
        .unwrap();
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            r#"[1, "x"]"#,
            r#"[1, null, "x"]"#,
            r#"[1, "x"]"#,
            "[1]",
            r#"{"a" : 1, "b" : null}"#,
            r#"{"a" : 1}"#,
            r#"{"a":1}"#,
            r#" { "a" : 1 } "#,
            r#""x""#,
            r#" { "a" : 1 } "#,
            r#"\x5b315d"#,
        ]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("json".to_string()),
            Some("json".to_string()),
            Some("text".to_string()),
            Some("jsonb".to_string()),
            Some("json".to_string()),
            Some("json".to_string()),
            Some("jsonb".to_string()),
            Some("json".to_string()),
            Some("json".to_string()),
            Some("text".to_string()),
            Some("bytea".to_string()),
        ]
    );
    let empty = session
        .execute("SELECT json_array(), json_object(), json_object(RETURNING jsonb)")
        .unwrap();
    assert_eq!(
        empty.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec!["[]", "{}", "{}"]
    );
    assert_eq!(
        empty.column_types,
        vec![
            Some("json".to_string()),
            Some("json".to_string()),
            Some("jsonb".to_string()),
        ]
    );

    let scalars = session
        .execute("SELECT json_scalar(1), json_scalar(true), json_scalar(NULL)")
        .unwrap();
    assert_eq!(scalars.rows[0][0].to_cell(), "1");
    assert_eq!(scalars.rows[0][1].to_cell(), "true");
    assert!(matches!(scalars.rows[0][2], SqlValue::Null));
    assert!(scalars
        .column_types
        .iter()
        .all(|pg_type| pg_type.as_deref() == Some("json")));

    let formatted = session
        .execute(
            r#"SELECT
                json_array('{"a":1}' FORMAT JSON),
                json_object('a': '{"b":2}' FORMAT JSON),
                json(convert_to('{"a":1}', 'UTF8')),
                json(convert_to('{"a":1}', 'UTF8') FORMAT JSON ENCODING UTF8),
                json_serialize('{"a":1}'::json RETURNING bytea FORMAT JSON ENCODING UTF8)"#,
        )
        .unwrap();
    assert_eq!(
        formatted.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            r#"[{"a":1}]"#,
            r#"{"a" : {"b":2}}"#,
            r#"{"a":1}"#,
            r#"{"a":1}"#,
            r#"\x7b2261223a317d"#,
        ]
    );
    assert_eq!(
        session
            .execute(r#"SELECT json_array('{"a":1}' FORMAT JSON ENCODING UTF8)"#)
            .unwrap_err()
            .sqlstate(),
        "22023"
    );
    assert_eq!(
        session
            .execute("SELECT json_array(123 RETURNING varchar(2))")
            .unwrap_err()
            .sqlstate(),
        "22001"
    );
    assert_eq!(
        session
            .execute("SELECT json_array(1 RETURNING text FORMAT JSON ENCODING UTF8)")
            .unwrap_err()
            .sqlstate(),
        "0A000"
    );

    session
        .execute("CREATE TABLE json_array_query_values (id INT PRIMARY KEY, value_number INT)")
        .unwrap();
    session
        .execute("INSERT INTO json_array_query_values VALUES (1, 2), (2, NULL), (3, 1)")
        .unwrap();
    let query_constructor = session
        .execute(
            "SELECT json_array(
                SELECT value_number FROM json_array_query_values ORDER BY id
             )",
        )
        .unwrap();
    assert_eq!(query_constructor.rows[0][0].to_cell(), "[2, 1]");
    assert_eq!(query_constructor.columns, ["json_array"]);
    assert_eq!(query_constructor.column_types, [Some("json".to_string())]);
    let query_constructor_jsonb = session
        .execute(
            "SELECT json_array(
                SELECT value_number FROM json_array_query_values ORDER BY id
                RETURNING jsonb
             )",
        )
        .unwrap();
    assert_eq!(query_constructor_jsonb.rows[0][0].to_cell(), "[2,1]");
    assert_eq!(
        query_constructor_jsonb.column_types,
        [Some("jsonb".to_string())]
    );
}

#[test]
fn json_each_preserves_duplicate_order_raw_values_and_text_nulls() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let values = session
        .execute(
            r#"SELECT key, value FROM json_each('{"z": 1, "a" : null, "z": { "n" : 2 }}'::json)"#,
        )
        .unwrap();
    assert_eq!(
        values
            .rows
            .iter()
            .map(|row| (row[0].to_cell(), row[1].to_cell()))
            .collect::<Vec<_>>(),
        vec![
            ("z".to_string(), "1".to_string()),
            ("a".to_string(), "null".to_string()),
            ("z".to_string(), r#"{ "n" : 2 }"#.to_string()),
        ]
    );
    assert_eq!(
        values.column_types,
        vec![Some("text".to_string()), Some("json".to_string())]
    );

    let text = session
        .execute(r#"SELECT key, value FROM json_each_text('{"z": 1, "a" : null, "s": "x"}'::json)"#)
        .unwrap();
    assert_eq!(
        text.rows,
        vec![
            vec![
                SqlValue::String("z".to_string()),
                SqlValue::String("1".to_string())
            ],
            vec![SqlValue::String("a".to_string()), SqlValue::Null],
            vec![
                SqlValue::String("s".to_string()),
                SqlValue::String("x".to_string())
            ],
        ]
    );
    assert_eq!(
        text.column_types,
        vec![Some("text".to_string()), Some("text".to_string())]
    );

    assert_eq!(
        session
            .execute("SELECT json_object('a': 1, 'a': 2 WITHOUT UNIQUE KEYS)")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        r#"{"a" : 1, "a" : 2}"#
    );
    assert_eq!(
        session
            .execute("SELECT json_object('a': 1, 'a': 2 WITH UNIQUE KEYS)")
            .unwrap_err()
            .sqlstate(),
        "22030"
    );
    assert_eq!(
        session
            .execute(r#"SELECT json('{"a":{"b":1,"b":2}}' WITH UNIQUE KEYS)"#)
            .unwrap_err()
            .sqlstate(),
        "22030"
    );
    assert_eq!(
        session
            .execute(r#"SELECT json('{"a":1,"a":2}' WITHOUT UNIQUE KEYS)"#)
            .unwrap()
            .rows[0][0]
            .to_cell(),
        r#"{"a":1,"a":2}"#
    );
}

#[test]
fn json_aggregate_variants_preserve_order_strictness_and_key_contracts() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE json_aggregate_rows (
                id INT PRIMARY KEY, grp TEXT NOT NULL, key_name TEXT, value_number INT
             )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO json_aggregate_rows VALUES
                (1, 'g', 'a', 1),
                (2, 'g', 'b', NULL),
                (3, 'g', 'a', 3),
                (4, 'g', 'c', 2)",
        )
        .unwrap();

    let aggregate = session
        .execute(
            "SELECT json_agg_strict(value_number ORDER BY id),
                    json_object_agg(key_name, value_number ORDER BY id),
                    json_object_agg_strict(key_name, value_number ORDER BY id),
                    jsonb_object_agg(key_name, value_number ORDER BY id),
                    jsonb_object_agg_strict(key_name, value_number ORDER BY id)
             FROM json_aggregate_rows",
        )
        .unwrap();
    assert_eq!(
        aggregate.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            "[1, 3, 2]",
            r#"{ "a" : 1, "b" : null, "a" : 3, "c" : 2 }"#,
            r#"{ "a" : 1, "a" : 3, "c" : 2 }"#,
            r#"{"a":3,"b":null,"c":2}"#,
            r#"{"a":3,"c":2}"#,
        ]
    );
    assert_eq!(
        aggregate.column_types,
        vec![
            Some("json".to_string()),
            Some("json".to_string()),
            Some("json".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT json_arrayagg(value_number RETURNING varchar(2))
                 FROM json_aggregate_rows",
            )
            .unwrap_err()
            .sqlstate(),
        "22001"
    );
    assert_eq!(
        session
            .execute(
                "SELECT json_arrayagg(value_number RETURNING text FORMAT JSON ENCODING UTF8)
                 FROM json_aggregate_rows",
            )
            .unwrap_err()
            .sqlstate(),
        "0A000"
    );

    let grouped = session
        .execute(
            "SELECT grp, json_object_agg(key_name, value_number ORDER BY id)
             FROM json_aggregate_rows GROUP BY grp",
        )
        .unwrap();
    assert_eq!(grouped.rows[0][1].to_cell(), aggregate.rows[0][1].to_cell());

    let standard = session
        .execute(
            "SELECT json_arrayagg(value_number ORDER BY id),
                    json_arrayagg(value_number ORDER BY id NULL ON NULL RETURNING text),
                    json_arrayagg(value_number ORDER BY id RETURNING jsonb),
                    json_objectagg(key_name VALUE value_number),
                    json_objectagg(key_name VALUE value_number ABSENT ON NULL RETURNING text),
                    json_objectagg(key_name VALUE value_number RETURNING bytea)
             FROM json_aggregate_rows",
        )
        .unwrap();
    assert_eq!(
        standard.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            "[1, 3, 2]",
            "[1, null, 3, 2]",
            "[1,3,2]",
            r#"{ "a" : 1, "b" : null, "a" : 3, "c" : 2 }"#,
            r#"{ "a" : 1, "a" : 3, "c" : 2 }"#,
            "\\x7b20226122203a20312c20226222203a206e756c6c2c20226122203a20332c20226322203a2032207d",
        ]
    );
    assert_eq!(
        standard.columns,
        vec![
            "json_arrayagg",
            "json_arrayagg",
            "json_arrayagg",
            "json_objectagg",
            "json_objectagg",
            "json_objectagg",
        ]
    );
    assert_eq!(
        standard.column_types,
        vec![
            Some("json".to_string()),
            Some("text".to_string()),
            Some("jsonb".to_string()),
            Some("json".to_string()),
            Some("text".to_string()),
            Some("bytea".to_string()),
        ]
    );

    for function in [
        "json_object_agg_unique",
        "json_object_agg_unique_strict",
        "jsonb_object_agg_unique",
        "jsonb_object_agg_unique_strict",
    ] {
        let error = session
            .execute(&format!(
                "SELECT {function}(key_name, value_number ORDER BY id) FROM json_aggregate_rows"
            ))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22030", "{function}: {error}");
    }
    assert_eq!(
        session
            .execute(
                "SELECT json_objectagg(key_name VALUE value_number WITH UNIQUE KEYS)
                 FROM json_aggregate_rows",
            )
            .unwrap_err()
            .sqlstate(),
        "22030"
    );
    assert_eq!(
        session
            .execute(
                "SELECT json_objectagg(key_name VALUE value_number WITHOUT UNIQUE KEYS)
                 FROM json_aggregate_rows",
            )
            .unwrap()
            .rows[0][0]
            .to_cell(),
        r#"{ "a" : 1, "b" : null, "a" : 3, "c" : 2 }"#
    );

    session.execute("TRUNCATE json_aggregate_rows").unwrap();
    session
        .execute("INSERT INTO json_aggregate_rows VALUES (1, 'g', 'a', NULL)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT json_agg_strict(value_number),
                        json_object_agg_strict(key_name, value_number)
                 FROM json_aggregate_rows",
            )
            .unwrap()
            .rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec!["[]", "{  }"]
    );

    session.execute("TRUNCATE json_aggregate_rows").unwrap();
    session
        .execute("INSERT INTO json_aggregate_rows VALUES (1, 'g', NULL, NULL)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT json_object_agg_strict(key_name, value_number) FROM json_aggregate_rows"
            )
            .unwrap_err()
            .sqlstate(),
        "22023"
    );
}

#[test]
fn json_to_record_functions_apply_typed_column_definitions() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let record = session
        .execute(
            r#"SELECT *
               FROM json_to_record('{"a":1,"b":"x","active":true,"ignored":9}'::json)
                    AS x(a int, b text, active bool, missing uuid)"#,
        )
        .unwrap();
    assert_eq!(
        record.rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::String("x".to_string()),
            SqlValue::Bool(true),
            SqlValue::Null,
        ]]
    );
    assert_eq!(
        record.column_types,
        vec![
            Some("int4".to_string()),
            Some("text".to_string()),
            Some("bool".to_string()),
            Some("uuid".to_string()),
        ]
    );
    let exact = session
        .execute(
            r#"SELECT *
               FROM json_to_record(
                    '{"a": { "x" : 1, "x" : 2 }, "b": [ 1,  2 ], "n":123456789012345678901234567890.123400}'::json
               ) AS x(a text, b text, n numeric)"#,
        )
        .unwrap();
    assert_eq!(
        exact.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            r#"{ "x" : 1, "x" : 2 }"#,
            "[ 1,  2 ]",
            "123456789012345678901234567890.123400",
        ]
    );

    let recordset = session
        .execute(
            r#"SELECT id, label
               FROM json_to_recordset('[{"id":2},{"id":1,"label":"first"}]'::json)
                    AS x(id int, label text)
               ORDER BY id"#,
        )
        .unwrap();
    assert_eq!(
        recordset.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::String("first".to_string())],
            vec![SqlValue::Int(2), SqlValue::Null],
        ]
    );

    let null_record = session
        .execute("SELECT * FROM json_to_record(NULL::json) AS x(a int, b text)")
        .unwrap();
    assert_eq!(null_record.rows, vec![vec![SqlValue::Null, SqlValue::Null]]);
    assert!(session
        .execute("SELECT * FROM json_to_recordset(NULL::json) AS x(a int, b text)")
        .unwrap()
        .rows
        .is_empty());

    assert_eq!(
        session
            .execute("SELECT * FROM json_to_record('null'::json) AS x(a int)")
            .unwrap_err()
            .sqlstate(),
        "22023"
    );
    assert_eq!(
        session
            .execute(r#"SELECT * FROM json_to_record('{"a":"not-an-int"}'::json) AS x(a int)"#)
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
}

#[test]
fn json_populate_record_functions_apply_typed_aliases_and_base_defaults() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let record = session
        .execute(
            r#"SELECT *
               FROM json_populate_record(
                   NULL::record,
                   '{"a":1,"b":"x","c":true}'::json
               ) AS x(a integer, b text, c boolean, missing text)"#,
        )
        .unwrap();
    assert_eq!(
        record.rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::String("x".to_string()),
            SqlValue::Bool(true),
            SqlValue::Null,
        ]]
    );
    assert_eq!(
        record.column_types,
        vec![
            Some("int4".to_string()),
            Some("text".to_string()),
            Some("bool".to_string()),
            Some("text".to_string()),
        ]
    );

    let recordset = session
        .execute(
            r#"SELECT *
               FROM json_populate_recordset(
                   NULL::record,
                   '[{"a":1,"b":"x"},{"a":2,"c":true}]'::json
               ) AS x(a integer, b text, c boolean)"#,
        )
        .unwrap();
    assert_eq!(
        recordset.rows,
        vec![
            vec![
                SqlValue::Int(1),
                SqlValue::String("x".to_string()),
                SqlValue::Null,
            ],
            vec![SqlValue::Int(2), SqlValue::Null, SqlValue::Bool(true)],
        ]
    );

    let null_record = session
        .execute(
            "SELECT * FROM json_populate_record(NULL::record, NULL::json)
             AS x(a integer, b text)",
        )
        .unwrap();
    assert_eq!(null_record.rows, vec![vec![SqlValue::Null, SqlValue::Null]]);
    let null_recordset = session
        .execute(
            "SELECT * FROM json_populate_recordset(NULL::record, NULL::json)
             AS x(a integer, b text)",
        )
        .unwrap();
    assert!(null_recordset.rows.is_empty());

    for legacy_flag in ["true", "false", "NULL"] {
        assert_eq!(
            session
                .execute(&format!(
                    "SELECT * FROM json_populate_record(
                        NULL::record, '{{\"a\":1}}'::json, {legacy_flag}
                     ) AS x(a integer)"
                ))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
    }
}

#[test]
fn is_json_predicate_matches_postgres_kinds_uniqueness_and_nulls() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            r#"SELECT
                '{"a":1}' IS JSON,
                '[1,2]' IS JSON ARRAY,
                '1' IS JSON SCALAR,
                '1' IS JSON VALUE,
                '1' IS NOT JSON OBJECT,
                '{broken' IS JSON,
                NULL::text IS JSON"#,
        )
        .unwrap();
    assert_eq!(
        result.rows[0],
        vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Null,
        ]
    );
    assert!(result.columns.iter().all(|name| name == "?column?"));

    assert_eq!(
        session
            .execute(
                r#"SELECT
                    '{"a":1,"a":2}' IS JSON WITHOUT UNIQUE KEYS,
                    '{"a":1,"a":2}' IS JSON WITH UNIQUE KEYS,
                    '{"a":{"b":1,"b":2}}' IS JSON WITH UNIQUE KEYS,
                    '[{"a":1},{"a":2}]' IS JSON WITH UNIQUE KEYS,
                    '"\uD800"' IS JSON"#,
            )
            .unwrap()
            .rows[0],
        vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]
    );

    assert_eq!(
        session.execute("SELECT 1 IS JSON").unwrap_err().sqlstate(),
        "42804"
    );
}

#[test]
fn json_defers_postgres_unicode_conversion_errors_but_jsonb_rejects_them() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let raw = session
        .execute(r#"SELECT '{"bad":"\uD800","nul":"\u0000"}'::json"#)
        .unwrap();
    assert_eq!(
        raw.rows[0][0].to_cell(),
        r#"{"bad":"\uD800","nul":"\u0000"}"#
    );
    assert_eq!(
        session
            .execute(
                r#"SELECT json_typeof('"\uD800"'::json), json_array_length('["\uD800"]'::json)"#
            )
            .unwrap()
            .rows[0],
        vec![SqlValue::String("string".to_string()), SqlValue::Int(1)]
    );
    assert_eq!(
        session
            .execute(r#"SELECT '{"face":"\uD83D\uDE00"}'::json->>'face'"#)
            .unwrap()
            .rows[0][0],
        SqlValue::String("😀".to_string())
    );
    assert_eq!(
        session
            .execute(r#"SELECT '{"bad":"\uD800"}'::json->>'bad'"#)
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
    assert_eq!(
        session
            .execute(r#"SELECT '{"nul":"\u0000"}'::json->>'nul'"#)
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
    assert_eq!(
        session
            .execute(r#"SELECT '"\uD800"'::jsonb"#)
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
}

#[test]
fn json_subscripting_is_rejected_like_postgres() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let error = session
        .execute(r#"SELECT ('{"a":1}'::json)['a']"#)
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42804");
    assert!(error.to_string().contains("cannot subscript type json"));
}
