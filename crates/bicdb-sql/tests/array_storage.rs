use bicdb_core::{BicDb, Record};
use bicdb_sql::{SqlSession, SqlValue, PG_TYPE_REGISTRY};
use serde_json::json;

const CREATE_TABLE: &str = r#"
    CREATE TABLE array_values (
        id TEXT PRIMARY KEY,
        matrix INT4[],
        exact_values NUMERIC[],
        labels TEXT[]
    )
"#;

#[test]
fn array_btree_and_gin_indexes_plan_mutate_and_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE indexed_arrays (
                    id INT4 PRIMARY KEY,
                    scores INT4[] NOT NULL,
                    tags TEXT[] NOT NULL
                 );
                 INSERT INTO indexed_arrays VALUES
                    (1, ARRAY[1,2], ARRAY['alpha','shared']),
                    (2, ARRAY[2,3], ARRAY['beta','shared']),
                    (3, '[0:1]={4,5}'::INT4[], ARRAY['gamma']);
                 CREATE INDEX indexed_arrays_scores_btree ON indexed_arrays (scores);
                 CREATE INDEX indexed_arrays_tags_gin ON indexed_arrays USING GIN (tags);",
            )
            .unwrap();

        drop(session);
        assert!(db.index_definitions().iter().any(|index| {
            index.name == "indexed_arrays_tags_gin" && index.kind == bicdb_core::IndexKind::Array
        }));
        let mut session = SqlSession::new(&mut db);
        let containment = session
            .execute(
                "EXPLAIN SELECT id FROM indexed_arrays
                 WHERE tags @> ARRAY['shared']",
            )
            .unwrap();
        assert!(containment.rows.iter().any(|row| row[0]
            .to_cell()
            .contains("ArrayIndexScan indexed_arrays_tags_gin")));
        let overlap = session
            .execute(
                "EXPLAIN SELECT id FROM indexed_arrays
                 WHERE tags && ARRAY['missing','gamma']",
            )
            .unwrap();
        assert!(overlap.rows.iter().any(|row| row[0]
            .to_cell()
            .contains("ArrayIndexScan indexed_arrays_tags_gin")));
        let ordering = session
            .execute(
                "EXPLAIN SELECT id FROM indexed_arrays
                 WHERE scores >= ARRAY[2,3]",
            )
            .unwrap();
        assert!(ordering.rows.iter().any(|row| row[0]
            .to_cell()
            .contains("IndexRangeScan indexed_arrays_scores_btree")));

        session
            .execute(
                "UPDATE indexed_arrays SET tags = ARRAY['updated'] WHERE id = 1;
                 DELETE FROM indexed_arrays WHERE id = 2;
                 INSERT INTO indexed_arrays VALUES
                    (4, ARRAY[8,9], ARRAY['shared','new']);",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM indexed_arrays
                     WHERE tags @> ARRAY['shared'] ORDER BY id",
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(4)]],
        );
    }
    db.close().unwrap();

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute(
                "SELECT id FROM indexed_arrays
                 WHERE tags && ARRAY['updated','shared'] ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(4)]],
    );
    assert!(session
        .execute(
            "EXPLAIN SELECT id FROM indexed_arrays
             WHERE tags @> ARRAY['updated']",
        )
        .unwrap()
        .rows
        .iter()
        .any(|row| row[0]
            .to_cell()
            .contains("ArrayIndexScan indexed_arrays_tags_gin")));
}

#[test]
fn array_operations_preserve_bounds_and_support_element_and_slice_assignment() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE array_operation_rows (
                id TEXT PRIMARY KEY,
                values_int INT4[],
                matrix INT4[],
                note TEXT
             );
             INSERT INTO array_operation_rows VALUES (
                'bounded',
                '[0:2]={10,20,30}'::INT4[],
                '[0:1][5:6]={{1,2},{3,4}}'::INT4[],
                'before'
             )",
        )
        .unwrap();
    session
        .execute(
            "UPDATE array_operation_rows
             SET values_int[-1] = 7, note = 'after, where preserved'
             WHERE id = 'bounded';
             UPDATE array_operation_rows
             SET values_int[0:1] = ARRAY[8,9], matrix[0][5] = 11
             WHERE id = 'bounded'",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT values_int, array_dims(values_int), note,
                        matrix, matrix[0], matrix[0][5], matrix[0:0][5:5],
                        ARRAY[1,2] @> ARRAY[2,2],
                        ARRAY[1,2] && ARRAY[3,2]
                 FROM array_operation_rows WHERE id = 'bounded'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Json(json!({
                "$bicdb_array_input": {
                    "lower_bounds": [-1],
                    "value": [7, 8, 9, 30],
                }
            })),
            SqlValue::String("[-1:2]".to_string()),
            SqlValue::String("after, where preserved".to_string()),
            SqlValue::Json(json!({
                "$bicdb_array_input": {
                    "lower_bounds": [0, 5],
                    "value": [[11, 2], [3, 4]],
                }
            })),
            SqlValue::Null,
            SqlValue::Int(11),
            SqlValue::Json(json!([[11]])),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
}

#[test]
fn arrays_preserve_rank_dimensions_lower_bounds_and_typed_elements() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute(CREATE_TABLE).unwrap();
        session
            .execute(
                "INSERT INTO array_values VALUES (
                    'typed',
                    '[0:1][5:6]={{1,2},{3,4}}'::int4[],
                    ARRAY['0.10'::numeric, NULL, '9007199254740993.0100'::numeric],
                    '{\"alpha,beta\",\"Grüße\"}'::text[]
                )",
            )
            .unwrap();
    }

    let stored = db.get("array_values", "typed").unwrap().unwrap();
    let matrix = &stored.metadata["matrix"]["$bicdb_typed"];
    assert_eq!(matrix["version"], 1);
    assert_eq!(matrix["pg_type"], "int4[]");
    assert_eq!(matrix["value"]["element_type"], "int4");
    assert_eq!(
        matrix["value"]["dimensions"],
        json!([
            {"lower_bound": 0, "length": 2},
            {"lower_bound": 5, "length": 2}
        ])
    );
    assert_eq!(matrix["value"]["elements"].as_array().unwrap().len(), 4);
    assert_eq!(matrix["value"]["elements"][0]["type"], "int4");
    assert_eq!(matrix["value"]["elements"][0]["value"], 1);
    assert!(!stored.metadata["matrix"].is_array());

    let exact = &stored.metadata["exact_values"]["$bicdb_typed"]["value"];
    assert_eq!(
        exact["dimensions"],
        json!([{"lower_bound": 1, "length": 3}])
    );
    assert_eq!(exact["elements"][0]["type"], "numeric");
    assert_eq!(exact["elements"][0]["value"]["display_scale"], 2);
    assert_eq!(exact["elements"][1]["type"], "null");
    assert_eq!(
        exact["elements"][2]["value"]["coefficient"],
        "90071992547409930100"
    );
    assert_eq!(
        stored.metadata["labels"]["$bicdb_typed"]["legacy"],
        json!(["alpha,beta", "Grüße"])
    );

    let expected = vec![
        SqlValue::Json(json!({
            "$bicdb_array_input": {
                "lower_bounds": [0, 5],
                "value": [[1, 2], [3, 4]],
            }
        })),
        SqlValue::Json(json!(["0.10", null, "9007199254740993.0100"])),
        SqlValue::Json(json!(["alpha,beta", "Grüße"])),
    ];
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT matrix, exact_values, labels FROM array_values WHERE id = 'typed'")
            .unwrap()
            .rows,
        vec![expected.clone()]
    );

    // Untyped arrays written before this encoding remain readable.
    db.insert(
        "array_values",
        Record::new("legacy").with_metadata(json!({
            "matrix": [7, 8],
            "exact_values": ["1.20"],
            "labels": ["old"]
        })),
    )
    .unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT matrix, exact_values FROM array_values WHERE id = 'legacy'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Json(json!([7, 8])),
            SqlValue::Json(json!(["1.20"])),
        ]]
    );

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT matrix, exact_values, labels FROM array_values WHERE id = 'typed'")
            .unwrap()
            .rows,
        vec![expected]
    );
}

#[test]
fn array_updates_replace_dimensions_and_ragged_arrays_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(CREATE_TABLE).unwrap();
    session
        .execute("INSERT INTO array_values (id, matrix) VALUES ('entry', '{1,2}'::int4[])")
        .unwrap();
    session
        .execute(
            "UPDATE array_values
             SET matrix = '[-2:-1][8:9]={{10,11},{12,13}}'::int4[]
             WHERE id = 'entry'",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT matrix FROM array_values WHERE id = 'entry'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!({
            "$bicdb_array_input": {
                "lower_bounds": [-2, 8],
                "value": [[10, 11], [12, 13]],
            }
        }))]]
    );
    assert!(session
        .execute("UPDATE array_values SET matrix = '{{1,2},{3}}'::int4[] WHERE id = 'entry'",)
        .is_err());
    assert!(session
        .execute("UPDATE array_values SET matrix = '[0:2]={1,2}'::int4[] WHERE id = 'entry'",)
        .is_err());
}

#[test]
fn catalog_vectors_and_nested_arrays_validate_and_survive_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE catalog_vectors (
                    id TEXT PRIMARY KEY,
                    small_ids INT2VECTOR,
                    object_ids OIDVECTOR,
                    small_history INT2VECTOR[],
                    object_history OIDVECTOR[]
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO catalog_vectors VALUES (
                    'row',
                    '1 2 -3'::int2vector,
                    '1 2 -1'::oidvector,
                    ARRAY['1 2'::int2vector, '-3 4'::int2vector],
                    ARRAY['1 2'::oidvector, '-1'::oidvector]
                )",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT small_ids, object_ids, small_history, object_history
                     FROM catalog_vectors WHERE id = 'row'",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("1 2 -3".to_string()),
                SqlValue::String("1 2 4294967295".to_string()),
                SqlValue::Json(json!(["1 2", "-3 4"])),
                SqlValue::Json(json!(["1 2", "4294967295"])),
            ]],
        );
        assert_eq!(
            session.execute("SELECT ''::int2vector").unwrap().rows,
            vec![vec![SqlValue::String(String::new())]],
        );
        assert_eq!(
            session.execute("SELECT '-2'::oidvector").unwrap().rows,
            vec![vec![SqlValue::String("4294967294".to_string())]],
        );
        assert_eq!(
            session
                .execute("SELECT '32768'::int2vector")
                .unwrap_err()
                .sqlstate(),
            "22003",
        );
    }
    db.close().unwrap();

    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT small_ids, object_ids FROM catalog_vectors WHERE id = 'row'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("1 2 -3".to_string()),
            SqlValue::String("1 2 4294967295".to_string()),
        ]],
    );
}

#[test]
fn arrays_enforce_postgres_dimension_and_bound_limits() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SELECT '{{{{{{1}}}}}}'::int4[]")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!([[[[[[1]]]]]]))]],
    );
    assert_eq!(
        session
            .execute("SELECT '{{{{{{{1}}}}}}}'::int4[]")
            .unwrap_err()
            .sqlstate(),
        "54000",
    );
    assert_eq!(
        session
            .execute("SELECT '[2:1]={}'::int4[]")
            .unwrap_err()
            .sqlstate(),
        "2202E",
    );
    assert_eq!(
        session
            .execute("SELECT '[2147483647:2147483647]={1}'::int4[]")
            .unwrap_err()
            .sqlstate(),
        "54000",
    );
    assert_eq!(
        session.execute("SELECT '{}'::int4[]").unwrap().rows,
        vec![vec![SqlValue::Json(json!([]))]],
    );
    assert_eq!(
        session
            .execute(
                "SELECT array_ndims('[0:1][5:6]={{1,2},{3,4}}'::int4[]),
                        array_dims('[0:1][5:6]={{1,2},{3,4}}'::int4[]),
                        array_lower('[0:1][5:6]={{1,2},{3,4}}'::int4[], 1),
                        array_upper('[0:1][5:6]={{1,2},{3,4}}'::int4[], 2),
                        cardinality('[0:1][5:6]={{1,2},{3,4}}'::int4[])"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(2),
            SqlValue::String("[0:1][5:6]".to_string()),
            SqlValue::Int(0),
            SqlValue::Int(6),
            SqlValue::Int(4),
        ]],
    );
}

#[test]
fn declared_array_rank_survives_catalog_storage_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE array_rank_catalog (
                    id TEXT PRIMARY KEY,
                    ordinary INT4[],
                    matrix INT4[2][3],
                    labels TEXT[4]
                )",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT attname, attndims, format_type(atttypid, atttypmod)
                     FROM pg_attribute
                     WHERE attrelid = 'array_rank_catalog'::regclass
                       AND attnum > 0
                     ORDER BY attnum",
                )
                .unwrap()
                .rows,
            vec![
                vec![
                    SqlValue::String("id".to_string()),
                    SqlValue::Int(0),
                    SqlValue::String("text".to_string()),
                ],
                vec![
                    SqlValue::String("ordinary".to_string()),
                    SqlValue::Int(1),
                    SqlValue::String("integer[]".to_string()),
                ],
                vec![
                    SqlValue::String("matrix".to_string()),
                    SqlValue::Int(2),
                    SqlValue::String("integer[]".to_string()),
                ],
                vec![
                    SqlValue::String("labels".to_string()),
                    SqlValue::Int(1),
                    SqlValue::String("text[]".to_string()),
                ],
            ],
        );
    }
    db.close().unwrap();

    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT attname, attndims
                 FROM pg_attribute
                 WHERE attrelid = 'array_rank_catalog'::regclass
                   AND attname = 'matrix'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("matrix".to_string()),
            SqlValue::Int(2),
        ]],
    );
}

#[test]
fn every_registered_array_type_is_persistent_and_storable() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let registered = PG_TYPE_REGISTRY
        .all()
        .iter()
        .filter(|spec| !spec.pseudo)
        .filter_map(|spec| spec.array_oid.map(|array_oid| (spec.name, array_oid)))
        .collect::<Vec<_>>();

    {
        let mut session = SqlSession::new(&mut db);
        for (name, array_oid) in &registered {
            let table = format!("registered_array_{name}");
            let declaration = if *name == "char" { "\"char\"" } else { name };
            session
                .execute(&format!(
                    "CREATE TABLE {table} (id integer PRIMARY KEY, values {declaration}[]); \
                     INSERT INTO {table} VALUES (1, '{{}}'::{declaration}[])"
                ))
                .unwrap_or_else(|error| panic!("{name}[] DDL/storage failed: {error}"));
            assert_eq!(
                session
                    .execute(&format!(
                        "SELECT cardinality(values), \
                                (SELECT atttypid FROM pg_attribute \
                                 WHERE attrelid = '{table}'::regclass AND attname = 'values') \
                         FROM {table} WHERE id = 1"
                    ))
                    .unwrap_or_else(|error| panic!("{name}[] catalog read failed: {error}"))
                    .rows,
                vec![vec![SqlValue::Int(0), SqlValue::Int(i64::from(*array_oid))]],
                "{name}[]"
            );
        }

        session
            .execute(
                "CREATE TABLE registered_vector_values (
                    id integer PRIMARY KEY,
                    values vector[]
                 );
                 INSERT INTO registered_vector_values VALUES (
                    1,
                    ARRAY['[1,2,3]'::vector, NULL, '[4,5,6]'::vector]
                 )",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT values, pg_typeof(values)::text
                     FROM registered_vector_values WHERE id = 1",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Json(json!(["[1.0,2.0,3.0]", null, "[4.0,5.0,6.0]"])),
                SqlValue::String("vector[]".to_string()),
            ]]
        );
    }

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    for (name, _) in &registered {
        assert_eq!(
            session
                .execute(&format!(
                    "SELECT cardinality(values) FROM registered_array_{name} WHERE id = 1"
                ))
                .unwrap_or_else(|error| panic!("{name}[] reopen failed: {error}"))
                .rows,
            vec![vec![SqlValue::Int(0)]],
            "{name}[]"
        );
    }
    assert_eq!(
        session
            .execute("SELECT values FROM registered_vector_values WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!([
            "[1.0,2.0,3.0]",
            null,
            "[4.0,5.0,6.0]"
        ]))]]
    );
}
