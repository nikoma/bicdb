use bicdb_core::BicDb;
use bicdb_sql::{
    infer_parameter_types, infer_query_result_types, SqlSession, SqlValue, PG_TYPE_REGISTRY,
};

fn declaration_name(name: &str) -> String {
    if name == "char" {
        "\"char\"".to_string()
    } else {
        name.to_string()
    }
}

#[test]
fn registry_catalog_attribute_and_planner_types_agree_exhaustively() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let mut declarations = Vec::new();
    let mut projections = Vec::new();
    let mut expected_catalog = Vec::new();
    let mut expected_planner = Vec::new();

    for spec in PG_TYPE_REGISTRY.all().iter().filter(|spec| !spec.pseudo) {
        let scalar_column = format!("scalar_{}", spec.oid);
        declarations.push(format!("{scalar_column} {}", declaration_name(spec.name)));
        projections.push(scalar_column.clone());
        expected_catalog.push(vec![
            SqlValue::String(scalar_column),
            SqlValue::Int(i64::from(spec.oid)),
        ]);
        expected_planner.push(Some(spec.name.to_string()));

        if let Some(array_oid) = spec.array_oid {
            let array_column = format!("array_{array_oid}");
            declarations.push(format!("{array_column} {}[]", declaration_name(spec.name)));
            projections.push(array_column.clone());
            expected_catalog.push(vec![
                SqlValue::String(array_column),
                SqlValue::Int(i64::from(array_oid)),
            ]);
            expected_planner.push(Some(format!("{}[]", spec.name)));
        }
    }

    session
        .execute(&format!(
            "CREATE TABLE registry_type_consistency (id INTEGER PRIMARY KEY, {})",
            declarations.join(", ")
        ))
        .unwrap();

    let attributes = session
        .execute(
            "SELECT a.attname, a.atttypid
             FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
             WHERE c.relname = 'registry_type_consistency'
               AND a.attnum > 0
               AND a.attname <> 'id'
             ORDER BY a.attnum",
        )
        .unwrap();
    assert_eq!(attributes.rows, expected_catalog);

    let planned = session
        .execute(&format!(
            "SELECT {} FROM registry_type_consistency LIMIT 0",
            projections.join(", ")
        ))
        .unwrap();
    assert!(planned.rows.is_empty());
    assert_eq!(planned.columns, projections);
    assert_eq!(planned.column_types, expected_planner);
}

#[test]
fn typed_result_views_preserve_expression_types_without_changing_sql_value() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT 7::int2 AS small_value,
                    7::int8 AS large_value,
                    9007199254740993.01::numeric AS exact_value,
                    ARRAY['a', 'b']::text[] AS tags",
        )
        .unwrap();

    // The legacy API remains the same generic shape.
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].len(), 4);

    let row = result.typed_row(0).unwrap();
    let observed = row
        .iter()
        .map(|cell| {
            (
                cell.column_name().unwrap().to_string(),
                cell.logical_type().unwrap().name().to_string(),
                cell.logical_type().unwrap().oid().unwrap(),
                cell.value().clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed
            .iter()
            .map(|item| item.0.as_str())
            .collect::<Vec<_>>(),
        ["small_value", "large_value", "exact_value", "tags"]
    );
    assert_eq!(
        observed
            .iter()
            .map(|item| item.1.as_str())
            .collect::<Vec<_>>(),
        ["int2", "int8", "numeric", "text[]"]
    );
    assert_eq!(
        observed.iter().map(|item| item.2).collect::<Vec<_>>(),
        [21, 20, 1700, 1009]
    );

    let mut stream = result.into_stream();
    let batch = stream.next_typed_batch(1);
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0][2].logical_type.as_deref(), Some("numeric"));
    assert_eq!(batch[0][2].value, observed[2].3);
    assert!(stream.is_done());
}

#[test]
fn parameter_types_follow_schema_assignment_and_expression_context() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE parameter_type_probe (
                id UUID PRIMARY KEY,
                quantity INT4,
                amount NUMERIC,
                label TEXT,
                active BOOL,
                created_at TIMESTAMPTZ
            )",
        )
        .unwrap();

    assert_eq!(
        infer_parameter_types(&db, "SELECT $1::int8, $2").unwrap(),
        vec![Some("int8".to_string()), None]
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "INSERT INTO parameter_type_probe VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .unwrap(),
        ["uuid", "int4", "numeric", "text", "bool", "timestamptz"]
            .map(|pg_type| Some(pg_type.to_string()))
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "UPDATE parameter_type_probe
             SET quantity = $1, amount = $2
             WHERE id = $3 AND active = $4",
        )
        .unwrap(),
        ["int4", "numeric", "uuid", "bool"].map(|pg_type| Some(pg_type.to_string()))
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "SELECT $1 + 1, 1::int8 + $2, lower($3), coalesce($4, 1.0)",
        )
        .unwrap(),
        ["int4", "int8", "text", "numeric"].map(|pg_type| Some(pg_type.to_string()))
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "SELECT id FROM parameter_type_probe WHERE id = ANY($1)",
        )
        .unwrap(),
        vec![Some("uuid[]".to_string())]
    );
    assert_eq!(
        infer_parameter_types(&db, "SELECT lower(trim(COALESCE($1, '')))").unwrap(),
        vec![Some("text".to_string())]
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "SELECT 1 WHERE EXISTS (
                 SELECT 1
                 FROM jsonb_array_elements_text('[\"a\"]'::jsonb) AS value
                 WHERE value = ANY($1::text[])
             )",
        )
        .unwrap(),
        vec![Some("text[]".to_string())]
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "SELECT 1 WHERE EXISTS (
                 SELECT 1
                 FROM jsonb_array_elements_text('[\"a\"]'::jsonb) v
                 WHERE v = ANY($1)
             )",
        )
        .unwrap(),
        vec![Some("text[]".to_string())]
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "WITH selected AS (
                 SELECT id FROM parameter_type_probe WHERE id = ANY($1)
             ) SELECT id FROM selected",
        )
        .unwrap(),
        vec![Some("uuid[]".to_string())]
    );
    assert_eq!(
        infer_parameter_types(
            &db,
            "WITH RECURSIVE neighborhood(id, hops) AS (
                 SELECT unnest(CAST($1 AS text[])), 0
               UNION
                 SELECT CASE WHEN e.id = n.id THEN e.id ELSE n.id END,
                        n.hops + 1
                 FROM neighborhood n
                 JOIN parameter_type_probe e ON true
                 WHERE n.hops < $2
             )
             SELECT id FROM neighborhood",
        )
        .unwrap(),
        vec![Some("text[]".to_string()), Some("int4".to_string())]
    );
}

#[test]
fn parameter_types_follow_postgres_common_type_selection() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();
    let cases = [
        (
            "SELECT CASE WHEN true THEN $1 ELSE 2::int8 END",
            "int8",
            "int8",
        ),
        ("SELECT COALESCE($1, 1::int4, 2::int8)", "int8", "int8"),
        ("SELECT ARRAY[$1, 2::int8]", "int8", "int8[]"),
        ("VALUES ($1), (2::int8)", "int8", "int8"),
        ("SELECT $1 UNION ALL SELECT 2::int8", "int8", "int8"),
        ("SELECT array_append(ARRAY[1::int8], $1)", "int8", "int8[]"),
        ("SELECT COALESCE($1, NULL)", "text", "text"),
    ];

    for (sql, parameter_type, result_type) in cases {
        assert_eq!(
            infer_parameter_types(&db, sql).unwrap(),
            vec![Some(parameter_type.to_string())],
            "parameter inference mismatch for {sql}"
        );
        assert_eq!(
            infer_query_result_types(&db, sql).unwrap(),
            Some(vec![Some(result_type.to_string())]),
            "result type inference mismatch for {sql}"
        );
    }
}

#[test]
fn query_result_types_survive_empty_catalog_reflection_results() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();
    let types = infer_query_result_types(
        &db,
        "SELECT i.indrelid, c.relname, i.indisunique,
                con.conrelid IS NOT NULL AS has_constraint, i.indoption,
                c.reloptions, am.amname,
                CASE WHEN i.indpred IS NOT NULL
                     THEN pg_catalog.pg_get_expr(i.indpred, i.indrelid)
                END AS filter_definition,
                i.indnkeyatts, i.indnullsnotdistinct, cols.elements,
                cols.elements_is_expr, cols.elements_opclass,
                cols.elements_opdefault
         FROM pg_catalog.pg_index i
         JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
         JOIN pg_catalog.pg_am am ON c.relam = am.oid
         LEFT JOIN (
             SELECT expanded.indexrelid,
                    array_agg(CASE WHEN expanded.attnum = 0
                                   THEN pg_catalog.pg_get_indexdef(
                                       expanded.indexrelid, expanded.ord + 1, true)
                                   ELSE CAST(a.attname AS TEXT)
                              END ORDER BY expanded.ord) AS elements,
                    array_agg(expanded.attnum = 0 ORDER BY expanded.ord) AS elements_is_expr,
                    array_agg(opc.opcname ORDER BY expanded.ord) AS elements_opclass,
                    array_agg(opc.opcdefault ORDER BY expanded.ord) AS elements_opdefault
             FROM (
                 SELECT source.indexrelid,
                        unnest(source.indkey) AS attnum,
                        unnest(source.indclass) AS att_opclass,
                        generate_subscripts(source.indkey, 1) AS ord
                 FROM pg_catalog.pg_index source
             ) expanded
             LEFT JOIN pg_catalog.pg_attribute a
               ON a.attrelid = expanded.indexrelid AND a.attnum = expanded.attnum
             LEFT JOIN pg_catalog.pg_opclass opc ON opc.oid = expanded.att_opclass
             GROUP BY expanded.indexrelid
         ) cols ON cols.indexrelid = i.indexrelid
         LEFT JOIN pg_catalog.pg_constraint con
           ON i.indrelid = con.conrelid AND i.indexrelid = con.conindid
         WHERE i.indrelid = $1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        types,
        [
            "oid",
            "name",
            "bool",
            "bool",
            "int2vector",
            "text[]",
            "name",
            "text",
            "int2",
            "bool",
            "text[]",
            "bool[]",
            "name[]",
            "bool[]",
        ]
        .map(|pg_type| Some(pg_type.to_string()))
    );
}

#[test]
fn table_unnest_joins_preserve_result_and_delete_parameter_types() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE graph_edge_type_probe (
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relationship_name TEXT NOT NULL,
                source_ref_keys VARCHAR[] NOT NULL DEFAULT '{}',
                PRIMARY KEY (source_id, target_id, relationship_name)
            )",
        )
        .unwrap();

    let joined = "SELECT e.source_id, e.source_ref_keys
         FROM graph_edge_type_probe e
         JOIN unnest(CAST($1 AS text[]), CAST($2 AS text[]), CAST($3 AS text[]))
              AS q(s, t, r)
           ON e.source_id = q.s AND e.target_id = q.t AND e.relationship_name = q.r";
    assert_eq!(
        infer_query_result_types(&db, joined).unwrap().unwrap(),
        vec![Some("text".to_string()), Some("varchar[]".to_string())]
    );
    assert_eq!(
        infer_parameter_types(&db, joined).unwrap(),
        vec![
            Some("text[]".to_string()),
            Some("text[]".to_string()),
            Some("text[]".to_string()),
        ]
    );

    let delete = "DELETE FROM graph_edge_type_probe e
         USING unnest(CAST($1 AS text[]), CAST($2 AS text[]), CAST($3 AS text[]))
               AS q(s, t, r)
         WHERE e.source_id = q.s AND e.target_id = q.t AND e.relationship_name = q.r";
    assert_eq!(
        infer_parameter_types(&db, delete).unwrap(),
        vec![
            Some("text[]".to_string()),
            Some("text[]".to_string()),
            Some("text[]".to_string()),
        ]
    );
}

#[test]
fn prepared_array_types_follow_every_context_used_by_cognee() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE parameter_array_probe (
                id VARCHAR PRIMARY KEY,
                tags VARCHAR[],
                notes TEXT[]
            )",
        )
        .unwrap();

    let cases = [
        ("SELECT $1::varchar[]", vec!["varchar[]"]),
        ("SELECT $1::text[]", vec!["text[]"]),
        (
            "INSERT INTO parameter_array_probe (id, tags, notes) VALUES ($1, $2, $3)",
            vec!["varchar", "varchar[]", "text[]"],
        ),
        (
            "UPDATE parameter_array_probe SET tags = CAST($1 AS varchar[]), notes = $2 WHERE id = $3",
            vec!["varchar[]", "text[]", "varchar"],
        ),
        (
            "SELECT id FROM parameter_array_probe WHERE id = ANY($1)",
            vec!["varchar[]"],
        ),
        (
            "DELETE FROM parameter_array_probe WHERE id = ANY($1)",
            vec!["varchar[]"],
        ),
        (
            "SELECT $1 = ANY(tags) FROM parameter_array_probe",
            vec!["varchar"],
        ),
        (
            "SELECT $1 <> ALL(notes) FROM parameter_array_probe",
            vec!["text"],
        ),
        (
            "SELECT tags @> ARRAY[$1] FROM parameter_array_probe",
            vec!["varchar"],
        ),
        (
            "SELECT $1 <@ tags FROM parameter_array_probe",
            vec!["varchar[]"],
        ),
        (
            "SELECT array_append($1, 'tail'::varchar)",
            vec!["varchar[]"],
        ),
        (
            "SELECT array_prepend($1, tags) FROM parameter_array_probe",
            vec!["varchar"],
        ),
        (
            "SELECT array_cat($1, notes) FROM parameter_array_probe",
            vec!["text[]"],
        ),
        (
            "SELECT CASE WHEN true THEN $1 ELSE tags END FROM parameter_array_probe",
            vec!["varchar[]"],
        ),
        (
            "INSERT INTO parameter_array_probe (id, tags, notes) VALUES ($1, $2, $3)
             ON CONFLICT (id) DO UPDATE SET tags = $4, notes = CASE WHEN true THEN $5 ELSE excluded.notes END",
            vec!["varchar", "varchar[]", "text[]", "varchar[]", "text[]"],
        ),
        (
            "SELECT * FROM unnest(CAST($1 AS text[]), CAST($2 AS text[]), CAST($3 AS text[]))",
            vec!["text[]", "text[]", "text[]"],
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(
            infer_parameter_types(&db, sql).unwrap(),
            expected
                .into_iter()
                .map(|pg_type| Some(pg_type.to_string()))
                .collect::<Vec<_>>(),
            "parameter inference mismatch for {sql}"
        );
    }
}
