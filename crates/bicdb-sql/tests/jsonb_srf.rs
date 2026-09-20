use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn jsonb_array_element_functions_preserve_postgres_null_and_text_semantics() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let json_values = session
        .execute(
            r#"SELECT value
               FROM jsonb_array_elements('[1,null,{"a":2}]'::jsonb) AS value"#,
        )
        .unwrap();
    assert_eq!(
        json_values.rows,
        vec![
            vec![SqlValue::Json(json!(1))],
            vec![SqlValue::Json(json!(null))],
            vec![SqlValue::Json(json!({"a": 2}))],
        ]
    );
    assert_eq!(json_values.column_types, vec![Some("jsonb".to_string())]);

    let default_column_names = session
        .execute(
            r#"SELECT value, expanded.value
               FROM jsonb_array_elements('[1]'::jsonb),
                    jsonb_array_elements_text('["x"]'::jsonb) AS expanded"#,
        )
        .unwrap();
    assert_eq!(
        default_column_names.rows,
        vec![vec![
            SqlValue::Json(json!(1)),
            SqlValue::String("x".to_string()),
        ]]
    );
    assert_eq!(
        default_column_names.columns,
        vec!["value".to_string(), "value".to_string()]
    );

    let text_values = session
        .execute(
            r#"SELECT value, ord
               FROM jsonb_array_elements_text(
                   '[null,"x",1,true,{"aa":1,"b":2},[2]]'::jsonb
               ) WITH ORDINALITY AS expanded(value, ord)"#,
        )
        .unwrap();
    assert_eq!(
        text_values.rows,
        vec![
            vec![SqlValue::Null, SqlValue::Int(1)],
            vec![SqlValue::String("x".to_string()), SqlValue::Int(2)],
            vec![SqlValue::String("1".to_string()), SqlValue::Int(3)],
            vec![SqlValue::String("true".to_string()), SqlValue::Int(4)],
            vec![
                SqlValue::String(r#"{"b": 2, "aa": 1}"#.to_string()),
                SqlValue::Int(5),
            ],
            vec![SqlValue::String("[2]".to_string()), SqlValue::Int(6)],
        ]
    );
    assert_eq!(
        text_values.column_types,
        vec![Some("text".to_string()), Some("int8".to_string())]
    );
}

#[test]
fn jsonb_text_elements_support_cognee_scalar_alias_filters_and_aggregation() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let scalar_alias = session
        .execute(
            r#"SELECT val, val.value
               FROM jsonb_array_elements_text('["a", "b"]'::jsonb) AS val"#,
        )
        .unwrap();
    assert_eq!(
        scalar_alias.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("a".to_string()),
            ],
            vec![
                SqlValue::String("b".to_string()),
                SqlValue::String("b".to_string()),
            ],
        ]
    );

    let filtered = session
        .execute(
            r#"SELECT val
               FROM jsonb_array_elements_text('["a", "b"]'::jsonb) AS val
               WHERE val <> ALL(ARRAY['a']::text[])"#,
        )
        .unwrap();
    assert_eq!(filtered.rows, vec![vec![SqlValue::String("b".to_string())]]);

    let bounded_array = session
        .execute("SELECT 'b' = ANY('[1:2]={a,b}'::text[])")
        .unwrap();
    assert_eq!(bounded_array.rows, vec![vec![SqlValue::Bool(true)]]);
    let null_array = session.execute("SELECT 'b' = ANY(NULL::text[])").unwrap();
    assert_eq!(null_array.rows, vec![vec![SqlValue::Null]]);

    let aggregated = session
        .execute(
            r#"SELECT COALESCE(jsonb_agg(val), '[]'::jsonb)
               FROM jsonb_array_elements_text('["a", "b"]'::jsonb) AS val
               WHERE val <> ALL(ARRAY['a']::text[])"#,
        )
        .unwrap();
    assert_eq!(aggregated.rows, vec![vec![SqlValue::Json(json!(["b"]))]]);
}

#[test]
fn cognee_jsonb_tag_selection_and_removal_flow_preserves_edge_cases() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE TABLE "Entity_name" (
                   id text PRIMARY KEY,
                   payload jsonb NOT NULL
               );
               INSERT INTO "Entity_name" VALUES
                   ('both', '{"belongs_to_set":["a","b"]}'::jsonb),
                   ('all', '{"belongs_to_set":["a"]}'::jsonb),
                   ('none', '{"belongs_to_set":["c"]}'::jsonb),
                   ('empty', '{"belongs_to_set":[]}'::jsonb),
                   ('missing', '{}'::jsonb);"#,
        )
        .unwrap();

    let selected = session
        .execute(
            r#"SELECT id
               FROM "Entity_name"
               WHERE payload::jsonb ? 'belongs_to_set'
                 AND EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements_text(
                     payload::jsonb -> 'belongs_to_set'
                   ) v
                   WHERE v = ANY(ARRAY['b']::text[])
                 )
               ORDER BY id"#,
        )
        .unwrap();
    assert_eq!(
        selected.rows,
        vec![vec![SqlValue::String("both".to_string())]]
    );

    session
        .execute(
            r#"UPDATE "Entity_name"
               SET payload = jsonb_set(
                 payload,
                 '{belongs_to_set}',
                 (
                   SELECT COALESCE(jsonb_agg(val), '[]'::jsonb)
                   FROM jsonb_array_elements_text(
                     payload::jsonb -> 'belongs_to_set'
                   ) AS val
                   WHERE val <> ALL(ARRAY['a']::text[])
                 )
               )
               WHERE payload::jsonb ? 'belongs_to_set'"#,
        )
        .unwrap();

    let updated = session
        .execute(
            r#"SELECT id, payload->'belongs_to_set'
               FROM "Entity_name"
               ORDER BY id"#,
        )
        .unwrap();
    assert_eq!(
        updated.rows,
        vec![
            vec![
                SqlValue::String("all".to_string()),
                SqlValue::Json(json!([])),
            ],
            vec![
                SqlValue::String("both".to_string()),
                SqlValue::Json(json!(["b"])),
            ],
            vec![
                SqlValue::String("empty".to_string()),
                SqlValue::Json(json!([])),
            ],
            vec![SqlValue::String("missing".to_string()), SqlValue::Null,],
            vec![
                SqlValue::String("none".to_string()),
                SqlValue::Json(json!(["c"])),
            ],
        ]
    );

    session
        .execute(
            r#"DELETE FROM "Entity_name"
               WHERE EXISTS (
                 SELECT 1
                 FROM jsonb_array_elements_text(
                   payload::jsonb -> 'belongs_to_set'
                 ) AS v
                 WHERE v = ANY(ARRAY['b']::text[])
               )"#,
        )
        .unwrap();
    let remaining = session
        .execute(r#"SELECT id FROM "Entity_name" ORDER BY id"#)
        .unwrap();
    assert_eq!(
        remaining.rows,
        vec![
            vec![SqlValue::String("all".to_string())],
            vec![SqlValue::String("empty".to_string())],
            vec![SqlValue::String("missing".to_string())],
            vec![SqlValue::String("none".to_string())],
        ]
    );
}

#[test]
fn jsonb_array_element_functions_expand_no_from_projections() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let json_values = session
        .execute(
            r#"SELECT 'constant' AS source,
                      jsonb_array_elements('[1,null,{"a":2}]'::jsonb) AS element"#,
        )
        .unwrap();
    assert_eq!(
        json_values.rows,
        vec![
            vec![
                SqlValue::String("constant".to_string()),
                SqlValue::Json(json!(1)),
            ],
            vec![
                SqlValue::String("constant".to_string()),
                SqlValue::Json(json!(null)),
            ],
            vec![
                SqlValue::String("constant".to_string()),
                SqlValue::Json(json!({"a": 2})),
            ],
        ]
    );
    assert_eq!(json_values.column_types[1], Some("jsonb".to_string()));

    let text_values = session
        .execute(r#"SELECT jsonb_array_elements_text('[null,"x",1]'::jsonb) AS element"#)
        .unwrap();
    assert_eq!(
        text_values.rows,
        vec![
            vec![SqlValue::Null],
            vec![SqlValue::String("x".to_string())],
            vec![SqlValue::String("1".to_string())],
        ]
    );
    assert_eq!(text_values.column_types, vec![Some("text".to_string())]);

    let sql_null = session
        .execute("SELECT jsonb_array_elements(NULL::jsonb)")
        .unwrap();
    assert!(sql_null.rows.is_empty());
    assert_eq!(sql_null.column_types, vec![Some("jsonb".to_string())]);
}

#[test]
fn jsonb_object_keys_matches_postgres_order_in_from_and_projection_forms() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let from_result = session
        .execute(
            r#"SELECT key
               FROM jsonb_object_keys('{"aa":1,"b":2,"ab":3,"a":4}'::jsonb) AS key"#,
        )
        .unwrap();
    let projection_result = session
        .execute(r#"SELECT jsonb_object_keys('{"aa":1,"b":2,"ab":3,"a":4}'::jsonb) AS key"#)
        .unwrap();
    let expected = vec![
        vec![SqlValue::String("a".to_string())],
        vec![SqlValue::String("b".to_string())],
        vec![SqlValue::String("aa".to_string())],
        vec![SqlValue::String("ab".to_string())],
    ];
    assert_eq!(from_result.rows, expected);
    assert_eq!(projection_result.rows, expected);
    assert_eq!(from_result.column_types, vec![Some("text".to_string())]);
    assert_eq!(
        projection_result.column_types,
        vec![Some("text".to_string())]
    );

    let carrier_union = session
        .execute(
            r#"WITH requested_fields AS (
                   SELECT jsonb_object_keys(
                       COALESCE('{"title":1}'::jsonb, '{}'::jsonb)
                   ) AS name
                   UNION
                   SELECT value #>> '{}' AS name
                   FROM jsonb_array_elements('["status"]'::jsonb) AS value
               )
               SELECT name FROM requested_fields ORDER BY name"#,
        )
        .unwrap();
    assert_eq!(
        carrier_union.rows,
        vec![
            vec![SqlValue::String("status".to_string())],
            vec![SqlValue::String("title".to_string())],
        ]
    );
}

#[test]
fn jsonb_each_and_typed_record_functions_match_postgres() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let each = session
        .execute(r#"SELECT key, value FROM jsonb_each('{"aa":1,"b":null}'::jsonb)"#)
        .unwrap();
    assert_eq!(
        each.rows,
        vec![
            vec![
                SqlValue::String("b".to_string()),
                SqlValue::Json(json!(null))
            ],
            vec![SqlValue::String("aa".to_string()), SqlValue::Json(json!(1))],
        ]
    );
    assert_eq!(
        each.column_types,
        vec![Some("text".to_string()), Some("jsonb".to_string())]
    );

    let each_text = session
        .execute(r#"SELECT key, value FROM jsonb_each_text('{"a":null,"b":2}'::jsonb)"#)
        .unwrap();
    assert_eq!(
        each_text.rows,
        vec![
            vec![SqlValue::String("a".to_string()), SqlValue::Null],
            vec![
                SqlValue::String("b".to_string()),
                SqlValue::String("2".to_string()),
            ],
        ]
    );

    let record = session
        .execute(
            r#"SELECT a, b FROM jsonb_to_record('{"a":1,"b":"x"}'::jsonb)
               AS x(a int, b text)"#,
        )
        .unwrap();
    assert_eq!(
        record.rows,
        vec![vec![SqlValue::Int(1), SqlValue::String("x".to_string())]]
    );

    let recordset = session
        .execute(
            r#"SELECT id, label FROM jsonb_to_recordset(
                   '[{"id":2},{"id":1,"label":"first"}]'::jsonb
               ) AS x(id int, label text) ORDER BY id"#,
        )
        .unwrap();
    assert_eq!(
        recordset.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::String("first".to_string())],
            vec![SqlValue::Int(2), SqlValue::Null],
        ]
    );
}

#[test]
fn jsonb_table_functions_are_implicitly_lateral_and_support_explicit_join_kinds() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE TABLE json_srf_docs (
                   id text PRIMARY KEY,
                   values_json jsonb NOT NULL,
                   object_json jsonb NOT NULL
               );
               INSERT INTO json_srf_docs (id, values_json, object_json) VALUES
                   ('a', '["x","y"]'::jsonb, '{"aa":1,"b":2}'::jsonb),
                   ('b', '[]'::jsonb, '{}'::jsonb),
                   ('c', '["z"]'::jsonb, '{"z":1}'::jsonb);"#,
        )
        .unwrap();

    let implicit = session
        .execute(
            r#"SELECT doc.id, value
               FROM json_srf_docs AS doc,
                    jsonb_array_elements_text(doc.values_json) AS value
               ORDER BY doc.id, value"#,
        )
        .unwrap();
    assert_eq!(
        implicit.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("x".to_string()),
            ],
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("y".to_string()),
            ],
            vec![
                SqlValue::String("c".to_string()),
                SqlValue::String("z".to_string()),
            ],
        ]
    );

    let projected_json = session
        .execute(
            r#"SELECT doc.id, jsonb_array_elements(doc.values_json) AS value
               FROM json_srf_docs AS doc
               ORDER BY doc.id"#,
        )
        .unwrap();
    assert_eq!(
        projected_json.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::Json(json!("x")),
            ],
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::Json(json!("y")),
            ],
            vec![
                SqlValue::String("c".to_string()),
                SqlValue::Json(json!("z")),
            ],
        ]
    );
    assert_eq!(
        projected_json.column_types,
        vec![Some("text".to_string()), Some("jsonb".to_string())]
    );

    let projected_text = session
        .execute(
            r#"SELECT doc.id, jsonb_array_elements_text(doc.values_json) AS value
               FROM json_srf_docs AS doc
               ORDER BY doc.id"#,
        )
        .unwrap();
    assert_eq!(
        projected_text.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("x".to_string()),
            ],
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("y".to_string()),
            ],
            vec![
                SqlValue::String("c".to_string()),
                SqlValue::String("z".to_string()),
            ],
        ]
    );
    assert_eq!(
        projected_text.column_types,
        vec![Some("text".to_string()), Some("text".to_string())]
    );

    let projected_keys = session
        .execute(
            r#"SELECT doc.id, jsonb_object_keys(doc.object_json) AS key
               FROM json_srf_docs AS doc
               ORDER BY doc.id"#,
        )
        .unwrap();
    assert_eq!(
        projected_keys.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("b".to_string()),
            ],
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("aa".to_string()),
            ],
            vec![
                SqlValue::String("c".to_string()),
                SqlValue::String("z".to_string()),
            ],
        ]
    );
    assert_eq!(
        projected_keys.column_types,
        vec![Some("text".to_string()), Some("text".to_string())]
    );

    let limited = session
        .execute(
            r#"SELECT doc.id, jsonb_array_elements_text(doc.values_json) AS value
               FROM json_srf_docs AS doc
               ORDER BY doc.id
               LIMIT 1"#,
        )
        .unwrap();
    assert_eq!(
        limited.rows,
        vec![vec![
            SqlValue::String("a".to_string()),
            SqlValue::String("x".to_string()),
        ]]
    );

    let cross = session
        .execute(
            r#"SELECT doc.id, expanded.value, expanded.ord
               FROM json_srf_docs AS doc
               CROSS JOIN LATERAL jsonb_array_elements_text(doc.values_json)
                   WITH ORDINALITY AS expanded(value, ord)
               ORDER BY doc.id, expanded.ord"#,
        )
        .unwrap();
    assert_eq!(
        cross.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("x".to_string()),
                SqlValue::Int(1),
            ],
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("y".to_string()),
                SqlValue::Int(2),
            ],
            vec![
                SqlValue::String("c".to_string()),
                SqlValue::String("z".to_string()),
                SqlValue::Int(1),
            ],
        ]
    );

    let inner = session
        .execute(
            r#"SELECT doc.id, expanded.value
               FROM json_srf_docs AS doc
               INNER JOIN LATERAL jsonb_array_elements_text(doc.values_json)
                   AS expanded(value)
                   ON expanded.value <> 'x'
               ORDER BY doc.id, expanded.value"#,
        )
        .unwrap();
    assert_eq!(
        inner.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("y".to_string()),
            ],
            vec![
                SqlValue::String("c".to_string()),
                SqlValue::String("z".to_string()),
            ],
        ]
    );

    let left = session
        .execute(
            r#"SELECT doc.id, expanded.value, expanded.ord
               FROM json_srf_docs AS doc
               LEFT JOIN LATERAL jsonb_array_elements_text(doc.values_json)
                   WITH ORDINALITY AS expanded(value, ord)
                   ON expanded.value <> 'x'
               ORDER BY doc.id, expanded.ord"#,
        )
        .unwrap();
    assert_eq!(
        left.rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("y".to_string()),
                SqlValue::Int(2),
            ],
            vec![
                SqlValue::String("b".to_string()),
                SqlValue::Null,
                SqlValue::Null
            ],
            vec![
                SqlValue::String("c".to_string()),
                SqlValue::String("z".to_string()),
                SqlValue::Int(1),
            ],
        ]
    );
    assert_eq!(
        left.column_types,
        vec![
            Some("text".to_string()),
            Some("text".to_string()),
            Some("int8".to_string()),
        ]
    );
}

#[test]
fn jsonb_set_functions_are_strict_and_reject_the_wrong_json_shape() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let null_result = session
        .execute("SELECT * FROM jsonb_array_elements(NULL::jsonb)")
        .unwrap();
    assert!(null_result.rows.is_empty());

    let error = session
        .execute("SELECT * FROM jsonb_array_elements('{}'::jsonb)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");

    let error = session
        .execute("SELECT * FROM jsonb_object_keys('[]'::jsonb)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");
}

#[test]
fn carrier_element_gallery_lateral_derived_query_matches_declared_and_derived_elements() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE app_manifests (
                id text PRIMARY KEY,
                org_id text NOT NULL,
                hub_id text NOT NULL,
                app_name text NOT NULL,
                display_name text NOT NULL,
                icon_url text,
                dashboard_providers jsonb NOT NULL,
                is_enabled boolean NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO app_manifests VALUES
                ('alpha', 'org-1', 'hub-1', 'alpha', 'Alpha', NULL,
                 '{"hub_elements":{"elements":[{"id":"declared","kind":"custom"}]}}'::jsonb,
                 true),
                ('beta', 'org-1', 'hub-1', 'beta', 'Beta', '/beta.svg',
                 '{"hub_elements":{
                    "pages":[
                      {"id":"p1","kind":"overview_page","title":"Home","route":"/home","resource":"dashboard"},
                      {"id":"ignored","kind":"other"}
                    ],
                    "forms":[{"id":"f1","title":"Intake","endpoint":"/forms","resource":"patients"}],
                    "custom_surfaces":[{"id":"s1","title":"Board","route":"/board","renderer":"grid","resource":"work"}]
                 }}'::jsonb,
                 true)"#,
        )
        .unwrap();

    // Exact query shape generated by Carrier's get_hub_element_gallery action.
    let result = session
        .execute(
            r#"SELECT
              app.app_name,
              app.display_name,
              app.icon_url,
              COALESCE(catalog.elements, '[]'::jsonb) AS elements
              FROM app_manifests app
              LEFT JOIN LATERAL (
              WITH hub_elements AS (
              SELECT COALESCE(app.dashboard_providers->'hub_elements', '{}'::jsonb) AS value
              ),
              declared AS (
              SELECT value->'elements' AS elements
              FROM hub_elements
              WHERE jsonb_typeof(value->'elements') = 'array'
              ),
              derived AS (
              SELECT jsonb_agg(entry.item ORDER BY entry.ordinality) AS elements
              FROM (
              SELECT
              jsonb_build_object(
              'id', app.app_name || ':page:' || COALESCE(page.item->>'id', page.item->>'route', page.ordinality::text),
              'element_id', COALESCE(page.item->>'id', page.item->>'route', page.ordinality::text),
              'kind', 'page',
              'page_kind', COALESCE(page.item->>'kind', 'page'),
              'title', COALESCE(page.item->>'title', page.item->>'label', page.item->>'id', 'Page'),
              'category', 'Pages',
              'tags', jsonb_build_array(COALESCE(page.item->>'kind', 'page'), COALESCE(page.item->>'resource', '')),
              'source', jsonb_build_object('type', 'hub_elements_page', 'route', page.item->>'route', 'resource', page.item->>'resource')
              ) AS item,
              page.ordinality
              FROM hub_elements
              CROSS JOIN LATERAL jsonb_array_elements(COALESCE(value->'pages', '[]'::jsonb)) WITH ORDINALITY AS page(item, ordinality)
              WHERE COALESCE(page.item->>'kind', '') IN ('overview_page', 'worklist', 'approval_inbox')
              UNION ALL
              SELECT
              jsonb_build_object(
              'id', app.app_name || ':form:' || COALESCE(form.item->>'id', form.ordinality::text),
              'element_id', COALESCE(form.item->>'id', form.ordinality::text),
              'kind', 'form',
              'title', COALESCE(form.item->>'title', form.item->>'label', form.item->>'id', 'Form'),
              'category', 'Forms',
              'tags', jsonb_build_array(COALESCE(form.item->>'kind', 'form'), COALESCE(form.item->>'resource', '')),
              'source', jsonb_build_object('type', 'hub_elements_form', 'endpoint', form.item->>'endpoint', 'resource', form.item->>'resource')
              ) AS item,
              10000 + form.ordinality
              FROM hub_elements
              CROSS JOIN LATERAL jsonb_array_elements(COALESCE(value->'forms', '[]'::jsonb)) WITH ORDINALITY AS form(item, ordinality)
              UNION ALL
              SELECT
              jsonb_build_object(
              'id', app.app_name || ':surface:' || COALESCE(surface.item->>'id', surface.item->>'route', surface.ordinality::text),
              'element_id', COALESCE(surface.item->>'id', surface.item->>'route', surface.ordinality::text),
              'kind', 'surface',
              'title', COALESCE(surface.item->>'title', surface.item->>'label', surface.item->>'id', 'Surface'),
              'category', 'Surfaces',
              'tags', jsonb_build_array(COALESCE(surface.item->>'kind', 'surface'), COALESCE(surface.item->>'resource', '')),
              'source', jsonb_build_object('type', 'custom_surface', 'route', surface.item->>'route', 'renderer', surface.item->>'renderer', 'resource', surface.item->>'resource')
              ) AS item,
              20000 + surface.ordinality
              FROM hub_elements
              CROSS JOIN LATERAL jsonb_array_elements(COALESCE(value->'custom_surfaces', '[]'::jsonb)) WITH ORDINALITY AS surface(item, ordinality)
              ) entry
              )
              SELECT COALESCE((SELECT elements FROM declared), (SELECT elements FROM derived)) AS elements
              ) catalog ON true
              WHERE app.org_id = 'org-1' AND app.hub_id = 'hub-1' AND app.is_enabled = true
              ORDER BY app.app_name ASC
              LIMIT 200"#,
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.column_types,
        vec![
            Some("text".to_string()),
            Some("text".to_string()),
            Some("text".to_string()),
            Some("jsonb".to_string()),
        ]
    );
    assert_eq!(result.rows[0][0], SqlValue::String("alpha".to_string()));
    assert_eq!(
        result.rows[0][3],
        SqlValue::Json(json!([{"id": "declared", "kind": "custom"}]))
    );
    assert_eq!(result.rows[1][0], SqlValue::String("beta".to_string()));
    let SqlValue::Json(elements) = &result.rows[1][3] else {
        panic!(
            "expected derived JSONB elements, got {:?}",
            result.rows[1][3]
        );
    };
    let elements = elements.as_array().expect("derived elements array");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0]["id"], json!("beta:page:p1"));
    assert_eq!(elements[1]["id"], json!("beta:form:f1"));
    assert_eq!(elements[2]["id"], json!("beta:surface:s1"));
}

#[test]
fn lateral_derived_jsonb_metadata_survives_an_empty_left_input() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE lateral_empty_docs (
                id bigint PRIMARY KEY,
                payload jsonb
            )",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT catalog.*
             FROM lateral_empty_docs doc
             LEFT JOIN LATERAL (
                 WITH extracted AS (
                     SELECT doc.payload->'items' AS elements
                 )
                 SELECT COALESCE(
                     (SELECT elements FROM extracted),
                     '[]'::jsonb
                 ) AS elements
             ) catalog ON true",
        )
        .unwrap();

    assert!(result.rows.is_empty());
    assert_eq!(result.columns, ["elements"]);
    assert_eq!(result.column_types, [Some("jsonb".to_string())]);
}
