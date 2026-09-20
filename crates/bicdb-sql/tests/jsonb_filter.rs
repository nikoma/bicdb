use bicdb_core::{BicDb, IndexKind};
use bicdb_sql::{PgJsonText, SqlSession, SqlValue};
use serde_json::json;

fn filter_session() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                r#"CREATE TABLE filter_items (
                       id bigint PRIMARY KEY,
                       value text NOT NULL,
                       ord bigint NOT NULL,
                       active boolean
                   )"#,
            )
            .unwrap();
        session
            .execute(
                r#"INSERT INTO filter_items (id, value, ord, active) VALUES
                       (1, 'a', 1, true),
                       (2, 'b', 2, false),
                       (3, 'c', 3, NULL),
                       (4, 'd', 4, true)"#,
            )
            .unwrap();
    }
    (dir, db)
}

fn json_text(value: &str) -> SqlValue {
    SqlValue::JsonText(PgJsonText::parse(value.to_string()).unwrap())
}

#[test]
fn json_aggregate_filter_works_in_record_execution() {
    let (_dir, mut db) = filter_session();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            r#"SELECT
                   json_agg(value ORDER BY ord DESC) FILTER (WHERE active),
                   jsonb_agg(value ORDER BY ord) FILTER (WHERE active IS NULL),
                   jsonb_agg(value) FILTER (WHERE false)
               FROM filter_items"#,
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            json_text(r#"["d", "a"]"#),
            SqlValue::Json(json!(["c"])),
            SqlValue::Null,
        ]]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("json".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
        ]
    );
}

#[test]
fn json_aggregate_filter_works_in_bound_and_unbound_row_execution() {
    let (_dir, mut db) = filter_session();
    let mut session = SqlSession::new(&mut db);

    let bound = session
        .execute(
            r#"SELECT
                   jsonb_agg(rows.value ORDER BY rows.ord) FILTER (WHERE rows.active),
                   json_agg(rows.value) FILTER (WHERE rows.active IS NULL),
                   jsonb_agg(CAST('not-an-integer' AS bigint)) FILTER (WHERE false)
               FROM (SELECT value, ord, active FROM filter_items) rows"#,
        )
        .unwrap();
    assert_eq!(
        bound.rows,
        vec![vec![
            SqlValue::Json(json!(["a", "d"])),
            json_text(r#"["c"]"#),
            SqlValue::Null,
        ]]
    );

    let unbound = session
        .execute(
            r#"SELECT COALESCE(
                   jsonb_agg(rows.value ORDER BY rows.ord DESC)
                       FILTER (WHERE rows.active),
                   '[]'::jsonb
               )
               FROM (SELECT value, ord, active FROM filter_items) rows"#,
        )
        .unwrap();
    assert_eq!(unbound.rows, vec![vec![SqlValue::Json(json!(["d", "a"]))]]);

    let empty = session
        .execute(
            r#"SELECT COALESCE(
                   jsonb_agg(rows.value) FILTER (WHERE rows.active IS NULL AND false),
                   '[]'::jsonb
               )
               FROM (SELECT value, active FROM filter_items) rows"#,
        )
        .unwrap();
    assert_eq!(empty.rows, vec![vec![SqlValue::Json(json!([]))]]);
}

#[test]
fn jsonb_existence_operators_obey_predicate_truth_and_null_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE jsonb_predicate_items (
                id bigint PRIMARY KEY,
                payload jsonb
            )",
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO jsonb_predicate_items (id, payload) VALUES
                (1, '{"roles":[],"tag":true}'::jsonb),
                (2, '{"other":1}'::jsonb),
                (3, NULL)"#,
        )
        .unwrap();

    let has_key = session
        .execute(
            "SELECT id FROM jsonb_predicate_items
             WHERE payload ? 'roles' ORDER BY id",
        )
        .unwrap();
    assert_eq!(has_key.rows, vec![vec![SqlValue::Int(1)]]);

    let lacks_key = session
        .execute(
            "SELECT id FROM jsonb_predicate_items
             WHERE NOT (payload ? 'roles') ORDER BY id",
        )
        .unwrap();
    assert_eq!(lacks_key.rows, vec![vec![SqlValue::Int(2)]]);

    let has_any = session
        .execute(
            "SELECT id FROM jsonb_predicate_items
             WHERE payload ?| ARRAY['roles', 'other'] ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        has_any.rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );

    let has_all = session
        .execute(
            "SELECT id FROM jsonb_predicate_items
             WHERE payload ?& ARRAY['roles', 'tag'] ORDER BY id",
        )
        .unwrap();
    assert_eq!(has_all.rows, vec![vec![SqlValue::Int(1)]]);

    for predicate in [
        "payload ? NULL",
        "NOT (payload ? NULL)",
        "payload ?| NULL::text[]",
        "payload ?& NULL::text[]",
    ] {
        let result = session
            .execute(&format!(
                "SELECT id FROM jsonb_predicate_items WHERE {predicate}"
            ))
            .unwrap();
        assert!(
            result.rows.is_empty(),
            "predicate should be UNKNOWN: {predicate}"
        );
    }
}

#[test]
fn carrier_jsonb_role_filter_accepts_missing_empty_and_visible_roles() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE carrier_jsonb_source (
                id bigint PRIMARY KEY,
                payload jsonb NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO carrier_jsonb_source (id, payload) VALUES (1, '[
                {"id":"public"},
                {"id":"member","required_role":"member","roles":["member"]},
                {"id":"owner-required","required_role":"owner"},
                {"id":"owner-visible","roles":["owner"]},
                {"id":"empty-roles","roles":[]},
                {"id":"scalar-roles","roles":"member"}
            ]'::jsonb)"#,
        )
        .unwrap();

    let result = session
        .execute(
            r#"WITH role_ranks(role, role_rank) AS (
                VALUES
                    ('guest', 10),
                    ('member', 20),
                    ('moderator', 30),
                    ('admin', 40),
                    ('owner', 50)
            ),
            actor_context AS (
                SELECT
                    'admin'::text AS role,
                    COALESCE((
                        SELECT role_rank FROM role_ranks WHERE role = 'admin'::text
                    ), 0) AS role_rank
            )
            SELECT COALESCE((
                SELECT jsonb_agg(entry.item ORDER BY entry.ordinality)
                FROM jsonb_array_elements(manifest.payload)
                    WITH ORDINALITY AS entry(item, ordinality)
                WHERE (
                    NULLIF(entry.item->>'required_role', '') IS NULL
                    OR EXISTS (
                        SELECT 1 FROM role_ranks required_role
                        WHERE required_role.role = NULLIF(entry.item->>'required_role', '')
                          AND required_role.role_rank <= actor_context.role_rank
                    )
                )
                AND (
                    NOT (entry.item ? 'roles')
                    OR jsonb_typeof(entry.item->'roles') != 'array'
                    OR jsonb_array_length(entry.item->'roles') = 0
                    OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(entry.item->'roles')
                            AS visible_role(role)
                        JOIN role_ranks visible_role_rank
                          ON visible_role_rank.role = visible_role.role
                        WHERE visible_role_rank.role_rank <= actor_context.role_rank
                    )
                )
            ), '[]'::jsonb)
            FROM carrier_jsonb_source manifest
            CROSS JOIN actor_context"#,
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Json(json!([
            {"id": "public"},
            {"id": "member", "required_role": "member", "roles": ["member"]},
            {"id": "empty-roles", "roles": []},
            {"id": "scalar-roles", "roles": "member"}
        ]))]]
    );
}

#[test]
fn jsonb_gin_index_accelerates_rechecked_predicates_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                r#"CREATE TABLE gin_documents (id bigint PRIMARY KEY, payload jsonb NOT NULL);
                   INSERT INTO gin_documents VALUES
                     (1, '{"kind":"patient","active":true,"tags":["urgent","west"]}'),
                     (2, '{"kind":"patient","active":false,"tags":["east"]}'),
                     (3, '{"kind":"invoice","nested":{"kind":"patient"}}'),
                     (4, '{"other":1}');
                   CREATE INDEX idx_gin_documents_payload
                     ON gin_documents USING GIN (payload);"#,
            )
            .unwrap();

        drop(session);
        assert!(db.index_definitions().iter().any(|index| {
            index.name == "idx_gin_documents_payload" && index.kind == IndexKind::Jsonb
        }));
        let mut session = SqlSession::new(&mut db);
        let explain = session
            .execute(
                r#"EXPLAIN SELECT id FROM gin_documents
                   WHERE payload @> '{"kind":"patient","active":true}'::jsonb"#,
            )
            .unwrap();
        assert!(explain.rows.iter().any(|row| row[0]
            .to_cell()
            .contains("JsonbIndexScan idx_gin_documents_payload")));

        assert_eq!(
            session
                .execute(
                    r#"SELECT id FROM gin_documents
                       WHERE payload @> '{"kind":"patient"}'::jsonb ORDER BY id"#,
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
        );
        assert_eq!(
            session
                .execute("SELECT id FROM gin_documents WHERE payload ? 'kind' ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::Int(1)],
                vec![SqlValue::Int(2)],
                vec![SqlValue::Int(3)],
            ]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM gin_documents WHERE payload ?| ARRAY['missing','other'] ORDER BY id",
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(4)]]
        );

        session
            .execute(r#"UPDATE gin_documents SET payload = '{"kind":"patient"}' WHERE id = 4"#)
            .unwrap();
        session
            .execute("DELETE FROM gin_documents WHERE id = 2")
            .unwrap();
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                r#"SELECT id FROM gin_documents
                   WHERE payload @> '{"kind":"patient"}'::jsonb ORDER BY id"#,
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(4)]]
    );
}
