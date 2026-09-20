use bicdb_core::BicDb;
use bicdb_sql::{PgJsonText, SqlSession, SqlValue};
use serde_json::json;

#[test]
fn whole_derived_rows_convert_inside_ordered_json_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"CREATE TABLE items (
                   id bigint PRIMARY KEY,
                   name text NOT NULL,
                   payload jsonb,
                   sort_order bigint NOT NULL
               )"#,
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO items (id, name, payload, sort_order) VALUES
                   (1, 'first', '{"nested":true}'::jsonb, 20),
                   (2, 'second', NULL, 10)"#,
        )
        .unwrap();

    let result = session
        .execute(
            r#"SELECT
                   json_agg(row_to_json(rows) ORDER BY rows.sort_order) AS json_rows,
                   jsonb_agg(to_jsonb(rows) - 'sort_order' ORDER BY rows.sort_order DESC)
                       AS jsonb_rows
               FROM (
                   SELECT id, name, payload, sort_order
                   FROM items
                   ORDER BY sort_order
               ) rows"#,
        )
        .unwrap();

    assert_eq!(
        result.rows[0][0].to_cell(),
        r#"[{"id":2,"name":"second","payload":null,"sort_order":10}, {"id":1,"name":"first","payload":{"nested": true},"sort_order":20}]"#
    );
    assert_eq!(
        result.rows[0][1],
        SqlValue::Json(json!([
            {"id": 1, "name": "first", "payload": {"nested": true}},
            {"id": 2, "name": "second", "payload": null}
        ]))
    );
    assert_eq!(
        result.column_types,
        vec![Some("json".to_string()), Some("jsonb".to_string())]
    );
}

#[test]
fn whole_row_conversion_does_not_change_scalar_to_jsonb() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE items (id bigint PRIMARY KEY, name text NOT NULL)")
        .unwrap();
    session
        .execute("INSERT INTO items (id, name) VALUES (1, 'plain text')")
        .unwrap();

    let result = session
        .execute(
            r#"SELECT row_to_json(item_row), to_jsonb(item_row), to_jsonb(item_row.name)
               FROM (SELECT id, name FROM items) item_row"#,
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::JsonText(
                PgJsonText::parse(r#"{"id":1,"name":"plain text"}"#.to_string()).unwrap()
            ),
            SqlValue::Json(json!({"id": 1, "name": "plain text"})),
            SqlValue::Json(json!("plain text")),
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

    let base_row = session
        .execute(
            "SELECT row_to_json(item), row_to_json(item, true),
                    row_to_json(item, NULL), to_json(item), to_jsonb(item.name)
             FROM items item",
        )
        .unwrap();
    assert_eq!(
        base_row.rows[0][0].to_cell(),
        r#"{"id":1,"name":"plain text"}"#
    );
    assert_eq!(
        base_row.rows[0][1].to_cell(),
        "{\"id\":1,\n \"name\":\"plain text\"}"
    );
    assert!(matches!(base_row.rows[0][2], SqlValue::Null));
    assert_eq!(base_row.rows[0][3].to_cell(), base_row.rows[0][0].to_cell());
    assert_eq!(base_row.rows[0][4], SqlValue::Json(json!("plain text")));
    assert_eq!(
        base_row.column_types,
        vec![
            Some("json".to_string()),
            Some("json".to_string()),
            Some("json".to_string()),
            Some("json".to_string()),
            Some("jsonb".to_string()),
        ]
    );
}
