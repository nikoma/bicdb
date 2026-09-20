use bicdb_core::{BicDb, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

#[test]
fn scalar_json_keys_keep_their_json_type_scale_and_precision() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE scalar_keys (id jsonb PRIMARY KEY, label text)")
        .unwrap();
    for (index, text) in ["1.00", "9007199254740993.125", "true", "null", "\"text\""]
        .iter()
        .enumerate()
    {
        sql.execute(&format!(
            "INSERT INTO scalar_keys VALUES ('{text}'::jsonb, 'v{index}')"
        ))
        .unwrap();
        let expected = sql.execute(&format!("SELECT '{text}'::jsonb")).unwrap();
        let actual = sql
            .execute(&format!(
                "SELECT id FROM scalar_keys WHERE id = '{text}'::jsonb"
            ))
            .unwrap();
        assert_eq!(actual.rows, expected.rows, "JSON scalar {text}");
        assert_eq!(actual.rows[0][0].to_cell(), expected.rows[0][0].to_cell());
    }
}

#[test]
fn jsonb_single_primary_key_uses_postgres_numeric_identity() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE jsonb_single_pk (
                    id jsonb PRIMARY KEY,
                    label text NOT NULL
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO jsonb_single_pk (id, label) VALUES
                    ('1.0'::jsonb, 'one'),
                    ('9007199254740992'::jsonb, 'large-a'),
                    ('9007199254740993'::jsonb, 'large-b')",
            )
            .unwrap();

        let equivalent = session
            .execute("SELECT id, label FROM jsonb_single_pk WHERE id = '1'::jsonb")
            .unwrap();
        assert_eq!(equivalent.rows.len(), 1);
        assert_eq!(equivalent.rows[0][0].to_cell(), "1.0");
        assert_eq!(equivalent.rows[0][1], SqlValue::String("one".to_string()));

        let duplicate = session
            .execute("INSERT INTO jsonb_single_pk (id, label) VALUES ('1'::jsonb, 'duplicate')")
            .unwrap_err();
        assert_eq!(duplicate.sqlstate(), "23505");

        let distinct_large = session
            .execute(
                "SELECT label FROM jsonb_single_pk
                 WHERE id = '9007199254740993'::jsonb",
            )
            .unwrap();
        assert_eq!(
            distinct_large.rows,
            vec![vec![SqlValue::String("large-b".to_string())]]
        );
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let reopened = SqlSession::new(&mut db)
        .execute("SELECT id, label FROM jsonb_single_pk WHERE id = '1.00'::jsonb")
        .unwrap();
    assert_eq!(reopened.rows.len(), 1);
    assert_eq!(reopened.rows[0][0].to_cell(), "1.0");
    assert_eq!(reopened.rows[0][1], SqlValue::String("one".to_string()));
}

#[test]
fn jsonb_composite_primary_key_canonicalizes_nested_values_and_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE jsonb_composite_pk_identity (
                    document jsonb,
                    shard text,
                    label text NOT NULL,
                    PRIMARY KEY (document, shard)
                )",
            )
            .unwrap();
        session
            .execute(
                r#"INSERT INTO jsonb_composite_pk_identity (document, shard, label) VALUES
                    ('{"n":1.0,"nested":[{"value":1.00}]}'::jsonb, 'a', 'first'),
                    ('{"nested":[{"value":1}],"n":1}'::jsonb, 'b', 'second'),
                    ('{"n":9007199254740992}'::jsonb, 'large', 'large-a'),
                    ('{"n":9007199254740993}'::jsonb, 'large', 'large-b')"#,
            )
            .unwrap();

        let explain = session
            .execute(
                r#"EXPLAIN SELECT shard FROM jsonb_composite_pk_identity
                   WHERE document = '{"n":1.00,"nested":[{"value":1.0}]}'::jsonb
                   ORDER BY shard"#,
            )
            .unwrap();
        assert!(explain
            .rows
            .iter()
            .any(|row| row[0].to_cell().contains("PrimaryKeyPrefixScan")));

        let exact = session
            .execute(
                r#"SELECT label FROM jsonb_composite_pk_identity
                   WHERE document = '{"nested":[{"value":1}],"n":1}'::jsonb
                     AND shard = 'a'"#,
            )
            .unwrap();
        assert_eq!(
            exact.rows,
            vec![vec![SqlValue::String("first".to_string())]]
        );

        let prefix = session
            .execute(
                r#"SELECT shard FROM jsonb_composite_pk_identity
                   WHERE document = '{"n":1.00,"nested":[{"value":1.0}]}'::jsonb
                   ORDER BY shard"#,
            )
            .unwrap();
        assert_eq!(
            prefix.rows,
            vec![
                vec![SqlValue::String("a".to_string())],
                vec![SqlValue::String("b".to_string())],
            ]
        );

        let duplicate = session
            .execute(
                r#"INSERT INTO jsonb_composite_pk_identity (document, shard, label)
                   VALUES ('{"nested":[{"value":1}],"n":1}'::jsonb, 'a', 'duplicate')"#,
            )
            .unwrap_err();
        assert_eq!(duplicate.sqlstate(), "23505");

        let distinct_large = session
            .execute(
                r#"SELECT label FROM jsonb_composite_pk_identity
                   WHERE document = '{"n":9007199254740993}'::jsonb
                     AND shard = 'large'"#,
            )
            .unwrap();
        assert_eq!(
            distinct_large.rows,
            vec![vec![SqlValue::String("large-b".to_string())]]
        );
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let reopened = SqlSession::new(&mut db)
        .execute(
            r#"SELECT shard FROM jsonb_composite_pk_identity
               WHERE document = '{"nested":[{"value":1.000}],"n":1}'::jsonb
               ORDER BY shard"#,
        )
        .unwrap();
    assert_eq!(
        reopened.rows,
        vec![
            vec![SqlValue::String("a".to_string())],
            vec![SqlValue::String("b".to_string())],
        ]
    );
}

#[test]
fn legacy_payload_field_jsonb_is_read_from_record_payload() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        SqlSession::new(&mut db)
            .execute(
                "CREATE TABLE legacy_payload_jsonb (
                    id text PRIMARY KEY,
                    payload jsonb NOT NULL
                )",
            )
            .unwrap();

        // Pre-canonical JSON storage treated any array-valued column literally
        // named `payload` as the Record payload bytes, leaving metadata empty.
        db.insert(
            "legacy_payload_jsonb",
            Record::new("legacy").with_payload(vec![1, 2, 255]),
        )
        .unwrap();
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let result = SqlSession::new(&mut db)
        .execute(
            "SELECT payload, payload ->> 1
             FROM legacy_payload_jsonb WHERE id = 'legacy'",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Json(json!([1, 2, 255])),
            SqlValue::String("2".to_string()),
        ]]
    );
}

#[test]
fn legacy_noncanonical_jsonb_primary_key_remains_addressable_and_unique() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        SqlSession::new(&mut db)
            .execute(
                "CREATE TABLE legacy_jsonb_pk (
                    id jsonb PRIMARY KEY,
                    label text NOT NULL
                )",
            )
            .unwrap();

        // Old single-column PKs omitted the logical key from metadata and used
        // SqlValue::to_cell() verbatim as the physical record id.
        db.insert(
            "legacy_jsonb_pk",
            Record::new("1.0").with_metadata(json!({"label": "legacy"})),
        )
        .unwrap();
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let selected = session
        .execute("SELECT id, label FROM legacy_jsonb_pk WHERE id = '1'::jsonb")
        .unwrap();
    assert_eq!(
        selected.rows,
        vec![vec![
            SqlValue::Json(json!(1.0)),
            SqlValue::String("legacy".to_string()),
        ]]
    );

    let duplicate = session
        .execute("INSERT INTO legacy_jsonb_pk VALUES ('1.00'::jsonb, 'duplicate')")
        .unwrap_err();
    assert_eq!(duplicate.sqlstate(), "23505");

    let ignored = session
        .execute(
            "INSERT INTO legacy_jsonb_pk VALUES ('1.000'::jsonb, 'ignored')
             ON CONFLICT DO NOTHING",
        )
        .unwrap();
    assert_eq!(ignored.command_complete_tag(), "INSERT 0 0");
}
