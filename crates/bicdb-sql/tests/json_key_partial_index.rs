use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn json_key_partial_unique_index_has_top_level_jsonb_existence_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE events(id INT PRIMARY KEY,code TEXT,payload_json JSONB); CREATE UNIQUE INDEX active_code ON events(code) WHERE payload_json ? 'active'; INSERT INTO events VALUES(1,'same','{\"active\":null}'::jsonb)").unwrap();
    for (index, value) in [
        "{}",
        "null",
        "1",
        "true",
        "\"inactive\"",
        "[1,\"inactive\"]",
        "{\"nested\":{\"active\":true}}",
    ]
    .into_iter()
    .enumerate()
    {
        sql.execute(&format!(
            "INSERT INTO events VALUES({},'same','{}'::jsonb)",
            index + 10,
            value
        ))
        .unwrap();
    }
    sql.execute("INSERT INTO events VALUES(30,'same',NULL)")
        .unwrap();
    for (index, value) in ["{\"active\":false}", "[\"active\"]", "\"active\""]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            sql.execute(&format!(
                "INSERT INTO events VALUES({},'same','{}'::jsonb)",
                index + 40,
                value
            ))
            .unwrap_err()
            .sqlstate(),
            "23505"
        );
    }
    assert_eq!(
        sql.execute("SELECT count(*) FROM events WHERE payload_json ? 'active'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn membership_changes_and_expression_index_survive_reopening() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE events(id INT PRIMARY KEY,payload_json JSONB); INSERT INTO events VALUES(1,'{\"enrollment\":\"a\"}'),(2,'{}'); CREATE UNIQUE INDEX enrollment_key ON events((payload_json->>'enrollment')) WHERE payload_json ? 'enrollment'; UPDATE events SET payload_json='{\"enrollment\":\"b\"}' WHERE id=2").unwrap();
        assert_eq!(
            sql.execute("SELECT jsonb_typeof(payload_json) FROM events WHERE id=2")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("object".into())]]
        );
        assert_eq!(
            sql.execute("UPDATE events SET payload_json='{\"enrollment\":\"a\"}' WHERE id=2")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
    }
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute("INSERT INTO events VALUES(3,'{\"enrollment\":\"a\"}')")
            .unwrap_err()
            .sqlstate(),
        "23505"
    );
    sql.execute("UPDATE events SET payload_json='{}' WHERE id=1; INSERT INTO events VALUES(3,'{\"enrollment\":\"a\"}')").unwrap();
}

#[test]
fn json_key_predicate_composes_with_tenant_and_soft_delete_filters() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE events(id INT PRIMARY KEY,code TEXT,tenant TEXT,deleted_at TIMESTAMPTZ,payload_json JSONB); CREATE UNIQUE INDEX current_key ON events(tenant,code) WHERE deleted_at IS NULL AND (payload_json ? 'active'); INSERT INTO events VALUES(1,'same','a',NULL,'{\"active\":true}'),(2,'same','b',NULL,'{\"active\":true}'),(3,'same','a',NOW(),'{\"active\":true}'),(4,'same','a',NULL,'{}')").unwrap();
    let error = sql
        .execute("UPDATE events SET payload_json='{\"active\":true}' WHERE id=4")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23505", "{error:?}");
    assert_eq!(
        sql.execute("UPDATE events SET deleted_at=NULL WHERE id=3")
            .unwrap_err()
            .sqlstate(),
        "23505"
    );
}
