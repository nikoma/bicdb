use bicdb_core::{BicDb, SecurityContext};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn stored_identity_helpers_work_inside_table_function_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("CREATE SCHEMA carrier_private").unwrap();
        owner.execute("CREATE FUNCTION carrier_private.current_roles() RETURNS JSONB LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT COALESCE(carrier_private.current_context() -> 'roles', '[]'::JSONB) $$").unwrap();
    }
    let context = SecurityContext::new("sis-staff", "sis-tenant").with_roles(["registrar"]);
    let mut session = SqlSession::new_secure(&mut db, context);
    assert_eq!(
        session
            .execute("SELECT carrier_private.current_roles()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(serde_json::json!(["registrar"]))]]
    );
    let result = session.execute("SELECT bool_or(role_key = 'registrar') FROM jsonb_array_elements_text(carrier_private.current_roles()) AS roles(role_key)").unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn partially_null_composite_foreign_keys_use_match_simple() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE parent (tenant TEXT NOT NULL, id INT NOT NULL, PRIMARY KEY (tenant,id))",
        )
        .unwrap();
    session.execute("CREATE TABLE child (tenant TEXT, parent_id INT, FOREIGN KEY (tenant,parent_id) REFERENCES parent(tenant,id))").unwrap();
    session
        .execute("INSERT INTO child(tenant) VALUES ('tenant-a')")
        .unwrap();
    session
        .execute("INSERT INTO child VALUES (NULL,1)")
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO child VALUES ('tenant-a',1)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    session
        .execute("INSERT INTO parent VALUES ('tenant-a',1)")
        .unwrap();
    session
        .execute("INSERT INTO child VALUES ('tenant-a',1)")
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO child VALUES ('tenant-b',1)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
}
