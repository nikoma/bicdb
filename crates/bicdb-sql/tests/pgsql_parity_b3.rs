use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn int(value: i64) -> SqlValue {
    SqlValue::Int(value)
}

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session.execute(sql).unwrap().rows
}

#[test]
fn result_types_follow_postgresql_resolution() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT 'x'::varchar(3), 1.5::numeric(5,2), date_trunc('day', '2024-01-02'::date), \
             ceil(1.5), floor(1.5::float8), sqrt(4), power(2, 3), random(), to_timestamp(0)",
        )
        .unwrap();
    let types: Vec<Option<&str>> = result.column_types.iter().map(Option::as_deref).collect();
    assert_eq!(
        types,
        vec![
            Some("varchar"),
            Some("numeric"),
            Some("timestamptz"),
            Some("numeric"),
            Some("float8"),
            Some("float8"),
            Some("float8"),
            Some("float8"),
            Some("timestamptz"),
        ]
    );
    session.execute("CREATE TABLE f(r real)").unwrap();
    let result = session.execute("SELECT sum(r) FROM f").unwrap();
    assert_eq!(result.column_types[0].as_deref(), Some("float4"));
}

#[test]
fn foreign_keys_are_checked_at_statement_end() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE tree(id int PRIMARY KEY, parent int REFERENCES tree)")
        .unwrap();
    session
        .execute("INSERT INTO tree VALUES (2, 1), (1, NULL), (3, 2)")
        .unwrap();
    let error = session
        .execute("INSERT INTO tree VALUES (4, NULL), (5, 99)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23503");
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM tree"),
        vec![vec![int(3)]]
    );
    session
        .execute("UPDATE tree SET parent = 3 WHERE id = 1")
        .unwrap();
}

#[test]
fn column_select_grants_admit_only_granted_columns() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE clerk; CREATE TABLE staff(id int, name text, salary int); \
             INSERT INTO staff VALUES (1, 'a', 100), (2, 'b', 200); \
             CREATE TABLE other(salary int); GRANT SELECT ON other TO clerk; \
             GRANT SELECT (id, name) ON staff TO clerk; GRANT CREATE ON SCHEMA public TO clerk",
        )
        .unwrap();
    session.execute("SET ROLE clerk").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, name FROM staff ORDER BY id"),
        vec![vec![int(1), text("a")], vec![int(2), text("b")]]
    );
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM staff WHERE id > 1"),
        vec![vec![int(1)]]
    );
    // Authorization canaries: every way of reaching salary is refused.
    for sql in [
        "SELECT salary FROM staff",
        "SELECT * FROM staff",
        "SELECT staff.* FROM staff",
        "SELECT id FROM staff WHERE salary > 150",
        "SELECT id FROM staff ORDER BY salary",
        "SELECT id FROM staff GROUP BY id HAVING max(salary) > 0",
        "SELECT row_to_json(staff) FROM staff",
        "SELECT (SELECT max(salary) FROM staff)",
        "SELECT id FROM staff s WHERE EXISTS (SELECT 1 WHERE s.salary > 0)",
        "SELECT id FROM (SELECT * FROM staff) x",
        "WITH c AS (SELECT salary FROM staff) SELECT 1 FROM c",
        "SELECT id FROM staff JOIN other USING (salary)",
        "SELECT id FROM staff NATURAL JOIN other",
        "SELECT count(salary) FROM staff",
        "SELECT id FROM staff WHERE (staff).salary > 0",
        "UPDATE staff SET name = 'x' WHERE salary > 0",
        "DELETE FROM staff WHERE salary > 0",
        "CREATE TABLE copied AS SELECT salary FROM staff",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{sql}: {error}");
    }
    session.execute("RESET ROLE").unwrap();
    session
        .execute("REVOKE SELECT (name) ON staff FROM clerk")
        .unwrap();
    session.execute("SET ROLE clerk").unwrap();
    assert_eq!(
        session
            .execute("SELECT name FROM staff")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    session.execute("SELECT id FROM staff").unwrap();
    session.execute("RESET ROLE").unwrap();
    session
        .execute("REVOKE SELECT ON staff FROM clerk")
        .unwrap();
    session.execute("SET ROLE clerk").unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM staff")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
}

#[test]
fn column_insert_update_grants_and_returning() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE writer; CREATE TABLE notes(id int, body text, secret text DEFAULT 'd'); \
             GRANT INSERT (id, body) ON notes TO writer; GRANT UPDATE (body) ON notes TO writer; \
             GRANT SELECT (id) ON notes TO writer",
        )
        .unwrap();
    session.execute("SET ROLE writer").unwrap();
    session
        .execute("INSERT INTO notes (id, body) VALUES (1, 'a')")
        .unwrap();
    session
        .execute("INSERT INTO notes VALUES (2, 'b')")
        .unwrap();
    session
        .execute("INSERT INTO notes (id) VALUES (3) RETURNING id")
        .unwrap();
    session
        .execute("UPDATE notes SET body = 'z' WHERE id = 1")
        .unwrap();
    for sql in [
        "INSERT INTO notes (id, secret) VALUES (4, 'x')",
        "INSERT INTO notes VALUES (5, 'b', 'c')",
        "INSERT INTO notes (id) VALUES (6) RETURNING secret",
        "INSERT INTO notes (id) VALUES (7) RETURNING *",
        "UPDATE notes SET secret = 'x'",
        "UPDATE notes SET body = secret",
        "UPDATE notes SET body = 'q' WHERE body = 'z'",
        "UPDATE notes SET body = 'q' RETURNING secret",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{sql}: {error}");
    }
    session.execute("RESET ROLE").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT id, body, secret FROM notes ORDER BY id"
        ),
        vec![
            vec![int(1), text("z"), text("d")],
            vec![int(2), text("b"), text("d")],
            vec![int(3), SqlValue::Null, text("d")],
        ]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT has_column_privilege('writer', 'notes', 'body', 'INSERT'), \
             has_column_privilege('writer', 'notes', 'secret', 'SELECT'), \
             has_any_column_privilege('writer', 'notes', 'UPDATE')"
        ),
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true)
        ]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT column_name, privilege_type FROM information_schema.column_privileges \
             WHERE table_name = 'notes' AND grantee = 'writer' ORDER BY 1, 2"
        ),
        vec![
            vec![text("body"), text("INSERT")],
            vec![text("body"), text("UPDATE")],
            vec![text("id"), text("INSERT")],
            vec![text("id"), text("SELECT")],
        ]
    );
}
