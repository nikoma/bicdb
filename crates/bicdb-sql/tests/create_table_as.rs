use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn int(value: i64) -> SqlValue {
    SqlValue::Int(value)
}

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

fn column_types(session: &mut SqlSession<'_>, table: &str) -> Vec<Vec<SqlValue>> {
    session
        .execute(&format!(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute \
             WHERE attrelid = '{table}'::regclass AND attnum > 0 ORDER BY attnum"
        ))
        .unwrap()
        .rows
}

#[test]
fn create_table_as_copies_rows_names_and_types() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE src(i int, v varchar(10), n numeric(8,2))")
        .unwrap();
    session
        .execute("INSERT INTO src VALUES (1, 'x', 1.5), (2, NULL, 2.25), (3, 'zz', NULL)")
        .unwrap();
    let created = session
        .execute(
            "CREATE TABLE copy AS SELECT i, v, n, i + 1, v::varchar(3) AS v3 FROM src WHERE i < 3",
        )
        .unwrap();
    assert_eq!(created.command_tag.as_deref(), Some("SELECT 2"));
    assert_eq!(
        column_types(&mut session, "copy"),
        vec![
            vec![text("i"), text("integer")],
            vec![text("v"), text("character varying(10)")],
            vec![text("n"), text("numeric(8,2)")],
            vec![text("?column?"), text("integer")],
            vec![text("v3"), text("character varying(3)")],
        ]
    );
    let rows = session
        .execute("SELECT i, v, \"?column?\" FROM copy ORDER BY i")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![int(1), text("x"), int(2)],
            vec![int(2), SqlValue::Null, int(3)],
        ]
    );
}

#[test]
fn create_table_as_column_list_no_data_and_if_not_exists() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE named(x, y) AS SELECT 1 AS a, 2 AS b, 3 AS c")
        .unwrap();
    assert_eq!(
        session.execute("SELECT * FROM named").unwrap().columns,
        vec!["x", "y", "c"]
    );
    let empty = session
        .execute("CREATE TABLE empty_copy AS SELECT 1 AS a, 'x'::varchar(5) AS b WITH NO DATA")
        .unwrap();
    assert_eq!(empty.command_tag.as_deref(), Some("CREATE TABLE AS"));
    assert_eq!(
        session
            .execute("SELECT count(*) FROM empty_copy")
            .unwrap()
            .rows,
        vec![vec![int(0)]]
    );
    assert_eq!(
        column_types(&mut session, "empty_copy"),
        vec![
            vec![text("a"), text("integer")],
            vec![text("b"), text("character varying(5)")],
        ]
    );
    session
        .execute("CREATE TABLE IF NOT EXISTS named AS SELECT 9 AS z")
        .unwrap();
    assert_eq!(
        session.execute("SELECT x FROM named").unwrap().rows,
        vec![vec![int(1)]]
    );
    let error = session
        .execute("CREATE TABLE named AS SELECT 9 AS z")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42P07");
    let error = session
        .execute("CREATE TABLE too_many(a, b) AS SELECT 1")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42601");
    let error = session
        .execute("CREATE TABLE dup AS SELECT 1 AS a, 2 AS a")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42701");
}

#[test]
fn create_table_as_takes_no_implicit_key_and_is_atomic() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ids AS SELECT 1 AS id UNION ALL SELECT 1")
        .unwrap();
    session.execute("INSERT INTO ids VALUES (1)").unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM ids").unwrap().rows,
        vec![vec![int(3)]]
    );
    let error = session
        .execute("CREATE TABLE failed AS SELECT 1 / (3 - g) AS x FROM generate_series(1, 3) g")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22012");
    assert_eq!(
        session
            .execute("SELECT count(*) FROM pg_class WHERE relname = 'failed'")
            .unwrap()
            .rows,
        vec![vec![int(0)]]
    );
    session.execute("BEGIN").unwrap();
    session
        .execute("CREATE TABLE rolled_back AS SELECT 1 AS a")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM pg_class WHERE relname = 'rolled_back'")
            .unwrap()
            .rows,
        vec![vec![int(0)]]
    );
}

#[test]
fn create_table_as_rows_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE kept AS SELECT g AS a, 'v' || g AS b FROM generate_series(1, 4) g",
            )
            .unwrap();
        session
            .execute("SELECT a * 10 AS a INTO also_kept FROM kept WHERE a > 2")
            .unwrap();
    }
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SELECT count(*), max(b) FROM kept")
            .unwrap()
            .rows,
        vec![vec![int(4), text("v4")]]
    );
    assert_eq!(
        session
            .execute("SELECT a FROM also_kept ORDER BY a")
            .unwrap()
            .rows,
        vec![vec![int(30)], vec![int(40)]]
    );
}

#[test]
fn select_into_creates_a_table_and_rejects_nested_into() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let created = session
        .execute("SELECT * INTO si FROM (VALUES (1, 'a'), (2, 'b')) v(a, b)")
        .unwrap();
    assert_eq!(created.command_tag.as_deref(), Some("SELECT 2"));
    assert!(created.columns.is_empty());
    assert_eq!(
        session
            .execute("SELECT a, b FROM si ORDER BY a")
            .unwrap()
            .rows,
        vec![vec![int(1), text("a")], vec![int(2), text("b")]]
    );
    session.execute("SELECT 1 AS a INTO TEMP si_temp").unwrap();
    session
        .execute("SELECT 1 AS a INTO TABLE si_table")
        .unwrap();
    assert_eq!(
        session.execute("SELECT a FROM si_table").unwrap().rows,
        vec![vec![int(1)]]
    );
    let error = session
        .execute("SELECT * FROM (SELECT 1 AS a INTO si_nested) s")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42601");
}

#[test]
fn create_table_as_needs_select_on_sources_and_keeps_row_security() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE reader; CREATE TABLE secret(a int); INSERT INTO secret VALUES (1); \
             CREATE TABLE owned(a int, who text); \
             INSERT INTO owned VALUES (1, 'reader'), (2, 'other'), (3, 'reader'); \
             ALTER TABLE owned ENABLE ROW LEVEL SECURITY; \
             CREATE POLICY mine ON owned USING (who = current_user); \
             GRANT SELECT ON owned TO reader; GRANT CREATE ON SCHEMA public TO reader",
        )
        .unwrap();
    session.execute("SET ROLE reader").unwrap();
    let error = session
        .execute("CREATE TABLE stolen AS SELECT * FROM secret")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    let error = session
        .execute("SELECT * INTO stolen2 FROM secret")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    session
        .execute("CREATE TABLE visible AS SELECT a FROM owned")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT a FROM visible ORDER BY a")
            .unwrap()
            .rows,
        vec![vec![int(1)], vec![int(3)]]
    );
    session.execute("RESET ROLE").unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM pg_class WHERE relname IN ('stolen', 'stolen2')")
            .unwrap()
            .rows,
        vec![vec![int(0)]]
    );
}
