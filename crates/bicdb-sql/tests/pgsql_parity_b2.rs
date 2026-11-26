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
fn insert_without_column_list_fills_trailing_defaults() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE t(a int, b text DEFAULT 'dflt', c int)")
        .unwrap();
    session.execute("INSERT INTO t VALUES (1)").unwrap();
    session
        .execute("INSERT INTO t VALUES (2, 'x'), (3, 'y')")
        .unwrap();
    session.execute("INSERT INTO t SELECT 4").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a, b, c FROM t ORDER BY a"),
        vec![
            vec![int(1), text("dflt"), SqlValue::Null],
            vec![int(2), text("x"), SqlValue::Null],
            vec![int(3), text("y"), SqlValue::Null],
            vec![int(4), text("dflt"), SqlValue::Null],
        ]
    );
    for (sql, message) in [
        (
            "INSERT INTO t VALUES (1, 'a', 2, 3)",
            "INSERT has more expressions than target columns",
        ),
        (
            "INSERT INTO t (a, b) VALUES (1)",
            "INSERT has more target columns than expressions",
        ),
        (
            "INSERT INTO t (a) VALUES (1, 'b')",
            "INSERT has more expressions than target columns",
        ),
        (
            "INSERT INTO t VALUES (1), (2, 'b')",
            "VALUES lists must all be the same length",
        ),
        (
            "INSERT INTO t (a, b) SELECT 1",
            "INSERT has more target columns than expressions",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42601", "{sql}");
        assert_eq!(error.postgres_message(), message, "{sql}");
    }
}

#[test]
fn output_columns_take_postgresql_names() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE n(i int, t text)").unwrap();
    session.execute("INSERT INTO n VALUES (1, 'a')").unwrap();
    let result = session
        .execute(
            "SELECT i + 1, n.i, upper(t), '1'::int, i::text, CASE WHEN i > 0 THEN 1 END, \
             EXISTS (SELECT 1), count(*) OVER () FROM n",
        )
        .unwrap();
    assert_eq!(
        result.columns,
        vec!["?column?", "i", "upper", "int4", "i", "case", "exists", "count"]
    );
    let result = session
        .execute("SELECT * FROM (SELECT i * 2, CASE WHEN true THEN t END FROM n) s")
        .unwrap();
    assert_eq!(result.columns, vec!["?column?", "case"]);
    session
        .execute("CREATE VIEW nv AS SELECT i + 1, 'x'::text, upper(t) FROM n")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT attname FROM pg_attribute WHERE attrelid = 'nv'::regclass AND attnum > 0 ORDER BY attnum"
        ),
        vec![vec![text("?column?")], vec![text("text")], vec![text("upper")]]
    );
    // An expression is still matched by ORDER BY and GROUP BY.
    session
        .execute("INSERT INTO n VALUES (1, 'b'), (2, 'c')")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT i * 10, count(*) FROM n GROUP BY i * 10 ORDER BY i * 10"
        ),
        vec![vec![int(10), int(2)], vec![int(20), int(1)]]
    );
}

#[test]
fn also_rules_run_with_the_statement() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE r(a int, b int); CREATE TABLE r_log(op text, old_b int, new_b int); \
             INSERT INTO r VALUES (1, 10), (2, 20)",
        )
        .unwrap();
    session
        .execute(
            "CREATE RULE r_ins AS ON INSERT TO r DO ALSO INSERT INTO r_log VALUES ('i', NULL, NEW.b)",
        )
        .unwrap();
    session
        .execute(
            "CREATE RULE r_upd AS ON UPDATE TO r DO ALSO INSERT INTO r_log VALUES ('u', OLD.b, NEW.b)",
        )
        .unwrap();
    session
        .execute(
            "CREATE RULE r_del AS ON DELETE TO r DO ALSO INSERT INTO r_log VALUES ('d', OLD.b, NULL)",
        )
        .unwrap();
    let inserted = session.execute("INSERT INTO r VALUES (3, 30)").unwrap();
    assert_eq!(inserted.command_tag.as_deref(), Some("INSERT 0 1"));
    let updated = session
        .execute("UPDATE r SET b = b + 1 WHERE a = 2")
        .unwrap();
    assert_eq!(updated.command_tag.as_deref(), Some("UPDATE 1"));
    let deleted = session.execute("DELETE FROM r WHERE a = 1").unwrap();
    assert_eq!(deleted.command_tag.as_deref(), Some("DELETE 1"));
    assert_eq!(
        rows(&mut session, "SELECT a, b FROM r ORDER BY a"),
        vec![vec![int(2), int(21)], vec![int(3), int(30)]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT op, old_b, new_b FROM r_log ORDER BY op"
        ),
        vec![
            vec![text("d"), int(10), SqlValue::Null],
            vec![text("i"), SqlValue::Null, int(30)],
            vec![text("u"), int(20), int(21)],
        ]
    );
    session
        .execute(
            "CREATE TABLE quiet(a int); CREATE RULE q AS ON DELETE TO quiet DO INSTEAD NOTHING",
        )
        .unwrap();
    session.execute("INSERT INTO quiet VALUES (1)").unwrap();
    session.execute("DELETE FROM quiet").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM quiet"),
        vec![vec![int(1)]]
    );
}

#[test]
fn common_errors_carry_postgresql_messages_and_codes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE e(a int PRIMARY KEY, b int); CREATE TABLE ep(id int PRIMARY KEY); \
             CREATE TABLE ec(p int REFERENCES ep); INSERT INTO ep VALUES (1); INSERT INTO ec VALUES (1); \
             CREATE ROLE nobody; CREATE VIEW ev AS SELECT a FROM e",
        )
        .unwrap();
    for (sql, sqlstate, message) in [
        ("SELECT * FROM missing_t", "42P01", "relation \"missing_t\" does not exist"),
        ("INSERT INTO missing_t VALUES (1)", "42P01", "relation \"missing_t\" does not exist"),
        ("UPDATE missing_t SET a = 1", "42P01", "relation \"missing_t\" does not exist"),
        ("DELETE FROM missing_t", "42P01", "relation \"missing_t\" does not exist"),
        ("DROP TABLE missing_t", "42P01", "table \"missing_t\" does not exist"),
        ("SELECT c FROM e", "42703", "column \"c\" does not exist"),
        ("SELECT e.c FROM e", "42703", "column e.c does not exist"),
        ("UPDATE e SET c = 1", "42703", "column \"c\" of relation \"e\" does not exist"),
        ("SELECT missing_fn(1)", "42883", "function missing_fn(integer) does not exist"),
        ("CREATE TABLE e(a int)", "42P07", "relation \"e\" already exists"),
        ("SELEC 1", "42601", "syntax error at or near \"SELEC\""),
        (
            "DELETE FROM ep",
            "23503",
            "update or delete on table \"ep\" violates foreign key constraint \"ec_p_fkey\" on table \"ec\"",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
        assert_eq!(error.postgres_message(), message, "{sql}");
    }
    session.execute("SET ROLE nobody").unwrap();
    for (sql, message) in [
        ("SELECT * FROM e", "permission denied for table e"),
        ("SELECT * FROM ev", "permission denied for view ev"),
        (
            "INSERT INTO e VALUES (5, 5)",
            "permission denied for table e",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{sql}");
        assert_eq!(error.postgres_message(), message, "{sql}");
    }
}
