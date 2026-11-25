//! PostgreSQL's DETAIL, HINT and NOTICE lines (agent G of the SQL-parity
//! campaign): constraint violations show the key or row values the way
//! PostgreSQL prints them, but only to a role that may read them, and
//! IF [NOT] EXISTS / CASCADE / transaction-control statements report the
//! notices PostgreSQL sends.
use bicdb_core::BicDb;
use bicdb_sql::{SqlError, SqlErrorField, SqlResult, SqlSession};

fn detail(error: &SqlError) -> Option<String> {
    error.fields().into_iter().find_map(|field| match field {
        SqlErrorField::Detail(detail) => Some(detail),
        _ => None,
    })
}

fn hint(error: &SqlError) -> Option<String> {
    error.fields().into_iter().find_map(|field| match field {
        SqlErrorField::Hint(hint) => Some(hint),
        _ => None,
    })
}

fn notices(result: &SqlResult) -> Vec<String> {
    result
        .notices
        .iter()
        .map(|notice| {
            let severity = notice
                .fields
                .iter()
                .find_map(|field| match field {
                    SqlErrorField::Severity(severity) => Some(severity.as_str()),
                    _ => None,
                })
                .unwrap_or("NOTICE");
            let detail = notice
                .fields
                .iter()
                .find_map(|field| match field {
                    SqlErrorField::Detail(detail) => Some(format!(" / {detail}")),
                    _ => None,
                })
                .unwrap_or_default();
            format!("{severity}: {}{detail}", notice.message)
        })
        .collect()
}

fn failure(session: &mut SqlSession<'_>, sql: &str) -> SqlError {
    session.execute(sql).expect_err(sql)
}

#[test]
fn unique_violations_show_the_duplicated_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE u1(a int PRIMARY KEY, b text, c bool, UNIQUE (b, c))")
        .unwrap();
    session
        .execute("INSERT INTO u1 VALUES (1, 'x y', true)")
        .unwrap();
    let error = failure(&mut session, "INSERT INTO u1 VALUES (1, 'z', false)");
    assert_eq!(error.sqlstate(), "23505");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (a)=(1) already exists.")
    );
    let error = failure(&mut session, "INSERT INTO u1 VALUES (2, 'x y', true)");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (b, c)=(x y, t) already exists.")
    );
    session
        .execute("INSERT INTO u1 VALUES (2, 'q', true)")
        .unwrap();
    let error = failure(&mut session, "UPDATE u1 SET a = 1 WHERE a = 2");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (a)=(1) already exists.")
    );
    let error = failure(
        &mut session,
        "INSERT INTO u1 VALUES (7, 'n', true), (7, 'm', true)",
    );
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (a)=(7) already exists.")
    );

    session.execute("CREATE TABLE u2(a int, b int)").unwrap();
    session
        .execute("INSERT INTO u2 VALUES (3, 1), (2, 1), (3, 1), (2, 1)")
        .unwrap();
    let error = failure(&mut session, "CREATE UNIQUE INDEX u2_a ON u2 (a)");
    assert_eq!(error.to_string(), "could not create unique index \"u2_a\"");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (a)=(2) is duplicated.")
    );
    let error = failure(
        &mut session,
        "ALTER TABLE u2 ADD CONSTRAINT u2_ab UNIQUE (a, b)",
    );
    assert_eq!(error.to_string(), "could not create unique index \"u2_ab\"");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (a, b)=(2, 1) is duplicated.")
    );
}

#[test]
fn foreign_key_violations_show_the_key_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE fp(a int PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE fc(x int REFERENCES fp(a))")
        .unwrap();
    session.execute("INSERT INTO fp VALUES (1)").unwrap();
    let error = failure(&mut session, "INSERT INTO fc VALUES (5)");
    assert_eq!(error.sqlstate(), "23503");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (x)=(5) is not present in table \"fp\".")
    );
    session.execute("INSERT INTO fc VALUES (1)").unwrap();
    for statement in ["DELETE FROM fp", "UPDATE fp SET a = 2"] {
        let error = failure(&mut session, statement);
        assert_eq!(error.sqlstate(), "23503", "{statement}");
        assert_eq!(
            error.to_string(),
            "update or delete on table \"fp\" violates foreign key constraint \"fc_x_fkey\" on table \"fc\"",
            "{statement}"
        );
        assert_eq!(
            detail(&error).as_deref(),
            Some("Key (a)=(1) is still referenced from table \"fc\"."),
            "{statement}"
        );
    }
}

#[test]
fn check_and_not_null_violations_show_the_failing_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ck(a int CHECK (a > 0), b text, c bool, d float8)")
        .unwrap();
    let error = failure(
        &mut session,
        "INSERT INTO ck VALUES (-1, 'hello world', NULL, 0.5)",
    );
    assert_eq!(error.sqlstate(), "23514");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Failing row contains (-1, hello world, null, 0.5).")
    );
    session
        .execute("CREATE TABLE nn(a int NOT NULL, b text)")
        .unwrap();
    let error = failure(&mut session, "INSERT INTO nn VALUES (NULL, 'x')");
    assert_eq!(error.sqlstate(), "23502");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Failing row contains (null, x).")
    );
    session.execute("INSERT INTO nn VALUES (1, 'x')").unwrap();
    let error = failure(&mut session, "UPDATE nn SET a = NULL");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Failing row contains (null, x).")
    );
    session
        .execute("CREATE TABLE long_ck(a text CHECK (length(a) < 5))")
        .unwrap();
    let error = failure(
        &mut session,
        "INSERT INTO long_ck VALUES (repeat('abcdefghij', 10))",
    );
    assert_eq!(
        detail(&error),
        Some(format!(
            "Failing row contains ({}...).",
            &"abcdefghij".repeat(7)[..64]
        ))
    );
    session.execute("CREATE TABLE late(a int)").unwrap();
    session.execute("INSERT INTO late VALUES (-5)").unwrap();
    let error = failure(
        &mut session,
        "ALTER TABLE late ADD CONSTRAINT late_pos CHECK (a > 0)",
    );
    assert_eq!(
        error.to_string(),
        "check constraint \"late_pos\" of relation \"late\" is violated by some row"
    );
}

/// Row values reach a DETAIL only for a role that may SELECT them, as in
/// PostgreSQL; a referencing violation then names the table without values.
#[test]
fn row_values_stay_hidden_from_roles_that_cannot_read_them() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE writer").unwrap();
    session
        .execute("CREATE TABLE secret(a int PRIMARY KEY, b text NOT NULL)")
        .unwrap();
    session
        .execute("CREATE TABLE sp(a int PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE sc(x int REFERENCES sp(a))")
        .unwrap();
    session
        .execute("INSERT INTO secret VALUES (1, 'hidden')")
        .unwrap();
    session
        .execute("GRANT INSERT, UPDATE ON secret, sc TO writer")
        .unwrap();
    session.execute("SET ROLE writer").unwrap();
    let error = failure(&mut session, "INSERT INTO secret VALUES (1, 'x')");
    assert_eq!(error.sqlstate(), "23505");
    assert_eq!(detail(&error), None);
    let error = failure(&mut session, "INSERT INTO secret VALUES (2, NULL)");
    assert_eq!(error.sqlstate(), "23502");
    assert_eq!(detail(&error), None);
    let error = failure(&mut session, "INSERT INTO sc VALUES (9)");
    assert_eq!(error.sqlstate(), "23503");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key is not present in table \"sp\".")
    );
    session.execute("RESET ROLE").unwrap();
    session.execute("GRANT SELECT ON secret TO writer").unwrap();
    session.execute("SET ROLE writer").unwrap();
    let error = failure(&mut session, "INSERT INTO secret VALUES (1, 'x')");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Key (a)=(1) already exists.")
    );
    session.execute("RESET ROLE").unwrap();
    session
        .execute("ALTER TABLE secret ENABLE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute("CREATE POLICY everything ON secret USING (true) WITH CHECK (true)")
        .unwrap();
    session.execute("SET ROLE writer").unwrap();
    let error = failure(&mut session, "INSERT INTO secret VALUES (1, 'x')");
    assert_eq!(error.sqlstate(), "23505");
    assert_eq!(detail(&error), None, "row-level security hides the key");
}

#[test]
fn skipped_and_cascaded_objects_are_reported_as_notices() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let run = |session: &mut SqlSession<'_>, sql: &str| notices(&session.execute(sql).unwrap());
    assert_eq!(
        run(&mut session, "DROP TABLE IF EXISTS nosuch"),
        ["NOTICE: table \"nosuch\" does not exist, skipping"]
    );
    session.execute("CREATE TABLE t1(a int)").unwrap();
    assert_eq!(
        run(&mut session, "CREATE TABLE IF NOT EXISTS t1(a int)"),
        ["NOTICE: relation \"t1\" already exists, skipping"]
    );
    assert_eq!(
        run(
            &mut session,
            "ALTER TABLE t1 ADD COLUMN IF NOT EXISTS a int"
        ),
        ["NOTICE: column \"a\" of relation \"t1\" already exists, skipping"]
    );
    assert_eq!(
        run(&mut session, "ALTER TABLE t1 DROP COLUMN IF EXISTS zz"),
        ["NOTICE: column \"zz\" of relation \"t1\" does not exist, skipping"]
    );
    assert_eq!(
        run(
            &mut session,
            "ALTER TABLE IF EXISTS nosuch ADD COLUMN b int"
        ),
        ["NOTICE: relation \"nosuch\" does not exist, skipping"]
    );
    assert_eq!(
        run(
            &mut session,
            "DROP VIEW IF EXISTS v0; DROP INDEX IF EXISTS i0; DROP SEQUENCE IF EXISTS s0"
        ),
        [
            "NOTICE: view \"v0\" does not exist, skipping",
            "NOTICE: index \"i0\" does not exist, skipping",
            "NOTICE: sequence \"s0\" does not exist, skipping",
        ]
    );
    assert_eq!(
        run(&mut session, "DROP FUNCTION IF EXISTS f0(int, text)"),
        ["NOTICE: function f0(pg_catalog.int4, text) does not exist, skipping"]
    );
    assert_eq!(
        run(&mut session, "DROP SCHEMA IF EXISTS s0"),
        ["NOTICE: schema \"s0\" does not exist, skipping"]
    );

    session
        .execute("CREATE TABLE p(a int PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE c(x int REFERENCES p)")
        .unwrap();
    let error = failure(&mut session, "DROP TABLE p");
    assert_eq!(error.sqlstate(), "2BP01");
    assert_eq!(
        detail(&error).as_deref(),
        Some("constraint c_x_fkey on table c depends on table p")
    );
    assert_eq!(
        hint(&error).as_deref(),
        Some("Use DROP ... CASCADE to drop the dependent objects too.")
    );
    let error = failure(&mut session, "TRUNCATE p");
    assert_eq!(error.sqlstate(), "0A000");
    assert_eq!(
        detail(&error).as_deref(),
        Some("Table \"c\" references \"p\".")
    );
    assert_eq!(
        run(&mut session, "TRUNCATE p CASCADE"),
        ["NOTICE: truncate cascades to table \"c\""]
    );
    assert_eq!(
        run(&mut session, "DROP TABLE p CASCADE"),
        ["NOTICE: drop cascades to constraint c_x_fkey on table c"]
    );
}

#[test]
fn raise_levels_keep_their_severity() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "DO $$BEGIN RAISE NOTICE 'n'; RAISE WARNING 'w'; RAISE INFO 'i'; RAISE LOG 'l'; RAISE DEBUG 'd'; END$$",
        )
        .unwrap();
    assert_eq!(notices(&result), ["NOTICE: n", "WARNING: w", "INFO: i"]);
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect()
}

/// point, line, lseg, box, path, polygon and circle columns store their
/// canonical text, survive a reopen in both storage modes and index with GiST.
#[test]
fn geometric_columns_store_query_and_index() {
    use bicdb_core::{DbConfig, StorageMode};
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let open = || {
            BicDb::open_with_config(
                root.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap()
        };
        {
            let mut db = open();
            let mut session = SqlSession::new(&mut db);
            session
                .execute(
                    "CREATE TABLE shapes(id int PRIMARY KEY, p point, l line, s lseg, b box, \
                     pa path, po polygon, c circle)",
                )
                .unwrap();
            session
                .execute(
                    "INSERT INTO shapes VALUES \
                     (1, '(1,2)', '{1,-1,0}', '[(0,0),(1,1)]', '(2,2),(0,0)', '[(0,0),(1,1)]', \
                      '((0,0),(1,1),(2,0))', '<(1,1),2>'), \
                     (2, '(-3.5, 4e1)', NULL, NULL, '1,5,3,1', '((0,0),(4,4))', NULL, '((5,5),1)')",
                )
                .unwrap();
            session
                .execute("CREATE INDEX shapes_b ON shapes USING gist (b)")
                .unwrap();
            let error = failure(
                &mut session,
                "INSERT INTO shapes (id, b) VALUES (3, '(1,2')",
            );
            assert_eq!(error.sqlstate(), "22P02");
            assert_eq!(
                error.to_string(),
                "invalid input syntax for type box: \"(1,2\""
            );
        }
        let mut db = open();
        let mut session = SqlSession::new(&mut db);
        assert_eq!(
            rows(
                &mut session,
                "SELECT p, l, s, b, pa, po, c FROM shapes WHERE id = 1"
            ),
            [[
                "(1,2)",
                "{1,-1,0}",
                "[(0,0),(1,1)]",
                "(2,2),(0,0)",
                "[(0,0),(1,1)]",
                "((0,0),(1,1),(2,0))",
                "<(1,1),2>",
            ]],
            "{mode:?}"
        );
        assert_eq!(
            rows(&mut session, "SELECT b, pa, c FROM shapes WHERE id = 2"),
            [["(3,5),(1,1)", "((0,0),(4,4))", "<(5,5),1>"]],
            "{mode:?}"
        );
        assert_eq!(
            rows(
                &mut session,
                "SELECT p[0], p[1], b[0], b[1][0] FROM shapes WHERE id = 2"
            ),
            [["-3.5", "40", "(3,5)", "1"]],
            "{mode:?}"
        );
        assert_eq!(
            rows(
                &mut session,
                "SELECT id FROM shapes WHERE b && box '(1.5,1.5),(1.6,1.6)' ORDER BY id"
            ),
            [["1"], ["2"]],
            "{mode:?}"
        );
        assert_eq!(
            rows(
                &mut session,
                "SELECT id FROM shapes WHERE c @> point '(5,5.5)'"
            ),
            [["2"]],
            "{mode:?}"
        );
    }
}
