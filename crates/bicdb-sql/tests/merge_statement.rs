//! PostgreSQL MERGE: several WHEN clauses in order, every action kind, the
//! three match kinds, RETURNING with merge_action() and old/new, and the
//! statement-wide rules (one change per target row, atomic failure, upfront
//! privileges). Expected results were checked against PostgreSQL 18.4.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;

fn open() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    (root, db)
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn run(session: &mut SqlSession<'_>, statements: &[&str]) {
    for sql in statements {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

fn grid(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

fn setup(session: &mut SqlSession<'_>) {
    run(
        session,
        &[
            "CREATE TABLE mt(id INT PRIMARY KEY, n INT, note TEXT DEFAULT 'dflt')",
            "CREATE TABLE ms(id INT, n INT)",
            "INSERT INTO mt VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')",
            "INSERT INTO ms VALUES (1,100),(3,300),(4,400),(5,-5)",
        ],
    );
}

#[test]
fn clauses_run_in_order_with_update_delete_insert_and_do_nothing() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    let result = session
        .execute(
            "MERGE INTO mt t USING ms s ON t.id = s.id \
             WHEN MATCHED AND s.n > 200 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET n = t.n + s.n \
             WHEN NOT MATCHED AND s.n < 0 THEN DO NOTHING \
             WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n)",
        )
        .unwrap();
    assert_eq!(result.command_tag.as_deref(), Some("MERGE 3"));
    assert_eq!(
        rows(&mut session, "SELECT id, n, note FROM mt ORDER BY id"),
        grid(&[&["1", "110", "a"], &["2", "20", "b"], &["4", "400", "dflt"]])
    );
}

#[test]
fn not_matched_by_source_and_default_values() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    run(
        &mut session,
        &[
            "CREATE TABLE md(id SERIAL PRIMARY KEY, tag TEXT DEFAULT 'none')",
            "MERGE INTO mt t USING ms s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE AND t.n > 25 THEN DELETE \
             WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'orphan' \
             WHEN NOT MATCHED BY TARGET THEN INSERT VALUES (s.id, s.n, DEFAULT)",
            "MERGE INTO md d USING (VALUES (1), (2)) v(k) ON false \
             WHEN NOT MATCHED THEN INSERT DEFAULT VALUES",
        ],
    );
    assert_eq!(
        rows(&mut session, "SELECT id, n, note FROM mt ORDER BY id"),
        grid(&[
            &["1", "10", "a"],
            &["2", "20", "orphan"],
            &["3", "30", "c"],
            &["4", "400", "dflt"],
            &["5", "-5", "dflt"],
        ])
    );
    assert_eq!(
        rows(&mut session, "SELECT id, tag FROM md ORDER BY id"),
        grid(&[&["1", "none"], &["2", "none"]])
    );
}

#[test]
fn returning_reports_action_old_and_new_images() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    let result = session
        .execute(
            "MERGE INTO mt t USING (SELECT * FROM ms WHERE id = 1) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET n = s.n \
             RETURNING merge_action(), t.id, old.n AS before, new.n AS after, s.n",
        )
        .unwrap();
    assert_eq!(
        result.columns,
        ["merge_action", "id", "before", "after", "n"]
    );
    assert_eq!(result.command_tag.as_deref(), Some("MERGE 1"));
    let cells: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect();
    assert_eq!(cells, grid(&[&["UPDATE", "1", "10", "100", "100"]]));
    let result = session
        .execute(
            "MERGE INTO mt t USING (VALUES (2, 0), (9, 90)) s(id, n) ON t.id = s.id \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n) \
             RETURNING merge_action() AS action, *",
        )
        .unwrap();
    assert_eq!(result.columns, ["action", "id", "n", "id", "n", "note"]);
    let mut cells: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect();
    cells.sort();
    assert_eq!(
        cells,
        grid(&[
            &["DELETE", "2", "0", "2", "20", "b"],
            &["INSERT", "9", "90", "9", "90", "dflt"],
        ])
    );
}

#[test]
fn a_target_row_changed_twice_fails_the_whole_statement() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    let error = session
        .execute(
            "MERGE INTO mt t USING (SELECT * FROM ms UNION ALL SELECT 1, 101) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET n = s.n \
             WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n)",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "21000");
    assert_eq!(
        rows(&mut session, "SELECT count(*), sum(n) FROM mt"),
        grid(&[&["3", "60"]])
    );
    // DO NOTHING changes nothing, so a repeated match is fine there.
    let result = session
        .execute(
            "MERGE INTO mt t USING (SELECT * FROM ms UNION ALL SELECT 1, 101) s ON t.id = s.id \
             WHEN MATCHED AND s.n = 101 THEN UPDATE SET n = s.n \
             WHEN MATCHED THEN DO NOTHING",
        )
        .unwrap();
    assert_eq!(result.command_tag.as_deref(), Some("MERGE 1"));
}

#[test]
fn constraint_failure_rolls_back_every_action() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    let error = session
        .execute(
            "MERGE INTO mt t USING ms s ON t.id = s.id \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED THEN INSERT (id, n) VALUES (1, s.n)",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23505");
    assert_eq!(
        rows(&mut session, "SELECT id FROM mt ORDER BY id"),
        grid(&[&["1"], &["2"], &["3"]])
    );
}

#[test]
fn shape_errors_match_postgres() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    for (sql, sqlstate) in [
        (
            "MERGE INTO mt t USING ms s ON t.id = s.id WHEN MATCHED THEN DELETE WHEN MATCHED THEN DO NOTHING",
            "42601",
        ),
        (
            "MERGE INTO mt t USING ms s ON t.id = s.id WHEN NOT MATCHED AND t.n > 1 THEN DO NOTHING",
            "42P01",
        ),
        (
            "MERGE INTO mt t USING ms s ON t.id = s.id WHEN NOT MATCHED BY SOURCE AND s.n > 1 THEN DELETE",
            "42P01",
        ),
        (
            "MERGE INTO mt t USING ms t ON t.id = t.id WHEN MATCHED THEN DELETE",
            "42712",
        ),
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), sqlstate, "{sql}");
    }
}

#[test]
fn triggers_fire_for_each_action() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    run(
        &mut session,
        &[
            "CREATE TABLE mlog(op TEXT, id INT)",
            "CREATE FUNCTION mlog_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN \
             IF TG_OP = 'DELETE' THEN INSERT INTO mlog VALUES (TG_OP, OLD.id); RETURN OLD; END IF; \
             INSERT INTO mlog VALUES (TG_OP, NEW.id); RETURN NEW; END $$",
            "CREATE TRIGGER mlog_t AFTER INSERT OR UPDATE OR DELETE ON mt FOR EACH ROW EXECUTE FUNCTION mlog_fn()",
            "MERGE INTO mt t USING ms s ON t.id = s.id \
             WHEN MATCHED AND s.id = 1 THEN UPDATE SET n = 0 \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED AND s.id = 4 THEN INSERT (id, n) VALUES (s.id, s.n)",
        ],
    );
    assert_eq!(
        rows(&mut session, "SELECT op, id FROM mlog ORDER BY id"),
        grid(&[&["UPDATE", "1"], &["DELETE", "3"], &["INSERT", "4"]])
    );
}

#[test]
fn every_action_privilege_is_required_up_front() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    run(
        &mut session,
        &[
            "CREATE ROLE merger LOGIN",
            "GRANT SELECT, UPDATE ON mt TO merger",
            "GRANT SELECT ON ms TO merger",
            "SET SESSION AUTHORIZATION merger",
        ],
    );
    // No row reaches the DELETE, but PostgreSQL still requires the grant.
    let error = session
        .execute(
            "MERGE INTO mt t USING ms s ON t.id = s.id AND false \
             WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN DO NOTHING",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    let error = session
        .execute(
            "MERGE INTO mt t USING ms s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET n = 0 \
             WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n)",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    let result = session
        .execute(
            "MERGE INTO mt t USING ms s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET n = 0 WHEN NOT MATCHED THEN DO NOTHING",
        )
        .unwrap();
    assert_eq!(result.command_tag.as_deref(), Some("MERGE 2"));
}

#[test]
fn placeholders_do_not_touch_strings_or_other_statements() {
    let (_root, mut db) = open();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    assert_eq!(
        rows(
            &mut session,
            "SELECT 'MERGE ... WHEN MATCHED THEN DO NOTHING', CASE WHEN true THEN 'DEFAULT VALUES' END"
        ),
        grid(&[&["MERGE ... WHEN MATCHED THEN DO NOTHING", "DEFAULT VALUES"]])
    );
    run(
        &mut session,
        &["INSERT INTO mt VALUES (7, 7, 'x') ON CONFLICT DO NOTHING"],
    );
}
