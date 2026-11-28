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
fn simple_view_accepts_insert_update_delete_and_returning() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE base(id int, v int, note text DEFAULT 'd'); \
             CREATE VIEW renamed AS SELECT id AS key, v, v * 2 AS doubled FROM base WHERE v >= 0",
        )
        .unwrap();
    let inserted = session
        .execute("INSERT INTO renamed (key, v) VALUES (1, 10), (2, 20) RETURNING *")
        .unwrap();
    assert_eq!(inserted.columns, vec!["key", "v", "doubled"]);
    assert_eq!(
        inserted.rows,
        vec![
            vec![int(1), int(10), int(20)],
            vec![int(2), int(20), int(40)]
        ]
    );
    let updated = session
        .execute("UPDATE renamed SET v = v + 1 WHERE doubled > 30 RETURNING key, doubled")
        .unwrap();
    assert_eq!(updated.command_tag.as_deref(), Some("UPDATE 1"));
    assert_eq!(updated.rows, vec![vec![int(2), int(42)]]);
    // Rows outside the view's WHERE are not reachable through it.
    session
        .execute("INSERT INTO base (id, v) VALUES (3, -5)")
        .unwrap();
    let deleted = session.execute("DELETE FROM renamed").unwrap();
    assert_eq!(deleted.command_tag.as_deref(), Some("DELETE 2"));
    assert_eq!(
        rows(&mut session, "SELECT id, v, note FROM base ORDER BY id"),
        vec![vec![int(3), int(-5), text("d")]]
    );
}

#[test]
fn non_updatable_views_and_columns_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE t(a int, secret int); \
             CREATE VIEW counted AS SELECT count(*) AS n FROM t; \
             CREATE VIEW computed AS SELECT a, a * 2 AS twice FROM t; \
             CREATE VIEW narrow AS SELECT a FROM t",
        )
        .unwrap();
    for (sql, sqlstate) in [
        ("INSERT INTO counted VALUES (1)", "0A000"),
        ("INSERT INTO computed (a, twice) VALUES (1, 2)", "0A000"),
        ("UPDATE computed SET twice = 1", "0A000"),
        // A base column the view does not expose stays out of reach.
        ("DELETE FROM narrow WHERE secret = 1", "42703"),
        ("UPDATE narrow SET a = 1 WHERE t.secret = 1", "42P01"),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
}

#[test]
fn check_option_is_enforced_local_and_cascaded() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE t(a int); \
             CREATE VIEW positive AS SELECT a FROM t WHERE a > 0; \
             CREATE VIEW small_cascaded AS SELECT a FROM positive WHERE a < 10 WITH CASCADED CHECK OPTION; \
             CREATE VIEW small_local AS SELECT a FROM positive WHERE a < 10 WITH LOCAL CHECK OPTION",
        )
        .unwrap();
    session
        .execute("INSERT INTO small_cascaded VALUES (5)")
        .unwrap();
    let error = session
        .execute("INSERT INTO small_cascaded VALUES (-5)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "44000");
    assert!(error.to_string().contains("\"positive\""), "{error}");
    // LOCAL checks only its own condition.
    session
        .execute("INSERT INTO small_local VALUES (-5)")
        .unwrap();
    let error = session
        .execute("INSERT INTO small_local VALUES (50)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "44000");
    // A failed check leaves no row behind, also mid-statement.
    let error = session
        .execute("INSERT INTO small_cascaded VALUES (6), (60)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "44000");
    let error = session
        .execute("UPDATE small_cascaded SET a = a * 100")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "44000");
    assert_eq!(
        rows(&mut session, "SELECT a FROM t ORDER BY a"),
        vec![vec![int(-5)], vec![int(5)]]
    );
    let error = session
        .execute("CREATE VIEW grouped AS SELECT DISTINCT a FROM t WITH CHECK OPTION")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
}

#[test]
fn view_privileges_follow_the_owner_and_invoker_rules() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE clerk; CREATE TABLE ledger(a int, who text); \
             CREATE VIEW definer_view AS SELECT a FROM ledger; \
             CREATE VIEW invoker_view WITH (security_invoker = true) AS SELECT a FROM ledger; \
             GRANT SELECT, INSERT, UPDATE, DELETE ON definer_view TO clerk; \
             GRANT SELECT, INSERT ON invoker_view TO clerk; \
             CREATE VIEW read_only AS SELECT a FROM ledger; GRANT SELECT ON read_only TO clerk",
        )
        .unwrap();
    session.execute("SET ROLE clerk").unwrap();
    // The definer view's owner holds the table privileges.
    session
        .execute("INSERT INTO definer_view VALUES (1)")
        .unwrap();
    session.execute("UPDATE definer_view SET a = 2").unwrap();
    // The caller needs the privilege on the view itself.
    let error = session
        .execute("INSERT INTO read_only VALUES (3)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    // A security_invoker view checks the base table as the caller.
    let error = session
        .execute("INSERT INTO invoker_view VALUES (3)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    // The caller still cannot write the table directly.
    let error = session
        .execute("INSERT INTO ledger VALUES (4)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    session.execute("RESET ROLE").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM ledger"),
        vec![vec![int(2)]]
    );
}

#[test]
fn row_level_security_of_the_base_table_still_applies() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE tenant; CREATE TABLE docs(a int, who text); \
             INSERT INTO docs VALUES (1, 'tenant'), (2, 'other'); \
             ALTER TABLE docs ENABLE ROW LEVEL SECURITY; \
             CREATE POLICY own ON docs USING (who = current_user); \
             GRANT SELECT, UPDATE, DELETE ON docs TO tenant; \
             CREATE VIEW invoker_docs WITH (security_invoker = true) AS SELECT a, who FROM docs; \
             CREATE VIEW definer_docs AS SELECT a, who FROM docs; \
             GRANT SELECT, UPDATE, DELETE ON invoker_docs, definer_docs TO tenant",
        )
        .unwrap();
    session.execute("SET ROLE tenant").unwrap();
    session
        .execute("UPDATE invoker_docs SET a = a + 10")
        .unwrap();
    session
        .execute("DELETE FROM invoker_docs WHERE who = 'other'")
        .unwrap();
    // Writing as the view owner under row security is refused, not guessed.
    let error = session
        .execute("UPDATE definer_docs SET a = 0")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    session.execute("RESET ROLE").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a, who FROM docs ORDER BY a"),
        vec![vec![int(2), text("other")], vec![int(11), text("tenant")]]
    );
}

#[test]
fn instead_of_triggers_handle_writes_to_views() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE store(a int, b int); INSERT INTO store VALUES (1, 1), (2, 2); \
             CREATE VIEW totals AS SELECT count(*) AS n FROM store; \
             CREATE VIEW pairs AS SELECT a, b, a + b AS s FROM store",
        )
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION pairs_write() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
               IF TG_OP = 'INSERT' THEN \
                 IF NEW.a < 0 THEN RETURN NULL; END IF; \
                 INSERT INTO store VALUES (NEW.a, NEW.b); RETURN NEW; \
               ELSIF TG_OP = 'UPDATE' THEN \
                 UPDATE store SET b = NEW.b WHERE a = OLD.a; RETURN NEW; \
               ELSE DELETE FROM store WHERE a = OLD.a; RETURN OLD; END IF; \
             END $$",
        )
        .unwrap();
    session
        .execute(
            "CREATE TRIGGER pairs_t INSTEAD OF INSERT OR UPDATE OR DELETE ON pairs \
             FOR EACH ROW EXECUTE FUNCTION pairs_write()",
        )
        .unwrap();
    let inserted = session
        .execute("INSERT INTO pairs (a, b) VALUES (3, 3), (-1, 0)")
        .unwrap();
    assert_eq!(inserted.command_tag.as_deref(), Some("INSERT 0 1"));
    let updated = session
        .execute("UPDATE pairs SET b = 20 WHERE a = 2 RETURNING a, b")
        .unwrap();
    assert_eq!(updated.rows, vec![vec![int(2), int(20)]]);
    let deleted = session.execute("DELETE FROM pairs WHERE s = 2").unwrap();
    assert_eq!(deleted.command_tag.as_deref(), Some("DELETE 1"));
    assert_eq!(
        rows(&mut session, "SELECT a, b FROM store ORDER BY a"),
        vec![vec![int(2), int(20)], vec![int(3), int(3)]]
    );
    // INSTEAD OF only on views; views take no row-level BEFORE/AFTER triggers.
    let error = session
        .execute(
            "CREATE TRIGGER bad INSTEAD OF INSERT ON store FOR EACH ROW EXECUTE FUNCTION pairs_write()",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42809");
    let error = session
        .execute(
            "CREATE TRIGGER bad BEFORE INSERT ON totals FOR EACH ROW EXECUTE FUNCTION pairs_write()",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42809");
}

#[test]
fn view_upsert_maps_aliases_partial_arbiters_and_excluded_columns() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE base(id int, fruit text); \
             CREATE UNIQUE INDEX berries ON base(id) WHERE fruit LIKE '%berry'; \
             CREATE VIEW view_fruit AS SELECT id AS key, fruit AS label, upper(fruit) AS caps \
               FROM base AS original; \
             CREATE VIEW view_labels AS SELECT key AS id, label AS name, caps AS upper_name \
               FROM view_fruit; \
             INSERT INTO view_labels(id, name) VALUES (1, 'blueberry')",
        )
        .unwrap();
    let updated = session
        .execute(
            "INSERT INTO view_labels AS dest(id, name) VALUES (1, 'cranberry') \
             ON CONFLICT(id) WHERE dest.name LIKE '%berry' \
             DO UPDATE SET name = excluded.upper_name || ':' || dest.name \
             WHERE dest.id = excluded.id RETURNING dest.*",
        )
        .unwrap();
    assert_eq!(updated.columns, vec!["id", "name", "upper_name"]);
    assert_eq!(
        updated.rows,
        vec![vec![
            int(1),
            text("CRANBERRY:blueberry"),
            text("CRANBERRY:BLUEBERRY")
        ]]
    );
    let error = session
        .execute("INSERT INTO base VALUES (1, 'strawberry')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23505");
    session
        .execute("INSERT INTO base VALUES (1, 'citrus'), (1, 'melon')")
        .unwrap();

    for (sql, sqlstate) in [
        (
            "INSERT INTO view_labels(id, name) VALUES (1, 'mulberry') ON CONFLICT(id) DO NOTHING",
            "42P10",
        ),
        (
            "INSERT INTO view_labels(id, name) VALUES (1, 'mulberry') ON CONFLICT(upper_name) DO NOTHING",
            "42P10",
        ),
        (
            "INSERT INTO view_labels(id, name) VALUES (1, 'mulberry') ON CONFLICT(missing) DO NOTHING",
            "42703",
        ),
        (
            "INSERT INTO view_labels(id, name) VALUES (1, 'mulberry') ON CONFLICT ON CONSTRAINT berries DO NOTHING",
            "42704",
        ),
        (
            "INSERT INTO view_labels(id, name) VALUES (1, 'mulberry') ON CONFLICT(id) WHERE name LIKE '%berry' DO UPDATE SET name = name",
            "42702",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
}

#[test]
fn view_upsert_checks_conflicting_visibility_and_written_rows_separately() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE base(id int PRIMARY KEY, live bool, label text); \
             INSERT INTO base VALUES (1, false, 'hidden'), (2, true, 'visible'); \
             CREATE VIEW inner_view AS SELECT * FROM base WHERE live; \
             CREATE VIEW checked AS SELECT id AS key, live, label FROM inner_view \
               WHERE length(label) > 3 WITH CASCADED CHECK OPTION",
        )
        .unwrap();
    let error = session
        .execute(
            "INSERT INTO checked VALUES (1, true, 'new name') ON CONFLICT(key) \
               DO UPDATE SET label = excluded.label WHERE false",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "55000");
    assert!(error.to_string().contains("inner_view"), "{error}");
    assert!(rows(
        &mut session,
        "INSERT INTO checked VALUES (1, false, 'bad') ON CONFLICT(key) DO NOTHING RETURNING key"
    )
    .is_empty());
    assert!(rows(
        &mut session,
        "INSERT INTO checked VALUES (2, false, 'bad') ON CONFLICT(key) \
           DO UPDATE SET live = excluded.live WHERE false RETURNING key"
    )
    .is_empty());
    let error = session
        .execute(
            "INSERT INTO checked VALUES (3, true, 'inserted'), (2, false, 'still visible') \
               ON CONFLICT(key) DO UPDATE SET live = excluded.live",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "44000");
    assert!(error.to_string().contains("inner_view"), "{error}");
    assert_eq!(
        rows(&mut session, "SELECT id, label FROM base ORDER BY id"),
        vec![vec![int(1), text("hidden")], vec![int(2), text("visible")]]
    );
    assert_eq!(
        rows(
            &mut session,
            "INSERT INTO inner_view VALUES (2, false, 'visible') ON CONFLICT(id) \
               DO UPDATE SET live = excluded.live RETURNING live"
        ),
        vec![vec![SqlValue::Bool(false)]]
    );
}

#[test]
fn view_upsert_checks_the_caller_and_view_owner_privileges() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE writer; CREATE TABLE base(id int PRIMARY KEY, name text, hidden text); \
             INSERT INTO base VALUES (1, 'old', 'secret'); \
             CREATE VIEW definer_view AS SELECT id AS key, name, hidden FROM base; \
             CREATE VIEW invoker_view WITH (security_invoker = true) AS SELECT * FROM base; \
             GRANT INSERT, UPDATE(name), SELECT(key, name) ON definer_view TO writer; \
             GRANT INSERT, UPDATE, SELECT ON invoker_view TO writer; SET ROLE writer",
        )
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "INSERT INTO definer_view(key, name) VALUES (1, 'new') ON CONFLICT(key) \
               DO UPDATE SET name = excluded.name RETURNING name"
        ),
        vec![vec![text("new")]]
    );
    for sql in [
        "INSERT INTO definer_view(key, name) VALUES (1, 'bad') ON CONFLICT(key) \
           WHERE hidden IS NOT NULL DO NOTHING",
        "INSERT INTO definer_view(key, name) VALUES (1, 'bad') ON CONFLICT(key) \
           DO UPDATE SET name = excluded.hidden",
        "INSERT INTO definer_view(key, name) VALUES (1, 'bad') ON CONFLICT(key) \
           DO UPDATE SET hidden = 'overwritten'",
        "INSERT INTO invoker_view(id, name) VALUES (1, 'bad') ON CONFLICT(id) \
           DO UPDATE SET name = excluded.name",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{sql}: {error}");
    }
    session.execute("RESET ROLE").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT name, hidden FROM base"),
        vec![vec![text("new"), text("secret")]]
    );
}

#[test]
fn view_instead_of_insert_accepts_do_nothing_without_inference() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE base(id int PRIMARY KEY); CREATE VIEW triggered AS SELECT id FROM base; \
             CREATE FUNCTION insert_view() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN INSERT INTO base VALUES (NEW.id) ON CONFLICT(id) DO NOTHING; RETURN NEW; END $$; \
             CREATE TRIGGER write_view INSTEAD OF INSERT ON triggered FOR EACH ROW \
             EXECUTE FUNCTION insert_view()",
        )
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            rows(
                &mut session,
                "INSERT INTO triggered VALUES (1) ON CONFLICT DO NOTHING RETURNING id"
            ),
            vec![vec![int(1)]]
        );
    }
    for (sql, sqlstate) in [
        (
            "INSERT INTO triggered VALUES (2) ON CONFLICT(id) DO NOTHING",
            "42P10",
        ),
        (
            "INSERT INTO triggered VALUES (2) ON CONFLICT(id) DO UPDATE SET id = excluded.id",
            "42P10",
        ),
        (
            "INSERT INTO triggered VALUES (2) ON CONFLICT ON CONSTRAINT base_pkey DO NOTHING",
            "42704",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
    assert_eq!(
        rows(&mut session, "SELECT id FROM base"),
        vec![vec![int(1)]]
    );
}

#[test]
fn view_upsert_keeps_base_relation_outside_the_expression_scope() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE base(id int PRIMARY KEY, shown int, other int); \
             CREATE VIEW visible AS SELECT id, shown FROM base",
        )
        .unwrap();
    for sql in [
        "INSERT INTO visible VALUES (1, 10) ON CONFLICT(id) \
           WHERE public.base.other > 0 DO NOTHING",
        "INSERT INTO visible VALUES (1, 10) ON CONFLICT(id) \
           DO UPDATE SET shown = public.base.other",
        "INSERT INTO visible VALUES (1, 10) ON CONFLICT(id) \
           DO NOTHING RETURNING public.base.other",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42P01", "{sql}: {error}");
    }
    assert!(rows(&mut session, "SELECT * FROM base").is_empty());
}

#[test]
fn qualified_view_upsert_resolves_sql_names_before_lowering() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE SCHEMA demo; CREATE TABLE demo.base(id int, shown int, other int); \
             CREATE UNIQUE INDEX visible_key ON demo.base(id) WHERE shown > 0; \
             CREATE VIEW demo.visible AS SELECT id AS key, shown AS item FROM demo.base; \
             INSERT INTO demo.visible VALUES (1, 10)",
        )
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "INSERT INTO demo.visible VALUES (1, 12) ON CONFLICT(key) WHERE visible.item > 0 \
               DO UPDATE SET item = visible.item + excluded.item RETURNING key, item"
        ),
        vec![vec![int(1), int(22)]]
    );
    for query in [
        "INSERT INTO demo.visible VALUES (1, 12) ON CONFLICT(key) WHERE base.other > 0 DO NOTHING",
        "INSERT INTO demo.visible VALUES (1, 12) ON CONFLICT(key) WHERE item > 0 \
           DO UPDATE SET item = base.other",
        "INSERT INTO demo.visible VALUES (1, 12) ON CONFLICT DO NOTHING RETURNING base.other",
    ] {
        let error = session.execute(query).unwrap_err();
        assert_eq!(error.sqlstate(), "42P01", "{query}: {error}");
    }
}
