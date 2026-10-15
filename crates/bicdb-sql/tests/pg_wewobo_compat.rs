//! PostgreSQL behaviour the WeWoBo owner API depends on (found by loading its database into BicDB,
//! 2026-10-13). Each test is the PostgreSQL result for a construct BicDB refused, crashed on or got wrong.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    (dir, db)
}

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

#[test]
fn partial_unique_index_only_constrains_rows_matching_its_predicate() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE invites (space int, email text, claimed_at timestamptz)")
        .unwrap();
    // Existing duplicates outside the predicate do not stop the index from being built.
    s.execute("INSERT INTO invites VALUES (1,'a@x',now()),(1,'a@x',now()),(1,'a@x',NULL)")
        .unwrap();
    s.execute(
        "CREATE UNIQUE INDEX invites_open_uq ON invites (space, email) WHERE claimed_at IS NULL",
    )
    .unwrap();
    // More claimed rows are fine; a second open row is not.
    s.execute("INSERT INTO invites VALUES (1,'a@x',now())")
        .unwrap();
    assert!(
        s.execute("INSERT INTO invites VALUES (1,'a@x',NULL)")
            .is_err(),
        "second open invitation must conflict"
    );
    // Moving a row into the predicate is checked too; moving it out frees the key.
    assert!(s
        .execute("UPDATE invites SET claimed_at=NULL WHERE claimed_at IS NOT NULL")
        .is_err());
    s.execute("UPDATE invites SET claimed_at=now() WHERE claimed_at IS NULL")
        .unwrap();
    s.execute("INSERT INTO invites VALUES (1,'a@x',NULL)")
        .unwrap();
    // ON CONFLICT can name the partial index by its columns and predicate.
    s.execute("INSERT INTO invites VALUES (1,'a@x',NULL) ON CONFLICT (space, email) WHERE claimed_at IS NULL DO NOTHING").unwrap();
    let open = s
        .execute("SELECT count(*) FROM invites WHERE claimed_at IS NULL")
        .unwrap()
        .rows;
    assert_eq!(open, vec![vec![SqlValue::Int(1)]]);
    let all = s.execute("SELECT count(*) FROM invites").unwrap().rows;
    assert_eq!(all, vec![vec![SqlValue::Int(5)]]);
    // Rows that never enter the predicate can repeat freely.
    s.execute("CREATE TABLE pu_t (a int, b bool); CREATE UNIQUE INDEX pu_t_uq ON pu_t (a) WHERE b")
        .unwrap();
    s.execute("INSERT INTO pu_t VALUES (1,false),(1,false),(1,true)")
        .unwrap();
    assert!(s.execute("INSERT INTO pu_t VALUES (1,true)").is_err());
}

#[test]
fn a_partial_index_never_answers_a_query_for_rows_outside_its_predicate() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    // network_notifications: one unseen mention per (recipient, post), other kinds unconstrained.
    s.execute("CREATE TABLE notes (id int PRIMARY KEY, recipient text, post int, kind text)")
        .unwrap();
    s.execute(
        "CREATE UNIQUE INDEX notes_mention_once ON notes (recipient, post) WHERE kind = 'mention'",
    )
    .unwrap();
    s.execute("INSERT INTO notes VALUES (1,'ben',1,'message'),(2,'ben',2,'message'),(3,'ben',3,'mention'),(4,'ann',1,'message')").unwrap();
    let count = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows;
    assert_eq!(
        count(&mut s, "SELECT count(*) FROM notes WHERE recipient='ben'"),
        vec![vec![SqlValue::Int(3)]]
    );
    assert_eq!(
        count(
            &mut s,
            "SELECT count(*) FROM notes WHERE recipient='ben' AND post=1"
        ),
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        count(
            &mut s,
            "SELECT count(*) FROM notes WHERE recipient='ben' AND kind='mention'"
        ),
        vec![vec![SqlValue::Int(1)]]
    );
    let joined = count(
        &mut s,
        "SELECT count(*) FROM (VALUES ('ben'),('ann')) v(r) JOIN notes n ON n.recipient=v.r",
    );
    assert_eq!(joined, vec![vec![SqlValue::Int(4)]]);
}

#[test]
fn lateral_set_returning_functions_work_inside_union_branches_and_views() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE members (id int, roles text[]); INSERT INTO members VALUES (1, ARRAY['admin','editor']), (2, ARRAY['reviewer']), (3, '{}')").unwrap();
    let union = "SELECT id, 'owner' AS role FROM members WHERE id = 1 \
                 UNION ALL SELECT m.id, r.role FROM members m CROSS JOIN LATERAL unnest(m.roles) r(role)";
    let expected = vec![
        vec![SqlValue::Int(1), text("admin")],
        vec![SqlValue::Int(1), text("editor")],
        vec![SqlValue::Int(1), text("owner")],
        vec![SqlValue::Int(2), text("reviewer")],
    ];
    let rows = s
        .execute(&format!("SELECT * FROM ({union}) q ORDER BY 1, 2"))
        .unwrap()
        .rows;
    assert_eq!(rows, expected);
    // Implicit lateral (comma) form and a WHERE on the function's column.
    let filtered = s.execute("SELECT 0 AS id, 'x' AS role WHERE false UNION ALL SELECT m.id, r.role FROM members m, unnest(m.roles) AS r(role) WHERE r.role = ANY (ARRAY['admin','reviewer']) ORDER BY 1").unwrap().rows;
    assert_eq!(
        filtered,
        vec![
            vec![SqlValue::Int(1), text("admin")],
            vec![SqlValue::Int(2), text("reviewer")]
        ]
    );
    s.execute(&format!("CREATE VIEW member_roles AS {union}"))
        .unwrap();
    let via_view = s
        .execute("SELECT id, role FROM member_roles ORDER BY 1, 2")
        .unwrap()
        .rows;
    assert_eq!(via_view, expected);
}

#[test]
fn function_bodies_may_contain_non_ascii_text() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute(
        "CREATE FUNCTION greet(t text) RETURNS text LANGUAGE sql AS $$ SELECT 'Grüß ' || t $$",
    )
    .unwrap();
    s.execute("CREATE FUNCTION fold(t text) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN replace(t, 'Ä', 'A') || '·é'; END $$").unwrap();
    assert_eq!(
        s.execute("SELECT greet('Zoë')").unwrap().rows,
        vec![vec![text("Grüß Zoë")]]
    );
    assert_eq!(
        s.execute("SELECT fold('Äpfel')").unwrap().rows,
        vec![vec![text("Apfel·é")]]
    );
}

#[test]
fn translate_replaces_and_deletes_characters_like_postgresql() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let rows = s.execute("SELECT translate('12345', '143', 'ax'), translate('Ärger über Öl', 'ÄÖÜäöü', 'AOUaou'), translate(NULL, 'a', 'b'), translate('abc', '', 'x')").unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![
            text("a2x5"),
            text("Arger uber Ol"),
            SqlValue::Null,
            text("abc")
        ]]
    );
    let rows = s.execute("SELECT left('Grüße', 3), right('Grüße', 2), left('abc', -1), right('abc', -2), left('ab', 9), left(NULL, 1)").unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![
            text("Grü"),
            text("ße"),
            text("ab"),
            text("c"),
            text("ab"),
            SqlValue::Null
        ]]
    );
    s.execute("CREATE TABLE names (n text); INSERT INTO names VALUES ('Café'), ('Señor')")
        .unwrap();
    assert_eq!(
        s.execute("SELECT left(n, 2) FROM names ORDER BY 1")
            .unwrap()
            .rows,
        vec![vec![text("Ca")], vec![text("Se")]]
    );
    let rows = s
        .execute("SELECT translate(n, 'éñ', 'en') FROM names ORDER BY 1")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![text("Cafe")], vec![text("Senor")]]);
}

#[test]
fn database_level_custom_settings_apply_to_new_sessions() {
    let (_dir, mut db) = db();
    let current = {
        let mut s = SqlSession::new(&mut db);
        let name = s.execute("SELECT current_database()").unwrap().rows[0][0].clone();
        let SqlValue::String(name) = name else {
            panic!("database name")
        };
        s.execute(&format!(
            "ALTER DATABASE {name} SET wewobo.network_tenant = 'shared-tenant'"
        ))
        .unwrap();
        name
    };
    let mut s = SqlSession::new(&mut db);
    let rows = s
        .execute("SELECT current_setting('wewobo.network_tenant', true)")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![text("shared-tenant")]]);
    // A session may still override it; RESET returns to the database default.
    s.execute("SET wewobo.network_tenant = 'mine'").unwrap();
    assert_eq!(
        s.execute("SELECT current_setting('wewobo.network_tenant')")
            .unwrap()
            .rows,
        vec![vec![text("mine")]]
    );
    s.execute("RESET wewobo.network_tenant").unwrap();
    assert_eq!(
        s.execute("SELECT current_setting('wewobo.network_tenant')")
            .unwrap()
            .rows,
        vec![vec![text("shared-tenant")]]
    );
    drop(s);
    let mut s = SqlSession::new(&mut db);
    s.execute(&format!(
        "ALTER DATABASE {current} RESET wewobo.network_tenant"
    ))
    .unwrap();
    drop(s);
    let mut s = SqlSession::new(&mut db);
    assert_eq!(
        s.execute("SELECT current_setting('wewobo.network_tenant', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn order_by_position_sorts_by_that_output_column() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE t (a int, b text); INSERT INTO t VALUES (1,'b'), (1,'a'), (0,'z')")
        .unwrap();
    let sorted = vec![
        vec![SqlValue::Int(0), text("z")],
        vec![SqlValue::Int(1), text("a")],
        vec![SqlValue::Int(1), text("b")],
    ];
    for sql in [
        "SELECT a, b FROM t ORDER BY 1, 2",
        "SELECT * FROM t ORDER BY 1, 2",
        "SELECT a, b FROM t ORDER BY a, 2",
        "SELECT a AS x, b AS y FROM t ORDER BY 1, y",
        "SELECT a AS x, b AS y FROM t ORDER BY x, y",
        "SELECT a, b AS y FROM t ORDER BY a, y",
        "SELECT * FROM (SELECT a, b FROM t) q ORDER BY 1, 2",
        "SELECT a, b FROM t WHERE false UNION ALL SELECT a, b FROM t ORDER BY 1, 2",
        "SELECT * FROM (VALUES (1,'b'),(1,'a'),(0,'z')) v(a, b) ORDER BY 1, 2",
    ] {
        assert_eq!(s.execute(sql).unwrap().rows, sorted, "{sql}");
    }
    assert_eq!(
        s.execute("SELECT a, b FROM t ORDER BY 1 DESC, 2 DESC")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(1), text("b")],
            vec![SqlValue::Int(1), text("a")],
            vec![SqlValue::Int(0), text("z")]
        ]
    );
    assert_eq!(
        s.execute("SELECT b, count(*) FROM t GROUP BY 1 ORDER BY 2 DESC, 1")
            .unwrap()
            .rows,
        vec![
            vec![text("a"), SqlValue::Int(1)],
            vec![text("b"), SqlValue::Int(1)],
            vec![text("z"), SqlValue::Int(1)]
        ]
    );
}

#[test]
fn wewobo_course_slug_function_loads_and_runs() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE learning_courses (id int, slug text)")
        .unwrap();
    s.execute(r#"CREATE FUNCTION public.learning_course_slug(title text) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE base text; cand text; n int := 1;
BEGIN
  base := trim(both '-' from left(trim(both '-' from regexp_replace(lower(translate(COALESCE(title,''),'ÄÖÜäöüßÉÈÊËéèêëÁÀÂáàâÍÌÎíìîÓÒÔóòôÚÙÛúùûÑñÇç','AOUaousEEEEeeeeAAAaaaIIIiiiOOOoooUUUuuuNnCc')),'[^a-z0-9]+','-','g')),70));
  IF length(base) < 3 THEN base := 'course' || CASE WHEN base='' THEN '' ELSE '-' || base END; END IF;
  LOOP
    cand := CASE WHEN n=1 THEN base ELSE base || '-' || n END;
    EXIT WHEN NOT EXISTS (SELECT 1 FROM learning_courses WHERE slug=cand);
    n := n + 1;
  END LOOP;
  RETURN cand;
END $$"#).unwrap();
    assert_eq!(
        s.execute("SELECT learning_course_slug('Ayurveda für Ärzte')")
            .unwrap()
            .rows,
        vec![vec![text("ayurveda-fur-arzte")]]
    );
    s.execute("INSERT INTO learning_courses VALUES (1, 'ayurveda-fur-arzte')")
        .unwrap();
    assert_eq!(
        s.execute("SELECT learning_course_slug('Ayurveda für Ärzte')")
            .unwrap()
            .rows,
        vec![vec![text("ayurveda-fur-arzte-2")]]
    );
    assert_eq!(
        s.execute("SELECT learning_course_slug('Ω')").unwrap().rows,
        vec![vec![text("course")]]
    );
}

#[test]
fn plpgsql_loop_while_and_exit() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute(
        r#"CREATE FUNCTION first_power_over(lim int) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 1;
BEGIN
  LOOP
    n := n * 2;
    EXIT WHEN n > lim;
  END LOOP;
  RETURN n;
END $$"#,
    )
    .unwrap();
    s.execute(
        r#"CREATE FUNCTION sum_to(lim int) RETURNS int LANGUAGE plpgsql AS $$
DECLARE i int := 0; total int := 0;
BEGIN
  WHILE i < lim LOOP
    i := i + 1;
    CONTINUE WHEN i = 3;
    total := total + i;
  END LOOP;
  RETURN total;
END $$"#,
    )
    .unwrap();
    s.execute(
        r#"CREATE FUNCTION first_multiple(k int) RETURNS int LANGUAGE plpgsql AS $$
DECLARE found int := NULL;
BEGIN
  FOR i IN 1..100 LOOP
    IF i % k = 0 THEN
      found := i;
      EXIT;
    END IF;
  END LOOP;
  LOOP
    EXIT;
  END LOOP;
  RETURN found;
END $$"#,
    )
    .unwrap();
    assert_eq!(
        s.execute("SELECT first_power_over(100), sum_to(5), first_multiple(7)")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(128),
            SqlValue::Int(12),
            SqlValue::Int(7)
        ]]
    );
    let created = s.execute(
        "CREATE FUNCTION bad() RETURNS int LANGUAGE plpgsql AS $$ BEGIN EXIT; RETURN 1; END $$",
    );
    assert!(
        created.is_err() || s.execute("SELECT bad()").is_err(),
        "EXIT outside a loop"
    );
}

#[test]
fn database_owner_creates_trusted_extensions_only() {
    let (_dir, mut db) = db();
    {
        let mut admin = SqlSession::new(&mut db);
        admin
            .execute("CREATE ROLE app_owner LOGIN; CREATE ROLE someone LOGIN")
            .unwrap();
        let name = admin.execute("SELECT current_database()").unwrap().rows[0][0].clone();
        let SqlValue::String(name) = name else {
            panic!("database name")
        };
        admin
            .execute(&format!("ALTER DATABASE {name} OWNER TO app_owner"))
            .unwrap();
    }
    let mut owner = SqlSession::new_unprivileged(&mut db, "app_owner");
    owner
        .execute("CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public")
        .unwrap();
    assert!(
        owner
            .execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .is_err(),
        "untrusted needs a superuser"
    );
    drop(owner);
    let mut other = SqlSession::new_unprivileged(&mut db, "someone");
    assert!(
        other
            .execute("CREATE EXTENSION IF NOT EXISTS citext")
            .is_err(),
        "only the database owner"
    );
}

#[test]
fn common_postgresql_functions_work_on_table_rows() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE f (t text, n int, ts timestamptz, d timestamp)")
        .unwrap();
    s.execute(
        "INSERT INTO f VALUES ('Hello World', 7, '2026-10-13 12:34:56+00', '2026-03-31 10:00:00')",
    )
    .unwrap();
    s.execute("SET TIME ZONE 'UTC'").unwrap();
    let cases: Vec<(&str, SqlValue)> = vec![
        ("strpos(t, 'World')", SqlValue::Int(7)),
        ("strpos(t, 'zz')", SqlValue::Int(0)),
        ("position('World' in t)", SqlValue::Int(7)),
        ("lpad(t, 13, '*')", text("**Hello World")),
        ("rpad('ab', 5, 'xy')", text("abxyx")),
        ("lpad(t, 3)", text("Hel")),
        ("ltrim('xxhixx', 'x')", text("hixx")),
        ("rtrim('xxhixx', 'x')", text("xxhi")),
        ("btrim('xxhixx', 'x')", text("hi")),
        ("repeat('ab', 3)", text("ababab")),
        ("reverse(t)", text("dlroW olleH")),
        ("starts_with(t, 'Hell')", SqlValue::Bool(true)),
        ("substring(t from 7 for 5)", text("World")),
        ("substr(t, 7)", text("World")),
        ("power(2, n)", SqlValue::Float(128.0)),
        ("sqrt(16)", SqlValue::Float(4.0)),
        ("ceil(n / 2.0)", text("4")),
        ("ceiling(1.2)", text("2")),
        ("floor(n / 2.0)", text("3")),
        ("extract(year from ts)", text("2026")),
        ("regexp_match(t, 'W(o)(r)')::text", text("{o,r}")),
        (
            "regexp_split_to_array('a,b,,c', ',')::text",
            text("{a,b,\"\",c}"),
        ),
        (
            "to_date('2026-10-13', 'YYYY-MM-DD')::text",
            text("2026-10-13"),
        ),
        (
            "age(ts, '2025-08-12 11:00:00+00')::text",
            text("1 year 2 mons 1 day 01:34:56"),
        ),
        (
            "make_interval(days => 2, hours => 3)::text",
            text("2 days 03:00:00"),
        ),
        ("make_interval(0, 1)::text", text("1 mon")),
        (
            "date_bin('15 minutes', ts, '2000-01-01 00:00:00+00')::text",
            text("2026-10-13 12:30:00+00"),
        ),
        ("(ts AT TIME ZONE 'UTC')::text", text("2026-10-13 12:34:56")),
        ("similarity('word', 'words') > 0.5", SqlValue::Bool(true)),
    ];
    for (expr, expected) in cases {
        let rows = s
            .execute(&format!("SELECT {expr} FROM f"))
            .unwrap_or_else(|error| panic!("{expr}: {error:?}"))
            .rows;
        let got = &rows[0][0];
        // Numeric results may come back as text or numbers depending on the path; compare renderings.
        let render = |value: &SqlValue| match value {
            SqlValue::Int(number) => number.to_string(),
            SqlValue::Float(number) => number.to_string(),
            SqlValue::String(text) => text.clone(),
            other => format!("{other:?}"),
        };
        assert_eq!(render(got), render(&expected), "{expr}");
    }
    assert_eq!(
        s.execute("SELECT strpos(NULL, 'a'), lpad(NULL, 3), age(NULL)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null, SqlValue::Null, SqlValue::Null]]
    );
    // CEIL/FLOOR syntax outside a table scan: constants and INSERT values.
    assert_eq!(
        s.execute("SELECT floor(2.5), ceil(2.1), floor(-2.5)")
            .unwrap()
            .rows,
        vec![vec![text("2"), text("3"), text("-3")]]
    );
    s.execute("CREATE TABLE fl (v int); INSERT INTO fl VALUES (floor(7 / 2.0)), (ceil(7 / 2.0))")
        .unwrap();
    assert_eq!(
        s.execute("SELECT v FROM fl ORDER BY v").unwrap().rows,
        vec![vec![SqlValue::Int(3)], vec![SqlValue::Int(4)]]
    );
    // The same functions without a FROM clause (the constant path).
    assert_eq!(
        s.execute("SELECT strpos('Hello', 'l'), lpad('x', 3, '*'), reverse('abc'), starts_with('abc', 'a'), regexp_match('ab', 'b')::text").unwrap().rows,
        vec![vec![SqlValue::Int(3), text("**x"), text("cba"), SqlValue::Bool(true), text("{b}")]]
    );
}

#[test]
fn float_columns_compare_with_numeric_literals() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE m (r real, d double precision); INSERT INTO m VALUES (0.57, 0.57), (0.25, 0.25)").unwrap();
    for (sql, expected) in [
        ("SELECT count(*) FROM m WHERE r > 0.5", 1),
        ("SELECT count(*) FROM m WHERE d < 0.5", 1),
        ("SELECT count(*) FROM m WHERE d = 0.25", 1),
        ("SELECT count(*) FROM m WHERE 0.3 BETWEEN r AND 1.0", 1),
    ] {
        assert_eq!(
            s.execute(sql).unwrap().rows,
            vec![vec![SqlValue::Int(expected)]],
            "{sql}"
        );
    }
    assert_eq!(
        s.execute("SELECT 0.57::float4 > 0.5, 0.57::float8 < 0.6")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]]
    );
    let ordered = s.execute("SELECT d FROM m ORDER BY d DESC").unwrap().rows;
    assert_eq!(
        ordered,
        vec![vec![SqlValue::Float(0.57)], vec![SqlValue::Float(0.25)]]
    );
}

#[test]
fn select_without_from_runs_subqueries() {
    // `SELECT EXISTS (...)` and scalar subqueries with no FROM are how applications ask yes/no and single-value
    // questions (Carrier's sql.scalar_as("Bool", "SELECT EXISTS (...)")).
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute(
        "CREATE TABLE sanctions (subject text, lifted_at timestamptz, expires_at timestamptz)",
    )
    .unwrap();
    s.execute("INSERT INTO sanctions VALUES ('a', NULL, NULL), ('b', now(), NULL)")
        .unwrap();
    let ask = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows[0][0].clone();
    assert_eq!(ask(&mut s, "SELECT EXISTS (SELECT 1 FROM sanctions WHERE subject = 'a' AND lifted_at IS NULL AND (expires_at IS NULL OR expires_at > NOW()))"), SqlValue::Bool(true));
    assert_eq!(
        ask(
            &mut s,
            "SELECT EXISTS (SELECT 1 FROM sanctions WHERE subject = 'b' AND lifted_at IS NULL)"
        ),
        SqlValue::Bool(false)
    );
    assert_eq!(
        ask(
            &mut s,
            "SELECT NOT EXISTS (SELECT 1 FROM sanctions WHERE subject = 'z')"
        ),
        SqlValue::Bool(true)
    );
    assert_eq!(
        ask(&mut s, "SELECT (SELECT count(*) FROM sanctions)"),
        SqlValue::Int(2)
    );
    assert_eq!(
        ask(
            &mut s,
            "SELECT COALESCE((SELECT subject FROM sanctions WHERE subject = 'z'), 'none')"
        ),
        text("none")
    );
    assert_eq!(
        ask(&mut s, "SELECT 'a' IN (SELECT subject FROM sanctions)"),
        SqlValue::Bool(true)
    );
    assert_eq!(
        ask(
            &mut s,
            "SELECT EXISTS (SELECT 1 FROM sanctions) AND (SELECT count(*) FROM sanctions) > 1"
        ),
        SqlValue::Bool(true)
    );
}

#[test]
fn update_can_change_a_primary_key() {
    // PostgreSQL allows UPDATE ... SET id = ...; applications and tests use it (e.g. to backdate a time-ordered id).
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute(
        "CREATE TABLE profiles (id text PRIMARY KEY, subject text UNIQUE, name text NOT NULL)",
    )
    .unwrap();
    s.execute("INSERT INTO profiles VALUES ('p1','ann','Ann'),('p2','ben','Ben')")
        .unwrap();
    assert_eq!(
        s.execute("UPDATE profiles SET id='p0', name=name || '!' WHERE subject='ann'")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("UPDATE 1")
    );
    let rows = s
        .execute("SELECT id, subject, name FROM profiles ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![text("p0"), text("ann"), text("Ann!")],
            vec![text("p2"), text("ben"), text("Ben")]
        ]
    );
    // Moving onto an existing key is a unique violation, and nothing changes.
    assert!(s
        .execute("UPDATE profiles SET id='p2' WHERE subject='ann'")
        .is_err());
    assert_eq!(
        s.execute("SELECT count(*) FROM profiles WHERE id='p0'")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(1)
    );
    // Inside a transaction the move rolls back with it.
    s.execute("BEGIN").unwrap();
    s.execute("UPDATE profiles SET id='p9' WHERE subject='ben'")
        .unwrap();
    s.execute("ROLLBACK").unwrap();
    assert_eq!(
        s.execute("SELECT id FROM profiles WHERE subject='ben'")
            .unwrap()
            .rows[0][0],
        text("p2")
    );
    // A key other rows refer to is refused rather than risking a cascading delete.
    s.execute("CREATE TABLE posts (id text PRIMARY KEY, author text REFERENCES profiles(id) ON DELETE CASCADE)").unwrap();
    s.execute("INSERT INTO posts VALUES ('x','p2')").unwrap();
    assert!(s
        .execute("UPDATE profiles SET id='p7' WHERE subject='ben'")
        .is_err());
    assert_eq!(
        s.execute("SELECT count(*) FROM posts").unwrap().rows[0][0],
        SqlValue::Int(1)
    );
}

#[test]
fn a_unique_value_freed_by_delete_can_be_reused_in_the_same_transaction() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE people (id text PRIMARY KEY, subject text UNIQUE)")
        .unwrap();
    s.execute("INSERT INTO people VALUES ('p1','ann')").unwrap();
    s.execute("BEGIN").unwrap();
    s.execute("DELETE FROM people WHERE subject='ann'").unwrap();
    s.execute("INSERT INTO people VALUES ('p2','ann')").unwrap();
    s.execute("COMMIT").unwrap();
    assert_eq!(
        s.execute("SELECT id FROM people").unwrap().rows,
        vec![vec![text("p2")]]
    );
}

#[test]
fn constant_where_accepts_a_cast_boolean() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    assert!(s
        .execute("SELECT ''::text AS subject, false AS pending WHERE false::BOOLEAN")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        s.execute("SELECT 1 WHERE 'true'::boolean").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    s.execute("CREATE TABLE t (a int); INSERT INTO t VALUES (1)")
        .unwrap();
    assert!(s
        .execute("SELECT a FROM t WHERE false::BOOLEAN")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn array_of_a_correlated_subquery_without_from() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let rows = s.execute("WITH s AS (SELECT '{\"c\":[\"de\",\"at\"]}'::jsonb AS v) SELECT ARRAY(SELECT jsonb_array_elements_text(v->'c')) AS c FROM s").unwrap().rows;
    assert_eq!(rows, s.execute("SELECT ARRAY['de','at']").unwrap().rows);
    let joined = s.execute("WITH s AS (SELECT '{\"c\":[\"de\",\"at\"]}'::jsonb AS v) SELECT array_to_string(ARRAY(SELECT jsonb_array_elements_text(v->'c')), ',') FROM s").unwrap().rows;
    assert_eq!(joined, vec![vec![text("de,at")]]);
    assert_eq!(
        s.execute("SELECT ARRAY(SELECT x FROM generate_series(1,3) x)")
            .unwrap()
            .rows,
        s.execute("SELECT ARRAY[1,2,3]").unwrap().rows
    );
}

#[test]
fn regexp_matches_is_a_table_function() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let body = "Hi @ann_x and @Ben22, also @ann_x again; mail a@b.com @no";
    let count = s.execute(&format!("SELECT (SELECT COUNT(DISTINCT lower(m[2])) FROM regexp_matches('{body}','(^|[^[:alnum:]_@/.])@([A-Za-z0-9_]{{3,30}})','g') AS r(m))::int")).unwrap().rows;
    assert_eq!(count, vec![vec![SqlValue::Int(2)]]);
    // Lookahead and WITH ORDINALITY, as in mention extraction.
    let rows = s.execute(&format!("SELECT lower(r.m[2]) AS u, MIN(r.ord) FROM regexp_matches('{body}','(^|[^[:alnum:]_@/.])@([A-Za-z0-9_]{{3,30}})(?![A-Za-z0-9_])','g') WITH ORDINALITY AS r(m,ord) GROUP BY 1 ORDER BY 2")).unwrap().rows;
    assert_eq!(
        rows,
        vec![
            vec![text("ann_x"), SqlValue::Int(1)],
            vec![text("ben22"), SqlValue::Int(2)]
        ]
    );
    // Without g only the first match; no groups gives the whole match.
    assert_eq!(
        s.execute("SELECT COUNT(*) FROM regexp_matches('a1 b2','[a-z][0-9]')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        s.execute("SELECT r[1] FROM regexp_matches('a1 b2','[a-z][0-9]','g') AS t(r) ORDER BY 1")
            .unwrap()
            .rows,
        vec![vec![text("a1")], vec![text("b2")]]
    );
    assert_eq!(s.execute("SELECT COUNT(*) FROM regexp_matches('x http://a.io www.b.org','(https?://|www\\.)[^[:space:]]+','gi')").unwrap().rows, vec![vec![SqlValue::Int(2)]]);
}

#[test]
fn for_share_locks_rows() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE spaces (id int PRIMARY KEY, name text); CREATE TABLE posts (id int PRIMARY KEY, space_id int)").unwrap();
    s.execute("INSERT INTO spaces VALUES (1,'a'); INSERT INTO posts VALUES (10,1)")
        .unwrap();
    s.execute("BEGIN").unwrap();
    assert_eq!(
        s.execute("SELECT name FROM spaces WHERE id=1 FOR SHARE")
            .unwrap()
            .rows,
        vec![vec![text("a")]]
    );
    assert_eq!(
        s.execute("SELECT p.id FROM posts p JOIN spaces sp ON sp.id=p.space_id FOR SHARE OF sp")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10)]]
    );
    s.execute("COMMIT").unwrap();
}

#[test]
fn parameters_inside_in_subqueries_are_counted() {
    let (_dir, db) = db();
    let types = bicdb_sql::infer_parameter_types(&db, "SELECT 1 FROM t g WHERE g.a=$1 AND g.b IN (SELECT m.b FROM m WHERE m.c=$1 AND m.roles && $3::text[])").unwrap();
    assert_eq!(types.len(), 3);
}

#[test]
fn pattern_operators_on_parameters_are_boolean() {
    // A client reading `$1 ~ '...'` as text 'f' treats it as true: WeWoBo's badge validation let '<script>' through.
    let (_dir, db) = db();
    for op in ["~", "~*", "!~", "!~*", "~~", "!~~"] {
        let types =
            bicdb_sql::infer_query_result_types(&db, &format!("SELECT $1 {op} '^[a-z]*$' AS m"))
                .unwrap();
        assert_eq!(types, Some(vec![Some("bool".to_string())]), "{op}");
    }
}

#[test]
fn pgcrypto_gen_random_bytes() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let rows = s.execute("WITH t AS (SELECT e AS email, encode(public.gen_random_bytes(24),'hex') AS token FROM unnest(ARRAY['a','b']) e) SELECT email, length(token), encode(public.digest(token,'sha256'),'hex') <> '' FROM t ORDER BY email").unwrap().rows;
    assert_eq!(
        rows,
        vec![
            vec![text("a"), SqlValue::Int(48), SqlValue::Bool(true)],
            vec![text("b"), SqlValue::Int(48), SqlValue::Bool(true)]
        ]
    );
    let distinct = s
        .execute(
            "SELECT count(DISTINCT encode(gen_random_bytes(16),'hex')) FROM generate_series(1,5)",
        )
        .unwrap()
        .rows;
    assert_eq!(distinct, vec![vec![SqlValue::Int(5)]]);
    assert!(s.execute("SELECT gen_random_bytes(0)").is_err());
}

#[test]
fn timestamps_cast_to_time() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    assert_eq!(
        s.execute("SELECT '2026-10-14 10:29:59'::timestamp::time::text")
            .unwrap()
            .rows,
        vec![vec![text("10:29:59")]]
    );
    assert_eq!(
        s.execute("SELECT '2026-10-14 10:29:59'::time::text")
            .unwrap()
            .rows,
        vec![vec![text("10:29:59")]]
    );
    assert_eq!(
        s.execute("SELECT ('2026-10-14 03:20:00'::timestamp)::time >= '03:15'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        s.execute("SELECT (NOW() AT TIME ZONE 'UTC')::time IS NOT NULL")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert!(s.execute("SELECT 'not a time'::time").is_err());
}

#[test]
fn regex_operators_and_replace_accept_lookahead() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    assert_eq!(s.execute("SELECT '[x](http://a)' ~ '\\]\\((?!https://)', '[x](https://a)' ~ '\\]\\((?!https://)'").unwrap().rows, vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]);
    assert_eq!(
        s.execute("SELECT regexp_replace('@ann_x and @bob', '@([a-z_]+)(?![a-z_])', '<\\1>', 'g')")
            .unwrap()
            .rows,
        vec![vec![text("<ann_x> and <bob>")]]
    );
    assert_eq!(
        s.execute("SELECT regexp_replace('aaa', 'a(?=a)', 'b')")
            .unwrap()
            .rows,
        vec![vec![text("baa")]]
    );
}

#[test]
fn coalesce_of_a_bigint_subquery_and_an_int_literal_is_bigint() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE plan_limits (id int, daily_limit bigint); INSERT INTO plan_limits VALUES (1, 5000000000), (2, NULL)").unwrap();
    for (sql, expected) in [
        (
            "SELECT COALESCE((SELECT daily_limit FROM plan_limits WHERE id=1), -1)",
            SqlValue::Int(5_000_000_000),
        ),
        (
            "SELECT COALESCE((SELECT daily_limit FROM plan_limits WHERE id=2), -1)",
            SqlValue::Int(-1),
        ),
        (
            "SELECT COALESCE((SELECT daily_limit FROM plan_limits WHERE id=3), -1)",
            SqlValue::Int(-1),
        ),
        (
            "SELECT COALESCE(-1, (SELECT daily_limit FROM plan_limits WHERE id=1))",
            SqlValue::Int(-1),
        ),
    ] {
        let result = s.execute(sql).unwrap();
        assert_eq!(result.rows, vec![vec![expected]], "{sql}");
        assert_eq!(
            result.column_types.first().cloned().flatten().as_deref(),
            Some("int8"),
            "{sql}"
        );
    }
    let rows = s
        .execute("SELECT COALESCE(daily_limit, -1) FROM plan_limits ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![SqlValue::Int(5_000_000_000)], vec![SqlValue::Int(-1)]]
    );
    let rows = s.execute("SELECT id FROM plan_limits WHERE COALESCE((SELECT max(daily_limit) FROM plan_limits), -1) > 4000000000 ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]);
}

#[test]
fn insert_accepts_a_union_all_source_with_returning_and_on_conflict() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE prices (id text PRIMARY KEY, product text NOT NULL, cents int NOT NULL, kind text)").unwrap();
    let mut s = s.with_positional_parameters(vec![
        text("p1"),
        text("[{\"id\":\"a\",\"cents\":100},{\"id\":\"b\",\"cents\":250}]"),
    ]);
    let result = s.execute(
        "INSERT INTO prices (id, product, cents, kind) \
         SELECT p.id, $1, p.cents, 'one_time' FROM jsonb_to_recordset($2::jsonb) AS p(id text, cents int) \
         UNION ALL SELECT 'free', $1, 0, 'free' \
         RETURNING id, cents",
    ).unwrap();
    let mut returned = result.rows.clone();
    returned.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(
        returned,
        vec![
            vec![text("a"), SqlValue::Int(100)],
            vec![text("b"), SqlValue::Int(250)],
            vec![text("free"), SqlValue::Int(0)],
        ]
    );
    // ON CONFLICT applies to every branch's rows; UNION removes duplicates first.
    s.execute(
        "INSERT INTO prices (id, product, cents) SELECT 'a', 'p1', 1 UNION SELECT 'a', 'p1', 1 UNION SELECT 'c', 'p1', 3 \
         ON CONFLICT (id) DO UPDATE SET cents = EXCLUDED.cents",
    ).unwrap();
    let rows = s
        .execute("SELECT id, cents FROM prices ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![text("a"), SqlValue::Int(1)],
            vec![text("b"), SqlValue::Int(250)],
            vec![text("c"), SqlValue::Int(3)],
            vec![text("free"), SqlValue::Int(0)],
        ]
    );
    let tag = s.execute("INSERT INTO prices (id, product, cents) SELECT 'd', 'p', 4 UNION ALL SELECT 'e', 'p', 5 ON CONFLICT DO NOTHING").unwrap();
    assert_eq!(tag.command_tag.as_deref(), Some("INSERT 0 2"));
}

#[test]
fn distinct_on_expressions_need_only_lead_the_order_by() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE u (v text, o int); INSERT INTO u VALUES ('Bob',2),('bob',1),('ALICE',5),('alice',3),('carl',9)").unwrap();
    let rows = s
        .execute("SELECT DISTINCT ON (lower(v)) v, o FROM u ORDER BY lower(v), o")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![text("alice"), SqlValue::Int(3)],
            vec![text("bob"), SqlValue::Int(1)],
            vec![text("carl"), SqlValue::Int(9)],
        ]
    );
    let rows = s
        .execute("SELECT DISTINCT ON (lower(v)) o FROM u ORDER BY lower(v), o DESC")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Int(5)],
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(9)]
        ]
    );
    assert!(
        s.execute("SELECT DISTINCT ON (lower(v)) v FROM u ORDER BY o")
            .is_err(),
        "DISTINCT ON must match the leading ORDER BY"
    );
}

#[test]
fn to_jsonb_and_row_to_json_accept_a_whole_row_reference() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE cards (id int, title text, tags text[]); INSERT INTO cards VALUES (1, 'One', ARRAY['x']), (2, 'Two', '{}')").unwrap();
    let expected = s
        .execute("SELECT '{\"id\": 1, \"tags\": [\"x\"], \"title\": \"One\"}'::jsonb")
        .unwrap()
        .rows;
    for sql in [
        "WITH c AS (SELECT * FROM cards WHERE id = 1) SELECT to_jsonb(c.*) FROM c",
        "WITH c AS (SELECT * FROM cards WHERE id = 1) SELECT to_jsonb(c) FROM c",
        "SELECT to_jsonb(c.*) FROM cards c WHERE c.id = 1",
        "SELECT to_jsonb(c) FROM cards c WHERE c.id = 1",
        "SELECT row_to_json(c.*)::jsonb FROM cards c WHERE c.id = 1",
        "WITH c AS (SELECT * FROM cards WHERE id = 1) SELECT row_to_json(c.*)::jsonb FROM c",
    ] {
        assert_eq!(s.execute(sql).unwrap().rows, expected, "{sql}");
    }
    let agg = s.execute("WITH c AS (SELECT id, title FROM cards) SELECT jsonb_agg(to_jsonb(c.*) ORDER BY c.id) FROM c").unwrap().rows;
    let want = s
        .execute("SELECT '[{\"id\":1,\"title\":\"One\"},{\"id\":2,\"title\":\"Two\"}]'::jsonb")
        .unwrap()
        .rows;
    assert_eq!(agg, want);
}

#[test]
fn ordered_set_aggregates_percentile_and_mode() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE m (g text, x float8, n int)")
        .unwrap();
    s.execute("INSERT INTO m VALUES ('a',1,1),('a',2,1),('a',3,2),('a',4,2),('a',NULL,2),('b',10,7),('c',NULL,NULL)").unwrap();
    let rows = s.execute("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0.5) WITHIN GROUP (ORDER BY x), mode() WITHIN GROUP (ORDER BY n) FROM m WHERE g = 'a'").unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::Float(2.5),
            SqlValue::Float(2.0),
            SqlValue::Int(2)
        ]]
    );
    let rows = s
        .execute("SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY x DESC) FROM m WHERE g = 'a'")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![SqlValue::Float(3.25)]]);
    let rows = s.execute("SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0.9) WITHIN GROUP (ORDER BY n) FROM m GROUP BY g ORDER BY g").unwrap().rows;
    assert_eq!(
        rows,
        vec![
            vec![text("a"), SqlValue::Float(2.5), SqlValue::Int(2)],
            vec![text("b"), SqlValue::Float(10.0), SqlValue::Int(7)],
            vec![text("c"), SqlValue::Null, SqlValue::Null],
        ]
    );
    // Empty input gives NULL.
    let rows = s.execute("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), mode() WITHIN GROUP (ORDER BY n) FROM m WHERE false").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Null, SqlValue::Null]]);
    // Array forms return one value per fraction.
    let got = s.execute("SELECT percentile_cont(ARRAY[0.25,0.5,1]) WITHIN GROUP (ORDER BY x), percentile_disc(ARRAY[0.5,0.1]) WITHIN GROUP (ORDER BY x) FROM m WHERE g = 'a'").unwrap().rows;
    let want = s
        .execute("SELECT ARRAY[1.75,2.5,4]::float8[], ARRAY[2,1]::float8[]")
        .unwrap()
        .rows;
    assert_eq!(got, want);
    // The WeWoBo shape: a scalar subquery over a CTE, inside jsonb_build_object.
    s.execute("CREATE TABLE scans (status text, created_at timestamptz, completed_at timestamptz)")
        .unwrap();
    s.execute("INSERT INTO scans VALUES ('done','2026-01-01 00:00:00+00','2026-01-01 00:00:10+00'),('done','2026-01-01 00:00:00+00','2026-01-01 00:00:30+00'),('pending','2026-01-01 00:00:00+00',NULL)").unwrap();
    let rows = s.execute("WITH d AS (SELECT * FROM scans) SELECT jsonb_build_object('scanned',(SELECT COUNT(*)::int FROM d WHERE status='done'),'median_seconds',(SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at-created_at)) FROM d WHERE completed_at IS NOT NULL)) = '{\"scanned\":2,\"median_seconds\":20}'::jsonb").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn plpgsql_functions_run_once_per_row_in_update_and_select() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE learning_courses (id int, title text, slug text)")
        .unwrap();
    s.execute(r#"CREATE FUNCTION learning_course_slug(title text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE base text; cand text; n int := 1;
BEGIN
  base := trim(both '-' from regexp_replace(lower(COALESCE(title,'')),'[^a-z0-9]+','-','g'));
  IF length(base) < 3 THEN base := 'course' || CASE WHEN base='' THEN '' ELSE '-' || base END; END IF;
  LOOP
    cand := CASE WHEN n=1 THEN base ELSE base || '-' || n END;
    EXIT WHEN NOT EXISTS (SELECT 1 FROM learning_courses WHERE slug=cand);
    n := n + 1;
  END LOOP;
  RETURN cand;
END $$"#).unwrap();
    s.execute("INSERT INTO learning_courses VALUES (1, 'Yoga Basics', 'yoga-basics'), (2, 'Yoga Basics', NULL), (3, 'Breath Work', NULL)").unwrap();
    let rows = s
        .execute("SELECT id, learning_course_slug(title) FROM learning_courses ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Int(1), text("yoga-basics-2")],
            vec![SqlValue::Int(2), text("yoga-basics-2")],
            vec![SqlValue::Int(3), text("breath-work")],
        ]
    );
    let mut p = s.with_positional_parameters(vec![SqlValue::Int(2)]);
    let updated = p.execute("UPDATE learning_courses SET slug=learning_course_slug(title) WHERE id=$1 AND slug IS NULL RETURNING slug").unwrap();
    assert_eq!(updated.rows, vec![vec![text("yoga-basics-2")]]);
    drop(p);
    let mut s = SqlSession::new(&mut db);
    // Each row sees the slugs written for earlier rows of the same statement only via its own queries;
    // a plain WHERE filter and an expression argument also work.
    s.execute("UPDATE learning_courses SET slug=learning_course_slug(title || ' ' || id) WHERE slug IS NULL").unwrap();
    let rows = s
        .execute("SELECT id, slug FROM learning_courses ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Int(1), text("yoga-basics")],
            vec![SqlValue::Int(2), text("yoga-basics-2")],
            vec![SqlValue::Int(3), text("breath-work-3")],
        ]
    );
    let rows = s.execute("SELECT id FROM learning_courses WHERE learning_course_slug(title) = 'breath-work' ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(3)]]);
    // Inside an explicit transaction the function's queries see the transaction's own rows.
    s.execute("BEGIN").unwrap();
    s.execute("INSERT INTO learning_courses VALUES (4, 'Breath Work', 'breath-work')")
        .unwrap();
    let rows = s
        .execute("SELECT learning_course_slug(title) FROM learning_courses WHERE id = 3")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![text("breath-work-2")]]);
    s.execute("ROLLBACK").unwrap();
    // BicDB boundary (PostgreSQL allows it): a function called per row may not modify data.
    // The statement fails as a whole instead of writing part of its effects.
    s.execute("CREATE FUNCTION touch(i int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN INSERT INTO learning_courses VALUES (i + 100, 'x', NULL); RETURN i; END $$").unwrap();
    assert!(s.execute("SELECT touch(id) FROM learning_courses").is_err());
    assert_eq!(
        s.execute("SELECT count(*) FROM learning_courses")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3)]]
    );
    // Called without FROM it runs in the session and may write.
    assert_eq!(
        s.execute("SELECT touch(1)").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        s.execute("SELECT count(*) FROM learning_courses")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(4)]]
    );
}

#[test]
fn plpgsql_functions_per_row_respect_execute_privilege_and_security_definer() {
    let (_dir, mut db) = db();
    {
        let mut admin = SqlSession::new(&mut db);
        admin.execute("CREATE ROLE reader LOGIN").unwrap();
        admin
            .execute(
                "CREATE TABLE secrets (id int, v text); INSERT INTO secrets VALUES (1, 'hidden')",
            )
            .unwrap();
        admin
            .execute("CREATE TABLE items (id int); INSERT INTO items VALUES (1), (2)")
            .unwrap();
        admin.execute("GRANT SELECT ON items TO reader").unwrap();
        admin.execute("CREATE FUNCTION peek(i int) RETURNS text LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN (SELECT v FROM secrets WHERE id = i); END $$").unwrap();
        admin.execute("CREATE FUNCTION peek_invoker(i int) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT v FROM secrets WHERE id = i); END $$").unwrap();
        admin.execute("CREATE FUNCTION locked(i int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN i; END $$").unwrap();
        admin
            .execute("REVOKE EXECUTE ON FUNCTION locked(int) FROM PUBLIC")
            .unwrap();
        admin
            .execute("GRANT EXECUTE ON FUNCTION peek(int), peek_invoker(int) TO reader")
            .unwrap();
    }
    let mut r = SqlSession::new_unprivileged(&mut db, "reader");
    let rows = r
        .execute("SELECT id, peek(id) FROM items ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Int(1), text("hidden")],
            vec![SqlValue::Int(2), SqlValue::Null]
        ]
    );
    assert!(
        r.execute("SELECT id, peek_invoker(id) FROM items").is_err(),
        "invoker rights cannot read secrets"
    );
    assert!(
        r.execute("SELECT locked(id) FROM items").is_err(),
        "EXECUTE was revoked"
    );
}

#[test]
fn table_row_types_cast_populate_and_expand() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE network_spaces (id int PRIMARY KEY, name text NOT NULL, tenant_id text NOT NULL, parent_id int, access_people text[] NOT NULL DEFAULT '{}', created_at bigint)").unwrap();
    s.execute("INSERT INTO network_spaces VALUES (1, 'Root', 't1', NULL, ARRAY['ann'], 10), (2, 'Child', 't1', 1, ARRAY['bob','cy'], 20)").unwrap();
    // A NULL of the row type, and field access on it.
    assert_eq!(
        s.execute("SELECT (NULL::network_spaces).name")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
    // jsonb_populate_record takes its columns and types from the table.
    let rows = s.execute("SELECT (jsonb_populate_record(NULL::network_spaces, '{\"id\":\"7\",\"name\":\"N\",\"extra\":1}'::jsonb)).id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(7)]]);
    let r = s.execute("SELECT x.id, x.name, x.tenant_id FROM network_spaces s CROSS JOIN LATERAL jsonb_populate_record(NULL::network_spaces, to_jsonb(s)) x ORDER BY x.id").unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![SqlValue::Int(1), text("Root"), text("t1")],
            vec![SqlValue::Int(2), text("Child"), text("t1")],
        ]
    );
    let r = s.execute("SELECT * FROM jsonb_populate_record(NULL::network_spaces, '{\"id\":3,\"name\":\"x\"}')").unwrap();
    assert_eq!(
        r.columns,
        vec![
            "id",
            "name",
            "tenant_id",
            "parent_id",
            "access_people",
            "created_at"
        ]
    );
    // (record).* expands to the table's columns with their names and types (actions.mjs:1777).
    let mut p = s.with_positional_parameters(vec![SqlValue::Int(2)]);
    let r = p.execute("SELECT (jsonb_populate_record(NULL::network_spaces,to_jsonb(s)||jsonb_build_object('tenant_id','','parent_id',NULL,'access_people','[]'::jsonb))).* FROM network_spaces s WHERE id=$1").unwrap();
    drop(p);
    assert_eq!(
        r.columns,
        vec![
            "id",
            "name",
            "tenant_id",
            "parent_id",
            "access_people",
            "created_at"
        ]
    );
    let mut s = SqlSession::new(&mut db);
    let want = s
        .execute("SELECT 2, 'Child', '', NULL::int, ARRAY[]::text[], 20::bigint")
        .unwrap()
        .rows;
    assert_eq!(r.rows, want);
    assert_eq!(
        r.column_types
            .iter()
            .map(|t| t.clone().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["int4", "text", "text", "int4", "text[]", "int8"]
    );
    // A CASE choosing between the whole row and a redacted copy (actions.mjs:1790).
    let mut p = s.with_positional_parameters(vec![text("[{\"id\":1,\"role\":\"owner\",\"discoverable\":true},{\"id\":2,\"role\":\"member\",\"discoverable\":true}]")]);
    let r = p.execute("WITH a AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(id int,role text,discoverable boolean)) SELECT (CASE WHEN a.role IN ('owner','admin') THEN s ELSE jsonb_populate_record(NULL::network_spaces,to_jsonb(s)||jsonb_build_object('tenant_id','','parent_id',NULL,'access_people','[]'::jsonb)) END).* FROM network_spaces s JOIN a ON a.id=s.id WHERE a.discoverable ORDER BY s.created_at DESC LIMIT 500").unwrap();
    drop(p);
    assert_eq!(
        r.columns,
        vec![
            "id",
            "name",
            "tenant_id",
            "parent_id",
            "access_people",
            "created_at"
        ]
    );
    let mut s = SqlSession::new(&mut db);
    let want = s.execute("SELECT 2, 'Child', '', NULL::int, ARRAY[]::text[], 20::bigint UNION ALL SELECT 1, 'Root', 't1', NULL::int, ARRAY['ann'], 10::bigint").unwrap().rows;
    assert_eq!(r.rows, want);
    // Field access and star expansion of a plain whole-row reference.
    let rows = s
        .execute("SELECT (s).name, (s).* FROM network_spaces s WHERE id = 1")
        .unwrap()
        .rows;
    let want = s
        .execute("SELECT 'Root', 1, 'Root', 't1', NULL::int, ARRAY['ann'], 10::bigint")
        .unwrap()
        .rows;
    assert_eq!(rows, want);
    // A row literal cast to the table type.
    let rows = s.execute("SELECT ('(5,Five,t9,,{},1)'::network_spaces).name, ('(5,Five,t9,,{},1)'::network_spaces).parent_id").unwrap().rows;
    assert_eq!(rows, vec![vec![text("Five"), SqlValue::Null]]);
}

#[test]
fn group_by_keeps_every_group_and_order_by_an_aggregate_sorts_them() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE media_jobs (id TEXT PRIMARY KEY, source_type TEXT NOT NULL, source_size BIGINT NOT NULL, source_hash TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER, error TEXT, created_at BIGINT, updated_at BIGINT)").unwrap();
    s.execute("INSERT INTO media_jobs VALUES \
        ('j1','video',10,'h1','ready',1,NULL,100,110), ('j2','video',20,'h2','queued',0,NULL,200,200), \
        ('j3','image',30,'h3','failed',3,'boom',300,330), ('j4','video',40,'h4','ready',1,NULL,400,440), \
        ('j5','video',50,'h5','queued',0,NULL,500,500), ('j6','video',60,'h6','ready',2,NULL,600,660)").unwrap();
    let rows = s
        .execute("SELECT state, count(*) FROM media_jobs GROUP BY 1 ORDER BY 1")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![text("failed"), SqlValue::Int(1)],
            vec![text("queued"), SqlValue::Int(2)],
            vec![text("ready"), SqlValue::Int(3)],
        ]
    );
    let mut unordered = s
        .execute("SELECT state, count(*) FROM media_jobs GROUP BY 1")
        .unwrap()
        .rows;
    unordered.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(unordered, rows);
    let rows = s
        .execute("SELECT state, count(*) FROM media_jobs GROUP BY state ORDER BY 2 DESC")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![text("ready"), SqlValue::Int(3)],
            vec![text("queued"), SqlValue::Int(2)],
            vec![text("failed"), SqlValue::Int(1)],
        ]
    );
    // ORDER BY an aggregate (by position, by expression or by alias) sorts the groups.
    for sql in [
        "SELECT state, count(*) FROM media_jobs GROUP BY state ORDER BY count(*) DESC LIMIT 2",
        "SELECT state, count(*) AS n FROM media_jobs GROUP BY state ORDER BY n DESC LIMIT 2",
    ] {
        assert_eq!(
            s.execute(sql).unwrap().rows,
            vec![
                vec![text("ready"), SqlValue::Int(3)],
                vec![text("queued"), SqlValue::Int(2)],
            ],
            "{sql}"
        );
    }
    let rows = s.execute("SELECT state, count(*), min(created_at), max(updated_at) FROM media_jobs GROUP BY 1 ORDER BY 2 DESC").unwrap().rows;
    assert_eq!(
        rows,
        vec![
            vec![
                text("ready"),
                SqlValue::Int(3),
                SqlValue::Int(100),
                SqlValue::Int(660)
            ],
            vec![
                text("queued"),
                SqlValue::Int(2),
                SqlValue::Int(200),
                SqlValue::Int(500)
            ],
            vec![
                text("failed"),
                SqlValue::Int(1),
                SqlValue::Int(300),
                SqlValue::Int(330)
            ],
        ]
    );
}

#[test]
fn to_char_copies_double_quoted_text_and_non_pattern_characters() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let fmt = "'YYYY-MM-DD\"T\"HH24:MI'";
    for value in [
        "TIMESTAMP '2026-10-14 10:30:00'",
        "TIMESTAMPTZ '2026-10-14 10:30:00+00'",
        "'2026-10-14 10:30:45.5'::timestamp",
    ] {
        let rows = s
            .execute(&format!("SELECT to_char({value}, {fmt})"))
            .unwrap()
            .rows;
        assert_eq!(rows, vec![vec![text("2026-10-14T10:30")]], "{value}");
    }
    let rows = s
        .execute(&format!("SELECT to_char(DATE '2026-10-14', {fmt})"))
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![text("2026-10-14T00:00")]]);
    // Quoted text keeps its case and may hold pattern words; a backslash escapes a quote.
    let rows = s.execute(r#"SELECT to_char(TIMESTAMP '2026-10-14 10:30:00', '"Year" YYYY "at" HH24\"'), to_char(TIMESTAMP '2026-10-14 10:30:00', '"quote \" mark" MI')"#).unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![text("Year 2026 at 10\""), text("quote \" mark 30")]]
    );
    // Characters that are not patterns pass through.
    let rows = s
        .execute("SELECT to_char(TIMESTAMP '2026-10-14 10:30:00', 'YYYY/MM/DD xz HH24hMI')")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![text("2026/10/14 xz 10h30")]]);
    // Name, fill-mode and 12-hour patterns.
    let rows = s.execute("SELECT to_char(TIMESTAMP '2026-10-04 15:05:09.123456', 'FMDay, FMMonth FMDDth YYYY HH12:MI:SS.MS PM US Dy Mon')").unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![text(
            "Sunday, October 4th 2026 03:05:09.123 PM 123456 Sun Oct"
        )]]
    );
    // NOW() AT TIME ZONE 'UTC' is a timestamp without time zone.
    let rows = s.execute(&format!("SELECT length(to_char(NOW() AT TIME ZONE 'UTC', {fmt})), to_char(NOW() AT TIME ZONE 'UTC', {fmt}) LIKE '____-__-__T__:__'")).unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(16), SqlValue::Bool(true)]]);
    // WeWoBo's job scheduler key: the ten-minute bucket of a timestamp.
    let rows = s.execute(&format!("SELECT to_char(date_trunc('hour',ts)+floor(extract(minute FROM ts)/10)*INTERVAL '10 minutes', {fmt}) FROM (SELECT TIMESTAMPTZ '2026-10-14 10:37:12+00' AS ts) t")).unwrap().rows;
    assert_eq!(rows, vec![vec![text("2026-10-14T10:30")]]);
}

#[test]
fn generate_series_steps_timestamps_by_an_interval() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let r = s.execute("SELECT count(*) FROM generate_series(TIMESTAMPTZ '2026-10-13 17:00:00+00', TIMESTAMPTZ '2026-10-13 20:00:00+00', INTERVAL '1 hour')").unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Int(4)]]);
    // Inclusive bounds, type preserved.
    let r = s.execute("SELECT g FROM generate_series(TIMESTAMPTZ '2026-10-13 17:00:00+00', TIMESTAMPTZ '2026-10-13 18:30:00+00', INTERVAL '30 minutes') g").unwrap();
    let want = s.execute("SELECT TIMESTAMPTZ '2026-10-13 17:00:00+00' UNION ALL SELECT TIMESTAMPTZ '2026-10-13 17:30:00+00' UNION ALL SELECT TIMESTAMPTZ '2026-10-13 18:00:00+00' UNION ALL SELECT TIMESTAMPTZ '2026-10-13 18:30:00+00'").unwrap();
    assert_eq!(r.rows, want.rows);
    assert_eq!(r.column_types, want.column_types);
    let r = s.execute("SELECT * FROM generate_series(TIMESTAMP '2026-10-13 20:00:00', TIMESTAMP '2026-10-13 17:00:00', INTERVAL '-90 minutes')").unwrap();
    let want = s.execute("SELECT TIMESTAMP '2026-10-13 20:00:00' UNION ALL SELECT TIMESTAMP '2026-10-13 18:30:00' UNION ALL SELECT TIMESTAMP '2026-10-13 17:00:00'").unwrap();
    assert_eq!(r.rows, want.rows);
    assert_eq!(r.column_types, want.column_types);
    // Month and day steps are added one after another, as PostgreSQL does.
    let rows = s.execute("SELECT g::text FROM generate_series(TIMESTAMP '2026-01-31 08:00', TIMESTAMP '2026-05-01', INTERVAL '1 month') g").unwrap().rows;
    assert_eq!(
        rows,
        vec![
            vec![text("2026-01-31 08:00:00")],
            vec![text("2026-02-28 08:00:00")],
            vec![text("2026-03-28 08:00:00")],
            vec![text("2026-04-28 08:00:00")],
        ]
    );
    let rows = s.execute("SELECT count(*) FROM generate_series(TIMESTAMP '2026-10-01', TIMESTAMP '2026-10-31 23:59', INTERVAL '1 day 12 hours')").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(21)]]);
    // Empty when the step points away from the stop; a zero step is an error.
    let rows = s.execute("SELECT count(*) FROM generate_series(TIMESTAMP '2026-10-13', TIMESTAMP '2026-10-12', INTERVAL '1 hour')").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(0)]]);
    let err = s.execute("SELECT * FROM generate_series(TIMESTAMP '2026-10-13', TIMESTAMP '2026-10-14', INTERVAL '0 hours')").unwrap_err();
    assert!(
        err.to_string().contains("step size cannot equal zero"),
        "{err}"
    );
    // In a projection.
    let r = s.execute("SELECT generate_series(TIMESTAMPTZ '2026-10-13 17:00:00+00', TIMESTAMPTZ '2026-10-13 19:00:00+00', INTERVAL '1 hour')").unwrap();
    let want = s.execute("SELECT TIMESTAMPTZ '2026-10-13 17:00:00+00' UNION ALL SELECT TIMESTAMPTZ '2026-10-13 18:00:00+00' UNION ALL SELECT TIMESTAMPTZ '2026-10-13 19:00:00+00'").unwrap();
    assert_eq!(r.rows, want.rows);
    assert_eq!(r.column_types, want.column_types);
    // Hourly buckets for the last day, joined and grouped (WeWoBo's activity chart).
    s.execute("CREATE TABLE events (id int, at timestamptz); INSERT INTO events VALUES (1, now()), (2, now() - interval '2 hours'), (3, now() - interval '2 hours'), (4, now() - interval '3 days')").unwrap();
    let rows = s.execute("SELECT h, count(e.id) FROM generate_series(date_trunc('hour',now())-interval '23 hours',date_trunc('hour',now()),interval '1 hour') h LEFT JOIN events e ON date_trunc('hour', e.at) = h GROUP BY h ORDER BY h").unwrap().rows;
    assert_eq!(rows.len(), 24);
    let counts = rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>();
    let mut want = vec![SqlValue::Int(0); 24];
    want[23] = SqlValue::Int(1);
    want[21] = SqlValue::Int(2);
    assert_eq!(counts, want);
    let first_is_23h_ago = s.execute("SELECT min(h) = date_trunc('hour', now()) - interval '23 hours' FROM generate_series(date_trunc('hour',now())-interval '23 hours',date_trunc('hour',now()),interval '1 hour') h").unwrap().rows;
    assert_eq!(first_is_23h_ago, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn partial_unique_index_frees_a_key_when_the_transaction_moves_the_row_out_of_its_predicate() {
    let (_dir, mut db) = db();
    // $1/$2 are the WeWoBo statement's parameters; the other statements take none.
    let mut s = SqlSession::new(&mut db).with_positional_parameters(vec![
        text("00000000-0000-4000-8000-000000000001"),
        text("{b@x,c@x}"),
        text("member"),
        text("owner"),
    ]);
    s.execute("CREATE TABLE network_space_email_invites (id UUID PRIMARY KEY, space_id UUID NOT NULL, email TEXT NOT NULL, role TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE, invited_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), claimed_by TEXT, claimed_at TIMESTAMPTZ, revoked_at TIMESTAMPTZ)").unwrap();
    s.execute("CREATE INDEX network_space_email_invites_space_idx ON network_space_email_invites (space_id)").unwrap();
    s.execute("CREATE UNIQUE INDEX network_space_email_invites_open_uq ON network_space_email_invites (space_id, email) WHERE claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner'),(gen_random_uuid(),'00000000-0000-4000-8000-000000000001','b@x','member',md5(random()::text),'owner'),(gen_random_uuid(),'00000000-0000-4000-8000-000000000001','c@x','member',md5(random()::text),'owner'),(gen_random_uuid(),'00000000-0000-4000-8000-000000000001','d@x','member',md5(random()::text),'owner')").unwrap();
    let open = |s: &mut SqlSession, email: &str| {
        s.execute(&format!("SELECT count(*) FROM network_space_email_invites WHERE email='{email}' AND claimed_at IS NULL AND revoked_at IS NULL")).unwrap().rows[0][0].clone()
    };
    // Autocommit statements: each is its own transaction.
    s.execute("UPDATE network_space_email_invites SET revoked_at=NOW() WHERE space_id='00000000-0000-4000-8000-000000000001' AND email='a@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner')").unwrap();
    // Revoke then re-invite inside one transaction.
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET revoked_at=NOW() WHERE space_id='00000000-0000-4000-8000-000000000001' AND email='a@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner')").unwrap();
    s.execute("COMMIT").unwrap();
    assert_eq!(open(&mut s, "a@x"), SqlValue::Int(1));
    // The WeWoBo form: parameterised revoke, then a CTE insert returning the new rows.
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET revoked_at=NOW() WHERE space_id=$1 AND email IN (SELECT lower(btrim(x)) FROM unnest($2::text[]) x) AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    let r = s.execute("WITH t AS (SELECT e.email, md5(random()::text) AS token FROM (SELECT DISTINCT lower(btrim(x)) AS email FROM unnest($2::text[]) x) e), ins AS (INSERT INTO network_space_email_invites (id,space_id,email,role,token_hash,invited_by,created_at) SELECT gen_random_uuid(),$1,t.email,$3,md5(t.token),$4,NOW() FROM t RETURNING id,email) SELECT ins.email FROM ins JOIN t ON t.email=ins.email ORDER BY ins.email").unwrap();
    assert_eq!(r.rows, vec![vec![text("b@x")], vec![text("c@x")]]);
    s.execute("COMMIT").unwrap();
    assert_eq!(open(&mut s, "b@x"), SqlValue::Int(1));
    assert_eq!(open(&mut s, "c@x"), SqlValue::Int(1));
    // Claiming (another predicate column) and changing the key column also free the old key.
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET claimed_at=NOW() WHERE email='b@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    s.execute("UPDATE network_space_email_invites SET email='e@x' WHERE email='c@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','b@x','member',md5(random()::text),'owner'),(gen_random_uuid(),'00000000-0000-4000-8000-000000000001','c@x','member',md5(random()::text),'owner')").unwrap();
    s.execute("COMMIT").unwrap();
    assert_eq!(open(&mut s, "b@x"), SqlValue::Int(1));
    assert_eq!(open(&mut s, "c@x"), SqlValue::Int(1));
    assert_eq!(open(&mut s, "e@x"), SqlValue::Int(1));
    // ON CONFLICT on the partial index sees the freed key too.
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET revoked_at=NOW() WHERE email='d@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    let r = s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','d@x','member',md5(random()::text),'owner') ON CONFLICT (space_id, email) WHERE claimed_at IS NULL AND revoked_at IS NULL DO NOTHING RETURNING email").unwrap();
    assert_eq!(r.rows, vec![vec![text("d@x")]]);
    s.execute("COMMIT").unwrap();
    assert_eq!(open(&mut s, "d@x"), SqlValue::Int(1));
    // A row that still matches the predicate after its update keeps its key.
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET role='admin' WHERE email='a@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    let err = s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner')").unwrap_err();
    assert!(
        err.to_string()
            .contains("network_space_email_invites_open_uq"),
        "{err}"
    );
    s.execute("ROLLBACK").unwrap();
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET role='admin' WHERE email='a@x' AND revoked_at IS NULL").unwrap();
    let r = s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner') ON CONFLICT (space_id, email) WHERE claimed_at IS NULL AND revoked_at IS NULL DO NOTHING RETURNING email").unwrap();
    assert!(r.rows.is_empty(), "{:?}", r.rows);
    s.execute("ROLLBACK").unwrap();
    // A second new open row for a freed key, or a row moved back into the predicate, conflicts.
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET revoked_at=NOW() WHERE email='a@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner')").unwrap();
    assert!(s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner')").is_err());
    s.execute("ROLLBACK").unwrap();
    s.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    s.execute("UPDATE network_space_email_invites SET email='z@x' WHERE email='a@x' AND claimed_at IS NULL AND revoked_at IS NULL").unwrap();
    s.execute("INSERT INTO network_space_email_invites (id, space_id, email, role, token_hash, invited_by) VALUES (gen_random_uuid(),'00000000-0000-4000-8000-000000000001','a@x','member',md5(random()::text),'owner')").unwrap();
    assert!(s
        .execute("UPDATE network_space_email_invites SET email='a@x' WHERE email='z@x'")
        .is_err());
    s.execute("ROLLBACK").unwrap();
    assert_eq!(open(&mut s, "a@x"), SqlValue::Int(1));
    assert_eq!(open(&mut s, "z@x"), SqlValue::Int(0));
}

#[test]
fn an_uncorrelated_in_subquery_runs_once_per_statement_not_once_per_row() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    // The owner API's "spaces holding these playlists/courses/collections" lookup
    // (network.carrier). With the IN list recomputed for each of 20,000 posts it
    // never finished and held the writer.
    s.execute("CREATE TABLE network_posts (id uuid PRIMARY KEY, space_id uuid, body text)")
        .unwrap();
    s.execute("CREATE TABLE learning_playlist_items (playlist_id uuid, post_id uuid)")
        .unwrap();
    s.execute("CREATE TABLE learning_lessons (course_id uuid, post_id uuid)")
        .unwrap();
    s.execute(
        "CREATE TABLE learning_collection_items (collection_id uuid, item_kind text, item_id uuid)",
    )
    .unwrap();
    s.execute("INSERT INTO network_posts SELECT md5('post'||i)::uuid, CASE WHEN i % 10 = 0 THEN NULL ELSE md5('space'||(i % 50))::uuid END, 'b' FROM generate_series(1, 20000) i").unwrap();
    s.execute("INSERT INTO learning_playlist_items SELECT md5('pl'||(i % 20))::uuid, md5('post'||i)::uuid FROM generate_series(1, 2000) i").unwrap();
    s.execute("INSERT INTO learning_lessons SELECT md5('c'||(i % 20))::uuid, md5('post'||(i + 5000))::uuid FROM generate_series(1, 2000) i").unwrap();
    s.execute("INSERT INTO learning_collection_items SELECT md5('col'||(i % 20))::uuid, CASE WHEN i % 2 = 0 THEN 'post' ELSE 'video' END, md5('post'||(i + 10000))::uuid FROM generate_series(1, 2000) i").unwrap();
    let uuid = |s: &mut SqlSession, seed: &str| match &s
        .execute(&format!("SELECT md5('{seed}')::uuid::text"))
        .unwrap()
        .rows[0][0]
    {
        SqlValue::String(value) => value.clone(),
        other => panic!("uuid text: {other:?}"),
    };
    // playlist 1 -> spaces {1,11,21,31,41}; course 2 -> {2,12,22,32,42}; collection 4 -> {4,14,..};
    // collection 3 holds only videos; playlist 10 holds only posts without a space.
    let refs = [
        ("playlist", "pl1"),
        ("course", "c2"),
        ("collection", "col4"),
        ("collection", "col3"),
        ("playlist", "pl10"),
    ]
    .iter()
    .map(|(kind, seed)| format!("{{\"kind\":\"{kind}\",\"id\":\"{}\"}}", uuid(&mut s, seed)))
    .collect::<Vec<_>>();
    let mut want = s.execute("SELECT md5('space'||k)::uuid FROM unnest(ARRAY[1,11,21,31,41,2,12,22,32,42,4,14,24,34,44]) k").unwrap().rows;
    want.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    let mut p = s.with_positional_parameters(vec![text(&format!("[{}]", refs.join(",")))]);
    let started = std::time::Instant::now();
    let result = p.execute(
        "WITH r AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(kind text,id uuid)) \
         SELECT DISTINCT p.space_id AS id FROM network_posts p \
         WHERE p.space_id IS NOT NULL AND p.id IN ( \
           SELECT post_id FROM learning_playlist_items WHERE playlist_id IN (SELECT id FROM r WHERE kind='playlist') \
           UNION SELECT post_id FROM learning_lessons WHERE course_id IN (SELECT id FROM r WHERE kind='course') \
           UNION SELECT item_id FROM learning_collection_items WHERE item_kind='post' AND collection_id IN (SELECT id FROM r WHERE kind='collection'))",
    ).unwrap();
    let elapsed = started.elapsed();
    drop(p);
    let mut rows = result.rows;
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(rows, want);
    // Guards against the subquery running once per row (minutes), not
    // against machine speed: the statement takes 2.7-3.6 s in a debug build
    // on a shared host, so a 3 s bound failed on the same code at random.
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "took {elapsed:?}"
    );
    eprintln!("uncorrelated IN over 20,000 posts: {elapsed:?}");
}

#[test]
fn memoized_subqueries_keep_null_semantics_and_correlated_ones_still_run_per_row() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE a (id int PRIMARY KEY, grp int, v int)")
        .unwrap();
    s.execute("CREATE TABLE b (grp int, w int)").unwrap();
    s.execute("CREATE TABLE n (w int)").unwrap();
    s.execute("INSERT INTO a SELECT i, i % 3, i FROM generate_series(1, 30) i")
        .unwrap();
    s.execute("INSERT INTO a VALUES (31, 0, NULL)").unwrap();
    s.execute("INSERT INTO b VALUES (0, 3), (0, 6), (1, 4), (1, 5), (2, 8), (2, 30)")
        .unwrap();
    s.execute("INSERT INTO n VALUES (1), (NULL)").unwrap();
    let ids = |s: &mut SqlSession, sql: &str| -> Vec<i64> {
        s.execute(sql)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| match row[0] {
                SqlValue::Int(id) => id,
                ref other => panic!("id: {other:?}"),
            })
            .collect()
    };
    // NOT IN over a list holding NULL is never true; IN is true only on a match, else NULL.
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v NOT IN (SELECT w FROM n) ORDER BY id"
        ),
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE NOT (v IN (SELECT w FROM n)) ORDER BY id"
        ),
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v IN (SELECT w FROM n) ORDER BY id"
        ),
        vec![1]
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT count(*) FROM a WHERE (v IN (SELECT w FROM n)) IS NULL"
        ),
        vec![30]
    );
    assert_eq!(ids(&mut s, "SELECT id FROM a WHERE v NOT IN (SELECT w FROM n WHERE w IS NOT NULL) AND id < 4 ORDER BY id"), vec![2, 3]);
    assert_eq!(ids(&mut s, "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM n WHERE w IS NULL) AND id <= 2 ORDER BY id"), vec![1, 2]);
    // Correlated: the qualified outer column, an unqualified outer-only column, and a
    // reference from a nested subquery each give the per-row answer.
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v IN (SELECT w FROM b WHERE b.grp = a.grp) ORDER BY id"
        ),
        vec![3, 4, 6, 8]
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE 1 IN (SELECT 1 FROM b WHERE b.w = v) ORDER BY id"
        ),
        vec![3, 4, 5, 6, 8, 30]
    );
    assert_eq!(ids(&mut s, "SELECT id FROM a WHERE v IN (SELECT w FROM b WHERE EXISTS (SELECT 1 FROM n WHERE n.w = a.id)) ORDER BY id"), Vec::<i64>::new());
    assert_eq!(ids(&mut s, "SELECT id FROM a WHERE 1 IN (SELECT w FROM n WHERE EXISTS (SELECT 1 FROM b WHERE b.w = a.v)) ORDER BY id"), vec![3, 4, 5, 6, 8, 30]);
    assert_eq!(ids(&mut s, "SELECT id FROM a WHERE 4 IN (SELECT w FROM b WHERE EXISTS (SELECT 1 FROM b b2 WHERE b2.w = a.v)) ORDER BY id"), vec![3, 4, 5, 6, 8, 30]);
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w = a.v) ORDER BY id"
        ),
        vec![3, 4, 5, 6, 8, 30]
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE (SELECT max(w) FROM b WHERE b.grp = a.grp) = v ORDER BY id"
        ),
        vec![6]
    );
    // An outer read that only some rows reach is still a correlation.
    assert_eq!(ids(&mut s, "SELECT id FROM a WHERE v IN (SELECT w FROM b WHERE CASE WHEN a.id > 5 THEN b.grp = a.grp ELSE true END) ORDER BY id"), vec![3, 4, 5, 6, 8]);
    assert_eq!(ids(&mut s, "SELECT id FROM a WHERE id < 3 OR v IN (SELECT w FROM b WHERE b.grp = a.grp) ORDER BY id"), vec![1, 2, 3, 4, 6, 8]);
    // A volatile subquery keeps per-row semantics: every row draws again.
    let drawn = ids(
        &mut s,
        "SELECT count(*) FROM a WHERE (SELECT random()) < 0.5",
    )[0];
    assert!(
        drawn > 0 && drawn < 31,
        "{drawn} of 31 rows: random() was drawn once"
    );
}

fn texts(values: &[&str]) -> SqlValue {
    SqlValue::Json(serde_json::Value::Array(
        values
            .iter()
            .map(|v| serde_json::Value::String(v.to_string()))
            .collect(),
    ))
}

/// One column's (value, planner type, statically described type) — the last is what pgwire's
/// RowDescription uses before falling back to the planner type.
fn typed_cell(s: &mut SqlSession, db_sql: &str) -> (SqlValue, Option<String>) {
    let result = s.execute(db_sql).unwrap();
    assert_eq!(result.rows.len(), 1, "{db_sql}");
    (
        result.rows[0][0].clone(),
        result.column_types.first().cloned().flatten(),
    )
}

#[test]
fn array_agg_over_a_lateral_unnest_column_is_a_typed_array() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE g (scope_id text, entitlements text[])")
        .unwrap();
    s.execute("INSERT INTO g VALUES ('u1', ARRAY['courses.publish','b.x']), ('u2', ARRAY['a.y'])")
        .unwrap();
    let text_array = || Some("text[]".to_string());
    for (sql, want) in [
        ("SELECT array_agg(DISTINCT e ORDER BY e) FROM g CROSS JOIN LATERAL unnest(g.entitlements) e WHERE g.scope_id = 'u1'", texts(&["b.x", "courses.publish"])),
        ("SELECT array_agg(e ORDER BY e) FROM g CROSS JOIN LATERAL unnest(g.entitlements) e", texts(&["a.y", "b.x", "courses.publish"])),
        ("SELECT array_agg(DISTINCT e) FROM g, unnest(g.entitlements) e WHERE g.scope_id = 'u2'", texts(&["a.y"])),
        ("SELECT array_agg(e) FROM g CROSS JOIN LATERAL unnest(g.entitlements) AS e WHERE g.scope_id = 'u2'", texts(&["a.y"])),
        ("SELECT array_agg(x) FROM unnest(ARRAY['a','b']) x", texts(&["a", "b"])),
    ] {
        assert_eq!(typed_cell(&mut s, sql), (want, text_array()), "{sql}");
    }
    for sql in [
        "SELECT pg_typeof(array_agg(DISTINCT e ORDER BY e)) FROM g CROSS JOIN LATERAL unnest(g.entitlements) e",
        "SELECT pg_typeof(array_agg(e)) FROM g CROSS JOIN LATERAL unnest(g.entitlements) e",
    ] {
        assert_eq!(s.execute(sql).unwrap().rows, vec![vec![text("text[]")]], "{sql}");
    }
    assert_eq!(
        s.execute("SELECT pg_typeof(e) FROM g CROSS JOIN LATERAL unnest(g.entitlements) e LIMIT 1")
            .unwrap()
            .rows,
        vec![vec![text("text")]]
    );
    // Statement description (pgwire RowDescription) types the column text[] (OID 1009).
    let described = bicdb_sql::infer_query_result_types(&db, "SELECT array_agg(DISTINCT e ORDER BY e) AS names FROM g CROSS JOIN LATERAL unnest(g.entitlements) e").unwrap();
    assert_eq!(described, Some(vec![Some("text[]".to_string())]));
    assert_eq!(bicdb_sql::pg_type_oid_by_name("text[]"), Some(1009));
}

#[test]
fn coalesce_resolves_an_untyped_literal_to_the_other_arguments_type() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let text_array = || Some("text[]".to_string());
    for (sql, want) in [
        ("SELECT COALESCE(array_agg(x), '{}') FROM unnest(ARRAY['a','b']) x", texts(&["a", "b"])),
        ("SELECT COALESCE(array_agg(x), '{}') FROM unnest(ARRAY['a','b']) x WHERE false", texts(&[])),
        ("SELECT COALESCE(array_agg(x ORDER BY x DESC), '{}') FROM unnest(ARRAY['a','b']) x", texts(&["b", "a"])),
        ("SELECT CASE WHEN count(*) > 0 THEN array_agg(x) ELSE '{}' END FROM unnest(ARRAY['a','b']) x WHERE false", texts(&[])),
        ("SELECT NULLIF(array_agg(x), '{}') FROM unnest(ARRAY['a','b']) x", texts(&["a", "b"])),
    ] {
        assert_eq!(typed_cell(&mut s, sql), (want, text_array()), "{sql}");
    }
    assert_eq!(
        s.execute("SELECT pg_typeof(COALESCE(array_agg(x), '{}')) FROM unnest(ARRAY['a','b']) x")
            .unwrap()
            .rows,
        vec![vec![text("text[]")]]
    );
    // Literals alone stay text.
    assert_eq!(
        typed_cell(&mut s, "SELECT COALESCE(NULL, 'x')"),
        (text("x"), Some("text".to_string()))
    );

    // The owner API's complimentary-entitlement lookup (billing.carrier).
    s.execute("CREATE TABLE billing_complimentary_grants (scope_kind text, scope_id text, entitlements text[], revoked_at timestamptz, expires_at timestamptz)").unwrap();
    s.execute("CREATE TABLE network_organizations (id uuid PRIMARY KEY, status text)")
        .unwrap();
    s.execute(
        "CREATE TABLE network_org_members (org_id uuid, subject text, status text, roles text[])",
    )
    .unwrap();
    s.execute("INSERT INTO network_organizations VALUES ('00000000-0000-0000-0000-00000000000a', 'active'), ('00000000-0000-0000-0000-00000000000b', 'closed')").unwrap();
    s.execute("INSERT INTO network_org_members VALUES ('00000000-0000-0000-0000-00000000000a', 'ann', 'active', ARRAY['owner']), ('00000000-0000-0000-0000-00000000000b', 'ann', 'active', ARRAY['owner'])").unwrap();
    s.execute("INSERT INTO billing_complimentary_grants VALUES \
        ('user', 'ann', ARRAY['courses.publish','streams.pro'], NULL, NULL), \
        ('user', 'ann', ARRAY['courses.publish'], NULL, now() + interval '1 day'), \
        ('user', 'ann', ARRAY['ai.writer'], now(), NULL), \
        ('org', '00000000-0000-0000-0000-00000000000a', ARRAY['org.analytics','courses.publish'], NULL, NULL), \
        ('org', '00000000-0000-0000-0000-00000000000b', ARRAY['org.closed'], NULL, NULL), \
        ('user', 'ben', ARRAY['streams.pro'], NULL, NULL)").unwrap();
    let sql = "SELECT COALESCE(array_agg(DISTINCT e ORDER BY e),'{}') AS names FROM billing_complimentary_grants g CROSS JOIN LATERAL unnest(g.entitlements) e WHERE g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at>NOW()) AND e=ANY($2::text[]) AND ((g.scope_kind='user' AND g.scope_id=$1) OR (g.scope_kind='org' AND g.scope_id IN (SELECT m.org_id::text FROM network_org_members m JOIN network_organizations o ON o.id=m.org_id AND o.status='active' WHERE m.subject=$1 AND m.status='active' AND m.roles && $3::text[])))";
    let run = |db: &mut BicDb, subject: &str, wanted: &str, roles: &str| {
        let mut p = SqlSession::new(db).with_positional_parameters(vec![
            text(subject),
            text(wanted),
            text(roles),
        ]);
        let result = p.execute(sql).unwrap();
        (result.columns, result.rows, result.column_types)
    };
    drop(s);
    let all = "{courses.publish,streams.pro,ai.writer,org.analytics,org.closed}";
    assert_eq!(
        run(&mut db, "ann", all, "{owner,admin}"),
        (
            vec!["names".to_string()],
            vec![vec![texts(&[
                "courses.publish",
                "org.analytics",
                "streams.pro"
            ])]],
            vec![text_array()]
        )
    );
    assert_eq!(
        run(&mut db, "ann", all, "{member}").1,
        vec![vec![texts(&["courses.publish", "streams.pro"])]]
    );
    assert_eq!(
        run(&mut db, "carl", all, "{owner}").1,
        vec![vec![texts(&[])]]
    );
    let described = bicdb_sql::infer_query_result_types(&db, sql).unwrap();
    assert_eq!(described, Some(vec![text_array()]));
}

#[test]
fn quantified_comparison_over_a_subquery_compares_with_every_row() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE a (id int PRIMARY KEY, grp int, v int)")
        .unwrap();
    s.execute("CREATE TABLE b (grp int, w int)").unwrap();
    s.execute("INSERT INTO a SELECT i, i % 3, i FROM generate_series(1, 10) i")
        .unwrap();
    s.execute("INSERT INTO a VALUES (11, 0, NULL)").unwrap();
    s.execute("INSERT INTO b VALUES (0, 3), (0, 6), (1, 4), (1, 5), (2, 8), (2, NULL)")
        .unwrap();
    let ids = |s: &mut SqlSession, sql: &str| -> Vec<SqlValue> {
        s.execute(sql)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    let ints = |values: &[i64]| values.iter().map(|v| SqlValue::Int(*v)).collect::<Vec<_>>();
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v = ANY (SELECT w FROM b) ORDER BY id"
        ),
        ints(&[3, 4, 5, 6, 8])
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v = SOME (SELECT w FROM b WHERE w IS NOT NULL) ORDER BY id"
        ),
        ints(&[3, 4, 5, 6, 8])
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v = ANY (SELECT w FROM b WHERE b.grp = a.grp) ORDER BY id"
        ),
        ints(&[3, 4, 6, 8])
    );
    // ALL: true over no rows; a NULL in the list makes a non-failing comparison unknown.
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v > ALL (SELECT w FROM b WHERE w IS NOT NULL) ORDER BY id"
        ),
        ints(&[9, 10])
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v > ALL (SELECT w FROM b) ORDER BY id"
        ),
        ints(&[])
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM a WHERE v < ALL (SELECT w FROM b WHERE false) ORDER BY id"
        ),
        ints(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT count(*) FROM a WHERE NOT (v = ANY (SELECT w FROM b))"
        ),
        ints(&[0])
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT count(*) FROM a WHERE NOT (v = ANY (SELECT w FROM b WHERE w IS NOT NULL))"
        ),
        ints(&[5])
    );
    assert!(s
        .execute("SELECT id FROM a WHERE v = ANY (SELECT w, grp FROM b)")
        .is_err());
}

#[test]
fn date_column_plus_or_minus_an_integer_is_a_day_offset() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE daily (subject text, day text)").unwrap();
    s.execute("INSERT INTO daily VALUES ('ann','2026-10-10'),('ann','2026-10-12'),('ann','2026-10-13'),('ann','2026-10-14'),('ann','2026-10-14')").unwrap();
    s.execute("CREATE TABLE dated (d date)").unwrap();
    s.execute("INSERT INTO dated VALUES ('2026-10-13')").unwrap();
    let rows = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows;
    assert_eq!(rows(&mut s, "SELECT d - 1, d + 2, 3 + d FROM dated"), vec![vec![text("2026-10-12"), text("2026-10-15"), text("2026-10-16")]]);
    assert_eq!(rows(&mut s, "WITH days AS (SELECT '2026-10-13'::date AS day) SELECT day - 1 FROM days"), vec![vec![text("2026-10-12")]]);
    assert_eq!(rows(&mut s, "SELECT day - (ROW_NUMBER() OVER (ORDER BY day))::int FROM (VALUES (DATE '2026-10-13'),(DATE '2026-10-14')) v(day)"), vec![vec![text("2026-10-12")], vec![text("2026-10-12")]]);
    // The flashcard streak (flashcards.carrier): consecutive days share a group.
    let streak = "WITH mine AS (SELECT * FROM daily WHERE subject='ann'), days AS (SELECT DISTINCT day::date AS day FROM mine), \
        g AS (SELECT day, day-(ROW_NUMBER() OVER (ORDER BY day))::int AS grp FROM days), \
        last AS (SELECT grp FROM g WHERE day >= DATE '2026-10-15' - 1 ORDER BY day DESC LIMIT 1) \
        SELECT COUNT(*)::int FROM g WHERE grp=(SELECT grp FROM last)";
    assert_eq!(rows(&mut s, streak), vec![vec![SqlValue::Int(3)]]);
}

#[test]
fn a_column_cast_to_text_compares_as_text_in_lookups_and_joins() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE grants (kind text, scope text)").unwrap();
    s.execute("CREATE TABLE orgs (id uuid PRIMARY KEY, handle text)").unwrap();
    s.execute("CREATE TABLE plain (id uuid, handle text)").unwrap();
    s.execute("CREATE INDEX plain_id ON plain (id)").unwrap();
    s.execute("INSERT INTO grants VALUES ('user','bob'),('org','00000000-0000-0000-0000-00000000000a')").unwrap();
    s.execute("INSERT INTO orgs VALUES ('00000000-0000-0000-0000-00000000000a','acme')").unwrap();
    s.execute("INSERT INTO plain VALUES ('00000000-0000-0000-0000-00000000000a','acme')").unwrap();
    let rows = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows;
    let none: Vec<Vec<SqlValue>> = vec![];
    assert_eq!(rows(&mut s, "SELECT handle FROM orgs WHERE id::text = 'bob'"), none);
    assert_eq!(rows(&mut s, "SELECT handle FROM plain WHERE id::text = 'bob'"), none);
    assert_eq!(rows(&mut s, "SELECT handle FROM orgs WHERE id::text = '00000000-0000-0000-0000-00000000000a'"), vec![vec![text("acme")]]);
    // A non-canonical spelling is a different text, though it is the same uuid.
    assert_eq!(rows(&mut s, "SELECT handle FROM orgs WHERE id::text = '00000000-0000-0000-0000-00000000000A'"), none);
    for table in ["orgs", "plain"] {
        let sql = format!("SELECT g.scope, o.handle FROM grants g LEFT JOIN {table} o ON g.kind='org' AND o.id::text=g.scope ORDER BY g.scope");
        assert_eq!(rows(&mut s, &sql), vec![vec![text("00000000-0000-0000-0000-00000000000a"), text("acme")], vec![text("bob"), SqlValue::Null]], "{sql}");
        let sql = format!("SELECT g.scope FROM grants g WHERE EXISTS (SELECT 1 FROM {table} o WHERE o.id::text=g.scope)");
        assert_eq!(rows(&mut s, &sql), vec![vec![text("00000000-0000-0000-0000-00000000000a")]], "{sql}");
    }
    // Casts that keep equality still find the row.
    s.execute("CREATE TABLE n (id int PRIMARY KEY, v text)").unwrap();
    s.execute("INSERT INTO n VALUES (7, 'seven')").unwrap();
    assert_eq!(rows(&mut s, "SELECT v FROM n WHERE id::bigint = 7"), vec![vec![text("seven")]]);
    assert_eq!(rows(&mut s, "SELECT id FROM n WHERE v::text = 'seven'"), vec![vec![SqlValue::Int(7)]]);
}

#[test]
fn select_for_update_waits_for_the_row_and_then_sees_the_committed_change() {
    // learning_add_module: lock the course, count its modules, append one. Parallel adds must
    // queue on the course row (READ COMMITTED), not fail with a serialization error.
    let (_dir, mut db) = db();
    {
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE TABLE courses (id int PRIMARY KEY, version int)").unwrap();
        s.execute("CREATE TABLE modules (course int, position int)").unwrap();
        s.execute("INSERT INTO courses VALUES (1, 0)").unwrap();
    }
    let mut first = SqlSession::new_shared(&db);
    first.execute("BEGIN").unwrap();
    first.execute("SELECT id FROM courses WHERE id=1 FOR UPDATE").unwrap();
    std::thread::scope(|scope| {
        let (done, result) = std::sync::mpsc::channel();
        let db = &db;
        scope.spawn(move || {
            let mut second = SqlSession::new_shared(db);
            second.execute("BEGIN").unwrap();
            let outcome = (|| {
                let locked = second.execute("SELECT id, version FROM courses WHERE id=1 FOR UPDATE")?.rows;
                let count = second.execute("SELECT count(*) FROM modules WHERE course=1")?.rows;
                second.execute("INSERT INTO modules VALUES (1, 2)")?;
                second.execute("UPDATE courses SET version=version+1 WHERE id=1")?;
                second.execute("COMMIT")?;
                Ok::<_, bicdb_sql::SqlError>((locked, count))
            })();
            done.send(outcome.map_err(|error| error.to_string())).unwrap();
        });
        let early = result.recv_timeout(std::time::Duration::from_millis(200));
        first.execute("INSERT INTO modules VALUES (1, 1)").unwrap();
        first.execute("UPDATE courses SET version=version+1 WHERE id=1").unwrap();
        first.execute("COMMIT").unwrap();
        assert!(early.is_err(), "the second locker must wait for the first");
        let (locked, count) = result.recv_timeout(std::time::Duration::from_secs(20)).unwrap().unwrap();
        assert_eq!(locked, vec![vec![SqlValue::Int(1), SqlValue::Int(1)]]);
        assert_eq!(count, vec![vec![SqlValue::Int(1)]]);
    });
    let mut s = SqlSession::new_shared(&db);
    assert_eq!(s.execute("SELECT version FROM courses").unwrap().rows, vec![vec![SqlValue::Int(2)]]);
    assert_eq!(s.execute("SELECT string_agg(position::text, ',' ORDER BY position) FROM modules").unwrap().rows, vec![vec![text("1,2")]]);
}

#[test]
fn a_primary_key_added_after_the_rows_finds_and_guards_those_rows() {
    // pg_dump order: CREATE TABLE, COPY the rows, then ADD CONSTRAINT ... PRIMARY KEY.
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE credential_categories (key text NOT NULL, label text NOT NULL)").unwrap();
    s.execute("INSERT INTO credential_categories VALUES ('course_completion','Certificate of completion'),('training_certificate','Training certificate')").unwrap();
    s.execute("ALTER TABLE ONLY credential_categories ADD CONSTRAINT credential_categories_pkey PRIMARY KEY (key)").unwrap();
    s.execute("CREATE TABLE pairs (a int NOT NULL, b int NOT NULL, v text)").unwrap();
    s.execute("INSERT INTO pairs VALUES (1,1,'x'),(1,2,'y')").unwrap();
    s.execute("ALTER TABLE pairs ADD PRIMARY KEY (a, b)").unwrap();
    s.execute("CREATE TABLE certs (code text, category text)").unwrap();
    s.execute("INSERT INTO certs VALUES ('C1','training_certificate'),('C2',NULL)").unwrap();
    let rows = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows;
    assert_eq!(rows(&mut s, "SELECT label FROM credential_categories WHERE key='training_certificate'"), vec![vec![text("Training certificate")]]);
    assert_eq!(rows(&mut s, "SELECT c.code, cc.label FROM certs c LEFT JOIN credential_categories cc ON cc.key=c.category ORDER BY c.code"), vec![vec![text("C1"), text("Training certificate")], vec![text("C2"), SqlValue::Null]]);
    assert_eq!(rows(&mut s, "SELECT v FROM pairs WHERE a=1 AND b=2"), vec![vec![text("y")]]);
    assert!(s.execute("INSERT INTO credential_categories VALUES ('course_completion','again')").is_err());
    assert!(s.execute("INSERT INTO pairs VALUES (1,1,'again')").is_err());
    s.execute("UPDATE credential_categories SET label='Completion' WHERE key='course_completion'").unwrap();
    assert_eq!(rows(&mut s, "SELECT key, label FROM credential_categories ORDER BY key"), vec![vec![text("course_completion"), text("Completion")], vec![text("training_certificate"), text("Training certificate")]]);
    // Rows that break the new key refuse it.
    s.execute("CREATE TABLE dupes (k text NOT NULL)").unwrap();
    s.execute("INSERT INTO dupes VALUES ('a'),('a')").unwrap();
    assert!(s.execute("ALTER TABLE dupes ADD PRIMARY KEY (k)").is_err());
}

#[test]
fn make_interval_with_named_arguments_works_in_insert_update_and_delete() {
    // jobs_run requeues a stuck job with run_after = NOW() + make_interval(mins => attempts^2).
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE jobs (id int PRIMARY KEY, attempts int, run_after timestamptz, wait interval)").unwrap();
    s.execute("INSERT INTO jobs VALUES (1, 2, now(), make_interval(secs => 30))").unwrap();
    s.execute("UPDATE jobs SET run_after = NOW() + make_interval(mins => (attempts*attempts)::int) WHERE id = 1").unwrap();
    let rows = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows;
    assert_eq!(rows(&mut s, "SELECT run_after > NOW() + interval '3 minutes', run_after < NOW() + interval '5 minutes', wait::text FROM jobs"), vec![vec![SqlValue::Bool(true), SqlValue::Bool(true), text("00:00:30")]]);
    s.execute("UPDATE jobs SET wait = make_interval(hours => 1, mins => attempts), attempts = ceil(attempts / 4.0) WHERE id = 1").unwrap();
    assert_eq!(rows(&mut s, "SELECT wait::text, attempts FROM jobs"), vec![vec![text("01:02:00"), SqlValue::Int(1)]]);
    s.execute("DELETE FROM jobs WHERE run_after > NOW() + make_interval(mins => 10)").unwrap();
    assert_eq!(rows(&mut s, "SELECT count(*) FROM jobs"), vec![vec![SqlValue::Int(1)]]);
    s.execute("DELETE FROM jobs WHERE run_after > NOW() + make_interval(mins => 1)").unwrap();
    assert_eq!(rows(&mut s, "SELECT count(*) FROM jobs"), vec![vec![SqlValue::Int(0)]]);
}

#[test]
fn now_and_clock_timestamp_carry_microseconds() {
    // Rapid progress heartbeats measure elapsed time with NOW(); whole seconds minted a second per call.
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let micros = |s: &mut SqlSession, sql: &str| -> Vec<String> {
        s.execute(sql).unwrap().rows.remove(0).into_iter().map(|value| value.to_cell()).collect()
    };
    let mut fractional = 0;
    for _ in 0..5 {
        let values = micros(&mut s, "SELECT now()::text, clock_timestamp()::text, (SELECT now()::text FROM (VALUES (1)) v(x))");
        fractional += values.iter().filter(|value| value.contains('.')).count();
        std::thread::sleep(std::time::Duration::from_millis(3));
    }
    assert!(fractional >= 13, "timestamps keep their fraction of a second ({fractional}/15)");
    s.execute("CREATE TABLE beats (id int PRIMARY KEY, at timestamptz)").unwrap();
    s.execute("INSERT INTO beats VALUES (1, now())").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let elapsed = s.execute("SELECT EXTRACT(EPOCH FROM now() - at)::float8 FROM beats").unwrap().rows.remove(0).remove(0);
    let elapsed = match elapsed { SqlValue::Float(f) => f, other => other.to_cell().parse::<f64>().unwrap() };
    assert!(elapsed > 0.0 && elapsed < 0.9, "elapsed {elapsed}");
    // now() is the transaction's start; clock_timestamp() moves on.
    s.execute("BEGIN").unwrap();
    let first = micros(&mut s, "SELECT now()::text");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let again = micros(&mut s, "SELECT now()::text, (now() < clock_timestamp())::text");
    s.execute("COMMIT").unwrap();
    assert_eq!(first[0], again[0]);
    assert_eq!(again[1], "true");
}

#[test]
fn pg_input_is_valid_checks_text_against_a_type() {
    // Profile credentials validate issued_on/expires_on with pg_input_is_valid($1,'date').
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let rows = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows;
    let b = SqlValue::Bool;
    assert_eq!(rows(&mut s, "SELECT pg_input_is_valid('2024-02-28','date'), pg_input_is_valid('2024-02-30','date'), pg_input_is_valid('2024-13-01','date'), pg_input_is_valid('42','integer'), pg_input_is_valid('4x','int4'), pg_input_is_valid(NULL,'date')"),
        vec![vec![b(true), b(false), b(false), b(true), b(false), SqlValue::Null]]);
    s.execute("CREATE TABLE creds (issued_on text)").unwrap();
    s.execute("INSERT INTO creds VALUES ('2024-01-02'),('2024-02-30')").unwrap();
    assert_eq!(rows(&mut s, "SELECT issued_on FROM creds WHERE NOT pg_input_is_valid(issued_on,'date')"), vec![vec![text("2024-02-30")]]);
    assert_eq!(rows(&mut s, "SELECT CASE WHEN ('2024-02-30'::text IS NOT NULL AND ('2024-02-30' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' OR NOT pg_input_is_valid('2024-02-30','date'))) THEN 'invalid' ELSE '' END"), vec![vec![text("invalid")]]);
}

#[test]
fn an_inner_join_chain_starts_from_the_table_its_where_clause_pins_by_index() {
    // Explore's cover image: medias JOIN posts ON .. WHERE posts.space_id = <stream>, per Stream.
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE posts (id int PRIMARY KEY, space_id int, visibility text)").unwrap();
    s.execute("CREATE INDEX posts_space ON posts (space_id)").unwrap();
    s.execute("CREATE TABLE medias (id int PRIMARY KEY, post_id int, kind text)").unwrap();
    s.execute("CREATE INDEX medias_post ON medias (post_id)").unwrap();
    s.execute("CREATE TABLE notes (id int PRIMARY KEY, media_id int)").unwrap();
    s.execute("INSERT INTO posts SELECT i, i % 50, CASE WHEN i % 3 = 0 THEN 'public' ELSE 'members' END FROM generate_series(1, 3000) i").unwrap();
    s.execute("INSERT INTO medias SELECT i, i, 'image' FROM generate_series(1, 2000) i").unwrap();
    s.execute("INSERT INTO notes SELECT i, i FROM generate_series(1, 10) i").unwrap();
    let plan = |s: &mut SqlSession, sql: &str| -> String {
        s.execute(&format!("EXPLAIN {sql}")).unwrap().rows.into_iter().map(|row| row[0].to_cell()).collect::<Vec<_>>().join("\n")
    };
    let one = "SELECT m.id FROM medias m JOIN posts p ON p.id = m.post_id WHERE p.space_id = 7 AND p.visibility = 'public' ORDER BY m.id DESC LIMIT 1";
    assert!(plan(&mut s, one).contains("JoinOrder posts -> medias"), "{}", plan(&mut s, one));
    assert_eq!(s.execute(one).unwrap().rows, vec![vec![SqlValue::Int(1857)]]);
    let two = "SELECT count(*) FROM medias m JOIN posts p ON p.id = m.post_id AND p.visibility = 'public' JOIN notes n ON n.media_id = m.id WHERE p.space_id = 3";
    assert!(plan(&mut s, two).starts_with("NestedLoopJoin") || plan(&mut s, two).contains("posts -> "), "{}", plan(&mut s, two));
    assert_eq!(s.execute(two).unwrap().rows, vec![vec![SqlValue::Int(1)]]);
    // Same answers as the written order for every Stream.
    for space in [0, 3, 7, 49] {
        let reordered = s.execute(&format!("SELECT string_agg(m.id::text, ',' ORDER BY m.id) FROM medias m JOIN posts p ON p.id = m.post_id WHERE p.space_id = {space}")).unwrap().rows;
        let written = s.execute(&format!("SELECT string_agg(m.id::text, ',' ORDER BY m.id) FROM medias m LEFT JOIN posts p ON p.id = m.post_id WHERE p.space_id = {space}")).unwrap().rows;
        assert_eq!(reordered, written);
    }
}

#[test]
fn or_of_indexed_branches_and_in_subquery_finds_the_same_rows() {
    // Explore's course cover: courses of a Stream or of its cohorts, per Stream (correlated).
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE spaces (id int PRIMARY KEY)").unwrap();
    s.execute("CREATE TABLE courses (id int PRIMARY KEY, space_id int, status text, cover int)").unwrap();
    s.execute("CREATE INDEX courses_space ON courses (space_id)").unwrap();
    s.execute("CREATE TABLE cohorts (id int PRIMARY KEY, course_id int, space_id int)").unwrap();
    s.execute("CREATE INDEX cohorts_space ON cohorts (space_id)").unwrap();
    s.execute("CREATE TABLE medias (id int PRIMARY KEY, ready bool)").unwrap();
    s.execute("INSERT INTO spaces SELECT i FROM generate_series(1, 20) i").unwrap();
    s.execute("INSERT INTO courses SELECT i, i % 20 + 1, CASE WHEN i % 2 = 0 THEN 'published' ELSE 'draft' END, i FROM generate_series(1, 400) i").unwrap();
    s.execute("INSERT INTO cohorts SELECT i, i * 7, 21 - (i % 20) FROM generate_series(1, 30) i").unwrap();
    s.execute("INSERT INTO medias SELECT i, i % 3 <> 0 FROM generate_series(1, 400) i").unwrap();
    let fast = "SELECT s.id, (SELECT c.cover FROM courses c JOIN medias m ON m.id = c.cover AND m.ready WHERE (c.space_id = s.id OR c.id IN (SELECT course_id FROM cohorts WHERE space_id = s.id)) AND c.status = 'published' ORDER BY c.id DESC LIMIT 1), \
        (SELECT count(*) FROM courses c WHERE c.space_id = s.id OR c.id IN (SELECT course_id FROM cohorts WHERE space_id = s.id)) FROM spaces s ORDER BY s.id";
    // The same predicate the planner cannot split (NOT NOT keeps it opaque).
    let slow = "SELECT s.id, (SELECT c.cover FROM courses c JOIN medias m ON m.id = c.cover AND m.ready WHERE NOT NOT (c.space_id = s.id OR c.id IN (SELECT course_id FROM cohorts WHERE space_id = s.id)) AND c.status = 'published' ORDER BY c.id DESC LIMIT 1), \
        (SELECT count(*) FROM courses c WHERE NOT NOT (c.space_id = s.id OR c.id IN (SELECT course_id FROM cohorts WHERE space_id = s.id))) FROM spaces s ORDER BY s.id";
    let expected = s.execute(slow).unwrap().rows;
    assert_eq!(expected.len(), 20);
    assert_eq!(s.execute(fast).unwrap().rows, expected);
    // A branch that depends on the scanned table itself is never run on its own.
    let own = "SELECT count(*) FROM courses c WHERE c.space_id = 3 OR c.id IN (SELECT course_id FROM cohorts WHERE cohorts.space_id = c.space_id)";
    let own_slow = "SELECT count(*) FROM courses c WHERE NOT NOT (c.space_id = 3 OR c.id IN (SELECT course_id FROM cohorts WHERE cohorts.space_id = c.space_id))";
    assert_eq!(s.execute(own).unwrap().rows, s.execute(own_slow).unwrap().rows);
}

#[test]
fn a_correlated_join_chain_is_driven_by_the_outer_rows_index_key() {
    // Explore's public tile image per Stream: medias JOIN posts WHERE posts.space_id = ids.id.
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE posts (id int PRIMARY KEY, space_id int, visibility text)").unwrap();
    s.execute("CREATE INDEX posts_space ON posts (space_id)").unwrap();
    s.execute("CREATE TABLE medias (id int PRIMARY KEY, post_id int, state text)").unwrap();
    s.execute("CREATE INDEX medias_post ON medias (post_id)").unwrap();
    s.execute("INSERT INTO posts SELECT i, i % 200, CASE WHEN i % 3 = 0 THEN 'public' ELSE 'members' END FROM generate_series(1, 6000) i").unwrap();
    s.execute("INSERT INTO medias SELECT i, i, CASE WHEN i % 5 = 0 THEN 'pending' ELSE 'ready' END FROM generate_series(1, 4000) i").unwrap();
    let sql = |pin: &str| format!("WITH ids AS (SELECT g AS id FROM generate_series(0, 199) g) SELECT ids.id, (SELECT m.id FROM medias m JOIN posts p ON p.id = m.post_id WHERE {pin} AND p.visibility = 'public' AND m.state = 'ready' ORDER BY m.id DESC LIMIT 1) FROM ids ORDER BY ids.id");
    let started = std::time::Instant::now();
    let fast = s.execute(&sql("p.space_id = ids.id")).unwrap().rows;
    let fast_time = started.elapsed();
    let slow = s.execute(&sql("p.space_id + 0 = ids.id")).unwrap().rows;
    assert_eq!(fast, slow);
    assert_eq!(fast[3], vec![SqlValue::Int(3), SqlValue::Int(3603)]);
    assert!(fast_time < std::time::Duration::from_secs(2), "{fast_time:?}");
}

#[test]
fn a_left_join_on_an_equality_keeps_unmatched_rows_and_is_hashed() {
    // The feed: network_posts LEFT JOIN permitted (a jsonb CTE of Streams) ON s.id = p.space_id.
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE posts (id int PRIMARY KEY, space_id int, kind text)").unwrap();
    s.execute("INSERT INTO posts SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i % 900 END, CASE WHEN i % 2 = 0 THEN 'a' ELSE 'b' END FROM generate_series(1, 9000) i").unwrap();
    let sql = |on: &str| format!("WITH permitted AS (SELECT g AS id, CASE WHEN g % 3 = 0 THEN 'member' ELSE '' END AS role FROM generate_series(1, 600) g) \
        SELECT count(*), count(s.id), count(*) FILTER (WHERE s.role = 'member'), sum(p.id) FILTER (WHERE s.id IS NULL) FROM posts p LEFT JOIN permitted s ON {on}");
    let started = std::time::Instant::now();
    let hashed = s.execute(&sql("s.id = p.space_id")).unwrap().rows;
    let hashed_time = started.elapsed();
    let nested = s.execute(&sql("s.id + 0 = p.space_id")).unwrap().rows;
    assert_eq!(hashed, nested);
    assert_eq!(hashed[0][0], SqlValue::Int(9000));
    assert!(hashed_time < std::time::Duration::from_secs(2), "{hashed_time:?}");
    // A residual condition on the left row only drops the match, never the row.
    let residual = s.execute(&sql("s.id = p.space_id AND p.kind = 'a'")).unwrap().rows;
    let residual_nested = s.execute(&sql("s.id + 0 = p.space_id AND p.kind = 'a'")).unwrap().rows;
    assert_eq!(residual, residual_nested);
}

#[test]
fn an_array_subquery_over_a_parameter_is_described_as_an_array() {
    // moderation_scopes: ARRAY(SELECT jsonb_array_elements_text($2::jsonb)) must reach the client as
    // text[] (a JS array), or an empty '{}' string has length 2 and outsiders see the moderation queue.
    let (_dir, mut db) = db();
    let sql = "SELECT $1::boolean AS platform, ARRAY(SELECT jsonb_array_elements_text($2::jsonb)) AS streams";
    let described = bicdb_sql::infer_query_result_types(&db, sql).unwrap();
    assert_eq!(described, Some(vec![Some("bool".to_string()), Some("text[]".to_string())]));
    let keys = bicdb_sql::infer_query_result_types(&db, "SELECT ARRAY(SELECT jsonb_object_keys($1::jsonb))").unwrap();
    assert_eq!(keys, Some(vec![Some("text[]".to_string())]));
}

#[test]
fn parameter_switches_in_filters_fold_before_rows_are_read() {
    // The learners page: `$2='' OR name matches`, `CASE WHEN $3='all' ..`, `$6<>'needs_review' OR EXISTS ..`.
    let (_dir, mut db) = db();
    {
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE TABLE enrollments (id int PRIMARY KEY, subject text, status text, at int)").unwrap();
        s.execute("CREATE TABLE subs (subject text, status text)").unwrap();
        s.execute("INSERT INTO enrollments SELECT i, 'l-'||i, CASE WHEN i % 10 = 0 THEN 'requested' WHEN i % 33 = 0 THEN 'removed' ELSE 'active' END, i FROM generate_series(1, 20000) i").unwrap();
        s.execute("INSERT INTO subs SELECT 'l-'||i, 'submitted' FROM generate_series(1, 40) i").unwrap();
    }
    let sql = "SELECT subject FROM enrollments e WHERE (CASE WHEN $1='all' THEN e.status<>'removed' ELSE e.status=$1 END) \
        AND ($2='' OR strpos(e.subject, $2)>0) AND ($3<>'needs_review' OR EXISTS (SELECT 1 FROM subs u WHERE u.subject=e.subject AND u.status='submitted')) \
        ORDER BY CASE WHEN $4='name' THEN e.subject END, e.at DESC, e.subject LIMIT 3";
    let run = |db: &mut BicDb, params: [&str; 4]| -> Vec<Vec<SqlValue>> {
        let mut s = SqlSession::new(db).with_positional_parameters(params.iter().map(|value| text(value)).collect());
        s.execute(sql).unwrap().rows
    };
    let started = std::time::Instant::now();
    assert_eq!(run(&mut db, ["active", "", "", "recent"]), vec![vec![text("l-19999")], vec![text("l-19997")], vec![text("l-19996")]]);
    assert!(started.elapsed() < std::time::Duration::from_secs(2), "{:?}", started.elapsed());
    assert_eq!(run(&mut db, ["all", "", "", "recent"]), vec![vec![text("l-20000")], vec![text("l-19999")], vec![text("l-19997")]]);
    assert_eq!(run(&mut db, ["requested", "l-19", "", "name"]), vec![vec![text("l-190")], vec![text("l-1900")], vec![text("l-19000")]]);
    assert_eq!(run(&mut db, ["active", "", "needs_review", "name"]), vec![vec![text("l-1")], vec![text("l-11")], vec![text("l-12")]]);
    // NULL never folds: a NULL parameter keeps the comparison unknown.
    let mut s = SqlSession::new(&mut db).with_positional_parameters(vec![SqlValue::Null]);
    assert_eq!(s.execute("SELECT count(*) FROM enrollments WHERE ($1 = '' OR id < 3)").unwrap().rows, vec![vec![SqlValue::Int(2)]]);
}

#[test]
fn a_union_of_three_branches_with_typed_nulls_matches_types() {
    // learning_list: playlists, collections (NULL::uuid owner_org_id) and courses (owner_org_id uuid).
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE playlists (id int, status text)").unwrap();
    s.execute("CREATE TABLE collections (id int, status text)").unwrap();
    s.execute("CREATE TABLE courses (id int, status text, owner_org_id uuid)").unwrap();
    s.execute("INSERT INTO playlists VALUES (1,'active')").unwrap();
    s.execute("INSERT INTO collections VALUES (2,'active')").unwrap();
    s.execute("INSERT INTO courses VALUES (3,'active','00000000-0000-0000-0000-00000000000a')").unwrap();
    let rows = s.execute("SELECT o.id, o.owner_org_id FROM (SELECT id,status,NULL::uuid AS owner_org_id FROM playlists UNION ALL SELECT id,status,NULL::uuid FROM collections UNION ALL SELECT id,status,owner_org_id FROM courses) o WHERE o.status<>'archived' ORDER BY o.id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(1), SqlValue::Null], vec![SqlValue::Int(2), SqlValue::Null], vec![SqlValue::Int(3), text("00000000-0000-0000-0000-00000000000a")]]);
}

#[test]
fn a_filter_switched_off_by_a_parameter_reads_no_rows() {
    // Progress heartbeats: SELECT cohort_id FROM learning_enrollments WHERE $1='course' AND ... outside a course.
    let (_dir, mut db) = db();
    {
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE TABLE enrollments (id int PRIMARY KEY, course_id text, subject text, cohort_id text)").unwrap();
        s.execute("INSERT INTO enrollments SELECT i, 'c'||(i % 7), 'l-'||i, 'h'||i FROM generate_series(1, 60000) i").unwrap();
    }
    let sql = "SELECT COALESCE((SELECT cohort_id FROM enrollments WHERE $1='course' AND course_id=lower($2) AND subject=$3),'') AS cohort";
    let started = std::time::Instant::now();
    for _ in 0..20 {
        let mut s = SqlSession::new(&mut db).with_positional_parameters(vec![text("feed"), text("C3"), text("l-3")]);
        assert_eq!(s.execute(sql).unwrap().rows, vec![vec![text("")]]);
    }
    assert!(started.elapsed() < std::time::Duration::from_secs(2), "{:?}", started.elapsed());
    let mut s = SqlSession::new(&mut db).with_positional_parameters(vec![text("course"), text("C3"), text("l-3")]);
    assert_eq!(s.execute(sql).unwrap().rows, vec![vec![text("h3")]]);
    let mut s = SqlSession::new(&mut db).with_positional_parameters(vec![text("feed")]);
    assert_eq!(s.execute("SELECT count(*) FROM enrollments WHERE $1 = 'course'").unwrap().rows, vec![vec![SqlValue::Int(0)]]);
    assert_eq!(s.execute("SELECT count(*) FROM enrollments e JOIN enrollments f ON f.id = e.id WHERE $1 = 'course'").unwrap().rows, vec![vec![SqlValue::Int(0)]]);
}

#[test]
fn primary_key_equals_any_array_reads_only_those_rows() {
    // SELECT * FROM network_posts WHERE id = ANY(ARRAY[...]::uuid[]) loads a page of posts.
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE posts (id uuid PRIMARY KEY, n int, body text)").unwrap();
    s.execute("INSERT INTO posts SELECT ('00000000-0000-0000-0000-'||lpad(i::text,12,'0'))::uuid, i, repeat('x', 200) FROM generate_series(1, 40000) i").unwrap();
    let sql = "SELECT n FROM posts WHERE id = ANY(ARRAY['00000000-0000-0000-0000-000000000007','00000000-0000-0000-0000-000000039999','00000000-0000-0000-0000-999999999999']::uuid[]) ORDER BY n";
    let started = std::time::Instant::now();
    for _ in 0..20 {
        assert_eq!(s.execute(sql).unwrap().rows, vec![vec![SqlValue::Int(7)], vec![SqlValue::Int(39999)]]);
    }
    assert!(started.elapsed() < std::time::Duration::from_secs(2), "{:?}", started.elapsed());
    assert_eq!(s.execute("SELECT count(*) FROM posts WHERE id = ANY(ARRAY[]::uuid[])").unwrap().rows, vec![vec![SqlValue::Int(0)]]);
    assert_eq!(s.execute("SELECT count(*) FROM posts WHERE id = ANY(ARRAY['00000000-0000-0000-0000-000000000003', NULL]::uuid[]) AND n > 2").unwrap().rows, vec![vec![SqlValue::Int(1)]]);
}
