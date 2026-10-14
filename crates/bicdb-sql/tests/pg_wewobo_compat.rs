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
    s.execute("CREATE TABLE invites (space int, email text, claimed_at timestamptz)").unwrap();
    // Existing duplicates outside the predicate do not stop the index from being built.
    s.execute("INSERT INTO invites VALUES (1,'a@x',now()),(1,'a@x',now()),(1,'a@x',NULL)").unwrap();
    s.execute("CREATE UNIQUE INDEX invites_open_uq ON invites (space, email) WHERE claimed_at IS NULL").unwrap();
    // More claimed rows are fine; a second open row is not.
    s.execute("INSERT INTO invites VALUES (1,'a@x',now())").unwrap();
    assert!(s.execute("INSERT INTO invites VALUES (1,'a@x',NULL)").is_err(), "second open invitation must conflict");
    // Moving a row into the predicate is checked too; moving it out frees the key.
    assert!(s.execute("UPDATE invites SET claimed_at=NULL WHERE claimed_at IS NOT NULL").is_err());
    s.execute("UPDATE invites SET claimed_at=now() WHERE claimed_at IS NULL").unwrap();
    s.execute("INSERT INTO invites VALUES (1,'a@x',NULL)").unwrap();
    // ON CONFLICT can name the partial index by its columns and predicate.
    s.execute("INSERT INTO invites VALUES (1,'a@x',NULL) ON CONFLICT (space, email) WHERE claimed_at IS NULL DO NOTHING").unwrap();
    let open = s.execute("SELECT count(*) FROM invites WHERE claimed_at IS NULL").unwrap().rows;
    assert_eq!(open, vec![vec![SqlValue::Int(1)]]);
    let all = s.execute("SELECT count(*) FROM invites").unwrap().rows;
    assert_eq!(all, vec![vec![SqlValue::Int(5)]]);
    // Rows that never enter the predicate can repeat freely.
    s.execute("CREATE TABLE pu_t (a int, b bool); CREATE UNIQUE INDEX pu_t_uq ON pu_t (a) WHERE b").unwrap();
    s.execute("INSERT INTO pu_t VALUES (1,false),(1,false),(1,true)").unwrap();
    assert!(s.execute("INSERT INTO pu_t VALUES (1,true)").is_err());
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
    let rows = s.execute(&format!("SELECT * FROM ({union}) q ORDER BY 1, 2")).unwrap().rows;
    assert_eq!(rows, expected);
    // Implicit lateral (comma) form and a WHERE on the function's column.
    let filtered = s.execute("SELECT 0 AS id, 'x' AS role WHERE false UNION ALL SELECT m.id, r.role FROM members m, unnest(m.roles) AS r(role) WHERE r.role = ANY (ARRAY['admin','reviewer']) ORDER BY 1").unwrap().rows;
    assert_eq!(filtered, vec![vec![SqlValue::Int(1), text("admin")], vec![SqlValue::Int(2), text("reviewer")]]);
    s.execute(&format!("CREATE VIEW member_roles AS {union}")).unwrap();
    let via_view = s.execute("SELECT id, role FROM member_roles ORDER BY 1, 2").unwrap().rows;
    assert_eq!(via_view, expected);
}

#[test]
fn function_bodies_may_contain_non_ascii_text() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE FUNCTION greet(t text) RETURNS text LANGUAGE sql AS $$ SELECT 'Grüß ' || t $$").unwrap();
    s.execute("CREATE FUNCTION fold(t text) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN replace(t, 'Ä', 'A') || '·é'; END $$").unwrap();
    assert_eq!(s.execute("SELECT greet('Zoë')").unwrap().rows, vec![vec![text("Grüß Zoë")]]);
    assert_eq!(s.execute("SELECT fold('Äpfel')").unwrap().rows, vec![vec![text("Apfel·é")]]);
}

#[test]
fn translate_replaces_and_deletes_characters_like_postgresql() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let rows = s.execute("SELECT translate('12345', '143', 'ax'), translate('Ärger über Öl', 'ÄÖÜäöü', 'AOUaou'), translate(NULL, 'a', 'b'), translate('abc', '', 'x')").unwrap().rows;
    assert_eq!(rows, vec![vec![text("a2x5"), text("Arger uber Ol"), SqlValue::Null, text("abc")]]);
    let rows = s.execute("SELECT left('Grüße', 3), right('Grüße', 2), left('abc', -1), right('abc', -2), left('ab', 9), left(NULL, 1)").unwrap().rows;
    assert_eq!(rows, vec![vec![text("Grü"), text("ße"), text("ab"), text("c"), text("ab"), SqlValue::Null]]);
    s.execute("CREATE TABLE names (n text); INSERT INTO names VALUES ('Café'), ('Señor')").unwrap();
    assert_eq!(s.execute("SELECT left(n, 2) FROM names ORDER BY 1").unwrap().rows, vec![vec![text("Ca")], vec![text("Se")]]);
    let rows = s.execute("SELECT translate(n, 'éñ', 'en') FROM names ORDER BY 1").unwrap().rows;
    assert_eq!(rows, vec![vec![text("Cafe")], vec![text("Senor")]]);
}

#[test]
fn database_level_custom_settings_apply_to_new_sessions() {
    let (_dir, mut db) = db();
    let current = {
        let mut s = SqlSession::new(&mut db);
        let name = s.execute("SELECT current_database()").unwrap().rows[0][0].clone();
        let SqlValue::String(name) = name else { panic!("database name") };
        s.execute(&format!("ALTER DATABASE {name} SET wewobo.network_tenant = 'shared-tenant'")).unwrap();
        name
    };
    let mut s = SqlSession::new(&mut db);
    let rows = s.execute("SELECT current_setting('wewobo.network_tenant', true)").unwrap().rows;
    assert_eq!(rows, vec![vec![text("shared-tenant")]]);
    // A session may still override it; RESET returns to the database default.
    s.execute("SET wewobo.network_tenant = 'mine'").unwrap();
    assert_eq!(s.execute("SELECT current_setting('wewobo.network_tenant')").unwrap().rows, vec![vec![text("mine")]]);
    s.execute("RESET wewobo.network_tenant").unwrap();
    assert_eq!(s.execute("SELECT current_setting('wewobo.network_tenant')").unwrap().rows, vec![vec![text("shared-tenant")]]);
    drop(s);
    let mut s = SqlSession::new(&mut db);
    s.execute(&format!("ALTER DATABASE {current} RESET wewobo.network_tenant")).unwrap();
    drop(s);
    let mut s = SqlSession::new(&mut db);
    assert_eq!(s.execute("SELECT current_setting('wewobo.network_tenant', true)").unwrap().rows, vec![vec![SqlValue::Null]]);
}

#[test]
fn order_by_position_sorts_by_that_output_column() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE t (a int, b text); INSERT INTO t VALUES (1,'b'), (1,'a'), (0,'z')").unwrap();
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
        s.execute("SELECT a, b FROM t ORDER BY 1 DESC, 2 DESC").unwrap().rows,
        vec![vec![SqlValue::Int(1), text("b")], vec![SqlValue::Int(1), text("a")], vec![SqlValue::Int(0), text("z")]]
    );
    assert_eq!(
        s.execute("SELECT b, count(*) FROM t GROUP BY 1 ORDER BY 2 DESC, 1").unwrap().rows,
        vec![vec![text("a"), SqlValue::Int(1)], vec![text("b"), SqlValue::Int(1)], vec![text("z"), SqlValue::Int(1)]]
    );
}

#[test]
fn wewobo_course_slug_function_loads_and_runs() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE learning_courses (id int, slug text)").unwrap();
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
    assert_eq!(s.execute("SELECT learning_course_slug('Ayurveda für Ärzte')").unwrap().rows, vec![vec![text("ayurveda-fur-arzte")]]);
    s.execute("INSERT INTO learning_courses VALUES (1, 'ayurveda-fur-arzte')").unwrap();
    assert_eq!(s.execute("SELECT learning_course_slug('Ayurveda für Ärzte')").unwrap().rows, vec![vec![text("ayurveda-fur-arzte-2")]]);
    assert_eq!(s.execute("SELECT learning_course_slug('Ω')").unwrap().rows, vec![vec![text("course")]]);
}

#[test]
fn plpgsql_loop_while_and_exit() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute(r#"CREATE FUNCTION first_power_over(lim int) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 1;
BEGIN
  LOOP
    n := n * 2;
    EXIT WHEN n > lim;
  END LOOP;
  RETURN n;
END $$"#).unwrap();
    s.execute(r#"CREATE FUNCTION sum_to(lim int) RETURNS int LANGUAGE plpgsql AS $$
DECLARE i int := 0; total int := 0;
BEGIN
  WHILE i < lim LOOP
    i := i + 1;
    CONTINUE WHEN i = 3;
    total := total + i;
  END LOOP;
  RETURN total;
END $$"#).unwrap();
    s.execute(r#"CREATE FUNCTION first_multiple(k int) RETURNS int LANGUAGE plpgsql AS $$
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
END $$"#).unwrap();
    assert_eq!(s.execute("SELECT first_power_over(100), sum_to(5), first_multiple(7)").unwrap().rows,
        vec![vec![SqlValue::Int(128), SqlValue::Int(12), SqlValue::Int(7)]]);
    let created = s.execute("CREATE FUNCTION bad() RETURNS int LANGUAGE plpgsql AS $$ BEGIN EXIT; RETURN 1; END $$");
    assert!(created.is_err() || s.execute("SELECT bad()").is_err(), "EXIT outside a loop");
}

#[test]
fn database_owner_creates_trusted_extensions_only() {
    let (_dir, mut db) = db();
    {
        let mut admin = SqlSession::new(&mut db);
        admin.execute("CREATE ROLE app_owner LOGIN; CREATE ROLE someone LOGIN").unwrap();
        let name = admin.execute("SELECT current_database()").unwrap().rows[0][0].clone();
        let SqlValue::String(name) = name else { panic!("database name") };
        admin.execute(&format!("ALTER DATABASE {name} OWNER TO app_owner")).unwrap();
    }
    let mut owner = SqlSession::new_unprivileged(&mut db, "app_owner");
    owner.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public").unwrap();
    assert!(owner.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements").is_err(), "untrusted needs a superuser");
    drop(owner);
    let mut other = SqlSession::new_unprivileged(&mut db, "someone");
    assert!(other.execute("CREATE EXTENSION IF NOT EXISTS citext").is_err(), "only the database owner");
}

#[test]
fn common_postgresql_functions_work_on_table_rows() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE f (t text, n int, ts timestamptz, d timestamp)").unwrap();
    s.execute("INSERT INTO f VALUES ('Hello World', 7, '2026-10-13 12:34:56+00', '2026-03-31 10:00:00')").unwrap();
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
        ("regexp_split_to_array('a,b,,c', ',')::text", text("{a,b,\"\",c}")),
        ("to_date('2026-10-13', 'YYYY-MM-DD')::text", text("2026-10-13")),
        ("age(ts, '2025-08-12 11:00:00+00')::text", text("1 year 2 mons 1 day 01:34:56")),
        ("make_interval(days => 2, hours => 3)::text", text("2 days 03:00:00")),
        ("make_interval(0, 1)::text", text("1 mon")),
        ("date_bin('15 minutes', ts, '2000-01-01 00:00:00+00')::text", text("2026-10-13 12:30:00+00")),
        ("(ts AT TIME ZONE 'UTC')::text", text("2026-10-13 12:34:56")),
        ("similarity('word', 'words') > 0.5", SqlValue::Bool(true)),
    ];
    for (expr, expected) in cases {
        let rows = s.execute(&format!("SELECT {expr} FROM f")).unwrap_or_else(|error| panic!("{expr}: {error:?}")).rows;
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
    assert_eq!(s.execute("SELECT strpos(NULL, 'a'), lpad(NULL, 3), age(NULL)").unwrap().rows, vec![vec![SqlValue::Null, SqlValue::Null, SqlValue::Null]]);
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
        assert_eq!(s.execute(sql).unwrap().rows, vec![vec![SqlValue::Int(expected)]], "{sql}");
    }
    assert_eq!(s.execute("SELECT 0.57::float4 > 0.5, 0.57::float8 < 0.6").unwrap().rows, vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]]);
    let ordered = s.execute("SELECT d FROM m ORDER BY d DESC").unwrap().rows;
    assert_eq!(ordered, vec![vec![SqlValue::Float(0.57)], vec![SqlValue::Float(0.25)]]);
}
