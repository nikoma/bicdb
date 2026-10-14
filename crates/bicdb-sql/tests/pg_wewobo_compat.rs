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
    // CEIL/FLOOR syntax outside a table scan: constants and INSERT values.
    assert_eq!(s.execute("SELECT floor(2.5), ceil(2.1), floor(-2.5)").unwrap().rows, vec![vec![text("2"), text("3"), text("-3")]]);
    s.execute("CREATE TABLE fl (v int); INSERT INTO fl VALUES (floor(7 / 2.0)), (ceil(7 / 2.0))").unwrap();
    assert_eq!(s.execute("SELECT v FROM fl ORDER BY v").unwrap().rows, vec![vec![SqlValue::Int(3)], vec![SqlValue::Int(4)]]);
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

#[test]
fn select_without_from_runs_subqueries() {
    // `SELECT EXISTS (...)` and scalar subqueries with no FROM are how applications ask yes/no and single-value
    // questions (Carrier's sql.scalar_as("Bool", "SELECT EXISTS (...)")).
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE sanctions (subject text, lifted_at timestamptz, expires_at timestamptz)").unwrap();
    s.execute("INSERT INTO sanctions VALUES ('a', NULL, NULL), ('b', now(), NULL)").unwrap();
    let ask = |s: &mut SqlSession, sql: &str| s.execute(sql).unwrap().rows[0][0].clone();
    assert_eq!(ask(&mut s, "SELECT EXISTS (SELECT 1 FROM sanctions WHERE subject = 'a' AND lifted_at IS NULL AND (expires_at IS NULL OR expires_at > NOW()))"), SqlValue::Bool(true));
    assert_eq!(ask(&mut s, "SELECT EXISTS (SELECT 1 FROM sanctions WHERE subject = 'b' AND lifted_at IS NULL)"), SqlValue::Bool(false));
    assert_eq!(ask(&mut s, "SELECT NOT EXISTS (SELECT 1 FROM sanctions WHERE subject = 'z')"), SqlValue::Bool(true));
    assert_eq!(ask(&mut s, "SELECT (SELECT count(*) FROM sanctions)"), SqlValue::Int(2));
    assert_eq!(ask(&mut s, "SELECT COALESCE((SELECT subject FROM sanctions WHERE subject = 'z'), 'none')"), text("none"));
    assert_eq!(ask(&mut s, "SELECT 'a' IN (SELECT subject FROM sanctions)"), SqlValue::Bool(true));
    assert_eq!(ask(&mut s, "SELECT EXISTS (SELECT 1 FROM sanctions) AND (SELECT count(*) FROM sanctions) > 1"), SqlValue::Bool(true));
}

#[test]
fn update_can_change_a_primary_key() {
    // PostgreSQL allows UPDATE ... SET id = ...; applications and tests use it (e.g. to backdate a time-ordered id).
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE profiles (id text PRIMARY KEY, subject text UNIQUE, name text NOT NULL)").unwrap();
    s.execute("INSERT INTO profiles VALUES ('p1','ann','Ann'),('p2','ben','Ben')").unwrap();
    assert_eq!(s.execute("UPDATE profiles SET id='p0', name=name || '!' WHERE subject='ann'").unwrap().command_tag.as_deref(), Some("UPDATE 1"));
    let rows = s.execute("SELECT id, subject, name FROM profiles ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![vec![text("p0"), text("ann"), text("Ann!")], vec![text("p2"), text("ben"), text("Ben")]]);
    // Moving onto an existing key is a unique violation, and nothing changes.
    assert!(s.execute("UPDATE profiles SET id='p2' WHERE subject='ann'").is_err());
    assert_eq!(s.execute("SELECT count(*) FROM profiles WHERE id='p0'").unwrap().rows[0][0], SqlValue::Int(1));
    // Inside a transaction the move rolls back with it.
    s.execute("BEGIN").unwrap();
    s.execute("UPDATE profiles SET id='p9' WHERE subject='ben'").unwrap();
    s.execute("ROLLBACK").unwrap();
    assert_eq!(s.execute("SELECT id FROM profiles WHERE subject='ben'").unwrap().rows[0][0], text("p2"));
    // A key other rows refer to is refused rather than risking a cascading delete.
    s.execute("CREATE TABLE posts (id text PRIMARY KEY, author text REFERENCES profiles(id) ON DELETE CASCADE)").unwrap();
    s.execute("INSERT INTO posts VALUES ('x','p2')").unwrap();
    assert!(s.execute("UPDATE profiles SET id='p7' WHERE subject='ben'").is_err());
    assert_eq!(s.execute("SELECT count(*) FROM posts").unwrap().rows[0][0], SqlValue::Int(1));
}

#[test]
fn a_unique_value_freed_by_delete_can_be_reused_in_the_same_transaction() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE people (id text PRIMARY KEY, subject text UNIQUE)").unwrap();
    s.execute("INSERT INTO people VALUES ('p1','ann')").unwrap();
    s.execute("BEGIN").unwrap();
    s.execute("DELETE FROM people WHERE subject='ann'").unwrap();
    s.execute("INSERT INTO people VALUES ('p2','ann')").unwrap();
    s.execute("COMMIT").unwrap();
    assert_eq!(s.execute("SELECT id FROM people").unwrap().rows, vec![vec![text("p2")]]);
}

#[test]
fn constant_where_accepts_a_cast_boolean() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    assert!(s.execute("SELECT ''::text AS subject, false AS pending WHERE false::BOOLEAN").unwrap().rows.is_empty());
    assert_eq!(s.execute("SELECT 1 WHERE 'true'::boolean").unwrap().rows, vec![vec![SqlValue::Int(1)]]);
    s.execute("CREATE TABLE t (a int); INSERT INTO t VALUES (1)").unwrap();
    assert!(s.execute("SELECT a FROM t WHERE false::BOOLEAN").unwrap().rows.is_empty());
}

#[test]
fn array_of_a_correlated_subquery_without_from() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let rows = s.execute("WITH s AS (SELECT '{\"c\":[\"de\",\"at\"]}'::jsonb AS v) SELECT ARRAY(SELECT jsonb_array_elements_text(v->'c')) AS c FROM s").unwrap().rows;
    assert_eq!(rows, s.execute("SELECT ARRAY['de','at']").unwrap().rows);
    let joined = s.execute("WITH s AS (SELECT '{\"c\":[\"de\",\"at\"]}'::jsonb AS v) SELECT array_to_string(ARRAY(SELECT jsonb_array_elements_text(v->'c')), ',') FROM s").unwrap().rows;
    assert_eq!(joined, vec![vec![text("de,at")]]);
    assert_eq!(s.execute("SELECT ARRAY(SELECT x FROM generate_series(1,3) x)").unwrap().rows, s.execute("SELECT ARRAY[1,2,3]").unwrap().rows);
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
    assert_eq!(rows, vec![vec![text("ann_x"), SqlValue::Int(1)], vec![text("ben22"), SqlValue::Int(2)]]);
    // Without g only the first match; no groups gives the whole match.
    assert_eq!(s.execute("SELECT COUNT(*) FROM regexp_matches('a1 b2','[a-z][0-9]')").unwrap().rows, vec![vec![SqlValue::Int(1)]]);
    assert_eq!(s.execute("SELECT r[1] FROM regexp_matches('a1 b2','[a-z][0-9]','g') AS t(r) ORDER BY 1").unwrap().rows, vec![vec![text("a1")], vec![text("b2")]]);
    assert_eq!(s.execute("SELECT COUNT(*) FROM regexp_matches('x http://a.io www.b.org','(https?://|www\\.)[^[:space:]]+','gi')").unwrap().rows, vec![vec![SqlValue::Int(2)]]);
}

#[test]
fn for_share_locks_rows() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE spaces (id int PRIMARY KEY, name text); CREATE TABLE posts (id int PRIMARY KEY, space_id int)").unwrap();
    s.execute("INSERT INTO spaces VALUES (1,'a'); INSERT INTO posts VALUES (10,1)").unwrap();
    s.execute("BEGIN").unwrap();
    assert_eq!(s.execute("SELECT name FROM spaces WHERE id=1 FOR SHARE").unwrap().rows, vec![vec![text("a")]]);
    assert_eq!(s.execute("SELECT p.id FROM posts p JOIN spaces sp ON sp.id=p.space_id FOR SHARE OF sp").unwrap().rows, vec![vec![SqlValue::Int(10)]]);
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
        let types = bicdb_sql::infer_query_result_types(&db, &format!("SELECT $1 {op} '^[a-z]*$' AS m")).unwrap();
        assert_eq!(types, Some(vec![Some("bool".to_string())]), "{op}");
    }
}

#[test]
fn pgcrypto_gen_random_bytes() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    let rows = s.execute("WITH t AS (SELECT e AS email, encode(public.gen_random_bytes(24),'hex') AS token FROM unnest(ARRAY['a','b']) e) SELECT email, length(token), encode(public.digest(token,'sha256'),'hex') <> '' FROM t ORDER BY email").unwrap().rows;
    assert_eq!(rows, vec![vec![text("a"), SqlValue::Int(48), SqlValue::Bool(true)], vec![text("b"), SqlValue::Int(48), SqlValue::Bool(true)]]);
    let distinct = s.execute("SELECT count(DISTINCT encode(gen_random_bytes(16),'hex')) FROM generate_series(1,5)").unwrap().rows;
    assert_eq!(distinct, vec![vec![SqlValue::Int(5)]]);
    assert!(s.execute("SELECT gen_random_bytes(0)").is_err());
}

#[test]
fn timestamps_cast_to_time() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    assert_eq!(s.execute("SELECT '2026-10-14 10:29:59'::timestamp::time::text").unwrap().rows, vec![vec![text("10:29:59")]]);
    assert_eq!(s.execute("SELECT '2026-10-14 10:29:59'::time::text").unwrap().rows, vec![vec![text("10:29:59")]]);
    assert_eq!(s.execute("SELECT ('2026-10-14 03:20:00'::timestamp)::time >= '03:15'").unwrap().rows, vec![vec![SqlValue::Bool(true)]]);
    assert_eq!(s.execute("SELECT (NOW() AT TIME ZONE 'UTC')::time IS NOT NULL").unwrap().rows, vec![vec![SqlValue::Bool(true)]]);
    assert!(s.execute("SELECT 'not a time'::time").is_err());
}

#[test]
fn regex_operators_and_replace_accept_lookahead() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    assert_eq!(s.execute("SELECT '[x](http://a)' ~ '\\]\\((?!https://)', '[x](https://a)' ~ '\\]\\((?!https://)'").unwrap().rows, vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]);
    assert_eq!(s.execute("SELECT regexp_replace('@ann_x and @bob', '@([a-z_]+)(?![a-z_])', '<\\1>', 'g')").unwrap().rows, vec![vec![text("<ann_x> and <bob>")]]);
    assert_eq!(s.execute("SELECT regexp_replace('aaa', 'a(?=a)', 'b')").unwrap().rows, vec![vec![text("baa")]]);
}

#[test]
fn coalesce_of_a_bigint_subquery_and_an_int_literal_is_bigint() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE plan_limits (id int, daily_limit bigint); INSERT INTO plan_limits VALUES (1, 5000000000), (2, NULL)").unwrap();
    for (sql, expected) in [
        ("SELECT COALESCE((SELECT daily_limit FROM plan_limits WHERE id=1), -1)", SqlValue::Int(5_000_000_000)),
        ("SELECT COALESCE((SELECT daily_limit FROM plan_limits WHERE id=2), -1)", SqlValue::Int(-1)),
        ("SELECT COALESCE((SELECT daily_limit FROM plan_limits WHERE id=3), -1)", SqlValue::Int(-1)),
        ("SELECT COALESCE(-1, (SELECT daily_limit FROM plan_limits WHERE id=1))", SqlValue::Int(-1)),
    ] {
        let result = s.execute(sql).unwrap();
        assert_eq!(result.rows, vec![vec![expected]], "{sql}");
        assert_eq!(result.column_types.first().cloned().flatten().as_deref(), Some("int8"), "{sql}");
    }
    let rows = s.execute("SELECT COALESCE(daily_limit, -1) FROM plan_limits ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(5_000_000_000)], vec![SqlValue::Int(-1)]]);
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
    assert_eq!(returned, vec![
        vec![text("a"), SqlValue::Int(100)],
        vec![text("b"), SqlValue::Int(250)],
        vec![text("free"), SqlValue::Int(0)],
    ]);
    // ON CONFLICT applies to every branch's rows; UNION removes duplicates first.
    s.execute(
        "INSERT INTO prices (id, product, cents) SELECT 'a', 'p1', 1 UNION SELECT 'a', 'p1', 1 UNION SELECT 'c', 'p1', 3 \
         ON CONFLICT (id) DO UPDATE SET cents = EXCLUDED.cents",
    ).unwrap();
    let rows = s.execute("SELECT id, cents FROM prices ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![
        vec![text("a"), SqlValue::Int(1)],
        vec![text("b"), SqlValue::Int(250)],
        vec![text("c"), SqlValue::Int(3)],
        vec![text("free"), SqlValue::Int(0)],
    ]);
    let tag = s.execute("INSERT INTO prices (id, product, cents) SELECT 'd', 'p', 4 UNION ALL SELECT 'e', 'p', 5 ON CONFLICT DO NOTHING").unwrap();
    assert_eq!(tag.command_tag.as_deref(), Some("INSERT 0 2"));
}

#[test]
fn distinct_on_expressions_need_only_lead_the_order_by() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE u (v text, o int); INSERT INTO u VALUES ('Bob',2),('bob',1),('ALICE',5),('alice',3),('carl',9)").unwrap();
    let rows = s.execute("SELECT DISTINCT ON (lower(v)) v, o FROM u ORDER BY lower(v), o").unwrap().rows;
    assert_eq!(rows, vec![
        vec![text("alice"), SqlValue::Int(3)],
        vec![text("bob"), SqlValue::Int(1)],
        vec![text("carl"), SqlValue::Int(9)],
    ]);
    let rows = s.execute("SELECT DISTINCT ON (lower(v)) o FROM u ORDER BY lower(v), o DESC").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(5)], vec![SqlValue::Int(2)], vec![SqlValue::Int(9)]]);
    assert!(s.execute("SELECT DISTINCT ON (lower(v)) v FROM u ORDER BY o").is_err(), "DISTINCT ON must match the leading ORDER BY");
}

#[test]
fn to_jsonb_and_row_to_json_accept_a_whole_row_reference() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE cards (id int, title text, tags text[]); INSERT INTO cards VALUES (1, 'One', ARRAY['x']), (2, 'Two', '{}')").unwrap();
    let expected = s.execute("SELECT '{\"id\": 1, \"tags\": [\"x\"], \"title\": \"One\"}'::jsonb").unwrap().rows;
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
    let want = s.execute("SELECT '[{\"id\":1,\"title\":\"One\"},{\"id\":2,\"title\":\"Two\"}]'::jsonb").unwrap().rows;
    assert_eq!(agg, want);
}

#[test]
fn ordered_set_aggregates_percentile_and_mode() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE m (g text, x float8, n int)").unwrap();
    s.execute("INSERT INTO m VALUES ('a',1,1),('a',2,1),('a',3,2),('a',4,2),('a',NULL,2),('b',10,7),('c',NULL,NULL)").unwrap();
    let rows = s.execute("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0.5) WITHIN GROUP (ORDER BY x), mode() WITHIN GROUP (ORDER BY n) FROM m WHERE g = 'a'").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Float(2.5), SqlValue::Float(2.0), SqlValue::Int(2)]]);
    let rows = s.execute("SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY x DESC) FROM m WHERE g = 'a'").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Float(3.25)]]);
    let rows = s.execute("SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0.9) WITHIN GROUP (ORDER BY n) FROM m GROUP BY g ORDER BY g").unwrap().rows;
    assert_eq!(rows, vec![
        vec![text("a"), SqlValue::Float(2.5), SqlValue::Int(2)],
        vec![text("b"), SqlValue::Float(10.0), SqlValue::Int(7)],
        vec![text("c"), SqlValue::Null, SqlValue::Null],
    ]);
    // Empty input gives NULL.
    let rows = s.execute("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), mode() WITHIN GROUP (ORDER BY n) FROM m WHERE false").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Null, SqlValue::Null]]);
    // Array forms return one value per fraction.
    let got = s.execute("SELECT percentile_cont(ARRAY[0.25,0.5,1]) WITHIN GROUP (ORDER BY x), percentile_disc(ARRAY[0.5,0.1]) WITHIN GROUP (ORDER BY x) FROM m WHERE g = 'a'").unwrap().rows;
    let want = s.execute("SELECT ARRAY[1.75,2.5,4]::float8[], ARRAY[2,1]::float8[]").unwrap().rows;
    assert_eq!(got, want);
    // The WeWoBo shape: a scalar subquery over a CTE, inside jsonb_build_object.
    s.execute("CREATE TABLE scans (status text, created_at timestamptz, completed_at timestamptz)").unwrap();
    s.execute("INSERT INTO scans VALUES ('done','2026-01-01 00:00:00+00','2026-01-01 00:00:10+00'),('done','2026-01-01 00:00:00+00','2026-01-01 00:00:30+00'),('pending','2026-01-01 00:00:00+00',NULL)").unwrap();
    let rows = s.execute("WITH d AS (SELECT * FROM scans) SELECT jsonb_build_object('scanned',(SELECT COUNT(*)::int FROM d WHERE status='done'),'median_seconds',(SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at-created_at)) FROM d WHERE completed_at IS NOT NULL)) = '{\"scanned\":2,\"median_seconds\":20}'::jsonb").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn plpgsql_functions_run_once_per_row_in_update_and_select() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE learning_courses (id int, title text, slug text)").unwrap();
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
    let rows = s.execute("SELECT id, learning_course_slug(title) FROM learning_courses ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![
        vec![SqlValue::Int(1), text("yoga-basics-2")],
        vec![SqlValue::Int(2), text("yoga-basics-2")],
        vec![SqlValue::Int(3), text("breath-work")],
    ]);
    let mut p = s.with_positional_parameters(vec![SqlValue::Int(2)]);
    let updated = p.execute("UPDATE learning_courses SET slug=learning_course_slug(title) WHERE id=$1 AND slug IS NULL RETURNING slug").unwrap();
    assert_eq!(updated.rows, vec![vec![text("yoga-basics-2")]]);
    drop(p);
    let mut s = SqlSession::new(&mut db);
    // Each row sees the slugs written for earlier rows of the same statement only via its own queries;
    // a plain WHERE filter and an expression argument also work.
    s.execute("UPDATE learning_courses SET slug=learning_course_slug(title || ' ' || id) WHERE slug IS NULL").unwrap();
    let rows = s.execute("SELECT id, slug FROM learning_courses ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![
        vec![SqlValue::Int(1), text("yoga-basics")],
        vec![SqlValue::Int(2), text("yoga-basics-2")],
        vec![SqlValue::Int(3), text("breath-work-3")],
    ]);
    let rows = s.execute("SELECT id FROM learning_courses WHERE learning_course_slug(title) = 'breath-work' ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(3)]]);
    // Inside an explicit transaction the function's queries see the transaction's own rows.
    s.execute("BEGIN").unwrap();
    s.execute("INSERT INTO learning_courses VALUES (4, 'Breath Work', 'breath-work')").unwrap();
    let rows = s.execute("SELECT learning_course_slug(title) FROM learning_courses WHERE id = 3").unwrap().rows;
    assert_eq!(rows, vec![vec![text("breath-work-2")]]);
    s.execute("ROLLBACK").unwrap();
    // BicDB boundary (PostgreSQL allows it): a function called per row may not modify data.
    // The statement fails as a whole instead of writing part of its effects.
    s.execute("CREATE FUNCTION touch(i int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN INSERT INTO learning_courses VALUES (i + 100, 'x', NULL); RETURN i; END $$").unwrap();
    assert!(s.execute("SELECT touch(id) FROM learning_courses").is_err());
    assert_eq!(s.execute("SELECT count(*) FROM learning_courses").unwrap().rows, vec![vec![SqlValue::Int(3)]]);
    // Called without FROM it runs in the session and may write.
    assert_eq!(s.execute("SELECT touch(1)").unwrap().rows, vec![vec![SqlValue::Int(1)]]);
    assert_eq!(s.execute("SELECT count(*) FROM learning_courses").unwrap().rows, vec![vec![SqlValue::Int(4)]]);
}

#[test]
fn plpgsql_functions_per_row_respect_execute_privilege_and_security_definer() {
    let (_dir, mut db) = db();
    {
        let mut admin = SqlSession::new(&mut db);
        admin.execute("CREATE ROLE reader LOGIN").unwrap();
        admin.execute("CREATE TABLE secrets (id int, v text); INSERT INTO secrets VALUES (1, 'hidden')").unwrap();
        admin.execute("CREATE TABLE items (id int); INSERT INTO items VALUES (1), (2)").unwrap();
        admin.execute("GRANT SELECT ON items TO reader").unwrap();
        admin.execute("CREATE FUNCTION peek(i int) RETURNS text LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN (SELECT v FROM secrets WHERE id = i); END $$").unwrap();
        admin.execute("CREATE FUNCTION peek_invoker(i int) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT v FROM secrets WHERE id = i); END $$").unwrap();
        admin.execute("CREATE FUNCTION locked(i int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN i; END $$").unwrap();
        admin.execute("REVOKE EXECUTE ON FUNCTION locked(int) FROM PUBLIC").unwrap();
        admin.execute("GRANT EXECUTE ON FUNCTION peek(int), peek_invoker(int) TO reader").unwrap();
    }
    let mut r = SqlSession::new_unprivileged(&mut db, "reader");
    let rows = r.execute("SELECT id, peek(id) FROM items ORDER BY id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(1), text("hidden")], vec![SqlValue::Int(2), SqlValue::Null]]);
    assert!(r.execute("SELECT id, peek_invoker(id) FROM items").is_err(), "invoker rights cannot read secrets");
    assert!(r.execute("SELECT locked(id) FROM items").is_err(), "EXECUTE was revoked");
}

#[test]
fn table_row_types_cast_populate_and_expand() {
    let (_dir, mut db) = db();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE network_spaces (id int PRIMARY KEY, name text NOT NULL, tenant_id text NOT NULL, parent_id int, access_people text[] NOT NULL DEFAULT '{}', created_at bigint)").unwrap();
    s.execute("INSERT INTO network_spaces VALUES (1, 'Root', 't1', NULL, ARRAY['ann'], 10), (2, 'Child', 't1', 1, ARRAY['bob','cy'], 20)").unwrap();
    // A NULL of the row type, and field access on it.
    assert_eq!(s.execute("SELECT (NULL::network_spaces).name").unwrap().rows, vec![vec![SqlValue::Null]]);
    // jsonb_populate_record takes its columns and types from the table.
    let rows = s.execute("SELECT (jsonb_populate_record(NULL::network_spaces, '{\"id\":\"7\",\"name\":\"N\",\"extra\":1}'::jsonb)).id").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(7)]]);
    let r = s.execute("SELECT x.id, x.name, x.tenant_id FROM network_spaces s CROSS JOIN LATERAL jsonb_populate_record(NULL::network_spaces, to_jsonb(s)) x ORDER BY x.id").unwrap();
    assert_eq!(r.rows, vec![
        vec![SqlValue::Int(1), text("Root"), text("t1")],
        vec![SqlValue::Int(2), text("Child"), text("t1")],
    ]);
    let r = s.execute("SELECT * FROM jsonb_populate_record(NULL::network_spaces, '{\"id\":3,\"name\":\"x\"}')").unwrap();
    assert_eq!(r.columns, vec!["id", "name", "tenant_id", "parent_id", "access_people", "created_at"]);
    // (record).* expands to the table's columns with their names and types (actions.mjs:1777).
    let mut p = s.with_positional_parameters(vec![SqlValue::Int(2)]);
    let r = p.execute("SELECT (jsonb_populate_record(NULL::network_spaces,to_jsonb(s)||jsonb_build_object('tenant_id','','parent_id',NULL,'access_people','[]'::jsonb))).* FROM network_spaces s WHERE id=$1").unwrap();
    drop(p);
    assert_eq!(r.columns, vec!["id", "name", "tenant_id", "parent_id", "access_people", "created_at"]);
    let mut s = SqlSession::new(&mut db);
    let want = s.execute("SELECT 2, 'Child', '', NULL::int, ARRAY[]::text[], 20::bigint").unwrap().rows;
    assert_eq!(r.rows, want);
    assert_eq!(r.column_types.iter().map(|t| t.clone().unwrap_or_default()).collect::<Vec<_>>(), vec!["int4", "text", "text", "int4", "text[]", "int8"]);
    // A CASE choosing between the whole row and a redacted copy (actions.mjs:1790).
    let mut p = s.with_positional_parameters(vec![text("[{\"id\":1,\"role\":\"owner\",\"discoverable\":true},{\"id\":2,\"role\":\"member\",\"discoverable\":true}]")]);
    let r = p.execute("WITH a AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(id int,role text,discoverable boolean)) SELECT (CASE WHEN a.role IN ('owner','admin') THEN s ELSE jsonb_populate_record(NULL::network_spaces,to_jsonb(s)||jsonb_build_object('tenant_id','','parent_id',NULL,'access_people','[]'::jsonb)) END).* FROM network_spaces s JOIN a ON a.id=s.id WHERE a.discoverable ORDER BY s.created_at DESC LIMIT 500").unwrap();
    drop(p);
    assert_eq!(r.columns, vec!["id", "name", "tenant_id", "parent_id", "access_people", "created_at"]);
    let mut s = SqlSession::new(&mut db);
    let want = s.execute("SELECT 2, 'Child', '', NULL::int, ARRAY[]::text[], 20::bigint UNION ALL SELECT 1, 'Root', 't1', NULL::int, ARRAY['ann'], 10::bigint").unwrap().rows;
    assert_eq!(r.rows, want);
    // Field access and star expansion of a plain whole-row reference.
    let rows = s.execute("SELECT (s).name, (s).* FROM network_spaces s WHERE id = 1").unwrap().rows;
    let want = s.execute("SELECT 'Root', 1, 'Root', 't1', NULL::int, ARRAY['ann'], 10::bigint").unwrap().rows;
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
    let rows = s.execute("SELECT state, count(*) FROM media_jobs GROUP BY 1 ORDER BY 1").unwrap().rows;
    assert_eq!(rows, vec![
        vec![text("failed"), SqlValue::Int(1)],
        vec![text("queued"), SqlValue::Int(2)],
        vec![text("ready"), SqlValue::Int(3)],
    ]);
    let mut unordered = s.execute("SELECT state, count(*) FROM media_jobs GROUP BY 1").unwrap().rows;
    unordered.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(unordered, rows);
    let rows = s.execute("SELECT state, count(*) FROM media_jobs GROUP BY state ORDER BY 2 DESC").unwrap().rows;
    assert_eq!(rows, vec![
        vec![text("ready"), SqlValue::Int(3)],
        vec![text("queued"), SqlValue::Int(2)],
        vec![text("failed"), SqlValue::Int(1)],
    ]);
    // ORDER BY an aggregate (by position, by expression or by alias) sorts the groups.
    for sql in [
        "SELECT state, count(*) FROM media_jobs GROUP BY state ORDER BY count(*) DESC LIMIT 2",
        "SELECT state, count(*) AS n FROM media_jobs GROUP BY state ORDER BY n DESC LIMIT 2",
    ] {
        assert_eq!(s.execute(sql).unwrap().rows, vec![
            vec![text("ready"), SqlValue::Int(3)],
            vec![text("queued"), SqlValue::Int(2)],
        ], "{sql}");
    }
    let rows = s.execute("SELECT state, count(*), min(created_at), max(updated_at) FROM media_jobs GROUP BY 1 ORDER BY 2 DESC").unwrap().rows;
    assert_eq!(rows, vec![
        vec![text("ready"), SqlValue::Int(3), SqlValue::Int(100), SqlValue::Int(660)],
        vec![text("queued"), SqlValue::Int(2), SqlValue::Int(200), SqlValue::Int(500)],
        vec![text("failed"), SqlValue::Int(1), SqlValue::Int(300), SqlValue::Int(330)],
    ]);
}
