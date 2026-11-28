//! PostgreSQL parity: set-returning SQL functions in select lists, hstore's
//! populate_record, text search catalog functions and defaults, and
//! inheritance ALTER forms.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn open(root: &std::path::Path) -> BicDb {
    BicDb::open_with_config(root, DbConfig::default().with_fsync(false)).unwrap()
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Bool(value) => if value { "t" } else { "f" }.to_string(),
                    other => other.to_cell(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn set_returning_sql_functions_expand_in_the_select_list() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE n(v int); INSERT INTO n VALUES (1), (2);
             CREATE FUNCTION s(k int) RETURNS SETOF int LANGUAGE sql
               AS 'SELECT g FROM generate_series(1, k) g';
             CREATE FUNCTION t(k int) RETURNS TABLE(a int, b text) LANGUAGE sql
               AS $$ SELECT g, 'v' || g FROM generate_series(1, k) g $$",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "SELECT s(2)"), vec!["1", "2"]);
    assert_eq!(
        rows(&mut session, "SELECT v, s(v) FROM n ORDER BY 1, 2"),
        vec!["1|1", "2|1", "2|2"]
    );
    assert_eq!(rows(&mut session, "SELECT (t(2)).b"), vec!["v1", "v2"]);
    assert_eq!(rows(&mut session, "SELECT s(2) * 10"), vec!["10", "20"]);
    assert_eq!(
        rows(&mut session, "SELECT * FROM t(2) WITH ORDINALITY"),
        vec!["1|v1|1", "2|v2|2"]
    );
}

#[test]
fn hstore_set_functions_and_populate_record() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE EXTENSION hstore; CREATE TABLE r(a int, b text)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT skeys('a=>1,b=>2'::hstore) ORDER BY 1"),
        vec!["a", "b"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM populate_record(NULL::r, 'a=>7,b=>x'::hstore)"
        ),
        vec!["7|x"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT (populate_record(ROW(5, 'q')::r, 'b=>z'::hstore)).*"
        ),
        vec!["5|z"]
    );
}

#[test]
fn text_search_catalog_functions_and_default_configuration() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT count(*) FROM ts_token_type('default')"
        ),
        vec!["23"]
    );
    session
        .execute("CREATE TABLE d(v tsvector); INSERT INTO d VALUES ('a:1A b:2'), ('a:3 c')")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM ts_stat('SELECT v FROM d') ORDER BY word"
        ),
        vec!["a|2|2", "b|1|1", "c|1|1"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM ts_stat('SELECT v FROM d', 'a')"
        ),
        vec!["a|1|1"]
    );
    session
        .execute("SET default_text_search_config = 'pg_catalog.simple'")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT get_current_ts_config()::text, to_tsvector('The Rats')::text"
        ),
        vec!["simple|'rats':2 'the':1"]
    );
    session.execute("RESET default_text_search_config").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT to_tsvector('The Rats')::text"),
        vec!["'rat':2"]
    );
    let error = session
        .execute("SET default_text_search_config = 'nosuch'")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");
}

#[test]
fn alter_table_inherit_lists_and_not_null_no_inherit() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE p1(a int); CREATE TABLE p2(b int); CREATE TABLE c(a int, b int);
             ALTER TABLE c INHERIT p1, INHERIT p2; INSERT INTO c VALUES (1, 2)",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "SELECT count(*) FROM p2"), vec!["1"]);
    session
        .execute("ALTER TABLE c NO INHERIT p1, NO INHERIT p2")
        .unwrap();
    assert_eq!(rows(&mut session, "SELECT count(*) FROM p1"), vec!["0"]);
    session
        .execute(
            "CREATE TABLE q(a int NOT NULL, CONSTRAINT q_c CHECK (a > 0));
             ALTER TABLE q ALTER CONSTRAINT q_a_not_null NO INHERIT",
        )
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT connoinherit FROM pg_constraint WHERE conname = 'q_a_not_null'"
        ),
        vec!["t"]
    );
    let error = session
        .execute("ALTER TABLE q ALTER CONSTRAINT q_c NO INHERIT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42809");
    // Several actions apply in order; a child cannot alter its copy.
    session
        .execute(
            "ALTER TABLE q ALTER CONSTRAINT q_a_not_null INHERIT,
               ALTER CONSTRAINT q_a_not_null NO INHERIT;
             CREATE TABLE q2 () INHERITS (q)",
        )
        .unwrap();
    let error = session
        .execute("ALTER TABLE q2 ALTER CONSTRAINT q_a_not_null INHERIT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42P16");
}

#[test]
fn ts_parse_reports_raw_tokens_and_their_types() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM pg_catalog.ts_parse(
                'pg_catalog.default',
                'Running café foo-bar, HTTP://Example.com/a?x=1 test@x.org! 1.23 -43 1e-2 <x>ok</x>&gt;'
            )",
        ),
        [
            "1|Running", "12| ", "2|café", "12| ", "16|foo-bar", "11|foo", "12|-",
            "11|bar", "12|, ", "14|HTTP://", "5|Example.com/a?x=1", "6|Example.com",
            "18|/a?x=1", "12| ", "4|test@x.org", "12|! ", "20|1.23", "12| ",
            "21|-43", "12| ", "7|1e-2", "12| ", "13|<x>", "1|ok", "13|</x>", "23|&gt;",
        ],
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT pg_typeof(k), pg_typeof(t), pg_typeof(pos), k, t, pos
             FROM ts_parse(3722::oid, 'Hi.!') WITH ORDINALITY p(k, t, pos)",
        ),
        ["integer|text|bigint|1|Hi|1", "integer|text|bigint|12|.!|2"],
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT d.id, t.kind, t.fragment, t.position
             FROM (VALUES (1, 'Hi!'), (2, NULL)) AS d(id, body)
             LEFT JOIN LATERAL ts_parse('default', d.body) WITH ORDINALITY
               AS t(kind, fragment, position) ON true
             ORDER BY d.id, t.position",
        ),
        ["1|1|Hi|1", "1|12|!|2", "2|NULL|NULL|NULL"],
    );
    assert!(rows(&mut session, "SELECT * FROM ts_parse('missing', NULL)").is_empty());
    assert_eq!(
        session
            .execute("SELECT * FROM ts_parse(1::oid, 'Hi')")
            .unwrap_err()
            .sqlstate(),
        "XX000",
    );
}

#[test]
fn ts_debug_shows_each_dictionary_choice_and_stop_word() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT alias, token, dictionary::text, lexemes::text
             FROM ts_debug('english', 'The Running &amp;')",
        ),
        [
            "asciiword|The|english_stem|{}",
            "blank| |NULL|NULL",
            "asciiword|Running|english_stem|{run}",
            "blank| |NULL|NULL",
            "entity|&amp;|NULL|NULL",
        ],
    );
    assert_eq!(
        rows(
            &mut session,
            "SET default_text_search_config = 'simple';
             SELECT pg_typeof(dictionaries), pg_typeof(dictionary), pg_typeof(lexemes),
                    dictionary::text, lexemes::text
             FROM ts_debug('Running')",
        ),
        ["regdictionary[]|regdictionary|text[]|simple|{running}"],
    );
}
