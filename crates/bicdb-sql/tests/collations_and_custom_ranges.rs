//! CREATE/ALTER/DROP COLLATION with ICU locales (deterministic and
//! nondeterministic), COLLATE in comparisons, ORDER BY and column
//! definitions; range types over float8, text, interval and time.
//! Expected results were checked against PostgreSQL 18.4.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn column(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    rows(session, sql)
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

fn with_session(test: impl FnOnce(&mut SqlSession<'_>)) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    test(&mut session);
}

#[test]
fn icu_collations_order_and_compare() {
    with_session(|session| {
        for sql in [
            "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
            "CREATE COLLATION de (provider = icu, locale = 'de-DE')",
            "CREATE COLLATION sv (provider = icu, locale = 'sv')",
            "CREATE COLLATION bytes FROM \"C\"",
        ] {
            session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(
            rows(
                session,
                "SELECT collname, collprovider, collisdeterministic, colllocale FROM pg_collation \
                 WHERE collname IN ('ci','de','sv','bytes') ORDER BY 1"
            ),
            vec![
                vec!["bytes", "c", "true", ""],
                vec!["ci", "i", "false", "und-u-ks-level2"],
                vec!["de", "i", "true", "de-DE"],
                vec!["sv", "i", "true", "sv"],
            ]
        );
        assert_eq!(
            rows(
                session,
                "SELECT 'abc' = 'ABC' COLLATE ci, 'abc' < 'ABD' COLLATE ci, 'a' = 'b' COLLATE ci, 'abc' = 'ABC' COLLATE de"
            ),
            vec![vec!["true", "true", "false", "false"]]
        );
        assert_eq!(
            column(
                session,
                "SELECT x FROM (VALUES ('b'),('A'),('a'),('B'),('ä')) v(x) ORDER BY x COLLATE de, x COLLATE \"C\""
            ),
            ["a", "A", "ä", "b", "B"]
        );
        assert_eq!(
            column(
                session,
                "SELECT x FROM (VALUES ('z'),('ä'),('a')) v(x) ORDER BY x COLLATE sv"
            ),
            ["a", "z", "ä"]
        );
        session
            .execute("CREATE TABLE words(s TEXT COLLATE de)")
            .unwrap();
        session
            .execute("INSERT INTO words VALUES ('b'),('A'),('a')")
            .unwrap();
        assert_eq!(
            column(session, "SELECT s FROM words ORDER BY s"),
            ["a", "A", "b"]
        );
        assert_eq!(
            column(session, "SELECT s FROM words WHERE s < 'b' ORDER BY s"),
            ["a", "A"]
        );
    });
}

#[test]
fn collation_ddl_errors_and_dependencies() {
    with_session(|session| {
        session
            .execute("CREATE COLLATION de (provider = icu, locale = 'de-DE')")
            .unwrap();
        for (sql, sqlstate) in [
            (
                "CREATE COLLATION de (provider = icu, locale = 'en')",
                "42710",
            ),
            (
                "CREATE COLLATION bad (provider = libc, locale = 'C', deterministic = false)",
                "0A000",
            ),
            ("SELECT 'a' COLLATE nope", "42704"),
            ("DROP COLLATION nope", "42704"),
        ] {
            assert_eq!(
                session.execute(sql).unwrap_err().sqlstate(),
                sqlstate,
                "{sql}"
            );
        }
        session
            .execute("CREATE TABLE uses_de(s TEXT COLLATE de)")
            .unwrap();
        assert_eq!(
            session.execute("DROP COLLATION de").unwrap_err().sqlstate(),
            "2BP01"
        );
        session.execute("DROP TABLE uses_de").unwrap();
        session.execute("ALTER COLLATION de RENAME TO de2").unwrap();
        session.execute("DROP COLLATION de2").unwrap();
        session.execute("DROP COLLATION IF EXISTS de2").unwrap();
        assert_eq!(
            rows(
                session,
                "SELECT count(*) FROM pg_collation WHERE collname IN ('de', 'de2')"
            ),
            vec![vec!["0"]]
        );
        // DDL is transactional.
        session.execute("BEGIN").unwrap();
        session
            .execute("CREATE COLLATION temp_coll (provider = icu, locale = 'fr')")
            .unwrap();
        session.execute("ROLLBACK").unwrap();
        assert_eq!(
            session
                .execute("SELECT 'a' COLLATE temp_coll")
                .unwrap_err()
                .sqlstate(),
            "42704"
        );
    });
}

#[test]
fn nondeterministic_collations_refuse_unsupported_uses() {
    with_session(|session| {
        session
            .execute(
                "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
            )
            .unwrap();
        session.execute("CREATE TABLE t(s TEXT)").unwrap();
        for sql in [
            "CREATE TABLE t2(s TEXT COLLATE ci)",
            "SELECT s COLLATE ci FROM t GROUP BY 1",
            "SELECT DISTINCT s COLLATE ci FROM t",
            "SELECT s FROM t WHERE s COLLATE ci IN ('A')",
            "CREATE INDEX ON t ((s COLLATE ci))",
        ] {
            assert_eq!(
                session.execute(sql).unwrap_err().sqlstate(),
                "0A000",
                "{sql}"
            );
        }
        session
            .execute("INSERT INTO t VALUES ('Hello'),('HELLO'),('x')")
            .unwrap();
        assert_eq!(
            column(
                session,
                "SELECT s FROM t WHERE s = 'hello' COLLATE ci ORDER BY s COLLATE \"C\""
            ),
            ["HELLO", "Hello"]
        );
    });
}

#[test]
fn ranges_over_float_text_interval_and_time() {
    with_session(|session| {
        for sql in [
            "CREATE TYPE floatrange AS RANGE (subtype = float8, subtype_diff = float8mi)",
            "CREATE TYPE textrange AS RANGE (subtype = text)",
            "CREATE TYPE ivrange AS RANGE (subtype = interval)",
            "CREATE TYPE timerange AS RANGE (subtype = time)",
        ] {
            session
                .execute(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(
            rows(
                session,
                "SELECT '[1.5,2.5)'::floatrange @> 2.0::float8, '[1,2]'::floatrange * '[1.5,3)'::floatrange, \
                 upper('[1,2]'::floatrange), '[1,5]'::floatrange - '[3,6]'::floatrange"
            ),
            vec![vec!["true", "[1.5,2]", "2", "[1,3)"]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT '{[1,2],[3,4]}'::floatmultirange, range_merge('{[1,2],[3,4]}'::floatmultirange)"
            ),
            vec![vec!["{[1,2],[3,4]}", "[1,4]"]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT '[a,m)'::textrange @> 'b'::text, '[a,m)'::textrange @> 'z'::text, textrange('ab','b c')"
            ),
            vec![vec!["true", "false", "[ab,\"b c\")"]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT '[1 hour,2 hours)'::ivrange @> '90 minutes'::interval, '[10:00,12:00)'::timerange"
            ),
            vec![vec!["true", "[10:00:00,12:00:00)"]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT rngsubopc, rngsubdiff::text FROM pg_range WHERE rngtypid = 'floatrange'::regtype"
            ),
            vec![vec!["3123", "float8mi"]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT upper(floatrange(1, 5)), lower(textrange('ab', 'c'))"
            ),
            vec![vec!["5", "ab"]]
        );
        session.execute("CREATE TABLE tr(r textrange)").unwrap();
        session
            .execute("INSERT INTO tr VALUES ('[b,c)'),('[a,z)'),('[a,b)'),('empty')")
            .unwrap();
        assert_eq!(
            column(session, "SELECT r FROM tr ORDER BY r"),
            ["empty", "[a,b)", "[a,z)", "[b,c)"]
        );
    });
}
#[test]
fn declared_collations_follow_columns_through_predicates_aliases_and_indexes() {
    with_session(|session| {
        session.execute("CREATE TABLE catalog_words(id int PRIMARY KEY, word text COLLATE \"en-x-icu\", ascii_word text COLLATE \"C\"); INSERT INTO catalog_words VALUES (1,'abc','bbc'),(2,'äbc','bbc'),(3,'bbc','bbc'),(4,'ABC','bbc')").unwrap();
        for sql in [
            "SELECT id FROM catalog_words WHERE word >= 'bbc' ORDER BY id",
            "SELECT w.id FROM catalog_words AS w WHERE w.word >= 'bbc' ORDER BY w.id",
            "SELECT w.id FROM catalog_words AS w(id, renamed) WHERE w.renamed >= 'bbc' ORDER BY w.id",
        ] {
            assert_eq!(column(session, sql), ["3"]);
        }
        session
            .execute("CREATE INDEX words_idx ON catalog_words(word)")
            .unwrap();
        assert_eq!(
            column(
                session,
                "SELECT id FROM catalog_words WHERE word >= 'bbc' ORDER BY word"
            ),
            ["3"]
        );
        assert_eq!(
            column(
                session,
                "SELECT id FROM catalog_words WHERE word COLLATE \"C\" >= ascii_word ORDER BY id"
            ),
            ["2", "3"]
        );
        assert_eq!(
            column(
                session,
                "SELECT word >= 'bbc' FROM catalog_words ORDER BY id"
            ),
            ["false", "false", "true", "false"]
        );
        for sql in [
            "SELECT word >= ascii_word FROM catalog_words",
            "SELECT id FROM catalog_words WHERE word = ascii_word",
            "SELECT w.word >= w.ascii_word FROM catalog_words AS w",
        ] {
            assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42P22");
        }
        let missing = session
            .execute("SELECT 'a' COLLATE \"ja_JP.eucjp-x-icu\" < 'b'")
            .unwrap_err();
        assert_eq!(missing.sqlstate(), "42704");
        assert!(missing
            .to_string()
            .contains("collation \"ja_JP.eucjp-x-icu\" for encoding \"UTF8\" does not exist"));
    });
}
