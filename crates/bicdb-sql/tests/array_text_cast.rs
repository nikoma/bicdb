//! Array values cast to text (or formatted as text) print in PostgreSQL's
//! array syntax on every path: plain and qualified columns, expressions,
//! aggregate and function results, record fields and predicates.
//! Expected results were checked against PostgreSQL 18.4.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;

fn column(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_cell())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn arrays_cast_to_text_in_array_syntax_on_every_path() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    let session = &mut session;
    session
        .execute(
            "CREATE TABLE atc(id int, a text[], n int[], j jsonb[]); \
             INSERT INTO atc VALUES (1, ARRAY['x','y z',NULL], ARRAY[1,2], ARRAY['{\"k\":1}'::jsonb]), \
             (2, '{}', NULL, NULL)",
        )
        .unwrap();
    let first = r#"{x,"y z",NULL}"#;
    let cases: &[(&str, &[&str])] = &[
        ("SELECT a::text FROM atc ORDER BY id", &[first, "{}"]),
        ("SELECT t.a::text FROM atc t ORDER BY t.id", &[first, "{}"]),
        ("SELECT t.n::text FROM atc t ORDER BY t.id", &["{1,2}", ""]),
        (
            "SELECT j::text FROM atc ORDER BY id",
            &[r#"{"{\"k\": 1}"}"#, ""],
        ),
        (
            "SELECT t.j::text FROM atc t ORDER BY t.id",
            &[r#"{"{\"k\": 1}"}"#, ""],
        ),
        (
            "SELECT array_agg(id ORDER BY id)::text FROM atc",
            &["{1,2}"],
        ),
        (
            "SELECT array_agg(t.id ORDER BY t.id)::text FROM atc t",
            &["{1,2}"],
        ),
        (
            "SELECT 'x' || array_agg(id ORDER BY id)::text FROM atc",
            &["x{1,2}"],
        ),
        (
            r#"SELECT id FROM atc WHERE a::text = '{x,"y z",NULL}'"#,
            &["1"],
        ),
        (
            "SELECT format('%s', t.a) FROM atc t ORDER BY t.id",
            &[first, "{}"],
        ),
        (
            "SELECT format('%s|%s', a, n) FROM atc ORDER BY id",
            &[&format!("{first}|{{1,2}}"), "{}|"],
        ),
        (r#"SELECT id FROM atc WHERE a = '{x,"y z",NULL}'"#, &["1"]),
        (
            r#"SELECT t.id FROM atc t WHERE upper(t.a::text) = upper('{x,"y z",NULL}')"#,
            &["1"],
        ),
        (r#"SELECT t.id FROM atc t WHERE t.a::text = '{}'"#, &["2"]),
        (
            r#"SELECT t.id FROM atc t WHERE t.n::text = '{1,2}'"#,
            &["1"],
        ),
        (
            r#"SELECT t.id FROM atc t WHERE t.a::text LIKE '{x%'"#,
            &["1"],
        ),
    ];
    let mut failures = Vec::new();
    for (sql, expected) in cases {
        let actual = column(session, sql);
        if actual != *expected {
            failures.push(format!("{sql}\n  got {actual:?}\n  want {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
