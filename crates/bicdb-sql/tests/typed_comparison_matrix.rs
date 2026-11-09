//! Comparisons between operands of different SQL types follow PostgreSQL's
//! cross-type rules in every query context, with and without indexes.
//!
//! `fixtures/typed_comparison/typed_comparison_pg16.json` is produced by
//! `fixtures/typed_comparison/generate.py` against PostgreSQL 16 (a database
//! created with LOCALE 'C'): every comparison pair (date/timestamp/timestamptz,
//! numeric/integer/float, char/varchar/text, bool) in every context (select
//! list, WHERE, JOIN ON, CASE, derived table, CTE, view, GROUP BY, HAVING,
//! ORDER BY, UNION, subqueries, PL/pgSQL), under two session time zones. A
//! comparison of two values of different types must never fall back to
//! comparing their text forms.

use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use std::collections::BTreeMap;

const FIXTURE: &str = include_str!("fixtures/typed_comparison/typed_comparison_pg16.json");

/// Every compared column gets an index in the indexed run, so joins and
/// correlated subqueries probe by the other side's value. `id` (the join
/// and correlation key) stays unindexed: an index probe on it exercises
/// key lookup, not comparison typing.
const INDEXED_COLUMNS: &[&str] = &[
    "d", "ts", "tz", "n", "i2", "i4", "i8", "f4", "f8", "c", "c3", "vc", "tx", "b",
];

fn render(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "<null>".to_string(),
        SqlValue::Bool(true) => "t".to_string(),
        SqlValue::Bool(false) => "f".to_string(),
        other => other.to_cell(),
    }
}

fn run_matrix(indexed: bool) -> (usize, Vec<String>) {
    let fixture: serde_json::Value = serde_json::from_str(FIXTURE).unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET TimeZone = 'UTC'").unwrap();
    for statement in fixture["setup"].as_array().unwrap() {
        let statement = statement.as_str().unwrap();
        session
            .execute(statement)
            .unwrap_or_else(|error| panic!("setup {statement}: {error}"));
    }
    if indexed {
        for column in INDEXED_COLUMNS {
            session
                .execute(&format!("CREATE INDEX m_{column} ON m ({column})"))
                .unwrap();
        }
    }
    let mut zone = "UTC".to_string();
    let mut failures = Vec::new();
    let mut by_context: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let mut by_pair: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let cases = fixture["cases"].as_array().unwrap();
    for case in cases {
        let case_zone = case["time_zone"].as_str().unwrap();
        if case_zone != zone {
            session
                .execute(&format!("SET TimeZone = '{case_zone}'"))
                .unwrap();
            zone = case_zone.to_string();
        }
        let sql = case["sql"].as_str().unwrap();
        let expected = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                row.as_array()
                    .unwrap()
                    .iter()
                    .map(|cell| cell.as_str().unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let actual = session.execute(sql).map(|result| {
            result
                .rows
                .iter()
                .map(|row| row.iter().map(render).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        });
        let passed = matches!(&actual, Ok(rows) if *rows == expected);
        let context = case["context"].as_str().unwrap().to_string();
        let pair = case["pair"].as_str().unwrap().to_string();
        for (key, map) in [
            (context.clone(), &mut by_context),
            (pair.clone(), &mut by_pair),
        ] {
            let entry = map.entry(key).or_default();
            entry.1 += 1;
            if passed {
                entry.0 += 1;
            }
        }
        if !passed {
            failures.push(format!(
                "[{zone}] {pair} {context}: {sql}\n  expected {expected:?}\n  actual   {actual:?}"
            ));
        }
    }
    eprintln!(
        "typed comparison matrix (indexed={indexed}): {}/{} pass",
        cases.len() - failures.len(),
        cases.len()
    );
    for (label, map) in [("context", &by_context), ("pair", &by_pair)] {
        for (key, (pass, total)) in map {
            eprintln!("  {label} {key}: {pass}/{total}");
        }
    }
    (cases.len(), failures)
}

fn assert_matrix(indexed: bool) {
    let (total, failures) = run_matrix(indexed);
    assert!(
        failures.is_empty(),
        "{} of {total} typed comparisons differ from PostgreSQL:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn cross_type_comparisons_match_postgres_in_every_context() {
    assert_matrix(false);
}

#[test]
fn cross_type_comparisons_match_postgres_with_indexes() {
    assert_matrix(true);
}

#[test]
fn explicit_text_operand_does_not_inherit_char_blank_padding() {
    for indexed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE x(id int primary key,c char(4))")
            .unwrap();
        session
            .execute("INSERT INTO x VALUES(1,'ab'),(2,'ab  '),(3,'x'),(4,NULL)")
            .unwrap();
        if indexed {
            session.execute("CREATE INDEX x_c ON x(c)").unwrap();
        }
        for sql in [
            "SELECT count(*) FROM x WHERE c = 'ab '::text",
            "SELECT count(*) FROM (SELECT c FROM x) q WHERE c = 'ab '::text",
            "SELECT count(*) FROM x WHERE CASE WHEN c = 'ab '::text THEN true ELSE false END",
        ] {
            assert_eq!(
                session.execute(sql).unwrap().rows,
                vec![vec![SqlValue::Int(0)]],
                "indexed={indexed} {sql}"
            );
        }
        assert_eq!(
            session
                .execute("SELECT c = 'ab '::text FROM x WHERE id < 4 ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::Bool(false)],
                vec![SqlValue::Bool(false)],
                vec![SqlValue::Bool(false)]
            ]
        );
    }
}

#[test]
fn simple_case_unknown_literals_take_the_operand_type() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE cases (i int, n numeric, d date, t timestamp)")
        .unwrap();
    session
        .execute("INSERT INTO cases VALUES(1, 2.00, '2002-07-04', '2002-07-04 00:00:00')")
        .unwrap();
    for from in ["cases", "(SELECT i,n,d,t FROM cases) s"] {
        let sql = format!("SELECT CASE i WHEN '1' THEN 'yes' ELSE 'no' END, CASE n WHEN '2.000' THEN 'yes' ELSE 'no' END, CASE d WHEN '2002-07-04' THEN 'yes' ELSE 'no' END, CASE t WHEN '2002-07-04' THEN 'yes' ELSE 'no' END FROM {from}");
        let expected = vec![vec![SqlValue::String("yes".into()); 4]];
        assert_eq!(session.execute(&sql).unwrap().rows, expected, "{sql}");
        assert_eq!(session.execute(&format!("WITH q AS (SELECT * FROM {from}) SELECT CASE i WHEN '1' THEN 'yes' ELSE 'no' END FROM q")).unwrap().rows, vec![vec![SqlValue::String("yes".into())]]);
    }
}

#[test]
fn widened_float_comparisons_keep_all_primary_key_matches() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE wide_keys(k bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute("INSERT INTO wide_keys VALUES(9007199254740992),(9007199254740993)")
        .unwrap();
    for sql in [
        "SELECT k FROM wide_keys WHERE k = 9007199254740992::float8 ORDER BY k",
        "SELECT k FROM wide_keys WHERE k IN (9007199254740992::float8) ORDER BY k",
        "SELECT k FROM wide_keys WHERE k <= 9007199254740992::float8 ORDER BY k",
        "SELECT k FROM wide_keys WHERE k IN (SELECT 9007199254740992::float8) ORDER BY k",
        "SELECT k FROM wide_keys WHERE k = ANY(ARRAY[9007199254740992::float8]) ORDER BY k",
        "SELECT k FROM wide_keys o WHERE (SELECT count(*) FROM wide_keys i WHERE i.k = o.k::float8) = 2 ORDER BY k",
        "SELECT w.k FROM (SELECT 9007199254740992::float8 AS f) p JOIN wide_keys w ON w.k = p.f ORDER BY w.k",
        "SELECT k FROM wide_keys o WHERE EXISTS (SELECT 1 FROM wide_keys i WHERE i.k = o.k::float8) ORDER BY k",
    ] {
        assert_eq!(session.execute(sql).unwrap().rows, vec![vec![SqlValue::Int(9007199254740992)],vec![SqlValue::Int(9007199254740993)]], "{sql}");
    }
}

#[test]
fn simple_case_user_types_keep_catalog_aware_fallback() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TYPE case_mood AS ENUM ('ok', 'happy')",
        "CREATE DOMAIN case_label AS text",
        "CREATE TABLE custom_cases (m case_mood, label case_label)",
        "INSERT INTO custom_cases VALUES ('ok', 'ok')",
    ] {
        session.execute(sql).unwrap();
    }
    assert_eq!(session.execute("SELECT CASE m WHEN 'ok' THEN 'yes' ELSE 'no' END, CASE label WHEN 'ok' THEN 'yes' ELSE 'no' END FROM custom_cases").unwrap().rows,
        vec![vec![SqlValue::String("yes".into()); 2]]);
}
