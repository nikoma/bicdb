use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn row(session: &mut SqlSession<'_>, sql: &str) -> Vec<SqlValue> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .remove(0)
}

fn text(value: &SqlValue) -> String {
    value.to_cell()
}

#[test]
fn between_symmetric_orders_its_bounds_as_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let values = row(
        &mut session,
        "SELECT 5 BETWEEN SYMMETRIC 10 AND 1, 5 BETWEEN 10 AND 1, \
         5 NOT BETWEEN SYMMETRIC 10 AND 1, 5 BETWEEN ASYMMETRIC 1 AND 10, \
         5 BETWEEN SYMMETRIC NULL AND 10, \
         50 BETWEEN SYMMETRIC (CASE WHEN true AND true THEN 100 END) AND 1",
    );
    assert_eq!(
        values,
        vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Null,
            SqlValue::Bool(true),
        ]
    );
    session.execute("CREATE TABLE bs_dates (d date)").unwrap();
    session
        .execute("INSERT INTO bs_dates VALUES ('1997-01-02'), ('1998-01-01')")
        .unwrap();
    let result = session
        .execute(
            "SELECT count(*) FROM bs_dates WHERE d BETWEEN SYMMETRIC '1997-01-31' AND '1997-01-01'",
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn session_zone_abbreviations_and_posix_rules_resolve_as_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET timezone = 'PST8PDT'").unwrap();
    let values = row(
        &mut session,
        "SELECT 'Jan 01 00:00:00 2024 LMT'::timestamptz::text, \
         to_timestamp('2011-12-18 00:00 LMT', 'YYYY-MM-DD HH24:MI TZ')::text",
    );
    assert_eq!(text(&values[0]), "2023-12-31 23:52:58-08");
    assert_eq!(text(&values[1]), "2011-12-17 23:52:58-08");
    session.execute("SET timezone = 'UTC'").unwrap();
    assert!(session
        .execute("SELECT 'Jan 01 00:00:00 2024 LMT'::timestamptz")
        .is_err());
    let values = row(
        &mut session,
        "SELECT '1912-01-01 00:00 MMT'::timestamptz::text",
    );
    assert_eq!(text(&values[0]), "1911-12-31 17:30:00+00");

    session
        .execute("SET TIME ZONE 'CST7CDT,M4.1.0,M10.5.0'")
        .unwrap();
    let values = row(
        &mut session,
        "SELECT (timestamp with time zone '2005-04-02 12:00-07' + interval '1 day')::text, \
         (timestamp with time zone '2005-04-02 12:00-07' + interval '24 hours')::text",
    );
    assert_eq!(text(&values[0]), "2005-04-03 12:00:00-06");
    assert_eq!(text(&values[1]), "2005-04-03 13:00:00-06");
}

#[test]
fn date_bin_and_input_validity_report_range_errors_instead_of_panicking() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for (sql, message) in [
        (
            "SELECT date_bin('15 minutes'::interval, timestamp '294276-12-30', timestamp '4000-12-20 BC')",
            "interval out of range",
        ),
        (
            "SELECT date_bin('200000000 days'::interval, '2024-02-01'::timestamp, '2024-01-01'::timestamp)",
            "interval out of range",
        ),
        (
            "SELECT date_bin('365000 days'::interval, '4400-01-01 BC'::timestamp, '4000-01-01 BC'::timestamp)",
            "timestamp out of range",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22008", "{sql}");
        assert!(error.to_string().contains(message), "{sql}: {error}");
    }
    let error = session
        .execute("SELECT date_bin('5 months'::interval, timestamp '2020-02-01 01:01:01', timestamp '2001-01-01')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    let values = row(
        &mut session,
        "SELECT pg_input_is_valid('6874898-01-01', 'date'), pg_input_is_valid('garbage', 'date'), \
         pg_input_is_valid('2024-02-29', 'date')",
    );
    assert_eq!(
        values,
        vec![
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true)
        ]
    );
}
