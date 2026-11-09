//! An interval with a month part that is computed at run time (`interval '1'
//! month * n`) adds to a timestamp on the calendar, in queries and in
//! PL/pgSQL. Expected values are PostgreSQL 16's (session time zone UTC).
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn cell(session: &mut SqlSession<'_>, sql: &str) -> String {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows[0][0]
        .to_cell()
}

#[test]
fn computed_calendar_intervals_add_on_the_calendar() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET TIME ZONE 'UTC'").unwrap();
    assert_eq!(
        cell(
            &mut session,
            "SELECT timestamptz '2024-01-31 00:00:00+00' + (interval '1' month * 1)"
        ),
        "2024-02-29 00:00:00+00"
    );
    assert_eq!(
        cell(
            &mut session,
            "SELECT timestamp '2024-01-31' - (interval '1' month * 13)"
        ),
        "2022-12-31 00:00:00"
    );
    assert_eq!(
        cell(
            &mut session,
            "SELECT (interval '1' month * 2) + timestamptz '2024-12-31 00:00:00+00'"
        ),
        "2025-02-28 00:00:00+00"
    );
    session
        .execute(
            "CREATE FUNCTION add_months(datetime timestamptz, months numeric) \
             RETURNS timestamptz LANGUAGE plpgsql AS $$ BEGIN \
             RETURN datetime + (interval '1' month * trunc(months)); END; $$",
        )
        .unwrap();
    assert_eq!(
        cell(
            &mut session,
            "SELECT add_months(timestamptz '2024-01-31 00:00:00+00', 1.7)"
        ),
        "2024-02-29 00:00:00+00"
    );
}

/// A computed interval added to a timestamp with time zone in PL/pgSQL keeps
/// the zone and crosses a daylight-saving change as elapsed time.
#[test]
fn computed_intervals_add_to_timestamptz_across_daylight_saving() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE FUNCTION adddays(datetime timestamptz, days numeric) \
             RETURNS timestamptz LANGUAGE plpgsql AS $$ BEGIN \
             RETURN datetime + (interval '1' second * (86400 * days)); END; $$",
        )
        .unwrap();
    session.execute("SET TIME ZONE 'America/New_York'").unwrap();
    assert_eq!(
        cell(
            &mut session,
            "SELECT adddays(timestamptz '2024-02-28 00:00:00-05', 30)"
        ),
        "2024-03-29 01:00:00-04"
    );
    assert_eq!(
        cell(
            &mut session,
            "SELECT adddays(timestamptz '2024-01-10 00:00:00-05', 5)"
        ),
        "2024-01-15 00:00:00-05"
    );
}
