//! Comparisons in PL/pgSQL bodies between date/time values of different
//! types compare as date/time, as in PostgreSQL 16, including against a
//! record field and a function result whose types the body does not declare.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn plpgsql_compares_dates_and_timestamps_as_datetimes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE holiday (day TIMESTAMP)",
        "INSERT INTO holiday VALUES ('2002-07-04 00:00:00')",
        "CREATE FUNCTION probe(p TIMESTAMPTZ) RETURNS TEXT LANGUAGE plpgsql AS $$
         DECLARE v DATE := '2002-07-04'; t TIMESTAMP := '2002-07-04'; h holiday%ROWTYPE;
                 r TEXT := '';
         BEGIN
           r := (v <> t)::text || (v = t)::text || (v < t + interval '1 hour')::text;
           FOR h IN SELECT * FROM holiday LOOP
             r := r || '/' || (v <> h.day)::text || (v = date_trunc('day', h.day))::text;
           END LOOP;
           IF v = t THEN r := r || '/if'; END IF;
           RETURN r;
         END $$",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let result = session
        .execute("SELECT probe(timestamptz '2002-07-04 00:00:00+00')")
        .unwrap()
        .rows[0][0]
        .to_cell();
    assert_eq!(result, "falsetruetrue/falsetrue/if");
}

/// The same comparisons in plain SQL over literals and scalar subqueries.
/// Expected values are PostgreSQL 16's. (Two untyped row columns compared in
/// a select list still compare as text: tracked separately.)
#[test]
fn sql_compares_dates_and_timestamps_as_datetimes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE days (d DATE, t TIMESTAMP, z TIMESTAMPTZ)",
        "INSERT INTO days VALUES ('2002-07-04', '2002-07-04 00:00:00', '2002-07-04 00:00:00+00')",
        "SET TIME ZONE 'UTC'",
    ] {
        session.execute(sql).unwrap();
    }
    let row = session
        .execute(
            "SELECT date '2002-07-04' <> timestamp '2002-07-04 00:00:00', \
             (SELECT d FROM days) = (SELECT t FROM days) \
             FROM days",
        )
        .unwrap()
        .rows
        .remove(0)
        .into_iter()
        .map(|value| value.to_cell())
        .collect::<Vec<_>>();
    assert_eq!(row, ["false", "true"]);
}
