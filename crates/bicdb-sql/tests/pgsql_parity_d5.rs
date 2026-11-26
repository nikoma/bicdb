//! PostgreSQL parity for DateStyle/IntervalStyle output, date/time input
//! decoding, OVERLAPS, make_* constructors, infinite intervals and jsonpath
//! printing and scanning. Expected values are the ones PostgreSQL 18
//! returns; `fixtures/postgres-compat/pgsql-parity/d5.cases` diffs the same
//! statements against a live server.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::datestyle_output::{parse_datestyle, TemporalOutput};
use bicdb_sql::{SqlSession, SqlValue};

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect())
        .collect()
}

fn one(session: &mut SqlSession<'_>, sql: &str) -> String {
    rows(session, sql)[0][0].clone()
}

fn error(session: &mut SqlSession<'_>, sql: &str) -> (String, String) {
    match session.execute(sql) {
        Ok(_) => panic!("{sql} succeeded"),
        Err(error) => (error.sqlstate().to_string(), error.to_string()),
    }
}

fn with_session(test: impl FnOnce(&mut SqlSession<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    test(&mut session);
}

#[test]
fn datestyle_output_renders_like_postgresql() {
    let (style, order) = parse_datestyle("Postgres, MDY");
    let output = TemporalOutput {
        style,
        order,
        timezone: "America/Los_Angeles".into(),
        interval_style: "postgres_verbose".into(),
    };
    assert_eq!(
        output.render(1184, "1997-02-10 17:32:01-08").as_deref(),
        Some("Mon Feb 10 17:32:01 1997 PST")
    );
    assert_eq!(
        output.render(1082, "1997-02-10").as_deref(),
        Some("02-10-1997")
    );
    assert_eq!(
        output.render(1186, "1 day 02:03:04").as_deref(),
        Some("@ 1 day 2 hours 3 mins 4 secs")
    );
    let (style, order) = parse_datestyle("SQL, DMY");
    let sql = TemporalOutput {
        style,
        order,
        timezone: "UTC".into(),
        interval_style: "postgres".into(),
    };
    assert_eq!(
        sql.render(1114, "1997-07-10 17:32:01").as_deref(),
        Some("10/07/1997 17:32:01")
    );
}

#[test]
fn date_input_follows_datestyle_order_and_errors() {
    with_session(|s| {
        s.execute("SET datestyle TO iso, ymd").unwrap();
        assert_eq!(one(s, "SELECT date '01/02/03'"), "2001-02-03");
        assert_eq!(one(s, "SELECT date '1999.008'"), "1999-01-08");
        let (state, message) = error(s, "SELECT date '1/8/1999'");
        assert_eq!(state, "22008");
        assert!(message.contains("Perhaps you need a different \"DateStyle\" setting."));
        s.execute("SET datestyle TO iso, dmy").unwrap();
        assert_eq!(one(s, "SELECT date '01/02/03'"), "2003-02-01");
        assert_eq!(one(s, "SELECT date 'Jan-08-99'"), "1999-01-08");
        s.execute("SET datestyle TO iso, mdy").unwrap();
        assert_eq!(one(s, "SELECT date '01/02/03'"), "2003-01-02");
        let (state, _) = error(s, "SELECT date '99-08-Jan'");
        assert_eq!(state, "22007");
        s.execute("SET timezone = 'UTC'").unwrap();
        assert_eq!(
            one(s, "SELECT timestamptz '2000-03-15 08:14:01 GMT+8'"),
            "2000-03-15 16:14:01+00"
        );
        assert_eq!(
            one(s, "SELECT timestamp 'Feb 10 5:32PM 1997'"),
            "1997-02-10 17:32:00"
        );
        let (state, message) = error(
            s,
            "SELECT timestamp '19970710 173201 America/Does_not_exist'",
        );
        assert_eq!(state, "22023");
        assert!(message.contains("time zone \"america/does_not_exist\" not recognized"));
    });
}

#[test]
fn overlaps_make_functions_and_infinite_intervals() {
    with_session(|s| {
        s.execute("SET timezone = 'UTC'").unwrap();
        assert_eq!(
            one(
                s,
                "SELECT (timestamp '2000-11-27', interval '12 hours') OVERLAPS (timestamp '2000-11-27 12:00', timestamp '2000-11-30')"
            ),
            "false"
        );
        assert_eq!(
            one(
                s,
                "SELECT (date '2000-01-01', NULL::date) OVERLAPS (date '1999-01-01', date '2001-01-01')"
            ),
            "true"
        );
        assert_eq!(
            one(
                s,
                "SELECT (date '2000-01-01', NULL::date) OVERLAPS (date '1998-01-01', date '1999-01-01')"
            ),
            ""
        );
        assert_eq!(one(s, "SELECT make_date(-44, 3, 15)"), "0044-03-15 BC");
        assert_eq!(
            one(s, "SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887)"),
            "2014-12-28 06:30:45.887"
        );
        assert_eq!(error(s, "SELECT make_date(2013, 2, 30)").0, "22008");
        assert_eq!(one(s, "SELECT 'infinity'::interval"), "infinity");
        assert_eq!(
            one(s, "SELECT timestamp '2000-01-01' + 'infinity'::interval"),
            "infinity"
        );
        assert_eq!(
            error(s, "SELECT 'infinity'::timestamp - 'infinity'::timestamp").0,
            "22008"
        );
    });
}

#[test]
fn jsonpath_prints_and_scans_like_postgresql() {
    with_session(|s| {
        assert_eq!(
            one(s, "SELECT '$ ? (@.a < 1)'::jsonpath"),
            "$?(@.\"a\" < 1)"
        );
        assert_eq!(one(s, "SELECT '1 + 2 * 3'::jsonpath"), "(1 + 2 * 3)");
        assert_eq!(one(s, "SELECT '(1 + 2) * 3'::jsonpath"), "((1 + 2) * 3)");
        assert_eq!(one(s, "SELECT '.001'::jsonpath"), "0.001");
        assert_eq!(one(s, "SELECT '0x42F'::jsonpath"), "1071");
        let (state, message) = error(s, "SELECT '1a'::jsonpath");
        assert_eq!(state, "42601");
        assert!(
            message.contains(
                "trailing junk after numeric literal at or near \"1a\" of jsonpath input"
            ),
            "{message}"
        );
        assert_eq!(
            one(
                s,
                "SELECT jsonb_path_query_array('[1,2,3,4]', '$[last - 1 to last]')"
            ),
            "[3,4]"
        );
        assert_eq!(
            rows(
                s,
                "SELECT * FROM jsonb_path_query('{\"a\": [1,2,3]}', '$.a[*] ? (@ > $x)', vars => '{\"x\": 1}', silent => true)"
            ),
            vec![vec!["2".to_string()], vec!["3".to_string()]]
        );
    });
}
