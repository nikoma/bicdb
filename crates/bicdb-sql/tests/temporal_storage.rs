use bicdb_core::{BicDb, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

const CREATE_TABLE: &str = r#"
    CREATE TABLE temporal_values (
        id TEXT PRIMARY KEY,
        day DATE,
        clock TIME,
        zoned_clock TIMETZ,
        local_ts TIMESTAMP,
        instant TIMESTAMPTZ,
        span INTERVAL
    )
"#;

#[test]
fn temporal_storage_is_typed_timezone_independent_and_backward_readable() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute(CREATE_TABLE).unwrap();
        session
            .execute(
                "INSERT INTO temporal_values VALUES (
                    'typed',
                    '2024-02-29'::date,
                    '23:59:58.123456'::time,
                    '23:59:58.123456+02:30'::timetz,
                    '2024-02-29 23:59:58.123456'::timestamp,
                    '2024-02-29 23:59:58.123456+02:30'::timestamptz,
                    '1 year 2 mons 3 days 04:05:06.000007'::interval
                )",
            )
            .unwrap();
    }

    let stored = db.get("temporal_values", "typed").unwrap().unwrap();
    let day = &stored.metadata["day"]["$bicdb_typed"];
    assert_eq!(day["version"], 1);
    assert_eq!(day["pg_type"], "date");
    // Compact envelope (1.0.366+): session-independent text plus hex index
    // key; no structured canonical copy.
    assert_eq!(day["text"], "2024-02-29");
    assert!(day["value"].is_null());
    assert!(day["index_key"].as_str().is_some_and(|key| !key.is_empty()));
    assert!(!stored.metadata["day"].is_string());

    let clock = &stored.metadata["clock"]["$bicdb_typed"];
    assert_eq!(clock["text"], "23:59:58.123456");
    let zoned_clock = &stored.metadata["zoned_clock"]["$bicdb_typed"];
    assert_eq!(zoned_clock["text"], "23:59:58.123456+02:30");

    assert_eq!(
        stored.metadata["local_ts"]["$bicdb_typed"]["text"],
        "2024-02-29 23:59:58.123456"
    );
    // timestamptz is stored as the UTC instant and rendered per session zone.
    assert_eq!(
        stored.metadata["instant"]["$bicdb_typed"]["text"],
        "2024-02-29 21:29:58.123456+00"
    );

    assert_eq!(
        stored.metadata["span"]["$bicdb_typed"]["text"],
        "1 year 2 mons 3 days 04:05:06.000007"
    );

    // Records from before typed temporal storage remain readable in-place.
    db.insert(
        "temporal_values",
        Record::new("legacy").with_metadata(json!({
            "day": "2001-01-01",
            "clock": "01:02:03",
            "zoned_clock": "01:02:03+00",
            "local_ts": "2001-01-01 01:02:03",
            "instant": "2001-01-01 01:02:03+00",
            "span": "2 days"
        })),
    )
    .unwrap();

    let expected_utc = vec![
        SqlValue::String("2024-02-29".to_string()),
        SqlValue::String("23:59:58.123456".to_string()),
        SqlValue::String("23:59:58.123456+02:30".to_string()),
        SqlValue::String("2024-02-29 23:59:58.123456".to_string()),
        SqlValue::String("2024-02-29 21:29:58.123456+00".to_string()),
        SqlValue::String("1 year 2 mons 3 days 04:05:06.000007".to_string()),
    ];
    let select = "SELECT day, clock, zoned_clock, local_ts, instant, span
                  FROM temporal_values WHERE id = 'typed'";
    {
        let mut session = SqlSession::new(&mut db);
        assert_eq!(
            session.execute(select).unwrap().rows,
            vec![expected_utc.clone()]
        );
        session
            .execute("SET timezone = 'America/Los_Angeles'")
            .unwrap();
        assert_eq!(
            session.execute(select).unwrap().rows,
            vec![vec![
                SqlValue::String("2024-02-29".to_string()),
                SqlValue::String("23:59:58.123456".to_string()),
                SqlValue::String("23:59:58.123456+02:30".to_string()),
                SqlValue::String("2024-02-29 23:59:58.123456".to_string()),
                SqlValue::String("2024-02-29 13:29:58.123456-08".to_string()),
                SqlValue::String("1 year 2 mons 3 days 04:05:06.000007".to_string()),
            ]]
        );
    }

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened).execute(select).unwrap().rows,
        vec![expected_utc]
    );
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT day, span FROM temporal_values WHERE id = 'legacy'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("2001-01-01".to_string()),
            SqlValue::String("2 days".to_string()),
        ]]
    );
}

#[test]
fn temporal_updates_replace_typed_payloads_and_invalid_dates_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(CREATE_TABLE).unwrap();
    session
        .execute(
            "INSERT INTO temporal_values (id, day, instant, span)
             VALUES ('entry', '2024-01-01', '2024-01-01 00:00:00+00', '1 day')",
        )
        .unwrap();
    session
        .execute(
            "UPDATE temporal_values
             SET day = '2024-12-31', instant = '2024-12-31 23:00:00-05', span = '2 mons'
             WHERE id = 'entry'",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT day, instant, span FROM temporal_values WHERE id = 'entry'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("2024-12-31".to_string()),
            SqlValue::String("2025-01-01 04:00:00+00".to_string()),
            SqlValue::String("2 mons".to_string()),
        ]]
    );
    let error = session
        .execute("UPDATE temporal_values SET day = '2023-02-29' WHERE id = 'entry'")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22008");
    assert!(error
        .to_string()
        .contains("date/time field value out of range"));
}

#[test]
fn timestamptz_matches_postgresql_session_zones_dst_arithmetic_and_indexes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET TIME ZONE 'America/New_York'").unwrap();
    session
        .execute(
            "CREATE TABLE zoned_timestamps (
                id TEXT PRIMARY KEY,
                value TIMESTAMPTZ(3) UNIQUE
            )",
        )
        .unwrap();
    session
        .execute("CREATE INDEX zoned_timestamps_value_idx ON zoned_timestamps (value)")
        .unwrap();
    session
        .execute(
            "INSERT INTO zoned_timestamps VALUES
                ('explicit', '2024-02-29 17:34:56.5555+00'),
                ('fold', '2024-11-03 01:30:00'),
                ('gap', '2024-03-10 02:30:00')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id, value FROM zoned_timestamps ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("explicit".to_string()),
                SqlValue::String("2024-02-29 12:34:56.556-05".to_string()),
            ],
            vec![
                SqlValue::String("fold".to_string()),
                SqlValue::String("2024-11-03 01:30:00-05".to_string()),
            ],
            vec![
                SqlValue::String("gap".to_string()),
                SqlValue::String("2024-03-10 03:30:00-04".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT
                    '2024-03-09 12:00:00'::timestamptz + interval '1 day',
                    '2024-03-09 12:00:00'::timestamptz + interval '24 hours',
                    '2024-11-03 01:30:00'::timestamp AT TIME ZONE 'America/New_York',
                    '2024-11-03 01:30:00-05'::timestamptz AT TIME ZONE 'UTC'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("2024-03-10 12:00:00-04".to_string()),
            SqlValue::String("2024-03-10 13:00:00-04".to_string()),
            SqlValue::String("2024-11-03 01:30:00-05".to_string()),
            SqlValue::String("2024-11-03 06:30:00".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id FROM zoned_timestamps
                 WHERE value = '2024-11-03 06:30:00+00'::timestamptz",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("fold".to_string())]]
    );
    session.execute("SET TIME ZONE 'Asia/Tokyo'").unwrap();
    assert_eq!(
        session
            .execute("SELECT value FROM zoned_timestamps WHERE id = 'fold'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("2024-11-03 15:30:00+09".to_string())]]
    );
}

#[test]
fn date_matches_postgresql_calendar_input_output_arithmetic_and_ordering() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let parsed = session
        .execute(
            "SELECT '2024-2-3'::date,
                    'February 3, 2024'::date,
                    '2024-034'::date,
                    'J2451545'::date,
                    '0001-01-01 BC'::date,
                    '+infinity'::date",
        )
        .unwrap();
    assert_eq!(
        parsed.rows,
        vec![vec![
            SqlValue::String("2024-02-03".to_string()),
            SqlValue::String("2024-02-03".to_string()),
            SqlValue::String("2024-02-03".to_string()),
            SqlValue::String("2000-01-01".to_string()),
            SqlValue::String("0001-01-01 BC".to_string()),
            SqlValue::String("infinity".to_string()),
        ]]
    );
    assert_eq!(parsed.column_types, vec![Some("date".to_string()); 6]);

    let arithmetic = session
        .execute(
            "SELECT '2024-02-29'::date + 1,
                    1 + '2024-02-29'::date,
                    '2024-03-01'::date - 1,
                    '2024-03-01'::date - '2024-02-28'::date,
                    '0001-01-01 BC'::date + 1,
                    '0001-01-01 BC'::date - 1,
                    'infinity'::date + 1",
        )
        .unwrap();
    assert_eq!(
        arithmetic.rows,
        vec![vec![
            SqlValue::String("2024-03-01".to_string()),
            SqlValue::String("2024-03-01".to_string()),
            SqlValue::String("2024-02-29".to_string()),
            SqlValue::Int(2),
            SqlValue::String("0001-01-02 BC".to_string()),
            SqlValue::String("0002-12-31 BC".to_string()),
            SqlValue::String("infinity".to_string()),
        ]]
    );
    assert_eq!(
        arithmetic.column_types,
        vec![
            Some("date".to_string()),
            Some("date".to_string()),
            Some("date".to_string()),
            Some("int4".to_string()),
            Some("date".to_string()),
            Some("date".to_string()),
            Some("date".to_string()),
        ]
    );

    assert_eq!(
        session
            .execute(
                "SELECT '-infinity'::date < '0001-01-01 BC'::date,
                        '0001-01-01 BC'::date < '0001-01-01'::date,
                        'infinity'::date > '5874897-12-31'::date"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );

    session
        .execute("CREATE TABLE date_canonical_values (id TEXT PRIMARY KEY, day DATE UNIQUE)")
        .unwrap();
    session
        .execute(
            "INSERT INTO date_canonical_values VALUES
                ('named', 'February 3, 2024'),
                ('bc', '0001-01-01 BC'),
                ('positive', 'infinity'),
                ('negative', '-infinity')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id, day FROM date_canonical_values ORDER BY day")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("negative".to_string()),
                SqlValue::String("-infinity".to_string()),
            ],
            vec![
                SqlValue::String("bc".to_string()),
                SqlValue::String("0001-01-01 BC".to_string()),
            ],
            vec![
                SqlValue::String("named".to_string()),
                SqlValue::String("2024-02-03".to_string()),
            ],
            vec![
                SqlValue::String("positive".to_string()),
                SqlValue::String("infinity".to_string()),
            ],
        ]
    );

    for sql in [
        "SELECT '2023-02-29'::date",
        "SELECT '0000-01-01'::date",
        "SELECT '5874898-01-01'::date",
        "SELECT 'infinity'::date - '-infinity'::date",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22008");
    }
    assert_eq!(
        session
            .execute("SELECT '2024-02-29junk'::date")
            .unwrap_err()
            .sqlstate(),
        "22007"
    );
}

#[test]
fn date_copy_input_is_canonical_indexed_and_rejects_invalid_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE copied_dates (id TEXT PRIMARY KEY, day DATE UNIQUE)")
        .unwrap();
    session
        .execute("CREATE INDEX copied_dates_day_idx ON copied_dates (day)")
        .unwrap();
    assert_eq!(
        session
            .copy_insert_rows(
                "copied_dates",
                &["id".to_string(), "day".to_string()],
                vec![
                    vec![Some("bc".to_string()), Some("0001-01-01 BC".to_string())],
                    vec![
                        Some("named".to_string()),
                        Some("February 3, 2024".to_string())
                    ],
                    vec![Some("infinity".to_string()), Some("infinity".to_string())],
                ],
            )
            .unwrap(),
        3
    );
    assert_eq!(
        session
            .execute("SELECT id, day FROM copied_dates ORDER BY day")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("bc".to_string()),
                SqlValue::String("0001-01-01 BC".to_string()),
            ],
            vec![
                SqlValue::String("named".to_string()),
                SqlValue::String("2024-02-03".to_string()),
            ],
            vec![
                SqlValue::String("infinity".to_string()),
                SqlValue::String("infinity".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM copied_dates WHERE day = '2024-02-03'::date")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("named".to_string())]]
    );

    let error = session
        .copy_insert_rows(
            "copied_dates",
            &["id".to_string(), "day".to_string()],
            vec![vec![
                Some("invalid".to_string()),
                Some("2023-02-29".to_string()),
            ]],
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22008");
}

#[test]
fn time_matches_postgresql_precision_parsing_arithmetic_copy_and_indexes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE clock_values (
                id TEXT PRIMARY KEY,
                raw_value TIME,
                rounded_value TIME(3) UNIQUE
            )",
        )
        .unwrap();
    session
        .execute("CREATE INDEX clock_values_raw_idx ON clock_values (raw_value)")
        .unwrap();
    session
        .execute(
            "INSERT INTO clock_values VALUES
                ('flexible', '4:5:6.123456789', '12:34:56.5555'),
                ('compact', 'T235959.9999995', '040506.789'),
                ('meridiem', '4:05 PM', '00:00:00.0005')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT id, raw_value, rounded_value FROM clock_values ORDER BY raw_value")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("flexible".to_string()),
                SqlValue::String("04:05:06.123457".to_string()),
                SqlValue::String("12:34:56.556".to_string()),
            ],
            vec![
                SqlValue::String("meridiem".to_string()),
                SqlValue::String("16:05:00".to_string()),
                SqlValue::String("00:00:00.001".to_string()),
            ],
            vec![
                SqlValue::String("compact".to_string()),
                SqlValue::String("24:00:00".to_string()),
                SqlValue::String("04:05:06.789".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '12:34:56.5'::time(0),
                        '12:34:56.55'::time(1),
                        '23:59:59.999999'::time(3)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("12:34:57".to_string()),
            SqlValue::String("12:34:56.6".to_string()),
            SqlValue::String("24:00:00".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '01:00:00'::time + interval '2 hours 3 minutes 4.5 seconds',
                        '01:00:00'::time - interval '2 hours',
                        interval '1.000001 seconds' + '12:00:00'::time,
                        '04:05:06.5'::time - '01:02:03.25'::time",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("03:03:04.5".to_string()),
            SqlValue::String("23:00:00".to_string()),
            SqlValue::String("12:00:01.000001".to_string()),
            SqlValue::String("03:03:03.25".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM clock_values WHERE raw_value = '4:05 PM'::time")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("meridiem".to_string())]]
    );
    assert_eq!(
        session
            .copy_insert_rows(
                "clock_values",
                &["id".to_string(), "raw_value".to_string()],
                vec![vec![
                    Some("copied".to_string()),
                    Some("allballs".to_string()),
                ]],
            )
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .execute("SELECT raw_value FROM clock_values WHERE id = 'copied'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("00:00:00".to_string())]]
    );

    for invalid in ["25:00:00", "24:00:00.1", "12:60:00"] {
        let error = session
            .execute(&format!("SELECT '{invalid}'::time"))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22008");
    }
    assert_eq!(
        session
            .execute("SELECT '04:05:06+99'::time")
            .unwrap_err()
            .sqlstate(),
        "22009"
    );
    assert_eq!(
        session
            .execute("SELECT '04:05:06 Bogus'::time")
            .unwrap_err()
            .sqlstate(),
        "22007"
    );
    assert_eq!(
        session
            .execute("SELECT 'nonsense'::time")
            .unwrap_err()
            .sqlstate(),
        "22007"
    );
}

#[test]
fn timetz_matches_postgresql_offsets_precision_arithmetic_and_indexes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET TIME ZONE 'UTC'").unwrap();
    session
        .execute("CREATE TABLE zoned_clocks (id TEXT PRIMARY KEY, value TIMETZ(3) UNIQUE)")
        .unwrap();
    session
        .execute("CREATE INDEX zoned_clocks_value_idx ON zoned_clocks (value)")
        .unwrap();
    session
        .execute(
            "INSERT INTO zoned_clocks VALUES
                ('rounded', '12:34:56.5555+02'),
                ('implicit', '12:34:56'),
                ('carry', '23:59:59.999999+05:30')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT value FROM zoned_clocks WHERE id = 'rounded'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("12:34:56.556+02".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT value FROM zoned_clocks WHERE id = 'implicit'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("12:34:56+00".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '23:00:00+02'::timetz + interval '2 hours',
                        '01:00:00-03'::timetz - interval '2 hours',
                        interval '1.25 seconds' + '12:00:00+05:30'::timetz",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("01:00:00+02".to_string()),
            SqlValue::String("23:00:00-03".to_string()),
            SqlValue::String("12:00:01.25+05:30".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '04:00:00+02'::timetz = '01:00:00-01'::timetz,
                        '04:00:00+02'::timetz < '04:00:00+01'::timetz,
                        '04:05:06+02'::timetz::time",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::String("04:05:06".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM zoned_clocks WHERE value = '12:34:56.5555+02'::timetz(3)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("rounded".to_string())]]
    );
}

#[test]
fn interval_matches_postgresql_typmods_styles_arithmetic_and_indexes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE durations (
                id TEXT PRIMARY KEY,
                value INTERVAL(3) UNIQUE,
                whole_years INTERVAL YEAR
            )",
        )
        .unwrap();
    session
        .execute("CREATE INDEX durations_value_idx ON durations (value)")
        .unwrap();
    session
        .execute(
            "INSERT INTO durations VALUES
                ('rounded', '1 year 2 mons 3 days 04:05:06.555555',
                    '1 year 2 mons 3 days 04:05:06.555555'),
                ('fractional', '1.1 mons', '2 years 11 mons')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT id, value, whole_years FROM durations ORDER BY value")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("fractional".to_string()),
                SqlValue::String("1 mon 3 days".to_string()),
                SqlValue::String("2 years".to_string()),
            ],
            vec![
                SqlValue::String("rounded".to_string()),
                SqlValue::String("1 year 2 mons 3 days 04:05:06.556".to_string()),
                SqlValue::String("1 year".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT interval '1 mon' + interval '15 days',
                        interval '1 mon' / 2,
                        interval '1 mon' * 1.5,
                        interval '1 mon' = interval '30 days',
                        interval '1 mon' < interval '31 days'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("1 mon 15 days".to_string()),
            SqlValue::String("15 days".to_string()),
            SqlValue::String("1 mon 15 days".to_string()),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT extract(year FROM interval '14 mons'),
                        extract(month FROM interval '14 mons'),
                        extract(epoch FROM interval '1 mon 2 days 3 seconds'),
                        justify_hours(interval '27 hours'),
                        justify_days(interval '35 days'),
                        justify_interval(interval '1 mon -1 day -1 hour')",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("1".to_string()),
            SqlValue::String("2".to_string()),
            SqlValue::String("2764803.000000".to_string()),
            SqlValue::String("1 day 03:00:00".to_string()),
            SqlValue::String("1 mon 5 days".to_string()),
            SqlValue::String("28 days 23:00:00".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id FROM durations
                 WHERE value = '1 year 2 mons 3 days 04:05:06.555555'::interval(3)",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("rounded".to_string())]]
    );

    let style_query = "SELECT interval '1 year 2 mons 3 days 04:05:06.7',
                interval '-1 year -2 mons -3 days -04:05:06.7',
                interval '10 mons 3 days -04:05:06.7'";
    session
        .execute("SET IntervalStyle = postgres_verbose")
        .unwrap();
    assert_eq!(
        session.execute(style_query).unwrap().rows,
        vec![vec![
            SqlValue::String("@ 1 year 2 mons 3 days 4 hours 5 mins 6.7 secs".to_string(),),
            SqlValue::String("@ 1 year 2 mons 3 days 4 hours 5 mins 6.7 secs ago".to_string(),),
            SqlValue::String("@ 10 mons 3 days -4 hours -5 mins -6.7 secs".to_string(),),
        ]]
    );
    session.execute("SET IntervalStyle = sql_standard").unwrap();
    assert_eq!(
        session.execute(style_query).unwrap().rows,
        vec![vec![
            SqlValue::String("+1-2 +3 +4:05:06.7".to_string()),
            SqlValue::String("-1-2 -3 -4:05:06.7".to_string()),
            SqlValue::String("+0-10 +3 -4:05:06.7".to_string()),
        ]]
    );
    session.execute("SET IntervalStyle = iso_8601").unwrap();
    assert_eq!(
        session.execute(style_query).unwrap().rows,
        vec![vec![
            SqlValue::String("P1Y2M3DT4H5M6.7S".to_string()),
            SqlValue::String("P-1Y-2M-3DT-4H-5M-6.7S".to_string()),
            SqlValue::String("P10M3DT-4H-5M-6.7S".to_string()),
        ]]
    );

    drop(session);
    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT value FROM durations WHERE id = 'rounded'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "1 year 2 mons 3 days 04:05:06.556".to_string()
        )]]
    );
}

#[test]
fn timestamp_matches_postgresql_precision_calendar_arithmetic_and_indexes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE events (id TEXT PRIMARY KEY, happened_at TIMESTAMP(3) UNIQUE)")
        .unwrap();
    session
        .execute("CREATE INDEX events_happened_at_idx ON events (happened_at)")
        .unwrap();
    session
        .execute(
            "INSERT INTO events VALUES
                ('rounded', '2024-02-29 12:34:56.5555'),
                ('carry', '2024-02-29 23:59:59.999999'),
                ('bc', '0001-01-01 00:00:00 BC'),
                ('infinity', 'infinity')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT happened_at FROM events WHERE id = 'rounded'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "2024-02-29 12:34:56.556".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '2024-01-31 23:59:59.5'::timestamp
                            + interval '1 mon 1 day 1.5 seconds',
                        '2024-03-31 00:00:00'::timestamp - interval '1 mon',
                        '2024-02-29 12:00:00.25'::timestamp
                            - '2024-02-28 10:30:00'::timestamp",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("2024-03-02 00:00:01".to_string()),
            SqlValue::String("2024-02-29 00:00:00".to_string()),
            SqlValue::String("1 day 01:30:00.25".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM events WHERE happened_at = '2024-02-29 12:34:56.5555'::timestamp(3)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("rounded".to_string())]]
    );
}

#[test]
fn timestamp_extract_cast_in_returning_preserves_int8_type_and_value() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE returning_events (id TEXT PRIMARY KEY, happened_at TIMESTAMP)")
        .unwrap();
    let result = session
        .execute(
            "INSERT INTO returning_events VALUES ('event', '2000-01-02 00:00:00')
             RETURNING EXTRACT(EPOCH FROM happened_at)::bigint AS epoch_seconds",
        )
        .unwrap();
    assert_eq!(result.column_types, vec![Some("int8".to_string())]);
    assert_eq!(result.rows, vec![vec![SqlValue::Int(946_771_200)]]);

    session
        .execute(
            "CREATE TABLE returning_deadlines (
                id TEXT PRIMARY KEY,
                reset_at TIMESTAMPTZ NOT NULL
            )",
        )
        .unwrap();
    let result = session
        .execute(
            "INSERT INTO returning_deadlines VALUES ('deadline', NOW() + interval '60 seconds')
             RETURNING EXTRACT(EPOCH FROM (reset_at - NOW()))::bigint AS retry_seconds",
        )
        .unwrap();
    assert_eq!(result.column_types, vec![Some("int8".to_string())]);
    assert!(
        matches!(result.rows.as_slice(), [row] if matches!(row.as_slice(), [SqlValue::Int(value)] if *value >= 0))
    );
}
