//! round(numeric, s) returns exactly s fractional digits, as in PostgreSQL 16.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn numeric_round_pads_to_the_requested_scale() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let row = session
        .execute(
            "SELECT round(0::numeric, 2), round(1.2::numeric, 2), round(1.255, 2), \
             round(-1.5, 0), round(123.456, -1), round(5::numeric, 3)",
        )
        .unwrap()
        .rows
        .remove(0)
        .into_iter()
        .map(|value| value.to_cell())
        .collect::<Vec<_>>();
    assert_eq!(row, ["0.00", "1.20", "1.26", "-2", "120", "5.000"]);
}
#[test]
fn numeric_rounding_and_special_arithmetic_follow_postgresql() {
    let root = tempfile::tempdir().unwrap();
    let mut db = bicdb_core::BicDb::open(root.path()).unwrap();
    let mut sql = bicdb_sql::SqlSession::new(&mut db);
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT round(2.5::numeric), round(2.5::float8), trunc(2.500::numeric), \
             round(1500::numeric,-3), round((-1500)::numeric,-3), length(round(1::numeric,1200)::text)",
            &["3", "2", "2", "2000", "-2000", "1202"],
        ),
        (
            "SELECT round('NaN'::numeric,8), trunc('-Infinity'::numeric), floor('Infinity'::numeric), \
             sign('NaN'::numeric), sign('NaN'::float8), numeric_inc(0.000::numeric)",
            &["NaN", "-Infinity", "Infinity", "NaN", "0", "1.000"],
        ),
        (
            "SELECT ceil(12345678901234567890123456789012345678901234567890.5::numeric), \
             floor((-12345678901234567890123456789012345678901234567890.5)::numeric), \
             div('NaN'::numeric,0), div(1,'Infinity'), div('Infinity'::numeric,-1), (-3.000::numeric) % 'Infinity'",
            &[
                "12345678901234567890123456789012345678901234567891",
                "-12345678901234567890123456789012345678901234567891",
                "NaN",
                "0",
                "-Infinity",
                "-3.000",
            ],
        ),
    ];
    for (statement, expected) in cases {
        let result = sql.execute(statement).unwrap();
        let cells: Vec<_> = result.rows[0].iter().map(|value| value.to_cell()).collect();
        assert_eq!(cells, *expected, "{statement}");
    }
    for statement in [
        "SELECT div('Infinity'::numeric, 0)",
        "SELECT '-Infinity'::numeric % '0'",
    ] {
        assert_eq!(sql.execute(statement).unwrap_err().sqlstate(), "22012");
    }
}
