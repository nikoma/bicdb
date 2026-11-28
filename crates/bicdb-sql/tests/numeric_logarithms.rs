use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn numeric_logarithms_keep_decimal_precision_and_compose_with_power() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);

    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT ln(2::numeric), log(2,8), log(10,2), pg_typeof(ln(2::numeric)), pg_typeof(log(2,8)), pg_typeof(ln('2'))",
            &[
                "0.6931471805599453",
                "3.0000000000000000",
                "0.3010299956639812",
                "numeric",
                "numeric",
                "double precision",
            ],
        ),
        (
            "SELECT power(10::numeric, ln(2::numeric)), pg_typeof(power(10::numeric,ln(2::numeric))), pg_typeof(2::numeric ^ 3::float8)",
            &["4.9334096679145962", "numeric", "double precision"],
        ),
        (
            "SELECT ln(1.000000000000000001::numeric), ln(1e-2000::numeric)::numeric(35,16)",
            &["0.0000000000000000010000000000000000", "-4605.1701859880913680"],
        ),
        (
            "SELECT log('Infinity'::numeric,2::numeric), log(2::numeric,'Infinity'::numeric), ln('NaN'::numeric)",
            &["0", "Infinity", "NaN"],
        ),
    ];
    for (statement, expected) in cases {
        let result = sql
            .execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
        let cells: Vec<_> = result.rows[0].iter().map(|value| value.to_cell()).collect();
        assert_eq!(cells, *expected, "{statement}");
    }
    for (statement, state) in [
        ("SELECT ln(0::numeric)", "2201E"),
        ("SELECT ln('-Infinity'::numeric)", "2201E"),
        ("SELECT log(1::numeric,2::numeric)", "22012"),
    ] {
        assert_eq!(
            sql.execute(statement).unwrap_err().sqlstate(),
            state,
            "{statement}"
        );
    }
}
