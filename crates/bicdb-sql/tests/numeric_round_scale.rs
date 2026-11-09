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
