//! ANALYZE records the sampled row width, so `pg_class.relpages` reports the
//! table's size in 8 KB pages as PostgreSQL's ANALYZE does.

use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn relpages(session: &mut SqlSession<'_>, table: &str) -> i64 {
    let result = session
        .execute(&format!(
            "SELECT relpages FROM pg_class WHERE relname = '{table}'"
        ))
        .unwrap();
    result.rows[0][0].to_cell().parse().unwrap()
}

#[test]
fn analyze_estimates_relpages_from_row_width() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE narrow_rows (id int4 PRIMARY KEY, a int4);
             CREATE TABLE wide_rows (id int4 PRIMARY KEY, note text);
             CREATE TABLE empty_rows (id int4 PRIMARY KEY);
             INSERT INTO narrow_rows SELECT g, g FROM generate_series(1, 10000) g;
             INSERT INTO wide_rows SELECT g, repeat('x', 500) FROM generate_series(1, 10000) g;",
        )
        .unwrap();
    assert_eq!(relpages(&mut session, "narrow_rows"), 0, "no ANALYZE yet");
    session
        .execute("ANALYZE narrow_rows; ANALYZE wide_rows; ANALYZE empty_rows")
        .unwrap();
    let narrow = relpages(&mut session, "narrow_rows");
    let wide = relpages(&mut session, "wide_rows");
    // 10,000 rows of ~500 bytes need at least 10000 * 500 / 8192 pages.
    assert!(wide >= 610, "wide table pages {wide}");
    assert!(
        narrow > 0 && narrow * 5 < wide,
        "narrow {narrow} vs wide {wide}"
    );
    assert_eq!(relpages(&mut session, "empty_rows"), 0);
}
