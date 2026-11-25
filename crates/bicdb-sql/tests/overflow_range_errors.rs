//! Arithmetic that overflowed and panicked (surfacing as "internal query execution panic was
//! contained") now follows PostgreSQL: a range error or the value PostgreSQL returns.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn cells(db: &mut BicDb, sql: &str) -> Vec<Vec<String>> {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

#[test]
fn array_fill_with_no_dimensions_is_the_empty_array() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    for fill in ["array_fill(42, '{}')", "array_fill(42, '{}', '{}')"] {
        let sql = format!(
            "select a::text, a = '{{}}', array_dims(a) is null from (select {fill} as a) ss"
        );
        assert_eq!(cells(&mut db, &sql), [["{}", "true", "true"]], "{sql}");
    }
}

#[test]
fn date_bin_overflow_is_interval_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    for sql in [
        "select date_bin('15 minutes'::interval, timestamp '294276-12-30', timestamp '4000-12-20 BC')",
        "select date_bin('15 minutes'::interval, timestamptz '294276-12-30', timestamptz '4000-12-20 BC')",
    ] {
        let error = SqlSession::new(&mut db).execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22008", "{sql}: {error}");
        assert_eq!(error.to_string(), "interval out of range", "{sql}");
    }
}

#[test]
fn window_frame_offsets_near_i64_max_clamp_to_the_partition() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE w (a int)")
        .unwrap();
    SqlSession::new(&mut db)
        .execute("INSERT INTO w VALUES (1), (2), (2), (3)")
        .unwrap();
    assert_eq!(
        cells(
            &mut db,
            "SELECT a, sum(a) over (order by a rows between 9223372036854775807 following and 1 following) FROM w ORDER BY a"
        ),
        [["1", ""], ["2", ""], ["2", ""], ["3", ""]]
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT a, sum(a) over (order by a groups between current row and 9223372036854775807 following) FROM w ORDER BY a"
        ),
        [["1", "8"], ["2", "7"], ["2", "7"], ["3", "3"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT a, sum(a) over (order by a rows between 9223372036854775807 preceding and current row) FROM w ORDER BY a"
        ),
        [["1", "1"], ["2", "3"], ["2", "5"], ["3", "8"]]
    );
}

#[test]
fn out_of_range_day_numbers_do_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        cells(&mut db, "SELECT pg_input_is_valid('6874898-01-01', 'date')"),
        [["false"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT to_timestamp(' Infinity'::float)::text, to_timestamp('-Infinity'::float)::text"
        ),
        [["infinity", "-infinity"]]
    );
    let error = SqlSession::new(&mut db)
        .execute("SELECT to_timestamp('NaN'::float)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22008");
    assert_eq!(error.to_string(), "timestamp cannot be NaN");
    let error = SqlSession::new(&mut db)
        .execute("SELECT to_timestamp(1e20)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22008", "{error}");
    assert_eq!(error.to_string(), "timestamp out of range: \"1e+20\"");
}
