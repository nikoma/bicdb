//! GROUP BY GROUPING SETS / ROLLUP / CUBE, mixed with plain grouping
//! columns, GROUPING(), HAVING per grouping set and ordering of the combined
//! rows. Expected results were checked against PostgreSQL 18.4.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn grid(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

fn with_sales(test: impl FnOnce(&mut SqlSession<'_>)) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE gs(region TEXT, product TEXT, qty INT)",
        "INSERT INTO gs VALUES ('east','a',1),('east','b',2),('west','a',3),('west','a',4)",
    ] {
        session.execute(sql).unwrap();
    }
    test(&mut session);
}

#[test]
fn rollup_fills_nulls_and_grouping_reports_the_set() {
    with_sales(|session| {
        assert_eq!(
            rows(
                session,
                "SELECT region, product, sum(qty), GROUPING(region, product) AS g FROM gs \
                 GROUP BY ROLLUP (region, product) ORDER BY region, product"
            ),
            grid(&[
                &["east", "a", "1", "0"],
                &["east", "b", "2", "0"],
                &["east", "", "3", "1"],
                &["west", "a", "7", "0"],
                &["west", "", "7", "1"],
                &["", "", "10", "3"],
            ])
        );
    });
}

#[test]
fn cube_and_grouping_sets_with_plain_columns_and_having() {
    with_sales(|session| {
        assert_eq!(
            rows(
                session,
                "SELECT region, product, count(*) FROM gs GROUP BY CUBE (region, product) \
                 ORDER BY 1, 2"
            )
            .len(),
            8
        );
        assert_eq!(
            rows(
                session,
                "SELECT region, product, sum(qty) AS total FROM gs \
                 GROUP BY region, GROUPING SETS ((product), ()) HAVING sum(qty) > 2 \
                 ORDER BY total DESC, product"
            ),
            grid(&[&["west", "a", "7"], &["west", "", "7"], &["east", "", "3"],])
        );
        assert_eq!(
            rows(
                session,
                "SELECT GROUPING(product), product, max(qty) FROM gs \
                 GROUP BY GROUPING SETS (product, ()) ORDER BY GROUPING(product), product"
            ),
            grid(&[&["0", "a", "4"], &["0", "b", "2"], &["1", "", "4"]])
        );
    });
}

#[test]
fn empty_grouping_set_yields_a_row_for_an_empty_input() {
    with_sales(|session| {
        assert_eq!(
            rows(
                session,
                "SELECT region, count(*) FROM gs WHERE qty > 100 GROUP BY ROLLUP (region)"
            ),
            grid(&[&["", "0"]])
        );
    });
}

#[test]
fn grouping_argument_must_be_a_grouping_expression() {
    with_sales(|session| {
        let error = session
            .execute("SELECT GROUPING(qty) FROM gs GROUP BY ROLLUP (region)")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "42803");
    });
}
