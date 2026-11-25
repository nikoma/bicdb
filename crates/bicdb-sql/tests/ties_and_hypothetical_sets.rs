//! `FETCH FIRST n ROWS WITH TIES` and the hypothetical-set aggregates
//! `rank|dense_rank|percent_rank|cume_dist(x) WITHIN GROUP (ORDER BY ...)`.
//! Expected results were checked against PostgreSQL 18.4.
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

fn session_with(statements: &[&str], test: impl FnOnce(&mut SqlSession<'_>)) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in statements {
        session.execute(sql).unwrap();
    }
    test(&mut session);
}

#[test]
fn fetch_with_ties_keeps_rows_tied_with_the_last_one() {
    session_with(
        &[
            "CREATE TABLE ft(id INT, g INT)",
            "INSERT INTO ft VALUES (1,1),(2,1),(3,2),(4,2),(5,3)",
        ],
        |session| {
            let result = session
                .execute("SELECT * FROM ft ORDER BY g FETCH FIRST 3 ROWS WITH TIES")
                .unwrap();
            assert_eq!(result.columns, ["id", "g"]);
            let mut ids = result
                .rows
                .iter()
                .map(|row| row[0].to_cell())
                .collect::<Vec<_>>();
            ids.sort();
            assert_eq!(ids, ["1", "2", "3", "4"]);
            let mut ids = rows(
                session,
                "SELECT id FROM ft ORDER BY g DESC OFFSET 1 ROWS FETCH FIRST 1 ROW WITH TIES",
            );
            ids.sort();
            assert_eq!(ids, grid(&[&["3"], &["4"]]));
            assert_eq!(
                rows(session, "SELECT g, count(*) AS c FROM ft GROUP BY g ORDER BY c DESC, g FETCH FIRST 1 ROWS WITH TIES"),
                grid(&[&["1", "2"]])
            );
            let error = session
                .execute("SELECT * FROM ft FETCH FIRST 1 ROWS WITH TIES")
                .unwrap_err();
            assert_eq!(error.sqlstate(), "42601");
        },
    );
}

#[test]
fn hypothetical_set_aggregates_rank_the_direct_argument() {
    session_with(
        &[
            "CREATE TABLE hs(g INT, x INT)",
            "INSERT INTO hs VALUES (1,10),(1,20),(1,20),(1,NULL),(2,5)",
        ],
        |session| {
            assert_eq!(
                rows(
                    session,
                    "SELECT g, rank(20) WITHIN GROUP (ORDER BY x), dense_rank(20) WITHIN GROUP (ORDER BY x), \
                     percent_rank(20) WITHIN GROUP (ORDER BY x), cume_dist(20) WITHIN GROUP (ORDER BY x) \
                     FROM hs GROUP BY g ORDER BY g"
                ),
                grid(&[&["1", "2", "2", "0.25", "0.8"], &["2", "2", "2", "1", "1"]])
            );
            assert_eq!(
                rows(
                    session,
                    "SELECT rank(15) WITHIN GROUP (ORDER BY x DESC NULLS LAST), \
                     cume_dist(NULL) WITHIN GROUP (ORDER BY x) FROM hs"
                ),
                grid(&[&["3", "1"]])
            );
            assert_eq!(
                rows(
                    session,
                    "SELECT rank(1) WITHIN GROUP (ORDER BY x) FROM hs WHERE false"
                ),
                grid(&[&["1"]])
            );
        },
    );
}
