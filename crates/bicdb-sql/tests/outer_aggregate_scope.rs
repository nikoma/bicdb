use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn fromless_scalar_aggregates_bind_in_the_parent_that_supplies_their_inputs() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE scope_rows (k int PRIMARY KEY, v int);
         INSERT INTO scope_rows VALUES (1, 2), (2, 4), (3, 6), (4, 8);
         CREATE INDEX scope_rows_v ON scope_rows(v);",
    )
    .unwrap();

    let cases = [
        (
            "SELECT (SELECT sum(o.v) AS total) FROM scope_rows o",
            "total",
            vec!["20"],
        ),
        (
            "SELECT (SELECT max((SELECT i.v FROM scope_rows i WHERE i.k = o.k)))              FROM scope_rows o",
            "max",
            vec!["8"],
        ),
        (
            "SELECT (SELECT count(*) FILTER (WHERE o.k > 2)) FROM scope_rows o",
            "count",
            vec!["2"],
        ),
        (
            "SELECT (SELECT sum(v)) FROM scope_rows",
            "sum",
            vec!["20"],
        ),
        (
            "SELECT (SELECT sum(o.v)) FROM scope_rows o WHERE false",
            "sum",
            vec![""],
        ),
        (
            "SELECT (SELECT max((SELECT o.v FROM scope_rows o WHERE o.k = 1)))              FROM scope_rows o ORDER BY o.k",
            "max",
            vec!["2", "2", "2", "2"],
        ),
        (
            "SELECT (SELECT count(1)) FROM scope_rows o ORDER BY o.k",
            "count",
            vec!["1", "1", "1", "1"],
        ),
    ];
    for (query, label, expected) in cases {
        let result = sql
            .execute(query)
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        assert_eq!(result.columns, [label], "{query}");
        let values: Vec<String> = result.rows.iter().map(|row| row[0].to_cell()).collect();
        assert_eq!(values, expected, "{query}");
    }
}
