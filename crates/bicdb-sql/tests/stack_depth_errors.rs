//! Statements that used to overflow the worker stack and abort the whole server process.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

/// Runs `f` on a thread with the protocol worker's stack size, so a stack overflow here is one
/// that would abort the server.
fn on_worker_stack(f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

fn rows(db: &mut BicDb, sql: &str) -> Vec<Vec<String>> {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

#[test]
fn order_by_an_alias_or_position_of_an_integer_constant_sorts_by_the_constant() {
    on_worker_stack(|| {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        assert_eq!(rows(&mut db, "SELECT 1 AS x ORDER BY x"), [["1"]]);
        assert_eq!(rows(&mut db, "SELECT 1 ORDER BY 1"), [["1"]]);
        assert_eq!(
            rows(&mut db, "SELECT 1 AS x, 2 AS y ORDER BY x, y DESC"),
            [["1", "2"]]
        );
        rows(&mut db, "CREATE TABLE t (a int)");
        rows(&mut db, "INSERT INTO t VALUES (3), (1), (2)");
        // A constant key leaves the remaining keys in charge.
        assert_eq!(
            rows(&mut db, "SELECT 7 AS k, a FROM t ORDER BY k, a"),
            [["7", "1"], ["7", "2"], ["7", "3"]]
        );
        // The grouping-sets path used to overflow the same way. (Its result for a FROM-less
        // rollup over an output alias still differs from PostgreSQL, which returns NULL and 1;
        // that is a separate grouping-sets gap.)
        SqlSession::new(&mut db)
            .execute("select 1 as one group by rollup(one) order by one nulls first")
            .unwrap();
    });
}

#[test]
fn a_function_that_calls_itself_fails_with_54001_instead_of_aborting() {
    on_worker_stack(|| {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        rows(
            &mut db,
            "create function infinite_recurse() returns int as 'select infinite_recurse()' language sql",
        );
        let error = SqlSession::new(&mut db)
            .execute("select infinite_recurse()")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "54001");
        assert_eq!(error.to_string(), "stack depth limit exceeded");
        // The session and the database stay usable.
        assert_eq!(rows(&mut db, "select 2"), [["2"]]);
    });
}
