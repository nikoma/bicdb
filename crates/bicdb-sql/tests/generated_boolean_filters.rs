//! GridTable appends each saved record to its already parenthesized filter.
//! SQL size grows linearly while the historical raw nesting check refused it
//! after about twenty saves. Keep the original parser safety limits enabled.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{infer_parameter_types, SqlSession, SqlValue};

fn appended_filter(count: usize, operator: &str) -> String {
    let mut filter = "(orders.is_sales = 'Y')".to_string();
    for id in 1..=count {
        filter = format!("(({filter}) {operator} (orders.id = {id}))");
    }
    filter
}

#[test]
fn growing_grid_filter_remains_executable_and_describable() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            directory.path(),
            DbConfig::default().with_storage_mode(mode),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE orders(id INT PRIMARY KEY, is_sales TEXT, processed TEXT)")
            .unwrap();
        session.execute("INSERT INTO orders VALUES (0, 'Y', 'N'), (1, 'N', 'N'), (5000, 'N', 'N'), (6000, NULL, 'N'), (7000, 'Y', 'Y')").unwrap();
        for count in [21, 100, 1000] {
            let filter = appended_filter(count, "OR");
            let sql = format!("SELECT count(*) FROM orders WHERE {filter} AND processed='N'");
            let result = session
                .execute(&sql)
                .unwrap_or_else(|e| panic!("{mode:?} {count}: {e}"));
            assert_eq!(result.rows, vec![vec![SqlValue::Int(2)]]);
        }
        let sql = format!(
            "SELECT id FROM orders WHERE {} AND processed=$1",
            appended_filter(100, "OR")
        );
        drop(session);
        assert_eq!(infer_parameter_types(&db, &sql).unwrap().len(), 1);
    }
}

#[test]
fn balancing_preserves_three_valued_logic_and_mixed_operator_groups() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for (seed, operator, term, expected) in [
        ("NULL", "OR", "FALSE", SqlValue::Null),
        ("NULL", "OR", "TRUE", SqlValue::Bool(true)),
        ("NULL", "AND", "TRUE", SqlValue::Null),
        ("NULL", "AND", "FALSE", SqlValue::Bool(false)),
        ("FALSE", "OR", "(TRUE AND FALSE)", SqlValue::Bool(false)),
        ("TRUE", "AND", "(FALSE OR TRUE)", SqlValue::Bool(true)),
    ] {
        let mut expression = seed.to_string();
        for _ in 0..100 {
            expression = format!("(({expression}) {operator} ({term}))");
        }
        let result = session.execute(&format!("SELECT {expression}")).unwrap();
        assert_eq!(result.rows, vec![vec![expected]]);
    }
}

#[test]
fn expensive_non_boolean_nesting_still_hits_the_original_guard() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let mut expression = "1".to_string();
    for _ in 0..60 {
        expression = format!("CAST({expression} AS INT)");
    }
    let error = session
        .execute(&format!("SELECT {expression}"))
        .unwrap_err();
    assert!(error.to_string().contains("limit of 40"), "{error}");
    assert_eq!(
        session.execute("SELECT 1").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}
