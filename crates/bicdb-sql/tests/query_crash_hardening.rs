//! Regression tests for the query-reachable process-crash class.
//!
//! Every case here was a working exploit: a client with nothing but query
//! access could kill the whole server — and with it every other tenant —
//! either by nesting input until the thread stack overflowed (a hardware
//! fault no `catch_unwind` can contain) or by naming a collection size the
//! allocator could not satisfy (an abort, likewise uncatchable).
//!
//! The acceptance criterion for all of them is identical and deliberately
//! weak on behaviour, strong on survival: BicDB may reject, refuse, or
//! error — it may **not** die. Each test therefore asserts the call
//! returned (Ok or Err) and then keeps using the same session, which is
//! only possible if the process is still alive.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

/// Geometric types are exercised in the default (non-paged) mode, matching
/// the rest of the geometric suite.
fn plain_db() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open(directory.path()).unwrap();
    (directory, db)
}

fn session_db() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (directory, db)
}

/// Run `sql` and report only whether the engine survived it.
fn survives(db: &mut BicDb, sql: &str) -> bool {
    let mut session = SqlSession::new(db);
    let _ = session.execute(sql);
    true
}

#[test]
fn deeply_nested_tsquery_is_refused_not_fatal() {
    let (_directory, mut db) = session_db();
    // 100k open parens: one recursive descent per byte before the fix.
    let bomb = format!("{}a{}", "(".repeat(100_000), ")".repeat(100_000));
    let sql = format!("SELECT to_tsquery('{bomb}')");
    assert!(survives(&mut db, &sql));

    // The budget refuses rather than crashing, and says why.
    let mut session = SqlSession::new(&mut db);
    let error = session
        .execute(&sql)
        .expect_err("a 100k-deep tsquery must be refused");
    assert!(
        format!("{error}").contains("nested"),
        "unexpected refusal: {error}"
    );
    // Ordinary queries still work on the same session afterwards.
    let result = session.execute("SELECT to_tsquery('cat & dog')").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn deeply_nested_jsonpath_is_refused_not_fatal() {
    let (_directory, mut db) = session_db();
    let bomb = format!("{}1{}", "(".repeat(100_000), ")".repeat(100_000));
    let sql = format!("SELECT jsonb_path_exists('{{}}'::jsonb, '{bomb}')");
    assert!(survives(&mut db, &sql));

    let mut session = SqlSession::new(&mut db);
    assert!(session.execute(&sql).is_err(), "the bomb must be refused");
    let result = session
        .execute("SELECT jsonb_path_exists('{\"a\":1}'::jsonb, '$.a')")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn deeply_nested_xpath_expression_and_document_are_refused_not_fatal() {
    let (_directory, mut db) = session_db();
    let expression = format!("{}1{}", "(".repeat(50_000), ")".repeat(50_000));
    assert!(survives(
        &mut db,
        &format!("SELECT xpath('{expression}', '<a/>')")
    ));

    // The document side recurses through every tree walk, serialization
    // above all.
    let document = format!("{}{}", "<a>".repeat(50_000), "</a>".repeat(50_000));
    assert!(survives(
        &mut db,
        &format!("SELECT xpath('/*', '{document}')")
    ));

    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute("SELECT xpath('/a/text()', '<a>ok</a>')")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn absurd_array_subscript_is_refused_not_fatal() {
    let (_directory, mut db) = session_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE arrays (id INT PRIMARY KEY, values INT[])")
            .unwrap();
        session
            .execute("INSERT INTO arrays VALUES (1, ARRAY[1,2])")
            .unwrap();
    }

    // Two billion nulls from a two-element array.
    assert!(survives(
        &mut db,
        "UPDATE arrays SET values[2000000000] = 9 WHERE id = 1"
    ));
    // The slice form, whose guard checked only that the widths matched.
    assert!(survives(
        &mut db,
        "UPDATE arrays SET values[-2000000000:-1999999999] = ARRAY[7,8] WHERE id = 1"
    ));

    let mut session = SqlSession::new(&mut db);
    let error = session
        .execute("UPDATE arrays SET values[2000000000] = 9 WHERE id = 1")
        .expect_err("an absurd subscript must be refused");
    assert!(
        format!("{error}").contains("array size"),
        "unexpected refusal: {error}"
    );
    // Ordinary subscript assignment is untouched.
    session
        .execute("UPDATE arrays SET values[3] = 9 WHERE id = 1")
        .unwrap();
    let result = session
        .execute("SELECT values[3] FROM arrays WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn absurd_polygon_point_count_is_refused_not_fatal() {
    let (_directory, mut db) = plain_db();
    assert!(survives(
        &mut db,
        "SELECT polygon(1000000000, '<(0,0),5>'::circle)"
    ));

    let mut session = SqlSession::new(&mut db);
    let error = session
        .execute("SELECT polygon(1000000000, '<(0,0),5>'::circle)")
        .expect_err("a billion-point polygon must be refused");
    assert!(
        format!("{error}").contains("more than"),
        "unexpected refusal: {error}"
    );
    let result = session
        .execute("SELECT polygon(8, '<(0,0),5>'::circle)")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn non_finite_geometric_bounds_do_not_panic() {
    let (_directory, mut db) = plain_db();
    // `f64::clamp` asserts min <= max; a NaN bound violated it mid-query.
    assert!(survives(
        &mut db,
        "SELECT point(0,0) <-> '((nan,5),(10,20))'::box"
    ));
    assert!(survives(
        &mut db,
        "SELECT point(0,0) <-> '((inf,5),(10,20))'::box"
    ));

    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute("SELECT point(0,0) <-> '((1,1),(2,2))'::box")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}
