//! A primary key constrains rows the current transaction has written, not only
//! rows it can see committed.
//!
//! Found by Jepsen. Elle reported a G0 write cycle on a list-append workload;
//! the underlying cause was that uniqueness validation consulted committed
//! state only (`db.get`, `lookup_index_exact`, `scan_collection`), so a row
//! inserted earlier in the *same* transaction was invisible. A second INSERT
//! of the same key therefore passed validation and silently replaced the
//! first, losing its data with no error. PostgreSQL raises 23505.

use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn session_db() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();
    (root, db)
}

#[test]
fn a_duplicate_insert_inside_one_transaction_is_rejected() {
    let (_root, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE pk_dup (id int PRIMARY KEY, v text)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO pk_dup (id, v) VALUES (1, 'first')")
        .unwrap();
    let error = session
        .execute("INSERT INTO pk_dup (id, v) VALUES (1, 'second')")
        .expect_err("the second insert of the same key must be rejected");
    assert!(
        error.to_string().contains("duplicate key value"),
        "expected a unique violation, got: {error}"
    );
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn an_upsert_sees_a_row_its_own_transaction_inserted() {
    let (_root, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE upsert_own (id int PRIMARY KEY, v text)")
        .unwrap();

    // The exact shape the Jepsen workload used: upsert-then-append, repeated,
    // with the row created by this same transaction. Each upsert must DO
    // NOTHING rather than replace the row and discard the appends so far.
    session.execute("BEGIN").unwrap();
    for value in ["2", "3", "4"] {
        session
            .execute("INSERT INTO upsert_own (id, v) VALUES (1, '') ON CONFLICT (id) DO NOTHING")
            .unwrap();
        session
            .execute(&format!(
                "UPDATE upsert_own SET v = v || ',' || '{value}' WHERE id = 1"
            ))
            .unwrap();
    }
    session.execute("COMMIT").unwrap();

    let rows = session
        .execute("SELECT v FROM upsert_own WHERE id = 1")
        .unwrap();
    let rendered = format!("{rows:?}");
    assert!(
        rendered.contains(",2,3,4"),
        "every append must survive; PostgreSQL yields ',2,3,4', got: {rendered}"
    );
}

#[test]
fn a_duplicate_insert_across_transactions_is_still_rejected() {
    let (_root, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE pk_across (id int PRIMARY KEY, v text)")
        .unwrap();
    session
        .execute("INSERT INTO pk_across (id, v) VALUES (1, 'first')")
        .unwrap();
    let error = session
        .execute("INSERT INTO pk_across (id, v) VALUES (1, 'second')")
        .expect_err("a committed duplicate must still be rejected");
    assert!(error.to_string().contains("duplicate key value"));
}
