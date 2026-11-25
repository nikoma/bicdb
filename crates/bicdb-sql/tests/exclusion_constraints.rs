//! Exclusion constraints: written inline in CREATE TABLE or added later, and
//! checked against this transaction's own earlier writes.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn open(root: &std::path::Path, mode: StorageMode) -> BicDb {
    BicDb::open_with_config(
        root,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap()
}

#[test]
fn inline_exclusion_constraints_are_named_and_enforced() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE booking(room INT, during INT4RANGE, EXCLUDE USING gist (room WITH =, during WITH &&))")
            .unwrap();
        assert_eq!(
            sql.execute(
                "SELECT conname, contype FROM pg_constraint WHERE conrelid='booking'::regclass"
            )
            .unwrap()
            .rows,
            vec![vec![
                SqlValue::String("booking_room_during_excl".into()),
                SqlValue::String("x".into())
            ]]
        );
        sql.execute("INSERT INTO booking VALUES (1,'[1,5)'),(2,'[1,5)'),(1,'[5,9)')")
            .unwrap();
        assert_eq!(
            sql.execute("INSERT INTO booking VALUES (1,'[3,6)')")
                .unwrap_err()
                .sqlstate(),
            "23P01"
        );
    }
}

#[test]
fn rows_written_earlier_in_the_transaction_conflict() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE spans(id INT PRIMARY KEY, r INT4RANGE, CONSTRAINT spans_no_overlap EXCLUDE USING gist (r WITH &&))")
            .unwrap();
        sql.execute("BEGIN; INSERT INTO spans VALUES (1,'[1,5)')")
            .unwrap();
        assert_eq!(
            sql.execute("INSERT INTO spans VALUES (2,'[2,3)')")
                .unwrap_err()
                .sqlstate(),
            "23P01"
        );
        sql.execute("ROLLBACK").unwrap();
        // An update may move a row within its own old range.
        sql.execute("INSERT INTO spans VALUES (1,'[1,3)'),(2,'[5,7)'); UPDATE spans SET r='[2,4)' WHERE id=1")
            .unwrap();
        assert_eq!(
            sql.execute("UPDATE spans SET r='[3,6)' WHERE id=1")
                .unwrap_err()
                .sqlstate(),
            "23P01"
        );
    }
}

#[test]
fn deferrable_exclusion_constraints_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path(), StorageMode::EmbeddedMemory);
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute("CREATE TABLE d(r INT4RANGE, EXCLUDE USING gist (r WITH &&) DEFERRABLE)")
            .unwrap_err()
            .sqlstate(),
        "0A000"
    );
    // The failed definition leaves no table behind.
    assert!(sql.execute("SELECT * FROM d").is_err());
}
