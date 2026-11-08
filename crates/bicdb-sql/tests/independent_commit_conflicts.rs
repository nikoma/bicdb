//! Nontransactional sequence allocation must not rebase earlier business writes.
use bicdb_core::{BicDb, DbConfig, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

#[test]
fn independent_sql_commit_preserves_absent_key_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE commands(id TEXT PRIMARY KEY,response TEXT);CREATE SEQUENCE independent_nonce").unwrap();
    let mut pending = db.begin_transaction().unwrap();
    pending.set_snapshot_isolation(false, 0).unwrap();
    assert!(pending
        .get_for_integrity_check("commands", "same-key")
        .unwrap()
        .is_none());
    db.insert(
        "commands",
        Record::new("same-key").with_metadata(json!({"id":"same-key","response":"winner"})),
    )
    .unwrap();
    pending
        .insert(
            "commands",
            Record::new("same-key").with_metadata(json!({"id":"same-key","response":"stale"})),
        )
        .unwrap();

    let mut session = SqlSession::new_shared(&db).with_pending_transaction(pending);
    assert_eq!(
        session
            .execute("SELECT nextval('independent_nonce')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert!(session.last_statement_had_independent_commits());
    assert_eq!(session.execute("COMMIT").unwrap_err().sqlstate(), "40001");
    assert_eq!(
        session
            .execute("SELECT response FROM commands WHERE id='same-key'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("winner".into())]]
    );
    assert_eq!(
        session
            .execute("SELECT nextval('independent_nonce')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]],
        "independent sequence allocation remains durable after parent conflict"
    );
}
