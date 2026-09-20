use bicdb_core::{BicDb, MutationPolicy};
use bicdb_sql::SqlSession;

#[test]
fn sql_and_pgwire_style_sessions_cannot_bypass_native_mutation_authority() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE protected_records (id TEXT PRIMARY KEY, value TEXT)")
            .unwrap();
    }
    db.set_mutation_policy(
        "protected_records",
        MutationPolicy::grants_required().with_audit(),
    )
    .unwrap();

    let error = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("INSERT INTO protected_records (id, value) VALUES ('one', 'bypass')")
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("mutation") || error.to_string().contains("grant"),
        "protected SQL write must fail in the core mutation path: {error}"
    );
    assert!(db.get("protected_records", "one").unwrap().is_none());
}
