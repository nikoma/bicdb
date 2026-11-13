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
    SqlSession::new(&mut db)
        .execute("INSERT INTO protected_records VALUES ('seed', 'original ACK')")
        .unwrap();
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
    assert_eq!(error.sqlstate(), "42501", "{error}");
    assert!(db.get("protected_records", "one").unwrap().is_none());
    for query in [
        "UPDATE protected_records SET value='forbidden' WHERE id='seed'",
        "DELETE FROM protected_records WHERE id='seed'",
    ] {
        let sequence = db.last_commit_seq();
        let error = SqlSession::new(&mut db).execute(query).unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{query}: {error}");
        assert_eq!(db.last_commit_seq(), sequence);
        assert_eq!(
            db.get("protected_records", "seed")
                .unwrap()
                .unwrap()
                .metadata["value"],
            "original ACK"
        );
    }
}
