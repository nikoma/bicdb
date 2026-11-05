use bicdb_core::BicDb;
use bicdb_sql::{oracle, SqlErrorField, SqlSession};
use std::collections::BTreeSet;

fn public_names(profile: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE public.\"Records/With.Dot\"(row_id INTEGER CONSTRAINT \"Primary.With.Dot\" PRIMARY KEY,value INTEGER CONSTRAINT \"Unique With Space\" UNIQUE)").unwrap();
    session
        .execute("INSERT INTO public.\"Records/With.Dot\" VALUES(1,10),(2,20)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT r.row_id FROM \"Records/With.Dot\" r WHERE r.value=10")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "1"
    );
    let names: BTreeSet<_> = session.execute("SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename='Records/With.Dot'").unwrap().rows.into_iter().map(|row| row[0].to_cell()).collect();
    assert_eq!(
        names,
        ["Primary.With.Dot", "Unique With Space"]
            .map(str::to_owned)
            .into_iter()
            .collect()
    );
    let error = session
        .execute("INSERT INTO \"Records/With.Dot\" VALUES(3,10)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23505");
    assert!(error
        .fields()
        .contains(&SqlErrorField::Table("Records/With.Dot".into())));
    assert!(error
        .fields()
        .contains(&SqlErrorField::Constraint("Unique With Space".into())));
    drop(session);
    drop(db);
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM public.\"Records/With.Dot\"")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "2"
    );
    session
        .execute("DROP TABLE public.\"Records/With.Dot\"")
        .unwrap();
    assert_eq!(session.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' AND tablename='Records/With.Dot'").unwrap().rows[0][0].to_cell(), "0");
}

#[test]
fn postgres_quoted_public_names_keep_logical_identity_after_reopen() {
    public_names(oracle::POSTGRES_PROFILE);
}

#[test]
fn oracle_transport_quoted_public_names_keep_logical_identity_after_reopen() {
    public_names(oracle::ORACLE_PREVIEW_PROFILE);
}
