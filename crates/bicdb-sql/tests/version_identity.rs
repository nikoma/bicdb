use std::collections::HashMap;

use bicdb_core::BicDb;
use bicdb_sql::{
    bicdb_version_banner, SqlSession, SqlValue, BICDB_VERSION, POSTGRES_COMPATIBILITY_VERSION,
    POSTGRES_COMPATIBILITY_VERSION_NUM,
};

fn one(value: impl Into<String>) -> Vec<Vec<SqlValue>> {
    vec![vec![SqlValue::String(value.into())]]
}

#[test]
fn default_sql_identity_is_bicdb_first_and_truthful() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session.execute("SELECT version()").unwrap().rows,
        one(bicdb_version_banner(POSTGRES_COMPATIBILITY_VERSION))
    );
    assert_eq!(
        session.execute("SELECT bicdb_version()").unwrap().rows,
        one(BICDB_VERSION)
    );
    assert_eq!(
        session
            .execute("SELECT pg_catalog.bicdb_version()")
            .unwrap()
            .rows,
        one(BICDB_VERSION)
    );
    assert_eq!(
        session.execute("SHOW bicdb_version").unwrap().rows,
        one(BICDB_VERSION)
    );
    assert_eq!(
        session.execute("SHOW server_version").unwrap().rows,
        one(POSTGRES_COMPATIBILITY_VERSION)
    );
    assert_eq!(
        session.execute("SHOW server_version_num").unwrap().rows,
        one(POSTGRES_COMPATIBILITY_VERSION_NUM)
    );

    let combined = session
        .execute("SELECT version(), bicdb_version()")
        .unwrap();
    assert_eq!(combined.columns, ["version", "bicdb_version"]);
    assert_eq!(
        combined.rows,
        vec![vec![
            SqlValue::String(bicdb_version_banner(POSTGRES_COMPATIBILITY_VERSION)),
            SqlValue::String(BICDB_VERSION.to_string()),
        ]]
    );
}

#[test]
fn host_compatibility_override_never_changes_bicdb_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db).with_session_gucs(HashMap::from([
        ("server_version".to_string(), "16.7".to_string()),
        ("server_version_num".to_string(), "160007".to_string()),
    ]));

    assert_eq!(
        session.execute("SELECT version()").unwrap().rows,
        one(bicdb_version_banner("16.7"))
    );
    assert_eq!(
        session.execute("SHOW server_version").unwrap().rows,
        one("16.7")
    );
    assert_eq!(
        session.execute("SHOW server_version_num").unwrap().rows,
        one("160007")
    );
    assert_eq!(
        session.execute("SELECT bicdb_version()").unwrap().rows,
        one(BICDB_VERSION)
    );
    assert_eq!(
        session
            .execute("SELECT version(), bicdb_version()")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String(bicdb_version_banner("16.7")),
            SqlValue::String(BICDB_VERSION.to_string()),
        ]]
    );
    assert!(session.execute("SET server_version = '99.0'").is_err());

    session.execute("RESET ALL").unwrap();
    assert_eq!(
        session.execute("SHOW server_version").unwrap().rows,
        one("16.7")
    );
    session.execute("DISCARD ALL").unwrap();
    assert_eq!(
        session.execute("SHOW server_version").unwrap().rows,
        one("16.7")
    );
}
