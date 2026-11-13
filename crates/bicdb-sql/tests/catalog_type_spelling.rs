use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{pg_user_type_oid_by_name, SqlSession};

#[test]
fn canonical_mixed_case_and_quoted_type_spellings_keep_one_catalog_identity() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE SCHEMA \"Owner\"; CREATE TYPE \"Owner\".\"MiXeD\" AS ENUM('ok')")
            .unwrap();
        let canonical = pg_user_type_oid_by_name(&db, "Owner.MiXeD")
            .unwrap()
            .unwrap();
        assert_eq!(
            pg_user_type_oid_by_name(&db, "\"Owner\".\"MiXeD\"").unwrap(),
            Some(canonical)
        );
        assert!(pg_user_type_oid_by_name(&db, "owner.mixed")
            .unwrap()
            .is_none());
        db.close().unwrap();
    }
}

#[test]
fn quoted_dots_and_escaped_quotes_do_not_change_type_identity() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE SCHEMA \"Owner.with.dot\"; CREATE TYPE \"Owner.with.dot\".\"MiXeD\"\"Type\" AS ENUM('ok')").unwrap();
        let oid = pg_user_type_oid_by_name(&db, "\"Owner.with.dot\".\"MiXeD\"\"Type\"")
            .unwrap()
            .unwrap();
        assert!(oid > 0);
        assert!(pg_user_type_oid_by_name(&db, "Owner.with.dot.MiXeDType")
            .unwrap()
            .is_none());
        db.close().unwrap();
    }
}
