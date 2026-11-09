//! SQL profile startup must publish matching scalar and native index semantics.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{
    oracle::{ensure_database_profile, ORACLE_PREVIEW_PROFILE},
    SqlSession, SqlValue,
};
use serde_json::json;

#[test]
fn fresh_oracle_profile_has_ready_scalar_case_semantics() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let rows = SqlSession::new(&mut db)
            .execute("SELECT upper('éǆß'),lower('ÉǄẞ')")
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            vec![vec![
                SqlValue::String("ÉǄß".into()),
                SqlValue::String("éǆß".into())
            ]]
        );
    }
}

#[test]
fn legacy_oracle_profile_migrates_case_indexes_before_sql_is_served() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        // Construct the old durable profile: its UPPER keys used ASCII semantics.
        SqlSession::new(&mut db).execute("CREATE TABLE case_rows(id INT PRIMARY KEY,v TEXT); INSERT INTO case_rows VALUES(1,'é'),(2,'É'); CREATE INDEX case_upper ON case_rows(upper(v))").unwrap();
        db.create_collection("__bicdb_sql_compatibility").unwrap();
        db.insert(
            "__bicdb_sql_compatibility",
            Record::new("profile")
                .with_metadata(json!({"format":1,"profile":ORACLE_PREVIEW_PROFILE})),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT id FROM case_rows WHERE upper(v)='É' ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
        );
        let index = db
            .index_definitions()
            .into_iter()
            .find(|d| d.name.ends_with("case_upper"))
            .unwrap();
        assert!(matches!(
            &index.fields[0],
            bicdb_core::IndexField::OracleUpperV1(_)
        ));
        assert!(db.verify_index(&index.name).unwrap().valid);
        db.flush().unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT count(*) FROM case_rows WHERE upper(v)='É'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)]]
        );
    }
}
