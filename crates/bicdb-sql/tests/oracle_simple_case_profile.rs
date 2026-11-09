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

#[test]
fn legacy_unique_case_collision_fences_sql_and_can_resume_after_native_repair() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE unique_case_rows(id INT PRIMARY KEY,v TEXT); INSERT INTO unique_case_rows VALUES(1,'é'),(2,'É'); CREATE UNIQUE INDEX unique_case_upper ON unique_case_rows(upper(v))").unwrap();
        db.create_collection("__bicdb_sql_compatibility").unwrap();
        db.insert(
            "__bicdb_sql_compatibility",
            Record::new("profile")
                .with_metadata(json!({"format":1,"profile":ORACLE_PREVIEW_PROFILE})),
        )
        .unwrap();
        let error = ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("unique"),
            "{error}"
        );
        assert!(db
            .get("__bicdb_sql_compatibility", "oracle_simple_case_v1")
            .unwrap()
            .is_none());
        assert!(SqlSession::new(&mut db)
            .execute("SELECT upper('é')")
            .is_err());
        let old = db
            .index_definitions()
            .into_iter()
            .find(|index| index.name.ends_with("unique_case_upper"))
            .unwrap();
        assert!(matches!(old.fields[0], bicdb_core::IndexField::Upper(_)));
        assert!(db.verify_index(&old.name).unwrap().valid);
        db.flush().unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        let duplicate = db
            .scan_collection(&old.collection)
            .unwrap()
            .into_iter()
            .find(|row| row.metadata["v"] == "É")
            .unwrap();
        db.delete(&old.collection, &duplicate.id).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT count(*) FROM unique_case_rows WHERE upper(v)='É'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
    }
}

#[test]
fn postgres_profile_retains_legacy_case_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, bicdb_sql::oracle::POSTGRES_PROFILE).unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT upper('é'),lower('É')")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("é".into()),
            SqlValue::String("É".into())
        ]]
    );
}

#[test]
fn national_case_uses_al16utf16_for_literals_columns_and_nested_queries() {
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
        let run = |db: &mut BicDb, sql: &str| {
            let sql = bicdb_sql::oracle::lower_session_sql(
                db,
                sql,
                ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )
            .unwrap();
            SqlSession::new(db).execute(&sql).unwrap().rows
        };
        run(&mut db, "CREATE TABLE national_case_rows(v NVARCHAR2(4))");
        run(&mut db, "INSERT INTO national_case_rows VALUES(N'𐐨')");
        for sql in [
            "SELECT upper(N'𐐨') FROM dual",
            "SELECT upper(v) FROM national_case_rows",
            "SELECT upper(q.v) FROM (SELECT v FROM national_case_rows) q",
            "WITH q AS (SELECT v FROM national_case_rows) SELECT upper(v) FROM q",
        ] {
            assert_eq!(
                run(&mut db, sql),
                vec![vec![SqlValue::String("𐐨".into())]],
                "{mode:?}: {sql}"
            );
        }
        assert_eq!(
            run(&mut db, "SELECT upper('𐐨') FROM dual"),
            vec![vec![SqlValue::String("𐐀".into())]]
        );
    }
}
