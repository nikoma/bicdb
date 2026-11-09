//! Metadata preparation must not repeatedly decode one table definition.
use crate::*;
use bicdb_core::StorageMode;

fn fixture(mode: bicdb_core::StorageMode) -> (tempfile::TempDir, BicDb, usize) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(false),
    )
    .unwrap();
    let columns = (0..32)
        .map(|n| format!("c{n} INTEGER"))
        .collect::<Vec<_>>()
        .join(",");
    SqlSession::new(&mut db)
        .execute(&format!("CREATE TABLE prep_rows({columns})"))
        .unwrap();
    let record = db.get(SCHEMA_COLLECTION, "prep_rows").unwrap().unwrap();
    let bytes = serde_json::to_vec(&record.metadata).unwrap().len();
    (root, db, bytes)
}

fn measured<T>(operation: impl FnOnce() -> T) -> (T, SqlProfileStats) {
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let value = operation();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    (value, stats)
}

#[test]
fn metadata_preparation_description_decodes_each_table_once() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_root, db, bytes) = fixture(mode);
        let projection = (0..32)
            .map(|n| format!("c{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT {projection} FROM prep_rows");
        let (columns, stats) = measured(|| infer_query_result_columns_for_describe(&db, &sql));
        let columns = columns.unwrap().unwrap();
        assert_eq!(columns.len(), 32);
        assert!(columns
            .iter()
            .all(|(_, kind)| kind.as_deref() == Some("int4")));
        assert!(
            stats.schema_load_bytes <= bytes,
            "{stats:?}; one schema is {bytes} bytes"
        );
    }
}

#[test]
fn metadata_preparation_origins_decode_each_table_once() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_root, db, bytes) = fixture(mode);
        let projection = (0..32)
            .map(|n| format!("c{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT {projection} FROM prep_rows");
        let (columns, stats) = measured(|| infer_query_result_column_metadata(&db, &sql));
        let columns = columns.unwrap().unwrap();
        assert_eq!(columns.len(), 32);
        assert!(columns.iter().all(|column| column.table_oid != 0));
        assert_eq!(
            columns
                .iter()
                .map(|column| column.attribute_number)
                .collect::<Vec<_>>(),
            (1..=32).collect::<Vec<_>>()
        );
        assert!(
            stats.schema_load_bytes <= bytes,
            "{stats:?}; one schema is {bytes} bytes"
        );
    }
}

#[test]
fn metadata_preparation_parameters_decode_each_table_once() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_root, db, bytes) = fixture(mode);
        let predicate = (0..32)
            .map(|n| format!("c{n}=${}", n + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let (types, stats) = measured(|| {
            infer_parameter_types(&db, &format!("SELECT c0 FROM prep_rows WHERE {predicate}"))
        });
        assert_eq!(types.unwrap(), vec![Some("int4".to_string()); 32]);
        assert!(
            stats.schema_load_bytes <= bytes,
            "{stats:?}; one schema is {bytes} bytes"
        );
    }
}

#[test]
fn metadata_preparation_oracle_lowering_reuses_schema_across_type_rewrites() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(false),
        )
        .unwrap();
        for source in [
            "CREATE SCHEMA calendar",
            "CREATE TABLE calendar.events(k NUMBER PRIMARY KEY,happened DATE)",
            "INSERT INTO calendar.events VALUES(1,TIMESTAMP '2024-02-29 23:59:58')",
        ] {
            let sql =
                oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        let record = db
            .scan_collection(SCHEMA_COLLECTION)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let bytes = serde_json::to_vec(&record.metadata).unwrap().len();
        let projection = (0..16)
            .map(|n| format!("TRUNC(happened,'MM')+k AS d{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let source = format!("SELECT {projection} FROM events");
        let (lowered, stats) = measured(|| {
            oracle::lower_session_sql(
                &db,
                &source,
                oracle::ORACLE_PREVIEW_PROFILE,
                Some("calendar"),
                "bicdb",
            )
        });
        let result = SqlSession::new(&mut db).execute(&lowered.unwrap()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["2024-02-02 00:00:00"; 16]
        );
        assert!(
            stats.schema_load_bytes <= bytes,
            "{stats:?}; one schema is {bytes} bytes"
        );
    }
}

#[test]
fn metadata_preparation_does_not_retain_schema_across_ddl_or_rollback() {
    let (_root, mut db, _) = fixture(StorageMode::ServerPaged);
    assert_eq!(
        infer_query_result_columns_for_describe(&db, "SELECT * FROM prep_rows")
            .unwrap()
            .unwrap()
            .len(),
        32
    );
    let mut session = SqlSession::new(&mut db);
    session
        .execute("BEGIN; ALTER TABLE prep_rows ADD COLUMN extra TEXT")
        .unwrap();
    assert_eq!(
        infer_query_result_columns_for_describe(session.db_ref(), "SELECT * FROM prep_rows")
            .unwrap()
            .unwrap()
            .len(),
        33
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        infer_query_result_columns_for_describe(session.db_ref(), "SELECT * FROM prep_rows")
            .unwrap()
            .unwrap()
            .len(),
        32
    );
}

#[test]
fn metadata_preparation_connection_cache_reuses_across_requests() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_root, db, bytes) = fixture(mode);
        let mut cache = SqlSessionCatalogCache::default();
        let (_, stats) = measured(|| {
            for _ in 0..4 {
                let columns = cache
                    .with_schema_cache(&db, || {
                        infer_query_result_columns_for_describe(&db, "SELECT * FROM prep_rows")
                    })
                    .unwrap()
                    .unwrap();
                assert_eq!(columns.len(), 32);
            }
        });
        assert!(
            stats.schema_load_bytes <= bytes,
            "{stats:?}; one schema is {bytes} bytes"
        );
    }
}

#[test]
fn metadata_preparation_connection_cache_observes_ddl_and_rollback() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_root, mut db, _) = fixture(mode);
        let mut cache = SqlSessionCatalogCache::default();
        let describe = |cache: &mut SqlSessionCatalogCache, db: &BicDb| {
            cache
                .with_schema_cache(db, || {
                    infer_query_result_columns_for_describe(db, "SELECT * FROM prep_rows")
                })
                .unwrap()
                .unwrap()
                .len()
        };
        assert_eq!(describe(&mut cache, &db), 32);
        let mut session = SqlSession::new(&mut db);
        session
            .execute("BEGIN; ALTER TABLE prep_rows ADD COLUMN extra TEXT")
            .unwrap();
        assert_eq!(describe(&mut cache, session.db_ref()), 33);
        session.execute("ROLLBACK").unwrap();
        assert_eq!(describe(&mut cache, session.db_ref()), 32);
        let (_other_root, mut other, _) = fixture(mode);
        SqlSession::new(&mut other)
            .execute("ALTER TABLE prep_rows ADD COLUMN other TEXT")
            .unwrap();
        assert_eq!(describe(&mut cache, &other), 33);
        assert_eq!(describe(&mut cache, &db), 32);
    }
}

#[test]
fn metadata_preparation_connection_cache_recovers_after_error_and_panic() {
    let (_root, db, _) = fixture(StorageMode::ServerPaged);
    let mut cache = SqlSessionCatalogCache::default();
    assert!(cache
        .with_schema_cache(&db, || {
            infer_query_result_columns_for_describe(&db, "SELECT missing FROM prep_rows")
        })
        .is_err());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cache.with_schema_cache(&db, || {
            infer_query_result_columns_for_describe(&db, "SELECT * FROM prep_rows").unwrap();
            panic!("preparation interrupted")
        });
    }));
    assert!(panic.is_err());
    let columns = cache
        .with_schema_cache(&db, || {
            infer_query_result_columns_for_describe(&db, "SELECT * FROM prep_rows")
        })
        .unwrap()
        .unwrap();
    assert_eq!(columns.len(), 32);
}
