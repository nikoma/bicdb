//! DROP INDEX preserves the constraints which own its backing.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn native_indexes(db: &BicDb) -> serde_json::Value {
    let mut values = db.index_definitions();
    values.sort_by(|a, b| a.name.cmp(&b.name));
    serde_json::to_value(values).unwrap()
}

#[test]
fn primary_and_unique_constraint_indexes_survive_direct_drop_and_cascade() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE guarded(item_key INT CONSTRAINT keep_pk PRIMARY KEY,code INT CONSTRAINT keep_unique UNIQUE,payload TEXT); INSERT INTO guarded VALUES(1,10,'acknowledged'); CREATE INDEX free_idx ON guarded(payload)").unwrap();
        let before = native_indexes(&db);
        for command in [
            "DROP INDEX keep_pk",
            "DROP INDEX IF EXISTS keep_unique",
            "DROP INDEX keep_pk CASCADE",
            "DROP INDEX keep_unique CASCADE",
        ] {
            let error = SqlSession::new(&mut db).execute(command).unwrap_err();
            assert_eq!(error.sqlstate(), "2BP01", "{mode}: {command}: {error}");
            assert_eq!(native_indexes(&db), before, "{mode}: {command}");
        }
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("INSERT INTO guarded VALUES(1,20,'bad primary')")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("INSERT INTO guarded VALUES(2,10,'bad unique')")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        SqlSession::new(&mut db)
            .execute("DROP INDEX free_idx")
            .unwrap();
        for reopen in 0..3 {
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT item_key,code,payload FROM guarded")
                    .unwrap()
                    .rows,
                vec![vec![
                    SqlValue::Int(1),
                    SqlValue::Int(10),
                    SqlValue::String("acknowledged".into())
                ]]
            );
            assert_eq!(SqlSession::new(&mut db).execute("SELECT count(*) FROM pg_constraint WHERE conrelid='guarded'::regclass AND contype IN ('p','u')").unwrap().rows,vec![vec![SqlValue::Int(2)]]);
            db.close().unwrap();
            if reopen == 2 {
                break;
            }
            db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        }
    }
}

#[test]
fn qualified_and_path_index_names_keep_their_own_constraint_or_standalone_backing() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone()),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE SCHEMA left_ns; CREATE SCHEMA right_ns; CREATE TABLE left_ns.target(item_key INT,payload INT,CONSTRAINT \"Bound.Dot\" UNIQUE(payload)); CREATE TABLE right_ns.target(item_key INT,payload INT,CONSTRAINT \"Bound.Dot\" UNIQUE(payload)); CREATE INDEX \"Loose.Dot\" ON left_ns.target(item_key); CREATE INDEX \"Loose.Dot\" ON right_ns.target(item_key); SET search_path=right_ns,left_ns").unwrap();
        for command in [
            "DROP INDEX left_ns.\"Bound.Dot\"",
            "DROP INDEX right_ns.\"Bound.Dot\"",
            "DROP INDEX \"Bound.Dot\"",
        ] {
            assert_eq!(
                sql.execute(command).unwrap_err().sqlstate(),
                "2BP01",
                "{mode}: {command}"
            );
        }
        sql.execute("DROP INDEX left_ns.\"Loose.Dot\"").unwrap();
        assert_eq!(sql.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='left_ns' AND indexname='Loose.Dot'").unwrap().rows,vec![vec![SqlValue::Int(0)]]);
        assert_eq!(sql.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='right_ns' AND indexname='Loose.Dot'").unwrap().rows,vec![vec![SqlValue::Int(1)]]);
        sql.execute("DROP INDEX \"Loose.Dot\"").unwrap();
        assert_eq!(
            sql.execute("SELECT count(*) FROM pg_indexes WHERE indexname='Loose.Dot'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(0)]]
        );
    }
}

#[test]
fn multi_index_drop_preflights_constraint_dependencies_before_removing_any_index() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone()),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE multi_guard(item_key INT CONSTRAINT multi_pk PRIMARY KEY,payload INT); CREATE INDEX first_free ON multi_guard(payload)").unwrap();
        let before = native_indexes(&db);
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("DROP INDEX first_free,multi_pk")
                .unwrap_err()
                .sqlstate(),
            "2BP01"
        );
        assert_eq!(
            native_indexes(&db),
            before,
            "{mode}: first index was removed before later refusal"
        );
        assert!(SqlSession::new(&mut db)
            .execute("DROP INDEX first_free,no_such_index")
            .is_err());
        assert_eq!(
            native_indexes(&db),
            before,
            "{mode}: missing later index removed the first"
        );
    }
}

#[test]
fn quoted_case_distinct_indexes_keep_exact_native_and_sql_identity_after_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE case_target(item_key INT,payload INT); CREATE INDEX \"CaseIndex\" ON case_target(payload); CREATE INDEX \"caseindex\" ON case_target(payload)").unwrap();
        let before = db.index_definitions().len();
        SqlSession::new(&mut db)
            .execute("DROP INDEX \"CaseIndex\"")
            .unwrap();
        assert_eq!(
            db.index_definitions().len(),
            before - 1,
            "{mode}: one quoted name removed another Native index"
        );
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        assert_eq!(SqlSession::new(&mut db).execute("SELECT indexname FROM pg_indexes WHERE tablename='case_target' AND indexname IN ('CaseIndex','caseindex')").unwrap().rows,vec![vec![SqlValue::String("caseindex".into())]]);
        SqlSession::new(&mut db)
            .execute("DROP INDEX \"caseindex\"")
            .unwrap();
        assert_eq!(
            db.index_definitions().len(),
            before - 2,
            "{mode}: remaining logical index did not remove its own Native index"
        );
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        assert_eq!(SqlSession::new(&mut db).execute("SELECT count(*) FROM pg_indexes WHERE tablename='case_target' AND indexname IN ('CaseIndex','caseindex')").unwrap().rows,vec![vec![SqlValue::Int(0)]]);
    }
}

#[test]
fn dropping_a_materialized_view_index_removes_its_stored_descriptor() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE base_rows(item_key INT,payload INT); CREATE MATERIALIZED VIEW materialized_rows AS SELECT item_key,payload FROM base_rows; CREATE INDEX view_idx ON materialized_rows(payload)").unwrap();
        sql.execute("DROP INDEX view_idx").unwrap();
        assert_eq!(sql.execute("SELECT count(*) FROM pg_indexes WHERE tablename='materialized_rows' AND indexname='view_idx'").unwrap().rows,vec![vec![SqlValue::Int(0)]]);
    }
}

#[test]
fn oracle_constraint_owned_indexes_keep_the_native_02429_diagnostic() {
    // Native diagnostic reference:
    // https://docs.oracle.com/en/error-help/db/ora-02429/
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        bicdb_sql::oracle::ensure_database_profile(
            &mut db,
            bicdb_sql::oracle::ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE oracle_guard(item_key INT CONSTRAINT oracle_keep_pk PRIMARY KEY,code INT CONSTRAINT oracle_keep_uq UNIQUE); INSERT INTO oracle_guard VALUES(1,10)").unwrap();
        let before = native_indexes(&db);
        for command in ["DROP INDEX oracle_keep_pk", "DROP INDEX oracle_keep_uq"] {
            let error = SqlSession::new(&mut db).execute(command).unwrap_err();
            assert_eq!(error.sqlstate(), "2BP01");
            assert_eq!(
                error.to_string(),
                "ORA-02429: cannot drop index used for enforcement of unique/primary key"
            );
            let detail = error
                .fields()
                .into_iter()
                .find_map(|field| match field {
                    bicdb_sql::SqlErrorField::Detail(value) => Some(value),
                    _ => None,
                })
                .unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&detail).unwrap()["oracle_error_stack"]
                    [0]["code"],
                -2429
            );
            assert_eq!(native_indexes(&db), before);
        }
    }
}

#[test]
fn explicitly_qualified_index_creation_and_drop_keep_exact_namespace_identity() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for oracle_profile in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            if oracle_profile {
                bicdb_sql::oracle::ensure_database_profile(
                    &mut db,
                    bicdb_sql::oracle::ORACLE_PREVIEW_PROFILE,
                )
                .unwrap();
            }
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE SCHEMA red; CREATE SCHEMA blue; CREATE TABLE red.entries(k INT PRIMARY KEY,payload INT); CREATE TABLE blue.entries(k INT PRIMARY KEY,payload INT); INSERT INTO red.entries VALUES(1,10); INSERT INTO blue.entries VALUES(2,20); CREATE INDEX red.local_idx ON red.entries(payload); CREATE INDEX blue.local_idx ON blue.entries(payload); DROP INDEX red.local_idx").unwrap();
            let before = db.index_definitions().len();
            assert_eq!(
                before, 3,
                "{mode} oracle={oracle_profile}: remove exactly one backing"
            );
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            let mut sql = SqlSession::new(&mut db);
            sql.execute("SET search_path=blue; DROP INDEX local_idx")
                .unwrap();
            assert_eq!(db.index_definitions().len(), before - 1);
            let mut sql = SqlSession::new(&mut db);
            assert!(sql.execute("INSERT INTO red.entries VALUES(1,30)").is_err());
            assert!(sql
                .execute("INSERT INTO blue.entries VALUES(2,30)")
                .is_err());
            assert_eq!(
                sql.execute("SELECT payload FROM red.entries WHERE k=1")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(10)]]
            );
            assert_eq!(
                sql.execute("SELECT payload FROM blue.entries WHERE k=2")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(20)]]
            );
        }
    }
}
