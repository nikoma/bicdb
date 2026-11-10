//! RENAME preserves a relation's namespace, including quoted identifiers and
//! a same-named relation in public. PostgreSQL and Oracle checks stay separate.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{oracle, SqlResult, SqlSession};

fn execute(db: &mut BicDb, source: &str, oracle_profile: bool) -> bicdb_sql::Result<SqlResult> {
    let lowered = if oracle_profile {
        oracle::lower_session_sql(db, source, oracle::ORACLE_PREVIEW_PROFILE, None, "bicdb")?
    } else {
        source.to_string()
    };
    SqlSession::new(db).execute(&lowered)
}

fn text_rows(db: &mut BicDb, source: &str, oracle_profile: bool) -> Vec<Vec<String>> {
    execute(db, source, oracle_profile)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn namespace_case(mode: StorageMode, oracle_profile: bool) {
    let root = tempfile::tempdir().unwrap();
    let config = DbConfig::default().with_storage_mode(mode);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    if oracle_profile {
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    }
    let text_type = if oracle_profile {
        "VARCHAR2(32)"
    } else {
        "TEXT"
    };
    for statement in [
        "CREATE SCHEMA \"RenameSpace\"".to_string(),
        "CREATE SCHEMA \"OtherSpace\"".to_string(),
        format!("CREATE TABLE public.\"Target\"(\"rowkey\" INT PRIMARY KEY,\"payload\" {text_type})"),
        format!("CREATE TABLE \"RenameSpace\".\"Source\"(\"rowkey\" INT PRIMARY KEY,\"payload\" {text_type})"),
        "INSERT INTO public.\"Target\" VALUES(99,'outside')".to_string(),
        "INSERT INTO \"RenameSpace\".\"Source\" VALUES(1,'kept')".to_string(),
        "ALTER TABLE \"RenameSpace\".\"Source\" RENAME TO \"Target\"".to_string(),
    ] {
        execute(&mut db, &statement, oracle_profile).unwrap();
    }
    assert_eq!(
        text_rows(
            &mut db,
            "SELECT \"rowkey\",\"payload\" FROM \"RenameSpace\".\"Target\"",
            oracle_profile,
        ),
        vec![vec!["1", "kept"]]
    );
    assert_eq!(
        text_rows(
            &mut db,
            "SELECT \"rowkey\",\"payload\" FROM public.\"Target\"",
            oracle_profile,
        ),
        vec![vec!["99", "outside"]]
    );
    assert!(execute(
        &mut db,
        "SELECT * FROM \"OtherSpace\".\"Target\"",
        oracle_profile,
    )
    .is_err());
    execute(
        &mut db,
        "ALTER TABLE \"RenameSpace\".\"Target\" RENAME TO \"Dot.Target\"",
        oracle_profile,
    )
    .unwrap();
    drop(db);
    let mut reopened = BicDb::open_with_config(root.path(), config).unwrap();
    assert_eq!(
        text_rows(
            &mut reopened,
            "SELECT \"rowkey\",\"payload\" FROM \"RenameSpace\".\"Dot.Target\"",
            oracle_profile,
        ),
        vec![vec!["1", "kept"]]
    );
    assert_eq!(
        text_rows(
            &mut reopened,
            "SELECT \"rowkey\",\"payload\" FROM public.\"Target\"",
            oracle_profile,
        ),
        vec![vec!["99", "outside"]]
    );
}

#[test]
fn postgres_rename_preserves_schema_and_quoted_names_with_public_collision() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        namespace_case(mode, false);
    }
}

#[test]
fn oracle_rename_preserves_schema_and_quoted_names_with_public_collision() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        namespace_case(mode, true);
    }
}

#[test]
fn postgres_rename_checks_type_collision_in_the_owning_schema() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE SCHEMA rename_types; CREATE TABLE rename_types.source(rowkey INT PRIMARY KEY); INSERT INTO rename_types.source VALUES(1); CREATE TYPE rename_types.taken AS (value INT)").unwrap();
        assert_eq!(
            sql.execute("ALTER TABLE rename_types.source RENAME TO taken")
                .unwrap_err()
                .sqlstate(),
            "42710"
        );
        assert_eq!(
            sql.execute("SELECT rowkey FROM rename_types.source")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
        assert_eq!(
            sql.execute("ALTER TABLE rename_types.source RENAME TO public.escaped")
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
    }
}
