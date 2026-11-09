//! Native Oracle 23.26 AL16UTF16 table assignments, without changing routine
//! buffer semantics or PostgreSQL character declarations.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{
    oracle::{ensure_database_profile, lower_session_sql, ORACLE_PREVIEW_PROFILE},
    SqlSession, SqlValue,
};
fn run(db: &mut BicDb, sql: &str) -> bicdb_sql::Result<Vec<Vec<SqlValue>>> {
    let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, Some("\"APP\""), "app")?;
    SqlSession::new(db)
        .execute(&lowered)
        .map(|result| result.rows)
}
#[test]
fn national_table_assignments_match_native_oracle_utf16_units() {
    let reference: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/oracle/national_columns_oracle_23_26.json"
    ))
    .unwrap();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode);
        {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db)
                .execute("CREATE SCHEMA \"APP\"")
                .unwrap();
            for case in reference["cases"].as_array().unwrap() {
                let kind = case["type"].as_str().unwrap();
                let ddl = format!("CREATE TABLE national_target(v {kind})");
                let created = run(&mut db, &ddl);
                if let Some(code) = case
                    .get("create_error")
                    .and_then(|error| error["code"].as_i64())
                {
                    assert!(
                        created
                            .unwrap_err()
                            .to_string()
                            .contains(&format!("ORA-{code:05}")),
                        "{kind}"
                    );
                    continue;
                }
                created.unwrap_or_else(|error| panic!("{ddl}: {error}"));
                for assignment in case["assignments"].as_array().unwrap() {
                    let value = assignment["value"].as_str().unwrap();
                    let inserted = run(
                        &mut db,
                        &format!(
                            "INSERT INTO national_target VALUES('{}')",
                            value.replace('\'', "''")
                        ),
                    );
                    if let Some(code) = assignment
                        .get("error")
                        .and_then(|error| error["code"].as_i64())
                    {
                        assert!(
                            inserted
                                .unwrap_err()
                                .to_string()
                                .contains(&format!("ORA-{code:05}")),
                            "{kind}/{value:?}"
                        );
                    } else {
                        inserted.unwrap_or_else(|error| panic!("{kind}/{value:?}: {error}"));
                        let expected = &assignment["rows"][0][0];
                        let actual = run(&mut db, "SELECT v FROM national_target").unwrap();
                        let expected = if expected.is_null() {
                            SqlValue::Null
                        } else {
                            SqlValue::String(expected.as_str().unwrap().to_string())
                        };
                        assert_eq!(actual, vec![vec![expected]], "{kind}/{value:?}");
                    }
                    run(&mut db, "DELETE FROM national_target").unwrap();
                }
                run(&mut db, "DROP TABLE national_target PURGE").unwrap();
            }
            run(
                &mut db,
                "CREATE TABLE reopened_national(v NCHAR(3),w NVARCHAR2(2))",
            )
            .unwrap();
            run(&mut db, "INSERT INTO reopened_national VALUES('𐐀','𐐀')").unwrap();
        }
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            run(&mut db, "SELECT v,w FROM reopened_national").unwrap(),
            vec![vec![
                SqlValue::String("𐐀 ".into()),
                SqlValue::String("𐐀".into())
            ]]
        );
    }
}
