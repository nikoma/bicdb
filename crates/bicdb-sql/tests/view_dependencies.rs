//! View rewrite dependencies observed on native PostgreSQL 16.13.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn view_rewrite_dependencies_keep_exact_columns_and_view_chains() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/postgres/view_dependencies_pg16.json"
    ))
    .unwrap();
    let expected = fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row.as_array()
                .unwrap()
                .iter()
                .map(|cell| match cell {
                    serde_json::Value::String(value) => SqlValue::String(value.clone()),
                    serde_json::Value::Number(value) => SqlValue::Int(value.as_i64().unwrap()),
                    serde_json::Value::Null => SqlValue::Null,
                    _ => panic!("unexpected native cell {cell:?}"),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let open = || {
            BicDb::open_with_config(
                root.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap()
        };
        {
            let mut db = open();
            let mut session = SqlSession::new(&mut db);
            for sql in fixture["setup"].as_array().unwrap() {
                session.execute(sql.as_str().unwrap()).unwrap();
            }
            assert_eq!(
                session
                    .execute(fixture["query"].as_str().unwrap())
                    .unwrap()
                    .rows,
                expected,
                "{mode:?}"
            );
        }
        let mut db = open();
        let mut session = SqlSession::new(&mut db);
        assert_eq!(
            session
                .execute(fixture["query"].as_str().unwrap())
                .unwrap()
                .rows,
            expected,
            "reopened {mode:?}"
        );
        // This is an actual type change through a dependent view; exposing
        // dependencies must not bypass the existing DDL safety guard.
        assert!(session
            .execute("ALTER TABLE dependency_probe.orders ALTER COLUMN description TYPE integer")
            .is_err());
    }
}
