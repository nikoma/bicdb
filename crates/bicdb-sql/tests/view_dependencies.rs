//! View rewrite dependencies observed on native PostgreSQL 16.13.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn view_rewrite_dependencies_keep_exact_columns_and_view_chains() {
    check_fixture(include_str!(
        "fixtures/postgres/view_dependencies_pg16.json"
    ));
}

#[test]
fn view_dependencies_bind_scopes_quotes_stars_and_joins() {
    check_fixture(include_str!(
        "fixtures/postgres/view_dependency_scopes_pg16.json"
    ));
}

#[test]
fn describing_view_dependencies_never_executes_view_functions() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE dependency_effects(id int); CREATE FUNCTION dependency_effect() RETURNS int LANGUAGE plpgsql VOLATILE AS $$ BEGIN INSERT INTO dependency_effects VALUES(1); RETURN 1; END $$; CREATE VIEW dependency_function_view AS SELECT dependency_effect() AS n").unwrap();
    session.execute("SELECT count(*) FROM pg_depend").unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM dependency_effects")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

fn check_fixture(json: &str) {
    let fixture: serde_json::Value = serde_json::from_str(json).unwrap();
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
