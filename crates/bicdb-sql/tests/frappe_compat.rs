//! Regressions found by running the Frappe Framework (PostgreSQL mode) against
//! BicDB. Each test states the PostgreSQL behaviour the framework relies on.
use bicdb_core::BicDb;
use bicdb_sql::{SqlEngine, SqlSession, SqlValue};
use std::{collections::HashMap, sync::Arc};

fn s(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

fn gucs(database: &str) -> HashMap<String, String> {
    HashMap::from([("database".to_string(), database.to_string())])
}

/// Frappe lists its tables with
/// `information_schema.tables WHERE table_catalog = <db name>`; PostgreSQL
/// reports the connected database as every information_schema catalog name.
#[test]
fn information_schema_catalog_is_the_connected_database() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db).with_session_gucs(gucs("site_db"));
        session
            .execute(
                "CREATE TABLE \"tabDefaultValue\" (name varchar(140) PRIMARY KEY, defkey text UNIQUE); \
                 CREATE VIEW v_defaults AS SELECT name FROM \"tabDefaultValue\"",
            )
            .unwrap();
        let tables = session
            .execute(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_catalog = 'site_db' AND table_type = 'BASE TABLE' AND table_schema = 'public'",
            )
            .unwrap();
        assert_eq!(tables.rows, vec![vec![s("tabDefaultValue")]]);
        for query in [
            "SELECT DISTINCT table_catalog FROM information_schema.columns WHERE table_name = 'tabDefaultValue'",
            "SELECT DISTINCT table_catalog FROM information_schema.views",
            "SELECT DISTINCT constraint_catalog FROM information_schema.table_constraints WHERE table_name = 'tabDefaultValue'",
            "SELECT current_database()",
        ] {
            assert_eq!(session.execute(query).unwrap().rows, vec![vec![s("site_db")]], "{query}");
        }
    }
    let engine = SqlEngine::new(&db).with_session_gucs(Arc::new(gucs("other_db")));
    assert_eq!(
        engine
            .execute("SELECT DISTINCT table_catalog FROM information_schema.tables WHERE table_name = 'tabDefaultValue'")
            .unwrap()
            .rows,
        vec![vec![s("other_db")]]
    );
    // Without a connected database (embedded use) the historical name stays.
    let engine = SqlEngine::new(&db);
    assert_eq!(
        engine
            .execute("SELECT DISTINCT table_catalog FROM information_schema.tables WHERE table_name = 'tabDefaultValue'")
            .unwrap()
            .rows,
        vec![vec![s("bicdb")]]
    );
}
