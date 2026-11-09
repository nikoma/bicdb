//! Catalog types and relation metadata checked against PostgreSQL 16.13.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn catalog_comparisons_match_postgresql_reference() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/typed_comparison/typed_catalog_pg16.json"
    ))
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in fixture["setup"].as_array().unwrap() {
        let sql = sql.as_str().unwrap();
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("setup {sql}: {error}"));
    }
    let mut failures = Vec::new();
    for case in fixture["cases"].as_array().unwrap() {
        let sql = case["sql"].as_str().unwrap();
        let actual = match session.execute(sql) {
            Ok(result) => serde_json::json!({"rows": result.rows.iter().map(|row| {
                row.iter().map(|value| {
                    if matches!(value, SqlValue::Null) { "<null>".to_string() }
                    else { value.to_cell() }
                }).collect::<Vec<_>>()
            }).collect::<Vec<_>>()}),
            Err(error) => serde_json::json!({"sqlstate":error.sqlstate()}),
        };
        let expected = if case.get("sqlstate").is_some() {
            serde_json::json!({"sqlstate":case["sqlstate"]})
        } else {
            serde_json::json!({"rows":case["rows"]})
        };
        if actual != expected {
            failures.push(format!("{}: {sql}\nexpected {expected}\nactual {actual}", case["label"]));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
