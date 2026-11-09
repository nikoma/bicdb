//! Catalog types and relation metadata checked against PostgreSQL 16.13.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use std::collections::BTreeMap;

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
            failures.push(format!(
                "{}: {sql}\nexpected {expected}\nactual {actual}",
                case["label"]
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

fn overload_identities(db: &mut BicDb) -> BTreeMap<String, (String, String, String)> {
    let result = SqlSession::new(db).execute(
        "SELECT proargtypes,oid,pg_get_function_identity_arguments(oid),pg_get_function_result(oid) \
         FROM pg_proc WHERE proname='catalog_overloaded'",
    ).unwrap();
    result
        .rows
        .into_iter()
        .map(|row| {
            (
                row[0].to_cell(),
                (row[1].to_cell(), row[2].to_cell(), row[3].to_cell()),
            )
        })
        .collect()
}

#[test]
fn overload_oids_identify_signatures_and_survive_promotion_replacement_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for sql in [
        "CREATE FUNCTION catalog_overloaded(a integer) RETURNS integer LANGUAGE SQL AS $$SELECT a$$",
        "CREATE FUNCTION catalog_overloaded(a text) RETURNS text LANGUAGE SQL AS $$SELECT a$$",
        "CREATE FUNCTION catalog_overloaded(a numeric) RETURNS numeric LANGUAGE SQL AS $$SELECT a$$",
        "CREATE FUNCTION catalog_overloaded() RETURNS integer LANGUAGE SQL AS $$SELECT 0$$",
    ] {
        SqlSession::new(&mut db).execute(sql).unwrap();
    }
    let original = overload_identities(&mut db);
    assert_eq!(original.len(), 4);
    let ids = original
        .values()
        .map(|row| &row.0)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 4, "each pg_proc signature needs a distinct OID");
    for (args, identity, result) in [
        ("23", "a integer", "integer"),
        ("25", "a text", "text"),
        ("1700", "a numeric", "numeric"),
        ("", "", "integer"),
    ] {
        assert_eq!(original[args].1, identity);
        assert_eq!(original[args].2, result);
    }
    SqlSession::new(&mut db)
        .execute("DROP FUNCTION catalog_overloaded(integer)")
        .unwrap();
    SqlSession::new(&mut db).execute(
        "CREATE OR REPLACE FUNCTION catalog_overloaded(a text) RETURNS text LANGUAGE SQL AS $$SELECT a || '!'$$",
    ).unwrap();
    let remaining = overload_identities(&mut db);
    for args in ["25", "1700", ""] {
        assert_eq!(remaining[args], original[args], "OID changed for {args}");
    }
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    assert_eq!(overload_identities(&mut db), remaining);
}

#[test]
fn operator_catalog_points_to_the_implementing_overload() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for sql in [
        "CREATE FUNCTION catalog_op(a integer,b integer) RETURNS boolean LANGUAGE SQL AS $$SELECT a=b$$",
        "CREATE FUNCTION catalog_op(a numeric,b numeric) RETURNS boolean LANGUAGE SQL AS $$SELECT a=b$$",
        "CREATE OPERATOR public.## (FUNCTION=catalog_op,LEFTARG=numeric,RIGHTARG=numeric)",
    ] {
        SqlSession::new(&mut db).execute(sql).unwrap();
    }
    let result = SqlSession::new(&mut db)
        .execute(
            "SELECT p.proargtypes FROM pg_operator o JOIN pg_proc p ON o.oprcode=p.oid \
         WHERE o.oprname='##' AND p.proname='catalog_op'",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("1700 1700".to_string())]]
    );
}
