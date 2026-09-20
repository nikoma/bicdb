use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    (dir, db)
}

fn assert_numeric_overflow(session: &mut SqlSession<'_>, json: &str) {
    let query = format!("SELECT '{json}'::jsonb");
    let error = session.execute(&query).unwrap_err();
    assert_eq!(error.sqlstate(), "22003", "query: {query}");
    assert!(
        error.to_string().contains("value overflows numeric format"),
        "query: {query}; error: {error}"
    );
}

#[test]
fn jsonb_numeric_exponent_boundaries_match_postgres_18() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let valid = session
        .execute(
            r#"SELECT
                   jsonb_typeof('1e131071'::jsonb),
                   jsonb_typeof('1e-16383'::jsonb),
                   jsonb_typeof('0e1073741823'::jsonb),
                   '0e1073741823'::jsonb = '0'::jsonb,
                   '1e131071'::jsonb = '10e131070'::jsonb"#,
        )
        .unwrap();
    assert_eq!(
        valid.rows,
        vec![vec![
            SqlValue::String("number".to_string()),
            SqlValue::String("number".to_string()),
            SqlValue::String("number".to_string()),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );

    for value in [
        "1e131072",
        "1e-16384",
        "0e1073741824",
        "1e999999999999999999999999",
        "0e999999999999999999999999",
    ] {
        assert_numeric_overflow(&mut session, value);
    }
}

#[test]
fn jsonb_numeric_digit_and_scale_boundaries_are_checked_without_expansion() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);

    let max_integer = "9".repeat(131_072);
    let max_fraction = format!("0.{}1", "0".repeat(16_382));
    for value in [&max_integer, &max_fraction] {
        let result = session
            .execute(&format!("SELECT jsonb_typeof('{value}'::jsonb)"))
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::String("number".to_string())]]
        );
    }

    let oversized_integer = format!("1{}", "0".repeat(131_072));
    let oversized_fraction = format!("0.{}1", "0".repeat(16_383));
    let oversized_zero_scale = format!("1.{}", "0".repeat(16_384));
    for value in [
        &oversized_integer,
        &oversized_fraction,
        &oversized_zero_scale,
    ] {
        assert_numeric_overflow(&mut session, value);
    }
}

#[test]
fn jsonb_numeric_validation_is_recursive_and_json_remains_distinct() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let huge = "1e999999999999999999999999";

    let json = session
        .execute(&format!(
            "SELECT json_typeof('{huge}'::json), json_typeof('{{\"nested\":[{huge}]}}'::json)"
        ))
        .unwrap();
    assert_eq!(
        json.rows,
        vec![vec![
            SqlValue::String("number".to_string()),
            SqlValue::String("object".to_string()),
        ]]
    );

    assert_numeric_overflow(&mut session, r#"{"outer":[1,{"nested":1e131072}]}"#);

    let cast_from_json = session
        .execute(&format!("SELECT ('{huge}'::json)::jsonb"))
        .unwrap_err();
    assert_eq!(cast_from_json.sqlstate(), "22003");

    session
        .execute("CREATE TABLE jsonb_documents (id bigint PRIMARY KEY, document jsonb)")
        .unwrap();
    let insert = session
        .execute(&format!(
            "INSERT INTO jsonb_documents (id, document) VALUES (1, '{{\"n\":{huge}}}')"
        ))
        .unwrap_err();
    assert_eq!(insert.sqlstate(), "22003", "{insert:?}");
}
