use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn sql_function_patterns_resolve_parameters_and_preserve_nulls() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION invalid_prefix(value text, pattern text) RETURNS boolean LANGUAGE sql AS $$ SELECT value NOT LIKE pattern $$;
        CREATE FUNCTION matching_prefix(value text, pattern text) RETURNS boolean LANGUAGE sql AS $$ SELECT value ILIKE pattern $$").unwrap();
    for (value, expected) in [
        ("'plain'", SqlValue::Bool(true)),
        ("'enc:v2:abc'", SqlValue::Bool(false)),
        ("NULL", SqlValue::Null),
        ("'plain'", SqlValue::Bool(true)),
    ] {
        assert_eq!(
            sql.execute(&format!("SELECT invalid_prefix({value},'enc:v2:%')"))
                .unwrap()
                .rows,
            vec![vec![expected]]
        );
    }
    assert_eq!(
        sql.execute("SELECT matching_prefix('ENC:v2:abc','enc:v2:%')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn procedural_pattern_guards_use_locals_and_return_the_intended_sqlstate() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION validate_delivery(p_sent boolean,p_attempt text,p_reminder text) RETURNS boolean LANGUAGE plpgsql AS $$
        DECLARE pattern text := 'enc:v2:%';
        BEGIN
          IF NOT p_sent AND (p_attempt IS NULL OR p_attempt NOT LIKE pattern OR p_reminder IS NULL OR p_reminder NOT LIKE pattern) THEN
            RAISE EXCEPTION 'Encrypted delivery failure required' USING ERRCODE='22023';
          END IF;
          RETURN p_sent OR p_attempt ILIKE lower(pattern);
        END $$").unwrap();
    for query in [
        "SELECT validate_delivery(false,'plain','plain')",
        "SELECT validate_delivery(false,NULL,'enc:v2:abc')",
    ] {
        assert_eq!(sql.execute(query).unwrap_err().sqlstate(), "22023");
    }
    for query in [
        "SELECT validate_delivery(false,'enc:v2:abc','enc:v2:def')",
        "SELECT validate_delivery(true,NULL,NULL)",
    ] {
        assert_eq!(
            sql.execute(query).unwrap().rows,
            vec![vec![SqlValue::Bool(true)]]
        );
    }
}
