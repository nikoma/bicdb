use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn timezone_catalog_validates_names_in_application_query_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for name in ["UTC", "America/Los_Angeles", "Asia/Kathmandu", "US/Eastern"] {
        let result = sql
            .execute(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_timezone_names WHERE name='{name}')"
            ))
            .unwrap();
        assert_eq!(result.rows, vec![vec![SqlValue::Bool(true)]]);
    }
    let result = sql
        .execute("SELECT 'Invented/Zone' IN (SELECT name FROM pg_catalog.pg_timezone_names)")
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Bool(false)]]);
    let result = sql.execute("SELECT name, abbrev, utc_offset, is_dst FROM pg_catalog.pg_timezone_names WHERE name='UTC'").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("UTC".into()),
            SqlValue::String("UTC".into()),
            SqlValue::String("00:00:00".into()),
            SqlValue::Bool(false)
        ]]
    );
    let result = sql
        .execute("SELECT utc_offset, is_dst FROM pg_timezone_names WHERE name='Asia/Kathmandu'")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("05:45:00".into()),
            SqlValue::Bool(false)
        ]]
    );
    let result = sql.execute("WITH candidate AS (SELECT 'Europe/Paris' AS timezone) SELECT EXISTS(SELECT 1 FROM pg_timezone_names WHERE name=candidate.timezone) FROM candidate").unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn extended_text_hash_matches_postgres_utf8_vectors() {
    // PostgreSQL 16, UTF-8, little-endian, deterministic default collation.
    let vectors = [
        ("", -6939563903564495251_i64, -5142811166230430645_i64),
        ("tenant:key", 2119148298895563973, -5005703973338175090),
        ("abcdefghijkl", -7556637188122330412, -3669402583748189229),
        (
            "abcdefghijklmnopqrstu",
            7996419909667933074,
            -2291028065551762409,
        ),
        ("雪é", 550074847648507964, 3653323685410488478),
    ];
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for (text, zero, negative) in vectors {
        let result = sql
            .execute(&format!(
                "SELECT hashtextextended('{text}', 0), pg_catalog.hashtextextended('{text}', -1)"
            ))
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Int(zero), SqlValue::Int(negative)]],
            "{text}"
        );
    }
    assert_eq!(
        sql.execute("SELECT hashtextextended(NULL,0), hashtextextended('x',NULL)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null, SqlValue::Null]]
    );
}

#[test]
fn scalar_in_subquery_preserves_null_and_empty_set_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for (query, expected) in [
        (
            "SELECT NULL IN (SELECT 1 WHERE false)",
            SqlValue::Bool(false),
        ),
        (
            "SELECT NULL NOT IN (SELECT 1 WHERE false)",
            SqlValue::Bool(true),
        ),
        ("SELECT NULL IN (SELECT NULL)", SqlValue::Null),
        ("SELECT 1 NOT IN (SELECT NULL)", SqlValue::Null),
        ("SELECT 1 IN (SELECT 1)", SqlValue::Bool(true)),
    ] {
        assert_eq!(
            sql.execute(query).unwrap().rows,
            vec![vec![expected]],
            "{query}"
        );
    }
}

#[test]
fn scalar_hash_support_functions_preserve_postgres_values_and_result_types() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute(
            "SELECT hashint2(-1::smallint), hashint4(-1), pg_catalog.hashint8(-1::bigint),
                    hashint4extended(-1, 0), hashoid('4294967295'::oid),
                    hashfloat4('NaN'::real), hashfloat8extended('-0'::double precision, -9),
                    hashchar('a'::\"char\"), hashname('a'::name), hashint4(NULL)",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(385747274),
            SqlValue::Int(385747274),
            SqlValue::Int(385747274),
            SqlValue::Int(-1888257769727981238),
            SqlValue::Int(385747274),
            SqlValue::Int(-1275764840),
            SqlValue::Int(-9),
            SqlValue::Int(463612535),
            SqlValue::Int(1075015857),
            SqlValue::Null,
        ]]
    );
    let result = sql
        .execute(
            "SELECT hashint4(v)::bit(32) = hashint4extended(v,0)::bit(32),
                    pg_typeof(hashint4(v))::text, pg_typeof(hashint4extended(v,0))::text
             FROM (VALUES (17), (42)) AS inputs(v)",
        )
        .unwrap();
    let expected = vec![
        SqlValue::Bool(true),
        SqlValue::String("integer".into()),
        SqlValue::String("bigint".into()),
    ];
    assert_eq!(result.rows, vec![expected; 2]);
    assert_eq!(
        sql.execute(
            "SELECT oid::int4, pronargs::int4, proisstrict
             FROM pg_proc WHERE proname = 'hashint4extended'"
        )
        .unwrap()
        .rows,
        vec![vec![
            SqlValue::Int(425),
            SqlValue::Int(2),
            SqlValue::Bool(true)
        ]]
    );
    assert_eq!(
        sql.execute("SELECT hashint4(1::bigint)")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
    assert_eq!(
        sql.execute("SELECT hashint4extended(1, 0::double precision)")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
}
