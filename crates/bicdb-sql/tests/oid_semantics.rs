use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn oid_uses_postgresql_unsigned_input_cast_comparison_and_catalog_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE oid_values (id int4 PRIMARY KEY, value oid NOT NULL)")
        .unwrap();
    session
        .execute(
            "INSERT INTO oid_values VALUES
                (1, '0'),
                (2, '-1'),
                (3, '0x1a'),
                (4, '037777777777')",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT id, value, pg_typeof(value), value::text
             FROM oid_values ORDER BY value, id",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Int(1),
                SqlValue::Int(0),
                SqlValue::String("oid".into()),
                SqlValue::String("0".into()),
            ],
            vec![
                SqlValue::Int(3),
                SqlValue::Int(26),
                SqlValue::String("oid".into()),
                SqlValue::String("26".into()),
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::Int(4_294_967_295),
                SqlValue::String("oid".into()),
                SqlValue::String("4294967295".into()),
            ],
            vec![
                SqlValue::Int(4),
                SqlValue::Int(4_294_967_295),
                SqlValue::String("oid".into()),
                SqlValue::String("4294967295".into()),
            ],
        ]
    );

    assert_eq!(
        session
            .execute(
                "SELECT
                    (-1)::int2::oid,
                    (-1)::int4::oid,
                    4294967295::int8::oid,
                    4294967295::oid::int4,
                    4294967295::oid::int8,
                    4294967295::oid = (-1)::int4,
                    2147483648::oid = (-2147483648)::int4",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(4_294_967_295),
            SqlValue::Int(4_294_967_295),
            SqlValue::Int(4_294_967_295),
            SqlValue::Int(-1),
            SqlValue::Int(4_294_967_295),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT min(value), max(value), 26::oid::regtype::text,
                        'pg_type'::regclass::oid
                 FROM oid_values",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(0),
            SqlValue::Int(4_294_967_295),
            SqlValue::String("oid".into()),
            SqlValue::Int(1247),
        ]]
    );
}

#[test]
fn oid_rejects_out_of_range_malformed_and_undefined_operations() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE oid_rejections (id int4 PRIMARY KEY, value oid);
             INSERT INTO oid_rejections VALUES (1, 1)",
        )
        .unwrap();

    for sql in [
        "SELECT '4294967296'::oid",
        "SELECT '-2147483649'::oid",
        "SELECT (-1)::int8::oid",
        "INSERT INTO oid_rejections VALUES (1, 2)
         ON CONFLICT (id) DO UPDATE SET value = (-1)::int8",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "22003",
            "{sql}"
        );
    }
    for sql in ["SELECT '08'::oid", "SELECT 'pg_type'::oid"] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "22P02",
            "{sql}"
        );
    }
    assert_eq!(
        session
            .execute("SELECT 1.0::numeric::oid")
            .unwrap_err()
            .sqlstate(),
        "42846"
    );
    for sql in [
        "SELECT 1::oid + 2::oid",
        "SELECT sum(value) FROM oid_rejections",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "42883",
            "{sql}"
        );
    }
}
