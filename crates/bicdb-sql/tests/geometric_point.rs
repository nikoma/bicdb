use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_point_is_planar_typed_and_durable() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE point_values (id text PRIMARY KEY, location point NOT NULL)")
            .unwrap();
        session
            .execute(
                "INSERT INTO point_values VALUES
                    ('parenthesized', '(1.5,-2)'),
                    ('bare', '1,2'),
                    ('unbounded', point(1000, -1000)),
                    ('special', '(NaN,Infinity)'),
                    ('hex', '(0x1p2,-0)')",
            )
            .unwrap();

        assert_eq!(
            session
                .execute("SELECT pg_typeof(point(1, 2)), point(1, 2)")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("point".to_string()),
                SqlValue::String("(1,2)".to_string()),
            ]],
        );
        assert_eq!(
            session
                .execute("SELECT id, location FROM point_values ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec!["bare", "(1,2)"],
                vec!["hex", "(4,-0)"],
                vec!["parenthesized", "(1.5,-2)"],
                vec!["special", "(NaN,Infinity)"],
                vec!["unbounded", "(1000,-1000)"],
            ]
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| SqlValue::String(value.to_string()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        );
        assert_eq!(
            session
                .execute("SELECT '(1)'::point")
                .unwrap_err()
                .sqlstate(),
            "22P02",
        );
        assert_eq!(
            session
                .execute("SELECT '(1e999,2)'::point")
                .unwrap_err()
                .sqlstate(),
            "22003",
        );
        assert!(session
            .execute("SELECT ST_Point('NaN'::float8, 1)")
            .is_err());
    }

    let stored = db.get("point_values", "unbounded").unwrap().unwrap();
    assert!(stored.geometry.is_none());
    assert_eq!(
        stored.metadata["location"]["$bicdb_typed"]["pg_type"],
        "point",
    );
    assert_eq!(
        stored.metadata["location"]["$bicdb_typed"]["value"]["type"],
        "geometric",
    );
    assert_eq!(
        stored.metadata["location"]["$bicdb_typed"]["value"]["value"]["kind"],
        "point",
    );

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT location FROM point_values WHERE id = 'unbounded'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("(1000,-1000)".to_string())]],
    );
}
