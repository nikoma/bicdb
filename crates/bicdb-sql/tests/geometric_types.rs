use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_geometric_types_are_canonical_typed_and_durable() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE geometric_values (
                    id text PRIMARY KEY,
                    line_value line NOT NULL,
                    segment_value lseg NOT NULL,
                    box_value box NOT NULL,
                    open_path path NOT NULL,
                    closed_path path NOT NULL,
                    polygon_value polygon NOT NULL,
                    circle_value circle NOT NULL
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO geometric_values VALUES (
                    'shapes',
                    '[(1,2),(3,4)]',
                    '1,2,3,4',
                    '1,2,3,4',
                    '[(1,2),(3,4)]',
                    '1,2,3,4',
                    '1,2,3,4,5,6',
                    '1,2,Infinity'
                )",
            )
            .unwrap();

        assert_eq!(
            session
                .execute(
                    "SELECT line_value, segment_value, box_value, open_path,
                            closed_path, polygon_value, circle_value
                     FROM geometric_values WHERE id = 'shapes'",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("{1,-1,1}".to_string()),
                SqlValue::String("[(1,2),(3,4)]".to_string()),
                SqlValue::String("(3,4),(1,2)".to_string()),
                SqlValue::String("[(1,2),(3,4)]".to_string()),
                SqlValue::String("((1,2),(3,4))".to_string()),
                SqlValue::String("((1,2),(3,4),(5,6))".to_string()),
                SqlValue::String("<(1,2),Infinity>".to_string()),
            ]],
        );
        assert_eq!(
            session
                .execute(
                    "SELECT '{NaN,Infinity,-0}'::line,
                            '[(NaN,Infinity),(-Infinity,-0)]'::lseg,
                            '(NaN,Infinity),(-Infinity,-0)'::box,
                            '((NaN,Infinity),(-Infinity,-0),(0x1p2,3))'::polygon,
                            '<(NaN,Infinity),NaN>'::circle",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("{NaN,Infinity,-0}".to_string()),
                SqlValue::String("[(NaN,Infinity),(-Infinity,-0)]".to_string()),
                SqlValue::String("(NaN,Infinity),(-Infinity,-0)".to_string()),
                SqlValue::String("((NaN,Infinity),(-Infinity,-0),(4,3))".to_string()),
                SqlValue::String("<(NaN,Infinity),NaN>".to_string()),
            ]],
        );
        for sql in [
            "SELECT '{0,0,0}'::line",
            "SELECT '[(1,2),(1,2)]'::line",
            "SELECT '[]'::path",
            "SELECT '()'::polygon",
            "SELECT '<(1,2),-1>'::circle",
        ] {
            assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22P02");
        }
        assert_eq!(
            session
                .execute("SELECT '[(1e999,2),(3,4)]'::lseg")
                .unwrap_err()
                .sqlstate(),
            "22003",
        );
    }

    let stored = db.get("geometric_values", "shapes").unwrap().unwrap();
    assert!(stored.geometry.is_none());
    for (column, pg_type) in [
        ("line_value", "line"),
        ("segment_value", "lseg"),
        ("box_value", "box"),
        ("open_path", "path"),
        ("closed_path", "path"),
        ("polygon_value", "polygon"),
        ("circle_value", "circle"),
    ] {
        assert_eq!(stored.metadata[column]["$bicdb_typed"]["pg_type"], pg_type);
        assert_eq!(
            stored.metadata[column]["$bicdb_typed"]["value"]["type"],
            "geometric",
        );
    }

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT line_value, segment_value, box_value, open_path,
                        closed_path, polygon_value, circle_value
                 FROM geometric_values WHERE id = 'shapes'",
            )
            .unwrap()
            .rows[0],
        vec![
            SqlValue::String("{1,-1,1}".to_string()),
            SqlValue::String("[(1,2),(3,4)]".to_string()),
            SqlValue::String("(3,4),(1,2)".to_string()),
            SqlValue::String("[(1,2),(3,4)]".to_string()),
            SqlValue::String("((1,2),(3,4))".to_string()),
            SqlValue::String("((1,2),(3,4),(5,6))".to_string()),
            SqlValue::String("<(1,2),Infinity>".to_string()),
        ],
    );
}
