use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn expected_arrays() -> Vec<SqlValue> {
    vec![
        SqlValue::Json(json!(["(1,2)", "(3,4)"])),
        SqlValue::Json(json!(["{1,2,3}", "{4,5,6}"])),
        SqlValue::Json(json!(["[(1,2),(3,4)]", "[(5,6),(7,8)]"])),
        SqlValue::Json(json!(["(3,4),(1,2)", "(7,8),(5,6)"])),
        SqlValue::Json(json!(["[(1,2),(3,4)]", "((5,6),(7,8))"])),
        SqlValue::Json(json!(["((1,2),(3,4),(5,6))", "((7,8),(9,10),(11,12))"])),
        SqlValue::Json(json!(["<(1,2),3>", "<(4,5),6>"])),
    ]
}

#[test]
fn geometric_arrays_catalog_copy_and_restart_match_postgresql() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                r#"CREATE TABLE geometric_transport (
                    id text PRIMARY KEY,
                    points point[],
                    lines line[],
                    segments lseg[],
                    boxes box[],
                    paths path[],
                    polygons polygon[],
                    circles circle[]
                );
                INSERT INTO geometric_transport VALUES (
                    'sql',
                    '{"(1,2)","(3,4)"}',
                    '{"{1,2,3}","{4,5,6}"}',
                    '{"[(1,2),(3,4)]","[(5,6),(7,8)]"}',
                    '{(1,2),(3,4);(5,6),(7,8)}',
                    '{"[(1,2),(3,4)]","((5,6),(7,8))"}',
                    '{"((1,2),(3,4),(5,6))","((7,8),(9,10),(11,12))"}',
                    '{"<(1,2),3>","<(4,5),6>"}'
                );"#,
            )
            .unwrap();

        let result = session
            .execute(
                "SELECT points, lines, segments, boxes, paths, polygons, circles
                 FROM geometric_transport WHERE id = 'sql'",
            )
            .unwrap();
        assert_eq!(
            result.column_types,
            vec![
                Some("point[]".to_string()),
                Some("line[]".to_string()),
                Some("lseg[]".to_string()),
                Some("box[]".to_string()),
                Some("path[]".to_string()),
                Some("polygon[]".to_string()),
                Some("circle[]".to_string()),
            ]
        );
        assert_eq!(result.rows, vec![expected_arrays()]);

        let columns = vec![
            "id".to_string(),
            "points".to_string(),
            "lines".to_string(),
            "segments".to_string(),
            "boxes".to_string(),
            "paths".to_string(),
            "polygons".to_string(),
            "circles".to_string(),
        ];
        let row = vec![
            Some("copy".to_string()),
            Some(r#"{"(1,2)","(3,4)"}"#.to_string()),
            Some(r#"{"{1,2,3}","{4,5,6}"}"#.to_string()),
            Some(r#"{"[(1,2),(3,4)]","[(5,6),(7,8)]"}"#.to_string()),
            Some("{(1,2),(3,4);(5,6),(7,8)}".to_string()),
            Some(r#"{"[(1,2),(3,4)]","((5,6),(7,8))"}"#.to_string()),
            Some(r#"{"((1,2),(3,4),(5,6))","((7,8),(9,10),(11,12))"}"#.to_string()),
            Some(r#"{"<(1,2),3>","<(4,5),6>"}"#.to_string()),
        ];
        assert_eq!(
            session
                .copy_insert_rows("geometric_transport", &columns, vec![row])
                .unwrap(),
            1
        );

        let catalog = session
            .execute(
                "SELECT oid, typelem, typarray, typdelim
                 FROM pg_type
                 WHERE oid IN (600, 601, 602, 603, 604, 628, 718, 1017, 1018, 1019, 1020, 1027, 629, 719)
                 ORDER BY oid",
            )
            .unwrap();
        assert_eq!(catalog.rows.len(), 14);
        assert_eq!(
            catalog
                .rows
                .iter()
                .find(|row| row[0] == SqlValue::Int(600))
                .unwrap()[1],
            SqlValue::Int(701)
        );
        assert_eq!(
            catalog
                .rows
                .iter()
                .find(|row| row[0] == SqlValue::Int(601))
                .unwrap()[1],
            SqlValue::Int(600)
        );
        assert_eq!(
            catalog
                .rows
                .iter()
                .find(|row| row[0] == SqlValue::Int(603))
                .unwrap()[3],
            SqlValue::String(";".to_string())
        );
        assert_eq!(
            catalog
                .rows
                .iter()
                .find(|row| row[0] == SqlValue::Int(1020))
                .unwrap()[3],
            SqlValue::String(";".to_string())
        );

        let dump_types = session
            .execute(
                "SELECT oid, typname, typelem, typarray,
                        typname[0] = '_' AND typelem != 0
                          AND (SELECT typarray FROM pg_type element WHERE element.oid = pg_type.typelem) = oid AS isarray
                 FROM pg_type
                 WHERE oid IN (600, 601, 602, 603, 604, 628, 718, 1017, 1018, 1019, 1020, 1027, 629, 719)
                 ORDER BY oid",
            )
            .unwrap();
        assert_eq!(dump_types.rows.len(), 14);
        for row in &dump_types.rows {
            let SqlValue::String(name) = &row[1] else {
                panic!("pg_type.typname was not text");
            };
            assert_eq!(
                row[4],
                SqlValue::Bool(name.starts_with('_')),
                "pg_dump array classification for {name}",
            );
        }
        assert_eq!(
            session
                .execute(
                    "SELECT format_type(1017, -1), format_type(1018, -1),
                            format_type(1019, -1), format_type(1020, -1),
                            format_type(1027, -1), format_type(629, -1),
                            format_type(719, -1)",
                )
                .unwrap()
                .rows[0],
            vec![
                SqlValue::String("point[]".to_string()),
                SqlValue::String("lseg[]".to_string()),
                SqlValue::String("path[]".to_string()),
                SqlValue::String("box[]".to_string()),
                SqlValue::String("polygon[]".to_string()),
                SqlValue::String("line[]".to_string()),
                SqlValue::String("circle[]".to_string()),
            ],
        );
    }

    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT points, lines, segments, boxes, paths, polygons, circles
             FROM geometric_transport ORDER BY id",
        )
        .unwrap();
    assert_eq!(result.rows, vec![expected_arrays(), expected_arrays()]);
}
