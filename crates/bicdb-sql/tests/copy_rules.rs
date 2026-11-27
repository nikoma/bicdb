//! COPY FROM rules through the embedded API: ON_ERROR IGNORE with verbose
//! notices, DEFAULT markers and WHERE.
use bicdb_core::BicDb;
use bicdb_sql::{CopyFromRules, SqlSession, SqlValue};

#[test]
fn copy_rules_skip_mark_and_filter() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE t(a int, b text DEFAULT 'dflt')")
        .unwrap();
    let columns = vec!["a".to_string(), "b".to_string()];
    let rows = vec![
        vec![Some("1".into()), Some("x".into())],
        vec![Some("bad".into()), Some("y".into())],
        vec![Some("3".into()), None],
        vec![Some("4".into()), Some("z".into())],
    ];
    let rules = CopyFromRules {
        on_error_ignore: true,
        verbose: true,
        where_clause: Some("a <> 4".into()),
        ..CopyFromRules::default()
    };
    let (outcome, notices) = sql
        .copy_insert_rows_with_rules(
            "t",
            &columns,
            rows,
            &[1, 2, 3, 4],
            &[vec![], vec![], vec![1], vec![]],
            &rules,
        )
        .unwrap();
    assert_eq!((outcome.inserted, outcome.skipped), (2, 1));
    assert_eq!(
        notices
            .iter()
            .map(|notice| notice.message.as_str())
            .collect::<Vec<_>>(),
        vec!["skipping row due to data type incompatibility at line 2 for column \"a\": \"bad\""]
    );
    assert_eq!(
        sql.execute("SELECT a, b FROM t ORDER BY a").unwrap().rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::String("x".into())],
            vec![SqlValue::Int(3), SqlValue::String("dflt".into())],
        ]
    );
}
