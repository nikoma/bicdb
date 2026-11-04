use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn describe_expands_typed_wildcards_without_executing_volatile_view_projections() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE describe_left(id INTEGER, label TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE describe_right(id INTEGER, amount NUMERIC(10,2))")
            .unwrap();
        session
            .execute("INSERT INTO describe_left VALUES(1,'left')")
            .unwrap();
        session
            .execute("INSERT INTO describe_right VALUES(1,2.5)")
            .unwrap();
        session.execute("CREATE SEQUENCE describe_counter").unwrap();
        session.execute("CREATE VIEW describe_volatile AS SELECT id,nextval('describe_counter') AS counter FROM describe_left").unwrap();
    }
    for sql in [
        "SELECT * FROM describe_left l JOIN describe_right r ON l.id=r.id",
        "SELECT l.*,r.* FROM describe_left l JOIN describe_right r ON l.id=r.id",
        "SELECT * FROM describe_volatile",
        "SELECT v.* FROM describe_volatile v",
    ] {
        let columns = bicdb_sql::infer_query_result_columns_for_describe(&db, sql)
            .unwrap()
            .expect("typed wildcard metadata must be available without execution");
        assert!(columns.iter().all(|(_, kind)| kind.is_some()));
        assert_eq!(
            columns.len(),
            if sql.contains("describe_right") { 4 } else { 2 }
        );
    }
    let result = SqlSession::new(&mut db)
        .execute("SELECT nextval('describe_counter')")
        .unwrap();
    assert_eq!(result.rows[0][0].to_cell(), "1");
}

#[test]
fn wildcard_columns_follow_source_order_while_the_join_keeps_its_cheaper_driver() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE wide_rows(row_key INTEGER PRIMARY KEY,time_value TIMESTAMP,marker VARCHAR(8))").unwrap();
    session
        .execute("CREATE TABLE small_rows(row_key INTEGER,amount NUMERIC(10,2),marker VARCHAR(8))")
        .unwrap();
    for key in 1..=32 {
        session
            .execute(&format!(
                "INSERT INTO wide_rows VALUES({key},TIMESTAMP '2024-02-29 12:34:56.123456','left')"
            ))
            .unwrap();
    }
    session
        .execute("INSERT INTO small_rows VALUES(7,2.50,'right')")
        .unwrap();
    session.execute("ANALYZE TABLE wide_rows").unwrap();
    session.execute("ANALYZE TABLE small_rows").unwrap();
    let sql = "SELECT * FROM wide_rows l JOIN small_rows r ON l.row_key=r.row_key";
    let plan = session.execute(&format!("EXPLAIN {sql}")).unwrap();
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell() == "JoinOrder small_rows -> wide_rows"));
    let result = session.execute(sql).unwrap();
    assert_eq!(
        result.columns,
        vec![
            "row_key",
            "time_value",
            "marker",
            "row_key",
            "amount",
            "marker"
        ]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            "7",
            "2024-02-29 12:34:56.123456",
            "left",
            "7",
            "2.50",
            "right"
        ]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("int4".into()),
            Some("timestamp".into()),
            Some("varchar".into()),
            Some("int4".into()),
            Some("numeric".into()),
            Some("varchar".into())
        ]
    );
    let empty = session.execute(&format!("{sql} WHERE 1=0")).unwrap();
    assert_eq!(empty.columns, result.columns);
    assert_eq!(empty.column_types, result.column_types);
    assert!(empty.rows.is_empty());
    session.execute("CREATE VIEW joined_view(l_key,l_time,l_marker,r_key,r_amount,r_marker) AS SELECT * FROM wide_rows l JOIN small_rows r ON l.row_key=r.row_key").unwrap();
    let view = session.execute("SELECT * FROM joined_view").unwrap();
    assert_eq!(
        view.columns,
        vec!["l_key", "l_time", "l_marker", "r_key", "r_amount", "r_marker"]
    );
    assert_eq!(view.rows, result.rows);
    assert_eq!(view.column_types, result.column_types);
    drop(session);
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    let reopened = SqlSession::new(&mut db)
        .execute("SELECT v.* FROM joined_view v")
        .unwrap();
    assert_eq!(reopened.columns, view.columns);
    assert_eq!(reopened.rows, view.rows);
    assert_eq!(reopened.column_types, view.column_types);
}

#[test]
fn derived_duplicate_labels_keep_separate_slots_in_each_wildcard_and_set_function_path() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT d.* FROM (SELECT 11 AS same,29 AS same) d",
        "SELECT * FROM (SELECT 11 AS same,29 AS same) d",
        "WITH d AS (SELECT 11 AS same,29 AS same) SELECT * FROM d",
    ] {
        let result = session.execute(sql).unwrap();
        assert_eq!(result.columns, vec!["same", "same"], "{sql}");
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Int(11), SqlValue::Int(29)]],
            "{sql}"
        );
    }
    let twice = session
        .execute("SELECT *,* FROM (SELECT 11 AS same,29 AS same) d")
        .unwrap();
    assert_eq!(
        twice.rows,
        vec![vec![
            SqlValue::Int(11),
            SqlValue::Int(29),
            SqlValue::Int(11),
            SqlValue::Int(29)
        ]]
    );
    for sql in [
        "SELECT d.*,unnest(ARRAY[1,2]) AS extra FROM (SELECT 11 AS same,29 AS same) d",
        "SELECT d.*,jsonb_array_elements_text('[\"x\",\"y\"]'::jsonb) AS extra FROM (SELECT 11 AS same,29 AS same) d",
    ] {
        let result = session.execute(sql).unwrap();
        assert_eq!(result.columns, vec!["same", "same", "extra"], "{sql}");
        assert_eq!(result.rows.len(), 2, "{sql}");
        for row in result.rows {
            assert_eq!(&row[..2], &[SqlValue::Int(11),SqlValue::Int(29)], "{sql}");
        }
    }
}

#[test]
fn positional_order_by_refers_to_logical_wildcard_output() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE many_rows(left_key INTEGER)")
        .unwrap();
    session
        .execute("CREATE TABLE one_row(right_key INTEGER)")
        .unwrap();
    for key in 1..=32 {
        session
            .execute(&format!("INSERT INTO many_rows VALUES({key})"))
            .unwrap();
    }
    session.execute("INSERT INTO one_row VALUES(99)").unwrap();
    let result = session
        .execute("SELECT * FROM many_rows l CROSS JOIN one_row r ORDER BY 1 DESC LIMIT 1")
        .unwrap();
    assert_eq!(result.columns, vec!["left_key", "right_key"]);
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(32), SqlValue::Int(99)]]
    );
}

#[test]
fn positional_sort_binds_each_repeated_label_to_its_own_typed_source_slot() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ranked_left(left_key NUMERIC(9),common_value TEXT)")
        .unwrap();
    session
        .execute("CREATE TABLE ranked_right(right_key INTEGER,common_value TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO ranked_left VALUES(2,'left_02'),(10,'left_10')")
        .unwrap();
    session
        .execute("INSERT INTO ranked_right VALUES(99,'right')")
        .unwrap();
    for ordering in ["2 DESC,1 ASC", "4 ASC,1 DESC"] {
        let sql = format!("SELECT d.* FROM (SELECT * FROM ranked_left l CROSS JOIN ranked_right r) d ORDER BY {ordering} LIMIT 1");
        let result = session.execute(&sql).unwrap();
        assert_eq!(
            result.columns,
            vec!["left_key", "common_value", "right_key", "common_value"]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["10", "left_10", "99", "right"],
            "{sql}"
        );
    }
}
