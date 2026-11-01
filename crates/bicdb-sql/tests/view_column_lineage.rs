use bicdb_core::BicDb;
use bicdb_sql::{infer_query_result_types, SqlSession, SqlValue};

#[test]
fn joined_view_columns_keep_their_own_types_through_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE first_source(id INTEGER PRIMARY KEY, code VARCHAR(3) NOT NULL)")
            .unwrap();
        sql.execute("CREATE TABLE second_source(id INTEGER PRIMARY KEY, code NUMERIC(8,2), stamp TIMESTAMP)")
            .unwrap();
        sql.execute("INSERT INTO first_source VALUES (1,'one'),(2,'two')")
            .unwrap();
        sql.execute("INSERT INTO second_source VALUES (1,12.34,TIMESTAMP '2020-02-29 12:34:56')")
            .unwrap();
        sql.execute("CREATE VIEW joined_view AS SELECT a.id, a.code AS label, b.code AS amount, b.stamp, a.id + 1 AS successor FROM first_source a LEFT JOIN second_source b ON a.id=b.id")
            .unwrap();
        sql.execute("CREATE VIEW \"UpperView\" AS SELECT a.id, b.code AS \"AMOUNT\" FROM first_source a JOIN second_source b ON a.id=b.id")
            .unwrap();
    }
    let mut db = BicDb::open(root.path()).unwrap();
    let types = vec!["int4", "varchar", "numeric", "timestamp", "int4"]
        .into_iter()
        .map(|name| Some(name.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        infer_query_result_types(&db, "SELECT * FROM joined_view WHERE false").unwrap(),
        Some(types.clone())
    );
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("SELECT * FROM joined_view ORDER BY id")
        .unwrap();
    assert_eq!(result.column_types, types, "columns: {:?}", result.columns);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[1][2], SqlValue::Null);
    let upper = sql.execute("SELECT \"AMOUNT\" FROM \"UpperView\"").unwrap();
    assert_eq!(upper.column_types, vec![Some("numeric".into())]);
    let attributes = sql.execute("SELECT a.attname, a.atttypid, a.atttypmod, a.attnotnull FROM pg_attribute a JOIN pg_class c ON a.attrelid=c.oid WHERE c.relname='joined_view' AND a.attnum>0 ORDER BY a.attnum").unwrap();
    assert_eq!(attributes.rows[1][2], SqlValue::Int(7)); // varchar(3) + header
    assert_eq!(attributes.rows[2][1], SqlValue::Int(1700));
    assert_eq!(attributes.rows[2][2], SqlValue::Int((8 << 16) + 2 + 4));
    assert!(attributes
        .rows
        .iter()
        .all(|row| row[3] == SqlValue::Bool(false)));
}
