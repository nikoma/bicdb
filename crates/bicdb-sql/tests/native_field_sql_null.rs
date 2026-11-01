use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn sql_null_in_native_field_names_remains_null_across_writes_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(true)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE nullable_fields(id INT PRIMARY KEY,metadata TEXT,payload TEXT); INSERT INTO nullable_fields(id,metadata,payload) VALUES(1,NULL,NULL); INSERT INTO nullable_fields(id) VALUES(2)").unwrap();
        assert_eq!(
            sql.execute("SELECT metadata,payload FROM nullable_fields ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Null, SqlValue::Null]; 2]
        );
        sql.execute(
            "UPDATE nullable_fields SET metadata='{\"plain\":true}',payload='text' WHERE id=1",
        )
        .unwrap();
        assert_eq!(
            sql.execute("SELECT metadata FROM nullable_fields WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("{\"plain\":true}".into())]]
        );
        sql.execute("UPDATE nullable_fields SET metadata=NULL,payload=NULL WHERE id=1")
            .unwrap();
    }
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute("SELECT metadata,payload FROM nullable_fields ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null, SqlValue::Null]; 2]
    );
}
