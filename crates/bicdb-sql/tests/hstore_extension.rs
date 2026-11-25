//! The hstore extension: its type and operators exist only while the
//! extension is installed.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

#[test]
fn hstore_type_operators_and_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    assert!(sql.execute("SELECT 'a=>1'::hstore").is_err());
    sql.execute("CREATE EXTENSION hstore; CREATE TABLE attrs(id INT, h hstore)")
        .unwrap();
    sql.execute("INSERT INTO attrs VALUES (1, 'b=>2, a=>1'), (2, 'c=>NULL')")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT h, h -> 'a', h ? 'c', h @> 'a=>1' FROM attrs ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                text("\"a\"=>\"1\", \"b\"=>\"2\""),
                text("1"),
                SqlValue::Bool(false),
                SqlValue::Bool(true)
            ],
            vec![
                text("\"c\"=>NULL"),
                SqlValue::Null,
                SqlValue::Bool(true),
                SqlValue::Bool(false)
            ],
        ]
    );
    assert_eq!(
        sql.execute("DROP EXTENSION hstore").unwrap_err().sqlstate(),
        "2BP01"
    );
    sql.execute("DROP TABLE attrs").unwrap();
    sql.execute("DROP EXTENSION hstore").unwrap();
    assert!(sql.execute("SELECT 'a=>1'::hstore").is_err());
}
