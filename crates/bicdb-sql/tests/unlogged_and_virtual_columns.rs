//! UNLOGGED tables and VIRTUAL generated columns.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

#[test]
fn unlogged_tables_report_their_persistence_and_keep_their_rows() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE UNLOGGED TABLE cache_rows(id INT PRIMARY KEY, v TEXT); INSERT INTO cache_rows VALUES (1,'a')")
            .unwrap();
        assert_eq!(
            sql.execute("SELECT relpersistence FROM pg_class WHERE relname='cache_rows'")
                .unwrap()
                .rows,
            vec![vec![text("u")]]
        );
        sql.execute("ALTER TABLE cache_rows SET LOGGED").unwrap();
        assert_eq!(
            sql.execute("SELECT relpersistence FROM pg_class WHERE relname='cache_rows'")
                .unwrap()
                .rows,
            vec![vec![text("p")]]
        );
        sql.execute("ALTER TABLE cache_rows SET UNLOGGED").unwrap();
    }
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute("SELECT v, (SELECT relpersistence FROM pg_class WHERE relname='cache_rows') FROM cache_rows")
            .unwrap()
            .rows,
        vec![vec![text("a"), text("u")]]
    );
}

#[test]
fn virtual_generated_columns_compute_and_report_v() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE gen(a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL, c INT GENERATED ALWAYS AS (a + 1), d INT GENERATED ALWAYS AS (a - 1) STORED, note TEXT DEFAULT 'GENERATED ALWAYS AS (0) VIRTUAL')")
        .unwrap();
    sql.execute("INSERT INTO gen(a) VALUES (3); UPDATE gen SET a = 5")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT a, b, c, d, note FROM gen")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(5),
            SqlValue::Int(10),
            SqlValue::Int(6),
            SqlValue::Int(4),
            text("GENERATED ALWAYS AS (0) VIRTUAL")
        ]]
    );
    assert_eq!(
        sql.execute("SELECT attname, attgenerated FROM pg_attribute WHERE attrelid='gen'::regclass AND attnum > 0 ORDER BY attnum")
            .unwrap()
            .rows,
        vec![
            vec![text("a"), text("")],
            vec![text("b"), text("v")],
            vec![text("c"), text("v")],
            vec![text("d"), text("s")],
            vec![text("note"), text("")],
        ]
    );
    assert_eq!(
        sql.execute("CREATE INDEX gen_b ON gen (b)")
            .unwrap_err()
            .sqlstate(),
        "0A000"
    );
    sql.execute("ALTER TABLE gen ADD COLUMN e INT GENERATED ALWAYS AS (a * 10) VIRTUAL")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT e FROM gen").unwrap().rows,
        vec![vec![SqlValue::Int(50)]]
    );
}
