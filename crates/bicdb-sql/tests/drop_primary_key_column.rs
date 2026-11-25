//! `ALTER TABLE ... DROP COLUMN` on a primary-key column drops the key with it.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn open(root: &std::path::Path, mode: StorageMode) -> BicDb {
    BicDb::open_with_config(
        root,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap()
}

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

#[test]
fn dropping_a_key_column_rekeys_rows_and_survives_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = open(root.path(), mode.clone());
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE k(id INT PRIMARY KEY, v TEXT); INSERT INTO k VALUES (1,'a'),(2,'b'); ALTER TABLE k DROP COLUMN id")
                .unwrap();
            // Duplicates are allowed once the key is gone.
            sql.execute("INSERT INTO k VALUES ('a')").unwrap();
            assert_eq!(
                sql.execute("SELECT v FROM k ORDER BY v").unwrap().rows,
                vec![vec![text("a")], vec![text("a")], vec![text("b")]]
            );
            assert_eq!(
                sql.execute("SELECT count(*) FROM pg_constraint WHERE conrelid='k'::regclass AND contype='p'")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(0)]]
            );
        }
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("UPDATE k SET v='c' WHERE v='b'").unwrap();
        assert_eq!(
            sql.execute("SELECT v FROM k ORDER BY v").unwrap().rows,
            vec![vec![text("a")], vec![text("a")], vec![text("c")]]
        );
        // A new key can be added afterwards.
        sql.execute("DELETE FROM k WHERE v='a'").unwrap();
        sql.execute("ALTER TABLE k ADD PRIMARY KEY (v)").unwrap();
        assert_eq!(
            sql.execute("INSERT INTO k VALUES ('c')")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
    }
}

#[test]
fn a_referenced_key_column_cannot_be_dropped() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path(), StorageMode::EmbeddedMemory);
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE parent(id INT PRIMARY KEY); CREATE TABLE child(p INT REFERENCES parent)",
    )
    .unwrap();
    assert_eq!(
        sql.execute("ALTER TABLE parent DROP COLUMN id")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
}
