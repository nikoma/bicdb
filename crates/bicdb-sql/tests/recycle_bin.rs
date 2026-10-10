//! Recycle bin: DROP TABLE keeps the table restorable with FLASHBACK TABLE
//! ... TO BEFORE DROP. Every test runs against both storage engines.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn modes() -> Vec<(&'static str, DbConfig)> {
    vec![
        ("embedded", DbConfig::default().with_fsync(false)),
        (
            "paged",
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        ),
    ]
}

fn open(config: DbConfig) -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(directory.path(), config).unwrap();
    (directory, db)
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

fn int(value: &SqlValue) -> i64 {
    match value {
        SqlValue::Int(value) => *value,
        other => panic!("expected int, got {other:?}"),
    }
}

fn text(value: &SqlValue) -> String {
    match value {
        SqlValue::String(value) => value.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn pairs(session: &mut SqlSession<'_>, sql: &str) -> Vec<(i64, String)> {
    rows(session, sql)
        .iter()
        .map(|row| (int(&row[0]), text(&row[1])))
        .collect()
}

fn seed(session: &mut SqlSession<'_>) {
    rows(
        session,
        "CREATE TABLE customers (id SERIAL PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
    );
    rows(session, "CREATE INDEX customers_name ON customers (name)");
    rows(
        session,
        "INSERT INTO customers (email, name) VALUES ('a@x.io', 'Ada'), ('b@x.io', 'Bob')",
    );
}

#[test]
fn drop_and_restore_keeps_everything() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        seed(&mut s);
        rows(&mut s, "CREATE ROLE reader LOGIN NOSUPERUSER");
        rows(&mut s, "GRANT SELECT ON customers TO reader");
        rows(&mut s, "ALTER TABLE customers FLASHBACK ARCHIVE");
        let before = int(&rows(&mut s, "SELECT current_scn()")[0][0]);
        rows(&mut s, "UPDATE customers SET name = 'Ada L.' WHERE id = 1");

        rows(&mut s, "DROP TABLE customers");
        assert!(s.execute("SELECT * FROM customers").is_err(), "{mode}");
        let bin = rows(&mut s, "SHOW RECYCLEBIN");
        assert_eq!(bin.len(), 1, "{mode}");
        assert_eq!(text(&bin[0][0]), "customers", "{mode}");

        rows(&mut s, "FLASHBACK TABLE customers TO BEFORE DROP");
        assert_eq!(
            pairs(&mut s, "SELECT id, name FROM customers ORDER BY id"),
            vec![(1, "Ada L.".to_string()), (2, "Bob".to_string())],
            "{mode}"
        );
        assert!(rows(&mut s, "SHOW RECYCLEBIN").is_empty(), "{mode}");
        // Sequence, secondary index and unique index all came back.
        rows(
            &mut s,
            "INSERT INTO customers (email, name) VALUES ('c@x.io', 'Cy')",
        );
        assert_eq!(
            pairs(&mut s, "SELECT id, name FROM customers WHERE name = 'Cy'"),
            vec![(3, "Cy".to_string())],
            "{mode}: serial continued"
        );
        assert!(
            s.execute("INSERT INTO customers (email, name) VALUES ('a@x.io', 'dup')")
                .is_err(),
            "{mode}: unique email enforced"
        );
        // Flashback history survived the round trip.
        assert_eq!(
            pairs(
                &mut s,
                &format!("SELECT id, name FROM customers AS OF SCN {before} WHERE id = 1")
            ),
            vec![(1, "Ada".to_string())],
            "{mode}"
        );
        // Grants came back.
        let mut reader = SqlSession::new_unprivileged(&mut db, "reader");
        assert_eq!(
            rows(&mut reader, "SELECT count(*) FROM customers")[0][0],
            SqlValue::Int(3),
            "{mode}"
        );
    }
}

#[test]
fn same_name_can_be_reused_and_both_restored() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        seed(&mut s);
        rows(&mut s, "DROP TABLE customers");
        // The name, its unique index and its sequence are free again at once.
        seed(&mut s);
        rows(&mut s, "DELETE FROM customers WHERE id = 1");
        rows(&mut s, "DROP TABLE customers");
        assert_eq!(rows(&mut s, "SHOW RECYCLEBIN").len(), 2, "{mode}");

        // The most recent drop comes back under its own name ...
        rows(&mut s, "FLASHBACK TABLE customers TO BEFORE DROP");
        assert_eq!(
            pairs(&mut s, "SELECT id, name FROM customers ORDER BY id"),
            vec![(2, "Bob".to_string())],
            "{mode}"
        );
        // ... the older one cannot take a name in use, but can be renamed.
        assert!(
            s.execute("FLASHBACK TABLE customers TO BEFORE DROP")
                .is_err(),
            "{mode}"
        );
        let older = text(&rows(&mut s, "SHOW RECYCLEBIN")[0][1]);
        // Its index and sequence names are taken by the restored table too;
        // they get derived names and the table keeps working.
        rows(
            &mut s,
            &format!("FLASHBACK TABLE {older} TO BEFORE DROP RENAME TO customers_2025"),
        );
        assert_eq!(
            pairs(&mut s, "SELECT id, name FROM customers_2025 ORDER BY id"),
            vec![(1, "Ada".to_string()), (2, "Bob".to_string())],
            "{mode}"
        );
        rows(
            &mut s,
            "INSERT INTO customers_2025 (email, name) VALUES ('z@x.io', 'Zed')",
        );
        assert_eq!(
            pairs(
                &mut s,
                "SELECT id, name FROM customers_2025 WHERE name = 'Zed'"
            ),
            vec![(3, "Zed".to_string())],
            "{mode}: the renamed sequence continues where it left off"
        );
        assert!(
            s.execute("INSERT INTO customers_2025 (email, name) VALUES ('a@x.io', 'dup')")
                .is_err(),
            "{mode}: unique index still enforced under its derived name"
        );
        assert_eq!(
            pairs(&mut s, "SELECT id, name FROM customers ORDER BY id"),
            vec![(2, "Bob".to_string())],
            "{mode}: the other table is untouched"
        );
        assert!(rows(&mut s, "SHOW RECYCLEBIN").is_empty(), "{mode}");
    }
}

#[test]
fn drop_inside_a_transaction_rolls_back() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        seed(&mut s);
        rows(&mut s, "BEGIN");
        rows(&mut s, "DROP TABLE customers");
        rows(&mut s, "ROLLBACK");
        assert_eq!(
            pairs(&mut s, "SELECT id, name FROM customers ORDER BY id"),
            vec![(1, "Ada".to_string()), (2, "Bob".to_string())],
            "{mode}"
        );
        assert!(rows(&mut s, "SHOW RECYCLEBIN").is_empty(), "{mode}");
        rows(&mut s, "BEGIN");
        rows(&mut s, "DROP TABLE customers");
        rows(&mut s, "COMMIT");
        assert_eq!(rows(&mut s, "SHOW RECYCLEBIN").len(), 1, "{mode}");
    }
}

#[test]
fn purge_and_disable() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        seed(&mut s);
        rows(&mut s, "DROP TABLE customers");
        rows(&mut s, "PURGE TABLE customers");
        assert!(rows(&mut s, "SHOW RECYCLEBIN").is_empty(), "{mode}");
        assert!(
            s.execute("FLASHBACK TABLE customers TO BEFORE DROP")
                .is_err(),
            "{mode}"
        );

        seed(&mut s);
        rows(&mut s, "DROP TABLE customers PURGE");
        assert!(rows(&mut s, "SHOW RECYCLEBIN").is_empty(), "{mode}");

        seed(&mut s);
        rows(&mut s, "SET bicdb.recyclebin = off");
        rows(&mut s, "DROP TABLE customers");
        assert!(rows(&mut s, "SHOW RECYCLEBIN").is_empty(), "{mode}");
        rows(&mut s, "SET bicdb.recyclebin = on");

        seed(&mut s);
        rows(&mut s, "DROP TABLE customers");
        rows(&mut s, "PURGE RECYCLEBIN");
        assert!(rows(&mut s, "SHOW RECYCLEBIN").is_empty(), "{mode}");
        // Purged storage is really gone: the name and data start fresh.
        seed(&mut s);
        assert_eq!(
            rows(&mut s, "SELECT count(*) FROM customers")[0][0],
            SqlValue::Int(2),
            "{mode}"
        );
    }
}

#[test]
fn recycle_bin_is_owner_scoped_and_hidden_from_catalogs() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        {
            let mut s = SqlSession::new(&mut db);
            seed(&mut s);
            rows(&mut s, "CREATE ROLE mallory LOGIN NOSUPERUSER");
            rows(&mut s, "DROP TABLE customers");
            let listed = rows(
                &mut s,
                "SELECT count(*) FROM pg_tables WHERE tablename LIKE '__bicdb_bin%' \
                 OR tablename LIKE '__bicdb_recyclebin%'",
            );
            assert_eq!(listed[0][0], SqlValue::Int(0), "{mode}");
        }
        let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
        assert!(
            rows(&mut mallory, "SHOW RECYCLEBIN").is_empty(),
            "{mode}: other owners' entries are invisible"
        );
        assert!(
            mallory
                .execute("FLASHBACK TABLE customers TO BEFORE DROP")
                .is_err(),
            "{mode}"
        );
        assert!(mallory.execute("PURGE TABLE customers").is_err(), "{mode}");
        let mut owner = SqlSession::new(&mut db);
        rows(&mut owner, "FLASHBACK TABLE customers TO BEFORE DROP");
        assert_eq!(
            rows(&mut owner, "SELECT count(*) FROM customers")[0][0],
            SqlValue::Int(2),
            "{mode}"
        );
    }
}

#[test]
fn bin_survives_restart() {
    for (mode, config) in modes() {
        let directory = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(directory.path(), config.clone()).unwrap();
            let mut s = SqlSession::new(&mut db);
            seed(&mut s);
            rows(&mut s, "DROP TABLE customers");
            seed(&mut s);
            rows(
                &mut s,
                "INSERT INTO customers (email, name) VALUES ('n@x.io', 'New')",
            );
        }
        let mut db = BicDb::open_with_config(directory.path(), config).unwrap();
        let mut s = SqlSession::new(&mut db);
        assert_eq!(
            rows(&mut s, "SELECT count(*) FROM customers")[0][0],
            SqlValue::Int(3),
            "{mode}"
        );
        rows(&mut s, "ALTER TABLE customers RENAME TO customers_new");
        rows(&mut s, "FLASHBACK TABLE customers TO BEFORE DROP");
        assert_eq!(
            pairs(&mut s, "SELECT id, name FROM customers ORDER BY id"),
            vec![(1, "Ada".to_string()), (2, "Bob".to_string())],
            "{mode}"
        );
        assert_eq!(
            rows(&mut s, "SELECT count(*) FROM customers_new")[0][0],
            SqlValue::Int(3),
            "{mode}"
        );
    }
}
