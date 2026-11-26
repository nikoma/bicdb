//! Table inheritance: children take the parent's columns and constraints,
//! and reads and writes of the parent reach the children's rows.
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

fn ints(values: &[i64]) -> Vec<Vec<SqlValue>> {
    values
        .iter()
        .map(|value| vec![SqlValue::Int(*value)])
        .collect()
}

#[test]
fn parent_reads_and_writes_reach_children_and_survive_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = open(root.path(), mode.clone());
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE cities(name TEXT, population INT CHECK (population >= 0)); CREATE TABLE capitals(state TEXT) INHERITS (cities)")
                .unwrap();
            sql.execute("INSERT INTO cities VALUES ('Springfield', 10); INSERT INTO capitals(name, population, state) VALUES ('Albany', 20, 'NY')")
                .unwrap();
            assert_eq!(
                sql.execute("SELECT population FROM cities ORDER BY population")
                    .unwrap()
                    .rows,
                ints(&[10, 20])
            );
            // The inherited CHECK applies to the child.
            assert_eq!(
                sql.execute("INSERT INTO capitals VALUES ('Bad', -1, 'XX')")
                    .unwrap_err()
                    .sqlstate(),
                "23514"
            );
            sql.execute("UPDATE cities SET population = population + 1")
                .unwrap();
            assert_eq!(
                sql.execute("SELECT population FROM capitals").unwrap().rows,
                ints(&[21])
            );
        }
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            sql.execute("SELECT count(*) FROM cities").unwrap().rows,
            ints(&[2])
        );
        assert_eq!(
            sql.execute("DELETE FROM cities WHERE population = 21")
                .unwrap()
                .command_tag
                .as_deref(),
            Some("DELETE 1")
        );
        assert_eq!(
            sql.execute("SELECT count(*) FROM capitals").unwrap().rows,
            ints(&[0])
        );
    }
}

#[test]
fn ddl_keeps_the_hierarchy_consistent() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path(), StorageMode::EmbeddedMemory);
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE p(a INT); CREATE TABLE c(b INT) INHERITS (p); INSERT INTO c VALUES (1, 2)",
    )
    .unwrap();
    sql.execute("ALTER TABLE p ADD COLUMN z INT DEFAULT 5")
        .unwrap();
    assert_eq!(sql.execute("SELECT z FROM c").unwrap().rows, ints(&[5]));
    for (statement, sqlstate) in [
        ("ALTER TABLE c DROP COLUMN a", "42P16"),
        ("ALTER TABLE c RENAME COLUMN a TO aa", "42P16"),
        ("DROP TABLE p", "2BP01"),
        ("CREATE TABLE bad(a TEXT) INHERITS (p)", "42804"),
    ] {
        assert_eq!(
            sql.execute(statement).unwrap_err().sqlstate(),
            sqlstate,
            "{statement}"
        );
    }
    sql.execute("DROP TABLE c").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM p").unwrap().rows,
        ints(&[0])
    );
    sql.execute("DROP TABLE p").unwrap();
}

#[test]
fn only_no_inherit_and_hierarchy_ddl() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path(), StorageMode::EmbeddedMemory);
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE p(a INT, CONSTRAINT p_pos CHECK (a > 0) NO INHERIT); CREATE TABLE c(b INT) INHERITS (p)")
        .unwrap();
    // The NO INHERIT constraint is not the child's.
    sql.execute("INSERT INTO c VALUES (-1, 0)").unwrap();
    sql.execute("INSERT INTO p VALUES (5)").unwrap();
    assert_eq!(
        sql.execute("SELECT a FROM ONLY p").unwrap().rows,
        ints(&[5])
    );
    sql.execute("UPDATE ONLY p SET a = a + 1").unwrap();
    assert_eq!(
        sql.execute("SELECT a FROM p ORDER BY a").unwrap().rows,
        ints(&[-1, 6])
    );
    sql.execute("DELETE FROM ONLY p").unwrap();
    assert_eq!(
        sql.execute("SELECT a FROM p ORDER BY a").unwrap().rows,
        ints(&[-1])
    );
    // Detach and re-attach the child.
    sql.execute("ALTER TABLE c NO INHERIT p").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM p").unwrap().rows,
        ints(&[0])
    );
    sql.execute("ALTER TABLE c INHERIT p").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM p").unwrap().rows,
        ints(&[1])
    );
    // Renames keep the links; CASCADE drops the descendants.
    sql.execute("ALTER TABLE p RENAME TO pp").unwrap();
    sql.execute("ALTER TABLE c RENAME TO cc").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM pp").unwrap().rows,
        ints(&[1])
    );
    sql.execute("DROP TABLE pp CASCADE").unwrap();
    assert!(sql.execute("SELECT * FROM cc").is_err());
}
