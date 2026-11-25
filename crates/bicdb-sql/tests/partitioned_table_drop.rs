//! DROP TABLE of a partitioned table drops its partitions, and a database left with a
//! partitioned table's catalog entry but no storage (by the earlier, broken DROP) opens again.
use bicdb_core::BicDb;
use bicdb_sql::{ensure_primary_key_indexes, ensure_unique_constraint_indexes, SqlSession};

fn run(db: &mut BicDb, sql: &str) {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn cells(db: &mut BicDb, sql: &str) -> Vec<Vec<String>> {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

/// What `bicdb serve` does on open before it accepts connections.
fn reopen(db: BicDb, path: &std::path::Path) -> BicDb {
    db.close().unwrap();
    let mut db = BicDb::open(path).unwrap();
    ensure_primary_key_indexes(&mut db).unwrap();
    ensure_unique_constraint_indexes(&mut db).unwrap();
    db
}

fn create_tree(db: &mut BicDb) {
    run(
        db,
        "CREATE TABLE p (a int PRIMARY KEY, b text) PARTITION BY LIST (a)",
    );
    run(db, "CREATE TABLE p1 PARTITION OF p FOR VALUES IN (1)");
    run(db, "CREATE TABLE p2 PARTITION OF p FOR VALUES IN (2, 3)");
    run(db, "INSERT INTO p VALUES (1, 'one'), (2, 'two')");
}

#[test]
fn dropping_a_partitioned_table_drops_its_partitions_and_the_database_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(&mut db, "CREATE TABLE keep (k int PRIMARY KEY)");
    run(&mut db, "INSERT INTO keep VALUES (7)");
    create_tree(&mut db);
    run(&mut db, "DROP TABLE p");
    for table in ["p", "p1", "p2"] {
        let error = SqlSession::new(&mut db)
            .execute(&format!("SELECT * FROM {table}"))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "42P01", "{table}: {error}");
    }
    let mut db = reopen(db, dir.path());
    assert_eq!(cells(&mut db, "SELECT k FROM keep"), [["7"]]);
    // The names are free again.
    create_tree(&mut db);
    assert_eq!(
        cells(&mut db, "SELECT a, b FROM p ORDER BY a"),
        [["1", "one"], ["2", "two"]]
    );
}

#[test]
fn rolling_back_a_partitioned_drop_keeps_the_whole_tree() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    create_tree(&mut db);
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        session.execute("DROP TABLE p").unwrap();
        session.execute("ROLLBACK").unwrap();
    }
    let mut db = reopen(db, dir.path());
    assert_eq!(
        cells(&mut db, "SELECT a, b FROM p ORDER BY a"),
        [["1", "one"], ["2", "two"]]
    );
    assert_eq!(cells(&mut db, "SELECT a FROM p2"), [["2"]]);
}

#[test]
fn a_partitioned_parent_without_storage_no_longer_keeps_the_database_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(&mut db, "CREATE TABLE keep (k int PRIMARY KEY)");
    run(&mut db, "INSERT INTO keep VALUES (7)");
    create_tree(&mut db);
    // The state the old DROP TABLE p committed: the parent's storage is gone, its catalog entry
    // (and its partitions) remain.
    assert!(db.drop_collection("p").unwrap());
    let mut db = reopen(db, dir.path());
    // Nothing else lost: the partitions keep their rows, unrelated tables keep theirs.
    assert_eq!(cells(&mut db, "SELECT k FROM keep"), [["7"]]);
    assert_eq!(cells(&mut db, "SELECT a, b FROM p1"), [["1", "one"]]);
    assert_eq!(cells(&mut db, "SELECT a, b FROM p2"), [["2", "two"]]);
    // And the interrupted DROP can now be completed.
    run(&mut db, "DROP TABLE p");
    let mut db = reopen(db, dir.path());
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT * FROM p1")
            .unwrap_err()
            .sqlstate(),
        "42P01"
    );
    assert_eq!(cells(&mut db, "SELECT k FROM keep"), [["7"]]);
}

#[test]
fn a_partitioned_table_with_a_unique_constraint_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE pu (i int UNIQUE) PARTITION BY LIST (i)",
    );
    run(
        &mut db,
        "CREATE TABLE pu1 PARTITION OF pu FOR VALUES IN (1)",
    );
    run(
        &mut db,
        "CREATE TABLE pu2 PARTITION OF pu FOR VALUES IN (2)",
    );
    run(&mut db, "INSERT INTO pu VALUES (1), (2)");
    // The partitions carry the parent's constraint name; opening used to refuse that as an
    // index definition conflict, so the database never opened again.
    let mut db = reopen(db, dir.path());
    assert_eq!(
        cells(&mut db, "SELECT i FROM pu ORDER BY i"),
        [["1"], ["2"]]
    );
    let error = SqlSession::new(&mut db)
        .execute("INSERT INTO pu VALUES (1)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23505", "{error}");
}
