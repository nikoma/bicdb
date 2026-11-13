use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn setup(sql: &mut SqlSession<'_>) {
    sql.execute("CREATE TABLE legacy_foods(id INT PRIMARY KEY); CREATE TABLE meals(id INT PRIMARY KEY, food INT REFERENCES legacy_foods(id)); INSERT INTO legacy_foods VALUES(1)").unwrap();
}

#[test]
fn parent_rename_keeps_references_with_a_legacy_compatibility_view_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
    let mut sql = SqlSession::new(&mut db);
    setup(&mut sql);
    sql.execute("ALTER TABLE legacy_foods RENAME TO foods; CREATE VIEW legacy_foods AS SELECT * FROM foods; INSERT INTO meals VALUES(1,1)").unwrap();
    assert_eq!(
        sql.execute("DELETE FROM foods WHERE id=1")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    drop(sql);
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("INSERT INTO foods VALUES(2); INSERT INTO meals VALUES(2,2)")
        .unwrap();
    assert_eq!(
        sql.execute("INSERT INTO meals VALUES(3,999)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
}

#[test]
fn transactional_parent_and_child_renames_restore_dependencies_at_savepoint_and_rollback() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
    let mut sql = SqlSession::new(&mut db);
    setup(&mut sql);
    sql.execute("BEGIN; ALTER TABLE legacy_foods RENAME TO foods; SAVEPOINT renamed; ALTER TABLE meals RENAME TO meal_entries; INSERT INTO meal_entries VALUES(1,1)").unwrap();
    sql.execute("ROLLBACK TO SAVEPOINT renamed; INSERT INTO meals VALUES(2,1); ROLLBACK")
        .unwrap();
    sql.execute("INSERT INTO meals VALUES(3,1)").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM meals").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        sql.execute("DELETE FROM legacy_foods WHERE id=1")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
}

#[test]
fn self_referencing_parent_rename_preserves_the_foreign_key() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE tree(id INT PRIMARY KEY,parent INT REFERENCES tree(id)); INSERT INTO tree VALUES(1,NULL); ALTER TABLE tree RENAME TO renamed_tree; INSERT INTO renamed_tree VALUES(2,1)").unwrap();
    assert_eq!(
        sql.execute("INSERT INTO renamed_tree VALUES(3,999)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    assert_eq!(
        sql.execute("DELETE FROM renamed_tree WHERE id=1")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
}

#[test]
fn committed_parent_rename_keeps_deferred_validation_and_cascades() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE parents(id INT PRIMARY KEY); CREATE TABLE children(id INT PRIMARY KEY,parent INT REFERENCES parents(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED); BEGIN; ALTER TABLE parents RENAME TO renamed_parents; COMMIT; BEGIN; INSERT INTO children VALUES(1,1); INSERT INTO renamed_parents VALUES(1); COMMIT").unwrap();
    sql.execute("DELETE FROM renamed_parents WHERE id=1")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM children").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
    sql.execute("BEGIN; INSERT INTO children VALUES(2,999)")
        .unwrap();
    assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23503");
}
