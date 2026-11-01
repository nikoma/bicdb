use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn sibling_updates_do_not_reserve_an_unchanged_parent_reference() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE parents(id INT PRIMARY KEY); CREATE TABLE children(id INT PRIMARY KEY,parent_id INT REFERENCES parents(id),value INT); INSERT INTO parents VALUES(1),(2); INSERT INTO children VALUES(1,1,0),(2,1,0)").unwrap();
    let mut first = SqlSession::new_shared(&db);
    let mut second = SqlSession::new_shared(&db);
    first.execute("BEGIN").unwrap();
    first
        .execute("UPDATE children SET value=10 WHERE id=1")
        .unwrap();
    second.execute("BEGIN").unwrap();
    second
        .execute("UPDATE children SET value=20,parent_id=parent_id WHERE id=2")
        .unwrap();
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();
    assert_eq!(
        second
            .execute("SELECT value FROM children ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10)], vec![SqlValue::Int(20)]]
    );
    drop(first);
    drop(second);
    let mut second = SqlSession::new(&mut db);
    assert!(second
        .execute("UPDATE children SET parent_id=999 WHERE id=2")
        .is_err());
    assert!(second.execute("DELETE FROM parents WHERE id=1").is_err());
    second
        .execute("UPDATE children SET parent_id=2 WHERE id=2")
        .unwrap();
    assert_eq!(
        second
            .execute("SELECT parent_id FROM children WHERE id=2")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn before_update_trigger_cannot_bypass_foreign_key_validation() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE parents(id INT PRIMARY KEY); CREATE TABLE children(id INT PRIMARY KEY,parent_id INT REFERENCES parents(id),value INT); INSERT INTO parents VALUES(1); INSERT INTO children VALUES(1,1,0)").unwrap();
    sql.execute("CREATE FUNCTION redirect_reference() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.parent_id := 999; RETURN NEW; END $$; CREATE TRIGGER redirect_reference BEFORE UPDATE ON children FOR EACH ROW EXECUTE FUNCTION redirect_reference()").unwrap();
    assert!(sql
        .execute("UPDATE children SET value=1 WHERE id=1")
        .is_err());
    assert_eq!(
        sql.execute("SELECT parent_id,value FROM children")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1), SqlValue::Int(0)]]
    );
}
