use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn seeded() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE parents(id INT PRIMARY KEY, patient INT, UNIQUE(id,patient)); CREATE TABLE children(id INT PRIMARY KEY, parent_id INT, patient INT, CONSTRAINT clinical_fk FOREIGN KEY(parent_id,patient) REFERENCES parents(id,patient) DEFERRABLE INITIALLY IMMEDIATE); INSERT INTO parents VALUES(1,10); INSERT INTO children VALUES(1,1,10)").unwrap();
    (root, db)
}

#[test]
fn deferred_child_before_parent_is_validated_and_persists_across_sessions() {
    let (root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; INSERT INTO children VALUES(2,2,20); INSERT INTO parents VALUES(2,20); COMMIT").unwrap();
    drop(sql);
    drop(db);
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT patient FROM children WHERE id=2")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(20)]]
    );
}

#[test]
fn composite_patient_move_allows_both_parent_first_and_child_first() {
    for parent_first in [true, false] {
        let (_root, mut db) = seeded();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED")
            .unwrap();
        let parent = "UPDATE parents SET patient=20 WHERE id=1";
        let child = "UPDATE children SET patient=20 WHERE id=1";
        for command in if parent_first {
            [parent, child]
        } else {
            [child, parent]
        } {
            sql.execute(command).unwrap();
        }
        sql.execute("SET CONSTRAINTS clinical_fk IMMEDIATE; COMMIT")
            .unwrap();
        assert_eq!(
            sql.execute(
                "SELECT c.patient,p.patient FROM children c JOIN parents p ON c.parent_id=p.id"
            )
            .unwrap()
            .rows,
            vec![vec![SqlValue::Int(20), SqlValue::Int(20)]]
        );
    }
}

#[test]
fn unresolved_deferred_reference_refuses_commit_and_rolls_back() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; UPDATE children SET patient=99 WHERE id=1",
    )
    .unwrap();
    assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23503");
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT patient FROM children").unwrap().rows,
        vec![vec![SqlValue::Int(10)]]
    );
}

#[test]
fn switching_to_immediate_checks_pending_rows_and_restores_on_savepoint_rollback() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; SAVEPOINT before_bad; UPDATE children SET patient=99 WHERE id=1").unwrap();
    assert_eq!(
        sql.execute("SET CONSTRAINTS clinical_fk IMMEDIATE")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    sql.execute("ROLLBACK TO SAVEPOINT before_bad; UPDATE children SET patient=20 WHERE id=1; UPDATE parents SET patient=20 WHERE id=1; SET CONSTRAINTS clinical_fk IMMEDIATE; COMMIT").unwrap();
}

#[test]
fn rollback_restores_constraint_mode_and_discards_rolled_back_checks() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SAVEPOINT immediate; SET CONSTRAINTS clinical_fk DEFERRED; INSERT INTO children VALUES(2,999,999); ROLLBACK TO SAVEPOINT immediate").unwrap();
    assert_eq!(
        sql.execute("INSERT INTO children VALUES(3,999,999)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    sql.execute("ROLLBACK TO SAVEPOINT immediate; COMMIT")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM children").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn deferred_checks_follow_the_final_row_and_allow_deleted_transient_rows() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; INSERT INTO children VALUES(2,999,999); DELETE FROM children WHERE id=2; UPDATE children SET patient=99 WHERE id=1; UPDATE children SET patient=10 WHERE id=1; COMMIT").unwrap();
}

#[test]
fn alter_constraint_in_do_block_is_catalogued_transactional_and_owner_checked() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("ALTER TABLE children ALTER CONSTRAINT clinical_fk NOT DEFERRABLE; BEGIN; DO $$ BEGIN ALTER TABLE children ALTER CONSTRAINT clinical_fk DEFERRABLE INITIALLY DEFERRED; END $$").unwrap();
    assert_eq!(
        sql.execute(
            "SELECT condeferrable,condeferred FROM pg_constraint WHERE conname='clinical_fk'"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]]
    );
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute(
            "SELECT condeferrable,condeferred FROM pg_constraint WHERE conname='clinical_fk'"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Bool(false), SqlValue::Bool(false)]]
    );
    sql.execute("CREATE ROLE visitor LOGIN NOSUPERUSER")
        .unwrap();
    drop(sql);
    assert_eq!(
        SqlSession::new_unprivileged(&mut db, "visitor")
            .execute("ALTER TABLE children ALTER CONSTRAINT clinical_fk DEFERRABLE")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
}

#[test]
fn initially_deferred_enforces_commit_and_restrict_remains_immediate() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE p(id INT PRIMARY KEY); CREATE TABLE c(id INT PRIMARY KEY,p INT CONSTRAINT deferred_fk REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED); BEGIN; INSERT INTO c VALUES(1,1); INSERT INTO p VALUES(1); COMMIT").unwrap();
    sql.execute("ALTER TABLE c ADD CONSTRAINT restrict_fk FOREIGN KEY(p) REFERENCES p(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED; BEGIN").unwrap();
    assert_eq!(
        sql.execute("DELETE FROM p WHERE id=1")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    sql.execute("ROLLBACK").unwrap();
}

#[test]
fn core_commit_cannot_bypass_pending_sql_validation() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; INSERT INTO children VALUES(2,999,999)",
    )
    .unwrap();
    let pending = sql.take_pending_transaction().unwrap();
    assert!(pending.commit().is_err());
    assert_eq!(
        sql.execute("SELECT count(*) FROM children").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn mode_is_transaction_local_and_does_not_leak_to_another_session() {
    let (_root, db) = seeded();
    let mut first = SqlSession::new_shared(&db);
    let mut second = SqlSession::new_shared(&db);
    first
        .execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED")
        .unwrap();
    assert_eq!(
        second
            .execute("INSERT INTO children VALUES(2,999,999)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    first
        .execute("INSERT INTO children VALUES(3,3,30); INSERT INTO parents VALUES(3,30); COMMIT")
        .unwrap();
    assert_eq!(
        first
            .execute("INSERT INTO children VALUES(4,999,999)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
}

#[test]
fn parent_delete_is_deferred_but_unresolved_deletion_fails() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SET CONSTRAINTS ALL DEFERRED; DELETE FROM parents WHERE id=1")
        .unwrap();
    assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23503");
    sql.execute("BEGIN; SET CONSTRAINTS public.clinical_fk DEFERRED; DELETE FROM parents WHERE id=1; DELETE FROM children WHERE id=1; COMMIT").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM parents").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn successful_immediate_validation_does_not_destroy_savepoint_check_history() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; UPDATE children SET patient=20 WHERE id=1; SAVEPOINT before_parent; UPDATE parents SET patient=20 WHERE id=1; SET CONSTRAINTS clinical_fk IMMEDIATE; ROLLBACK TO SAVEPOINT before_parent").unwrap();
    assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23503");
}

#[test]
fn not_valid_constraint_does_not_revalidate_untouched_legacy_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE p(id INT PRIMARY KEY); CREATE TABLE c(id INT PRIMARY KEY,p INT); INSERT INTO p VALUES(1); INSERT INTO c VALUES(1,999); ALTER TABLE c ADD CONSTRAINT legacy_fk FOREIGN KEY(p) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED NOT VALID; BEGIN; INSERT INTO c VALUES(2,1); COMMIT").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM c").unwrap().rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn deferred_child_still_reserves_the_parent_key_against_concurrent_deletion() {
    let (_root, db) = seeded();
    let mut first = SqlSession::new_shared(&db);
    let mut second = SqlSession::new_shared(&db);
    first
        .execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; INSERT INTO children VALUES(2,1,10)")
        .unwrap();
    assert!(second.execute("DELETE FROM parents WHERE id=1").is_err());
    first.execute("COMMIT").unwrap();
    assert_eq!(
        second.execute("SELECT count(*) FROM parents").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn autocommit_defers_until_statement_end_and_never_persists_an_orphan() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE tree(id INT PRIMARY KEY, parent INT CONSTRAINT tree_fk REFERENCES tree(id) DEFERRABLE INITIALLY DEFERRED); INSERT INTO tree VALUES(1,2),(2,NULL)").unwrap();
    assert_eq!(
        sql.execute("INSERT INTO tree VALUES(3,999)")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    assert_eq!(
        sql.execute("SELECT count(*) FROM tree").unwrap().rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn pending_checks_cannot_be_bypassed_by_renaming_the_constraint_or_table() {
    for rename in [
        "ALTER TABLE children RENAME CONSTRAINT clinical_fk TO renamed_fk",
        "ALTER TABLE children RENAME TO renamed_children",
    ] {
        let (_root, mut db) = seeded();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED")
            .unwrap();
        if rename.contains("RENAME TO") {
            // Queue from the parent side so the child relation itself has no
            // buffered write when its physical collection is renamed.
            sql.execute("DELETE FROM parents WHERE id=1").unwrap();
        } else {
            sql.execute("UPDATE children SET patient=99 WHERE id=1")
                .unwrap();
        }
        sql.execute(rename).unwrap();
        assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23503");
        assert_eq!(
            sql.execute("SELECT patient FROM children").unwrap().rows,
            vec![vec![SqlValue::Int(10)]]
        );
    }
}

#[test]
fn a_changed_row_primary_key_keeps_its_pending_foreign_key_validation() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; INSERT INTO children VALUES(2,999,999); UPDATE children SET id=3 WHERE id=2").unwrap();
    assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23503");
}

#[test]
fn constraint_mode_follows_renames_but_a_new_constraint_has_its_own_identity() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN; SET CONSTRAINTS clinical_fk DEFERRED; ALTER TABLE children RENAME CONSTRAINT clinical_fk TO renamed_fk; UPDATE children SET patient=20 WHERE id=1; UPDATE parents SET patient=20 WHERE id=1; SET CONSTRAINTS renamed_fk IMMEDIATE; COMMIT").unwrap();
}
