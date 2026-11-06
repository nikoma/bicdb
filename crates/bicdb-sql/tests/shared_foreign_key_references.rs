use bicdb_core::BicDb;
use bicdb_sql::{oracle, SqlSession, SqlValue};

const PROFILES: [&str; 2] = [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE];

fn seeded(profile: &str, timing: &str) -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    SqlSession::new(&mut db).execute(&format!("CREATE TABLE parents(id INTEGER PRIMARY KEY,label INTEGER,code INTEGER UNIQUE); CREATE TABLE children(id INTEGER PRIMARY KEY,pid INTEGER,CONSTRAINT child_fk FOREIGN KEY(pid) REFERENCES parents(id) {timing}); CREATE TABLE unique_children(id INTEGER PRIMARY KEY,code INTEGER REFERENCES parents(code)); INSERT INTO parents VALUES(1,0,10)")).unwrap();
    (root, db)
}

#[test]
fn independent_child_inserts_share_parent_keys_for_immediate_and_deferred_constraints() {
    for profile in PROFILES {
        for timing in ["", "DEFERRABLE INITIALLY DEFERRED"] {
            let (_root, db) = seeded(profile, timing);
            let mut one = SqlSession::new_shared(&db);
            let mut two = SqlSession::new_shared(&db);
            one.execute("BEGIN; INSERT INTO children VALUES(1,1)")
                .unwrap();
            two.execute("BEGIN; INSERT INTO children VALUES(2,1)")
                .unwrap();
            one.execute("COMMIT").unwrap();
            two.execute("COMMIT").unwrap();
            assert_eq!(
                two.execute("SELECT COUNT(*) FROM children").unwrap().rows,
                vec![vec![SqlValue::Int(2)]],
                "{profile} {timing}"
            );
        }
    }
}

#[test]
fn parent_key_mutations_still_conflict_with_pending_child_references() {
    for profile in PROFILES {
        for mutation in [
            "DELETE FROM parents WHERE id=1",
            "UPDATE parents SET id=2 WHERE id=1",
        ] {
            let (_root, db) = seeded(profile, "DEFERRABLE INITIALLY DEFERRED");
            let mut child = SqlSession::new_shared(&db);
            let mut parent = SqlSession::new_shared(&db);
            child
                .execute("BEGIN; INSERT INTO children VALUES(1,1)")
                .unwrap();
            parent.execute("BEGIN").unwrap();
            assert_eq!(
                parent.execute(mutation).unwrap_err().sqlstate(),
                "40001",
                "{profile} {mutation}"
            );
            parent.execute("ROLLBACK").unwrap();
            child.execute("ROLLBACK").unwrap();
            parent.execute(mutation).unwrap();
            assert_eq!(
                child
                    .execute("INSERT INTO children VALUES(2,1)")
                    .unwrap_err()
                    .sqlstate(),
                "23503"
            );
        }
    }
}

#[test]
fn nonkey_parent_updates_and_unique_parent_references_do_not_exclude_child_writers() {
    for profile in PROFILES {
        let (_root, db) = seeded(profile, "");
        let mut one = SqlSession::new_shared(&db);
        let mut two = SqlSession::new_shared(&db);
        let mut parent = SqlSession::new_shared(&db);
        one.execute("BEGIN; INSERT INTO unique_children VALUES(1,10)")
            .unwrap();
        two.execute("BEGIN; INSERT INTO unique_children VALUES(2,10)")
            .unwrap();
        parent
            .execute("UPDATE parents SET label=7 WHERE id=1")
            .unwrap();
        one.execute("COMMIT").unwrap();
        two.execute("COMMIT").unwrap();
        assert_eq!(
            parent
                .execute("DELETE FROM parents WHERE id=1")
                .unwrap_err()
                .sqlstate(),
            "23503",
            "{profile}"
        );
    }
}

#[test]
fn savepoint_rollback_releases_only_new_reference_reservations() {
    for profile in PROFILES {
        let (_root, db) = seeded(profile, "DEFERRABLE INITIALLY DEFERRED");
        let mut child = SqlSession::new_shared(&db);
        let mut parent = SqlSession::new_shared(&db);
        child.execute("BEGIN; SAVEPOINT before_child; INSERT INTO children VALUES(1,1); ROLLBACK TO SAVEPOINT before_child").unwrap();
        parent.execute("DELETE FROM parents WHERE id=1").unwrap();
        child.execute("COMMIT").unwrap();
        assert_eq!(
            parent
                .execute("SELECT COUNT(*) FROM children")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(0)]],
            "{profile}"
        );
    }
}

#[test]
fn deferred_point_checks_follow_final_child_identity_and_discard_deleted_rows() {
    for profile in PROFILES {
        let (_root, db) = seeded(profile, "DEFERRABLE INITIALLY DEFERRED");
        let mut child = SqlSession::new_shared(&db);
        child.execute("BEGIN; INSERT INTO children VALUES(1,999); DELETE FROM children WHERE id=1; INSERT INTO children VALUES(2,1); UPDATE children SET id=3 WHERE id=2; COMMIT").unwrap();
        assert_eq!(
            child.execute("SELECT id,pid FROM children").unwrap().rows,
            vec![vec![SqlValue::Int(3), SqlValue::Int(1)]],
            "{profile}"
        );
        child
            .execute("BEGIN; UPDATE children SET pid=999 WHERE id=3")
            .unwrap();
        assert_eq!(
            child.execute("COMMIT").unwrap_err().sqlstate(),
            "23503",
            "{profile}"
        );
        child.execute("ROLLBACK").unwrap();
        assert_eq!(
            child.execute("SELECT pid FROM children").unwrap().rows,
            vec![vec![SqlValue::Int(1)]],
            "{profile}"
        );
    }
}

#[test]
fn deferred_parent_key_moves_and_child_followup_preserve_references() {
    for profile in PROFILES {
        let (_root, db) = seeded(profile, "DEFERRABLE INITIALLY DEFERRED");
        let mut sql = SqlSession::new_shared(&db);
        sql.execute("BEGIN; INSERT INTO children VALUES(1,1); UPDATE parents SET id=2 WHERE id=1; UPDATE children SET pid=2 WHERE id=1; COMMIT").unwrap();
        assert_eq!(
            sql.execute("SELECT p.id,c.pid FROM parents p JOIN children c ON p.id=c.pid")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2), SqlValue::Int(2)]],
            "{profile}"
        );
    }
}

#[test]
fn committed_references_reject_parent_key_moves_without_losing_rows() {
    for profile in PROFILES {
        let (_root, mut db) = seeded(profile, "");
        SqlSession::new(&mut db)
            .execute("INSERT INTO children VALUES(1,1)")
            .unwrap();
        let mut sql = SqlSession::new_shared(&db);
        assert_eq!(
            sql.execute("UPDATE parents SET id=2 WHERE id=1")
                .unwrap_err()
                .sqlstate(),
            "23503",
            "{profile}"
        );
        assert_eq!(
            sql.execute("SELECT p.id,c.pid FROM parents p JOIN children c ON p.id=c.pid")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1), SqlValue::Int(1)]],
            "{profile}"
        );
    }
}

#[test]
fn parent_key_cascade_updates_nonkey_child_references() {
    let (_root, mut db) = seeded(oracle::POSTGRES_PROFILE, "");
    SqlSession::new(&mut db).execute("ALTER TABLE children DROP CONSTRAINT child_fk; ALTER TABLE children ADD CONSTRAINT child_fk FOREIGN KEY(pid) REFERENCES parents(id) ON UPDATE CASCADE; INSERT INTO children VALUES(1,1)").unwrap();
    let mut sql = SqlSession::new_shared(&db);
    sql.execute("UPDATE parents SET id=2 WHERE id=1").unwrap();
    assert_eq!(
        sql.execute("SELECT p.id,c.pid FROM parents p JOIN children c ON p.id=c.pid")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2), SqlValue::Int(2)]]
    );
}

#[test]
fn no_action_observes_the_final_key_set_with_profile_specific_uniqueness() {
    for profile in PROFILES {
        let (_root, mut db) = seeded(profile, "");
        SqlSession::new(&mut db)
            .execute("INSERT INTO parents VALUES(2,8,20); INSERT INTO children VALUES(1,1)")
            .unwrap();
        let mut sql = SqlSession::new_shared(&db);
        let moved = sql.execute("UPDATE parents SET id=3-id");
        let label = if profile == oracle::POSTGRES_PROFILE {
            assert_eq!(moved.unwrap_err().sqlstate(), "23505");
            0
        } else {
            moved.unwrap();
            8
        };
        assert_eq!(
            sql.execute("SELECT label FROM parents WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(label)]],
            "{profile}"
        );
        assert_eq!(
            sql.execute("SELECT pid FROM children").unwrap().rows,
            vec![vec![SqlValue::Int(1)]],
            "{profile}"
        );
    }
}
