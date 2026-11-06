use bicdb_core::{BicDb, DbConfig, Record};

fn database() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    (root, db)
}

#[test]
fn write_suffix_truncation_keeps_reservations_for_surviving_writes() {
    let (_root, db) = database();
    let mut owner = db.begin_transaction().unwrap();
    owner.reserve_integrity_key_shared("parent", "1").unwrap();
    owner.reserve_integrity_key("unique", "kept").unwrap();
    owner.insert("rows", Record::new("kept")).unwrap();
    let kept = owner.write_len();
    owner.insert("rows", Record::new("removed")).unwrap();
    owner.truncate_writes(kept).unwrap();
    // Repeating a write-only truncation must not erase surviving protection.
    owner.truncate_writes(kept).unwrap();
    let mut contender = db.begin_transaction().unwrap();
    assert!(contender.reserve_integrity_key("parent", "1").is_err());
    assert!(contender.reserve_integrity_key("unique", "kept").is_err());
    contender.insert("rows", Record::new("removed")).unwrap();
    contender.rollback().unwrap();
    owner.commit().unwrap();
    assert!(db.get("rows", "kept").unwrap().is_some());
    assert!(db.get("rows", "removed").unwrap().is_none());
    let released = db.begin_transaction().unwrap();
    released.reserve_integrity_key("parent", "1").unwrap();
    released.reserve_integrity_key("unique", "kept").unwrap();
    released.rollback().unwrap();
}

#[test]
fn removing_all_writes_releases_both_reservation_modes_and_record_locks() {
    let (_root, db) = database();
    let mut owner = db.begin_transaction().unwrap();
    owner.reserve_integrity_key_shared("parent", "1").unwrap();
    owner.reserve_integrity_key("unique", "removed").unwrap();
    owner.insert("rows", Record::new("removed")).unwrap();
    owner.truncate_writes(0).unwrap();
    let mut contender = db.begin_transaction().unwrap();
    contender.reserve_integrity_key("parent", "1").unwrap();
    contender
        .reserve_integrity_key("unique", "removed")
        .unwrap();
    contender.insert("rows", Record::new("removed")).unwrap();
    owner.rollback().unwrap();
    // The former owner's cleanup must not remove the new owner's reservations.
    let probe = db.begin_transaction().unwrap();
    assert!(probe.reserve_integrity_key_shared("parent", "1").is_err());
    assert!(probe.reserve_integrity_key("unique", "removed").is_err());
    probe.rollback().unwrap();
    contender.commit().unwrap();
    assert!(db.get("rows", "removed").unwrap().is_some());
}

#[test]
fn read_lock_cleanup_refuses_integrity_reservations_atomically() {
    let (_root, db) = database();
    let owner = db.begin_transaction().unwrap();
    let mark = owner.rollback_mark();
    owner.reserve_integrity_key_shared("parent", "1").unwrap();
    owner.reserve_integrity_key("unique", "1").unwrap();
    assert!(owner.release_read_locks_since(mark).is_err());
    let contender = db.begin_transaction().unwrap();
    assert!(contender.reserve_integrity_key("parent", "1").is_err());
    assert!(contender.reserve_integrity_key("unique", "1").is_err());
    owner.rollback().unwrap();
    contender.reserve_integrity_key("parent", "1").unwrap();
    contender.reserve_integrity_key("unique", "1").unwrap();
    contender.rollback().unwrap();
}
