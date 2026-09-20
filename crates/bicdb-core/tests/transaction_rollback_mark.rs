use bicdb_core::BicDb;
use serde_json::json;

#[test]
fn rollback_marks_preserve_earlier_hooks_and_reject_other_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    let mut first = db.begin_transaction().unwrap();
    first.push_deferred_hook(json!({"id": 1}));
    let mark = first.rollback_mark();
    first.push_deferred_hook(json!({"id": 2}));
    first.rollback_to_mark(mark).unwrap();
    first.push_deferred_hook(json!({"id": 3}));
    first.rollback_to_mark(mark).unwrap();
    assert_eq!(first.take_deferred_hooks(), vec![json!({"id": 1})]);

    let mut second = db.begin_transaction().unwrap();
    second.push_deferred_hook(json!({"id": 4}));
    assert!(second.rollback_to_mark(mark).is_err());
    assert_eq!(second.take_deferred_hooks(), vec![json!({"id": 4})]);
}

#[test]
fn read_lock_release_preserves_earlier_locks_and_refuses_write_locks() {
    use bicdb_core::Record;
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", Record::new("1")).unwrap();
    db.insert("rows", Record::new("2")).unwrap();
    let mut first = db.begin_transaction().unwrap();
    assert!(first.try_lock_visible_record(None, "rows", "1").unwrap());
    let mark = first.rollback_mark();
    assert!(first.try_lock_visible_record(None, "rows", "2").unwrap());
    let later_mark = first.rollback_mark();
    first.release_read_locks_since(mark).unwrap();
    assert!(first.release_read_locks_since(later_mark).is_err());
    first.release_read_locks_since(mark).unwrap();
    let second = db.begin_transaction().unwrap();
    assert!(second.release_read_locks_since(mark).is_err());
    assert!(!second.try_lock_visible_record(None, "rows", "1").unwrap());
    assert!(second.try_lock_visible_record(None, "rows", "2").unwrap());
    second.rollback().unwrap();
    first
        .update(
            "rows",
            Record::new("2").with_metadata(json!({"changed":true})),
        )
        .unwrap();
    assert!(first.release_read_locks_since(mark).is_err());
    let probe = db.begin_transaction().unwrap();
    assert!(!probe.try_lock_visible_record(None, "rows", "2").unwrap());
    probe.rollback().unwrap();
    first.rollback().unwrap();
}
