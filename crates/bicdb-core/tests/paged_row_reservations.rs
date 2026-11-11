use bicdb_core::{BicDb, BicDbError, DbConfig, Record, StorageMode};
use serde_json::json;

fn configuration(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(false)
}

fn row(value: i64) -> Record {
    Record::new("counter").with_metadata(json!({"value": value}))
}

#[test]
fn cold_baselines_remain_present_for_every_read_committed_reservation() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for operation in [
            "record",
            "stored",
            "sequence",
            "delete",
            "try-lock",
            "wait-lock",
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), configuration(mode.clone())).unwrap();
            db.create_collection("counters").unwrap();
            db.insert("counters", row(7)).unwrap();
            db.close().unwrap();
            let db = BicDb::open_with_config(root.path(), configuration(mode.clone())).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.set_snapshot_isolation(false, 0).unwrap();
            assert_eq!(
                tx.get("counters", "counter").unwrap().unwrap().metadata["value"],
                7
            );
            match operation {
                "record" => assert!(
                    tx.read_committed_update_record("counters", "counter")
                        .unwrap()
                        .is_none(),
                    "{mode} {operation}"
                ),
                "stored" => assert!(
                    tx.read_committed_update_stored("counters", "counter")
                        .unwrap()
                        .is_none(),
                    "{mode} {operation}"
                ),
                "sequence" => assert!(
                    tx.read_committed_update_stored_reservation("counters", "counter")
                        .unwrap()
                        .is_none(),
                    "{mode} {operation}"
                ),
                "delete" => assert!(
                    tx.read_committed_delete_record("counters", "counter")
                        .unwrap()
                        .is_none(),
                    "{mode} {operation}"
                ),
                "try-lock" => assert!(
                    tx.try_lock_visible_record(None, "counters", "counter")
                        .unwrap(),
                    "{mode} {operation}"
                ),
                "wait-lock" => assert!(
                    tx.lock_visible_record_waiting(None, "counters", "counter")
                        .unwrap(),
                    "{mode} {operation}"
                ),
                _ => unreachable!(),
            }
            tx.update("counters", row(8)).unwrap();
            tx.commit().unwrap();
            assert_eq!(
                db.get("counters", "counter").unwrap().unwrap().metadata["value"],
                8
            );
            db.close().unwrap();
            let db = BicDb::open_with_config(root.path(), configuration(mode.clone())).unwrap();
            assert_eq!(
                db.get("counters", "counter").unwrap().unwrap().metadata["value"],
                8
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn paged_reservations_distinguish_missing_updated_and_deleted_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), configuration(StorageMode::ServerPaged)).unwrap();
    db.create_collection("counters").unwrap();
    db.insert("counters", row(1)).unwrap();
    db.close().unwrap();
    let db = BicDb::open_with_config(root.path(), configuration(StorageMode::ServerPaged)).unwrap();
    let mut reader = db.begin_transaction().unwrap();
    reader.set_snapshot_isolation(false, 0).unwrap();
    assert_eq!(
        reader.get("counters", "counter").unwrap().unwrap().metadata["value"],
        1
    );
    let mut writer = db.begin_transaction().unwrap();
    writer.update("counters", row(2)).unwrap();
    writer.commit().unwrap();
    let (current, sequence) = reader
        .read_committed_update_stored_reservation("counters", "counter")
        .unwrap()
        .unwrap();
    assert_eq!(current.unwrap().to_record().unwrap().metadata["value"], 2);
    assert!(sequence > 0);
    reader.rollback().unwrap();
    let mut reader = db.begin_transaction().unwrap();
    reader.set_snapshot_isolation(false, 0).unwrap();
    let mut writer = db.begin_transaction().unwrap();
    writer.delete("counters", "counter").unwrap();
    writer.commit().unwrap();
    let (current, sequence) = reader
        .read_committed_update_stored_reservation("counters", "counter")
        .unwrap()
        .unwrap();
    assert!(current.is_none());
    assert!(sequence > 0);
    reader.rollback().unwrap();
    let mut reader = db.begin_transaction().unwrap();
    reader.set_snapshot_isolation(false, 0).unwrap();
    assert!(matches!(
        reader
            .read_committed_update_stored_reservation("counters", "absent")
            .unwrap(),
        Some((None, 0))
    ));
    reader.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn paged_locks_still_reject_a_changed_fixed_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), configuration(StorageMode::ServerPaged)).unwrap();
    db.create_collection("counters").unwrap();
    db.insert("counters", row(1)).unwrap();
    db.close().unwrap();
    let db = BicDb::open_with_config(root.path(), configuration(StorageMode::ServerPaged)).unwrap();
    let mut reader = db.begin_transaction().unwrap();
    reader.set_snapshot_isolation(true, 0).unwrap();
    assert_eq!(
        reader.get("counters", "counter").unwrap().unwrap().metadata["value"],
        1
    );
    let mut writer = db.begin_transaction().unwrap();
    writer.update("counters", row(2)).unwrap();
    writer.commit().unwrap();
    assert!(matches!(
        reader.try_lock_visible_record(None, "counters", "counter"),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert!(reader
        .read_committed_update_stored_reservation("counters", "counter")
        .unwrap()
        .is_none());
    reader.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn refreshed_lock_replaces_an_earlier_unlocked_observation() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for nonblocking in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), configuration(mode.clone())).unwrap();
            db.create_collection("counters").unwrap();
            db.insert("counters", row(1)).unwrap();
            let mut reader = db.begin_transaction().unwrap();
            reader.set_snapshot_isolation(false, 0).unwrap();
            assert_eq!(
                reader.get("counters", "counter").unwrap().unwrap().metadata["value"],
                1
            );
            let mut writer = db.begin_transaction().unwrap();
            writer.update("counters", row(2)).unwrap();
            writer.commit().unwrap();
            reader.refresh_read_committed_snapshot().unwrap();
            let locked = if nonblocking {
                reader.try_lock_visible_record(None, "counters", "counter")
            } else {
                reader.lock_visible_record_waiting(None, "counters", "counter")
            }
            .unwrap();
            assert!(locked, "{mode} nonblocking={nonblocking}");
            let current = reader.get("counters", "counter").unwrap().unwrap();
            assert_eq!(current.metadata["value"], 2);
            reader
                .update(
                    "counters",
                    row(current.metadata["value"].as_i64().unwrap() + 1),
                )
                .unwrap();
            reader.commit().unwrap();
            assert_eq!(
                db.get("counters", "counter").unwrap().unwrap().metadata["value"],
                3
            );
            db.close().unwrap();
            let db = BicDb::open_with_config(root.path(), configuration(mode.clone())).unwrap();
            assert_eq!(
                db.get("counters", "counter").unwrap().unwrap().metadata["value"],
                3
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn refresh_and_late_lock_cannot_rebase_an_already_buffered_stale_write() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for late_lock in [None, Some(false), Some(true)] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), configuration(mode.clone())).unwrap();
            db.create_collection("counters").unwrap();
            db.insert("counters", row(1)).unwrap();
            let mut reader = db.begin_transaction().unwrap();
            reader.set_snapshot_isolation(false, 0).unwrap();
            assert_eq!(
                reader.get("counters", "counter").unwrap().unwrap().metadata["value"],
                1
            );
            let mut writer = db.begin_transaction().unwrap();
            writer.update("counters", row(2)).unwrap();
            writer.commit().unwrap();
            reader.refresh_read_committed_snapshot().unwrap();
            // A normal re-read keeps native optimistic read dependencies.
            assert_eq!(
                reader.get("counters", "counter").unwrap().unwrap().metadata["value"],
                2
            );
            reader.update("counters", row(99)).unwrap();
            if let Some(nonblocking) = late_lock {
                assert!(if nonblocking {
                    reader.try_lock_visible_record(None, "counters", "counter")
                } else {
                    reader.lock_visible_record_waiting(None, "counters", "counter")
                }
                .unwrap());
            }
            assert!(
                matches!(reader.commit(), Err(BicDbError::TransactionConflict(_))),
                "{mode} {late_lock:?}"
            );
            assert_eq!(
                db.get("counters", "counter").unwrap().unwrap().metadata["value"],
                2
            );
            db.close().unwrap();
        }
    }
}
