//! Runtime native admission, separate from SQL catalog visibility/recovery.
use bicdb_core::{BicDb, BicDbError, DbConfig, Record, StorageMode};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default().with_fsync(true).with_storage_mode(mode)
}

fn names() -> Vec<String> {
    vec!["source".into(), "renamed".into()]
}

#[test]
fn scoped_native_rename_keeps_unrelated_ack_and_blocks_later_access_to_both_names() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
        db.create_collection("source").unwrap();
        db.create_collection("unrelated").unwrap();
        let mut owner = db.begin_transaction().unwrap();
        owner.insert("source", Record::new("owned")).unwrap();
        let mut other = db.begin_transaction().unwrap();
        other
            .insert("unrelated", Record::new("acknowledged"))
            .unwrap();
        let already_begun = db.begin_transaction().unwrap();
        let scope = owner.reserve_catalog_collections(&names()).unwrap();
        db.rename_collection_preserving_pending_writes(&mut owner, "source", "renamed")
            .unwrap();
        for reader in [&already_begun, &db.begin_transaction().unwrap()] {
            for name in names() {
                let result = reader.get(&name, "owned");
                assert!(
                    matches!(result, Err(BicDbError::TransactionConflict(_))),
                    "{mode} {name}: {result:?}"
                );
                assert!(reader
                    .try_lock_visible_record(None, &name, "owned")
                    .is_err());
            }
        }
        let mut later_writer = db.begin_transaction().unwrap();
        for name in names() {
            let result = later_writer.insert(&name, Record::new("forbidden"));
            assert!(
                matches!(result, Err(BicDbError::TransactionConflict(_))),
                "{mode} {name}: {result:?}"
            );
        }
        assert_eq!(later_writer.write_len(), 0);
        assert_eq!(later_writer.lock_len(), 0);
        later_writer.rollback().unwrap();
        assert!(owner.get("renamed", "owned").unwrap().is_some());
        other.commit().unwrap();
        // A statement snapshot refresh must not release ownership or the
        // active-observation registration used to admit future scopes.
        owner.refresh_read_committed_snapshot().unwrap();
        assert!(already_begun.get("renamed", "owned").is_err());
        assert!(already_begun.release_catalog_scope(&scope).is_err());
        owner.commit().unwrap();
        already_begun.rollback().unwrap();
        let reader = db.begin_transaction().unwrap();
        assert!(reader.get("renamed", "owned").unwrap().is_some());
        reader.rollback().unwrap();
        db.close().unwrap();
        let db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        assert!(db.get("renamed", "owned").unwrap().is_some());
        assert!(db.get("unrelated", "acknowledged").unwrap().is_some());
        db.close().unwrap();
    }
}

#[test]
fn existing_reads_and_retained_read_locks_refuse_scope_before_any_catalog_change() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for lock in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            db.create_collection("source").unwrap();
            db.insert("source", Record::new("seed")).unwrap();
            let owner = db.begin_transaction().unwrap();
            let reader = db.begin_transaction().unwrap();
            assert!(reader.get("source", "seed").unwrap().is_some());
            if lock {
                assert!(reader
                    .try_lock_visible_record(None, "source", "seed")
                    .unwrap());
            }
            assert!(owner.reserve_catalog_collections(&names()).is_err());
            assert!(db.has_collection("source"));
            assert!(!db.has_collection("renamed"));
            reader.rollback().unwrap();
            owner.reserve_catalog_collections(&names()).unwrap();
            owner.rollback().unwrap();
            db.close().unwrap();
        }
    }
}

#[test]
fn rollback_drop_and_exact_release_restore_access_without_foreign_token_authority() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
        db.create_collection("source").unwrap();
        let foreign_root = tempfile::tempdir().unwrap();
        let foreign_db = BicDb::open_with_config(foreign_root.path(), config(mode)).unwrap();
        let foreign = foreign_db.begin_transaction().unwrap();
        for outcome in ["release", "rollback", "drop"] {
            let owner = db.begin_transaction().unwrap();
            let scope = owner.reserve_catalog_collections(&names()).unwrap();
            assert!(foreign.release_catalog_scope(&scope).is_err());
            let reader = db.begin_transaction().unwrap();
            assert!(reader.get("source", "missing").is_err());
            match outcome {
                "release" => {
                    owner.release_catalog_scope(&scope).unwrap();
                    assert!(owner.release_catalog_scope(&scope).is_err());
                    owner.rollback().unwrap();
                }
                "rollback" => owner.rollback().unwrap(),
                "drop" => drop(owner),
                _ => unreachable!(),
            }
            assert!(reader.get("source", "missing").unwrap().is_none());
            reader.rollback().unwrap();
        }
        foreign.rollback().unwrap();
        foreign_db.close().unwrap();
        db.close().unwrap();
    }
}

#[test]
fn first_observation_and_scope_acquisition_have_one_atomic_winner() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("source").unwrap();
        db.insert("source", Record::new("seed")).unwrap();
        for _ in 0..32 {
            let owner = db.begin_transaction().unwrap();
            let reader = db.begin_transaction().unwrap();
            let barrier = std::sync::Barrier::new(2);
            let ((owner, reserved), (reader, read)) = std::thread::scope(|threads| {
                let barrier = &barrier;
                let writer = threads.spawn(move || {
                    barrier.wait();
                    let reserved = owner.reserve_catalog_collections(&names());
                    (owner, reserved)
                });
                let observer = threads.spawn(move || {
                    barrier.wait();
                    let read = reader.get("source", "seed");
                    (reader, read)
                });
                (writer.join().unwrap(), observer.join().unwrap())
            });
            assert_ne!(
                reserved.is_ok(),
                read.is_ok(),
                "both sides crossed native admission"
            );
            reader.rollback().unwrap();
            owner.rollback().unwrap();
        }
        db.close().unwrap();
    }
}
