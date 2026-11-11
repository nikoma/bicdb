//! Catalog records need finer reservations than whole metadata collections.
use bicdb_core::{
    BicDb, BicDbError, CatalogScopeToken, DbConfig, Record, StorageMode, Transaction,
};
use serde_json::json;

fn reserve(owner: &Transaction, ids: &[&str]) -> bicdb_core::Result<CatalogScopeToken> {
    let records = ids
        .iter()
        .map(|id| ("catalog".to_string(), (*id).to_string()))
        .collect::<Vec<_>>();
    owner.reserve_catalog_resources(&[], &records)
}

fn value(id: &str, number: i64) -> Record {
    Record::new(id).with_metadata(json!({"value":number}))
}

fn database(path: &std::path::Path, mode: StorageMode) -> BicDb {
    let mut db = BicDb::open_with_config(
        path,
        DbConfig::default().with_fsync(true).with_storage_mode(mode),
    )
    .unwrap();
    db.create_collection("catalog").unwrap();
    db.insert("catalog", value("first", 0)).unwrap();
    db.insert("catalog", value("second", 0)).unwrap();
    db
}

#[test]
fn a_catalog_record_reservation_leaves_an_unrelated_record_readable() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let owner = db.begin_transaction().unwrap();
        let token = reserve(&owner, &["first"]).unwrap();
        assert!(matches!(
            db.check_catalog_read_access("catalog", Some("first")),
            Err(BicDbError::TransactionConflict(_))
        ));
        db.check_catalog_read_access("catalog", Some("second"))
            .expect("a reservation must not fence an unrelated metadata record");
        assert!(matches!(
            db.check_catalog_read_access("catalog", None),
            Err(BicDbError::TransactionConflict(_))
        ));
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}

#[test]
fn internal_catalog_update_cannot_escape_its_declared_record() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let owner = db.begin_transaction().unwrap();
        let token = reserve(&owner, &["first"]).unwrap();
        let mut internal = {
            let _access = db.enter_catalog_scope(&token).unwrap();
            db.begin_transaction().unwrap()
        };
        let error = internal
            .update("catalog", value("second", 1))
            .expect_err("a metadata collection name must not authorize an undeclared record");
        assert!(matches!(error, BicDbError::TransactionConflict(_)));
        assert_eq!(internal.write_len(), 0);
        assert_eq!(internal.lock_len(), 0);
        internal.rollback().unwrap();
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}

#[test]
fn internal_catalog_batch_refuses_all_effects_before_acquiring_any_row_lock() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let owner = db.begin_transaction().unwrap();
        let token = reserve(&owner, &["first"]).unwrap();
        let mut internal = {
            let _access = db.enter_catalog_scope(&token).unwrap();
            db.begin_transaction().unwrap()
        };
        let error = internal
            .batch_insert("catalog", vec![value("first", 1), value("second", 1)])
            .expect_err(
                "validate every declared metadata effect before buffering or locking any row",
            );
        assert!(matches!(error, BicDbError::TransactionConflict(_)));
        assert_eq!(internal.write_len(), 0);
        assert_eq!(internal.lock_len(), 0);
        internal.rollback().unwrap();
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}

#[test]
fn an_observation_of_another_catalog_record_does_not_prevent_reservation() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let reader = db.begin_transaction().unwrap();
        {
            let _read = reader.enter_catalog_read_access().unwrap();
            db.check_catalog_read_access("catalog", Some("second"))
                .unwrap();
        }
        let owner = db.begin_transaction().unwrap();
        let token = reserve(&owner, &["first"])
            .expect("an unrelated metadata observation must not reserve the whole catalog");
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
        reader.rollback().unwrap();
    }
}

#[test]
fn disjoint_catalog_owners_can_publish_their_own_metadata_without_crossing_scopes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let one = db.begin_transaction().unwrap();
        let first = reserve(&one, &["first"]).unwrap();
        let two = db.begin_transaction().unwrap();
        assert!(matches!(
            reserve(&two, &["first"]),
            Err(BicDbError::TransactionConflict(_))
        ));
        let second = reserve(&two, &["second"])
            .expect("disjoint catalog resources must admit independent owners");
        {
            let _access = db.enter_catalog_scope(&first).unwrap();
            let mut internal = db.begin_transaction().unwrap();
            internal.update("catalog", value("first", 1)).unwrap();
            internal.commit().unwrap();
        }
        {
            let _access = db.enter_catalog_scope(&second).unwrap();
            let mut internal = db.begin_transaction().unwrap();
            internal.update("catalog", value("second", 2)).unwrap();
            internal.commit().unwrap();
        }
        one.release_catalog_scope(&first).unwrap();
        two.release_catalog_scope(&second).unwrap();
        one.rollback().unwrap();
        two.rollback().unwrap();
        assert_eq!(
            db.get("catalog", "first").unwrap().unwrap().metadata["value"],
            1
        );
        assert_eq!(
            db.get("catalog", "second").unwrap().unwrap().metadata["value"],
            2
        );
    }
}

#[test]
fn a_pending_internal_record_write_leaves_other_record_reservations_available() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), mode);
        let one = db.begin_transaction().unwrap();
        let first = reserve(&one, &["first"]).unwrap();
        let mut child = {
            let _access = db.enter_catalog_scope(&first).unwrap();
            db.begin_transaction().unwrap()
        };
        child.update("catalog", value("first", 1)).unwrap();
        assert_eq!(child.write_len(), 1);
        let two = db.begin_transaction().unwrap();
        let second = reserve(&two, &["second"])
            .expect("a declared single-record internal write must not observe the whole catalog");
        assert!(matches!(
            reserve(&two, &["first"]),
            Err(BicDbError::TransactionConflict(_))
        ));
        child.commit().unwrap();
        one.release_catalog_scope(&first).unwrap();
        two.release_catalog_scope(&second).unwrap();
        one.rollback().unwrap();
        two.rollback().unwrap();
        assert_eq!(
            db.get("catalog", "first").unwrap().unwrap().metadata["value"],
            1
        );
    }
}
