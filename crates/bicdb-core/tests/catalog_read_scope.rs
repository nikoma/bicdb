//! Read-only catalog identity does not delegate trusted mutation authority.
use bicdb_core::{BicDb, BicDbError, DbConfig, Record, StorageMode};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default().with_fsync(true).with_storage_mode(mode)
}

fn refused(result: bicdb_core::Result<()>) {
    assert!(
        matches!(result, Err(BicDbError::TransactionConflict(_))),
        "{result:?}"
    );
}

#[test]
fn owner_reads_keep_their_identity_without_delegating_internal_writes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        db.create_collection("business").unwrap();
        db.insert("catalog", Record::new("metadata")).unwrap();
        let owner = db.begin_transaction().unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        refused(db.check_catalog_read_access("catalog", Some("metadata")));
        {
            let _read = owner.enter_catalog_read_access().unwrap();
            db.check_catalog_read_access("catalog", Some("metadata"))
                .unwrap();
            db.check_catalog_read_access("catalog", None).unwrap();
            let mut ordinary = db.begin_transaction().unwrap();
            assert!(matches!(
                ordinary.update("catalog", Record::new("metadata")),
                Err(BicDbError::TransactionConflict(_))
            ));
            assert_eq!(ordinary.write_len(), 0);
            assert_eq!(ordinary.lock_len(), 0);
            ordinary
                .insert("business", Record::new("acknowledged"))
                .unwrap();
            ordinary.commit().unwrap();
        }
        refused(db.check_catalog_read_access("catalog", None));
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
        assert!(db.get("business", "acknowledged").unwrap().is_some());
    }
}

#[test]
fn expired_read_identity_cannot_read_a_retained_undecided_catalog() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        let owner = db.begin_transaction().unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        owner
            .retain_catalog_scope(&token, "read-identity-runtime-test")
            .unwrap();
        let read = owner.enter_catalog_read_access().unwrap();
        db.check_catalog_read_access("catalog", Some("absent"))
            .unwrap();
        owner.rollback().unwrap();
        refused(db.check_catalog_read_access("catalog", Some("absent")));
        {
            let _trusted = db.enter_catalog_scope(&token).unwrap();
            db.check_catalog_read_access("catalog", None).unwrap();
        }
        db.finish_catalog_scope(&token).unwrap();
        refused(db.check_catalog_read_access("catalog", None));
        drop(read);
        db.check_catalog_read_access("catalog", None).unwrap();
    }
}

#[test]
fn catalog_observations_are_retained_until_the_reader_finishes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        let reader = db.begin_transaction().unwrap();
        {
            let _read = reader.enter_catalog_read_access().unwrap();
            db.check_catalog_read_access("catalog", Some("absent"))
                .unwrap();
        }
        let owner = db.begin_transaction().unwrap();
        assert!(matches!(
            owner.reserve_catalog_collections(&["catalog".into()]),
            Err(BicDbError::TransactionConflict(_))
        ));
        reader.rollback().unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}

#[test]
fn catalog_read_identity_is_database_bound_and_its_drop_never_dereferences_database() {
    let one_root = tempfile::tempdir().unwrap();
    let two_root = tempfile::tempdir().unwrap();
    let mut one = BicDb::open(one_root.path()).unwrap();
    let mut two = BicDb::open(two_root.path()).unwrap();
    one.create_collection("catalog").unwrap();
    two.create_collection("catalog").unwrap();
    let one_owner = one.begin_transaction().unwrap();
    let read = one_owner.enter_catalog_read_access().unwrap();
    let two_owner = two.begin_transaction().unwrap();
    let token = two_owner
        .reserve_catalog_collections(&["catalog".into()])
        .unwrap();
    refused(two.check_catalog_read_access("catalog", Some("metadata")));
    two_owner.release_catalog_scope(&token).unwrap();
    two_owner.rollback().unwrap();
    drop(one);
    drop(one_owner);
    two.check_catalog_read_access("catalog", None).unwrap();
    drop(read);
}

#[test]
fn catalog_identity_reads_do_not_acquire_a_data_snapshot_or_erase_observations() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for scan in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
            db.create_collection("catalog").unwrap();
            db.insert("catalog", Record::new("metadata")).unwrap();
            let mut reader = db.begin_transaction().unwrap();
            {
                let _read = reader.enter_catalog_read_access().unwrap();
                db.check_catalog_read_access("catalog", if scan { None } else { Some("metadata") })
                    .unwrap();
            }
            assert!(!reader.statement_snapshot_acquired());
            reader
                .defer_statement_snapshot()
                .expect("metadata identity does not observe MVCC data");
            reader.set_deferred_snapshot_isolation(true, 0).unwrap();
            let owner = db.begin_transaction().unwrap();
            assert!(matches!(
                owner.reserve_catalog_resources(&[], &[("catalog".into(), "metadata".into())]),
                Err(BicDbError::TransactionConflict(_))
            ));
            // A subsequent ordinary native data read must permanently restore
            // the original refusal to defer after observing transaction data.
            assert!(reader.get("catalog", "metadata").unwrap().is_some());
            refused(reader.defer_statement_snapshot());
            reader.rollback().unwrap();
            let token = owner
                .reserve_catalog_resources(&[], &[("catalog".into(), "metadata".into())])
                .unwrap();
            owner.release_catalog_scope(&token).unwrap();
            owner.rollback().unwrap();
        }
    }
}
