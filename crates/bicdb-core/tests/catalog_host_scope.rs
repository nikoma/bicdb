//! Host-held fences survive the native owner until journal resolution.
use bicdb_core::{
    BicDb, BicDbError, CatalogScopeToken, DbConfig, IndexDefinition, IndexField, IndexKind,
    MutationPolicy, Record, StorageMode,
};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default().with_fsync(true).with_storage_mode(mode)
}

fn seed(value: i64) -> Record {
    Record::new("seed").with_metadata(json!({"value":value}))
}

fn fenced(db: &BicDb) {
    let reader = db.begin_transaction().unwrap();
    assert!(matches!(
        reader.get("catalog", "seed"),
        Err(BicDbError::TransactionConflict(_))
    ));
    reader.rollback().unwrap();
}

fn restore(db: &BicDb, token: &CatalogScopeToken) {
    let _access = db.enter_catalog_scope(token).unwrap();
    let mut internal = db.begin_transaction().unwrap();
    internal.update("catalog", seed(0)).unwrap();
    internal.commit().unwrap();
}

#[test]
fn retained_scope_survives_rollback_and_drop_until_explicit_host_resolution() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for rollback in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            db.create_collection("catalog").unwrap();
            db.insert("catalog", seed(0)).unwrap();
            let owner = db.begin_transaction().unwrap();
            let token = owner
                .reserve_catalog_collections(&["catalog".into()])
                .unwrap();
            owner.retain_catalog_scope(&token, "journal-one").unwrap();
            assert!(owner.release_catalog_scope(&token).is_err());
            {
                let _access = db.enter_catalog_scope(&token).unwrap();
                let mut internal = db.begin_transaction().unwrap();
                internal.update("catalog", seed(2)).unwrap();
                internal.commit().unwrap();
            }
            fenced(&db);
            if rollback {
                owner.rollback().unwrap();
            } else {
                drop(owner);
            }
            fenced(&db);
            let recovered = db
                .catalog_scope_for_context("journal-one")
                .unwrap()
                .unwrap();
            restore(&db, &recovered);
            fenced(&db);
            db.finish_catalog_scope(&recovered).unwrap();
            assert!(db
                .catalog_scope_for_context("journal-one")
                .unwrap()
                .is_none());
            let reader = db.begin_transaction().unwrap();
            assert_eq!(
                reader.get("catalog", "seed").unwrap().unwrap().metadata["value"],
                0
            );
            reader.rollback().unwrap();
            db.check_catalog_recovery_quiescent().unwrap();
            db.close().unwrap();
        }
    }
}

#[test]
fn pending_internal_work_refuses_outer_commit_and_host_finalization() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        db.create_collection("business").unwrap();
        db.insert("catalog", seed(0)).unwrap();
        let mut owner = db.begin_transaction().unwrap();
        owner
            .insert("business", Record::new("must-not-commit"))
            .unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        owner
            .retain_catalog_scope(&token, "journal-pending")
            .unwrap();
        let mut internal = {
            let _access = db.enter_catalog_scope(&token).unwrap();
            db.begin_transaction().unwrap()
        };
        internal.update("catalog", seed(2)).unwrap();
        assert!(internal
            .reserve_catalog_collections(&["business".into()])
            .is_err());
        assert!(db.finish_catalog_scope(&token).is_err());
        let error = owner.commit().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("internal catalog work is still pending"),
            "{error}"
        );
        assert!(db.get("business", "must-not-commit").unwrap().is_none());
        fenced(&db);
        internal.rollback().unwrap();
        db.resume_catalog_scope_for_recovery(&token).unwrap();
        restore(&db, &token);
        db.finish_catalog_scope(&token).unwrap();
        db.check_catalog_recovery_quiescent().unwrap();
        db.close().unwrap();
    }
}

#[test]
fn a_successful_owner_decision_stays_closed_and_cannot_be_reopened_for_undo() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        db.create_collection("business").unwrap();
        db.insert("catalog", seed(0)).unwrap();
        let mut owner = db.begin_transaction().unwrap();
        owner.insert("business", Record::new("committed")).unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        owner
            .retain_catalog_scope(&token, "journal-committed")
            .unwrap();
        {
            let _access = db.enter_catalog_scope(&token).unwrap();
            let mut internal = db.begin_transaction().unwrap();
            internal.update("catalog", seed(2)).unwrap();
            internal.commit().unwrap();
        }
        owner.commit().unwrap();
        fenced(&db);
        assert!(db.enter_catalog_scope(&token).is_err());
        assert!(db.resume_catalog_scope_for_recovery(&token).is_err());
        assert!(db.get("business", "committed").unwrap().is_some());
        db.finish_catalog_scope(&token).unwrap();
        let reader = db.begin_transaction().unwrap();
        assert_eq!(
            reader.get("catalog", "seed").unwrap().unwrap().metadata["value"],
            2
        );
        reader.rollback().unwrap();
        db.close().unwrap();
    }
}

#[test]
fn failed_unique_commit_keeps_the_host_fence_available_for_recovery() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        db.create_collection("business").unwrap();
        db.insert("catalog", seed(0)).unwrap();
        db.create_index(IndexDefinition {
            name: "unique_code".into(),
            collection: "business".into(),
            fields: vec![IndexField::MetadataPath(vec!["code".into()])],
            kind: IndexKind::BTree,
            unique: true,
            nulls_not_distinct: false,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        let mut owner = db.begin_transaction().unwrap();
        owner
            .insert(
                "business",
                Record::new("loser").with_metadata(json!({"code":7})),
            )
            .unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        owner
            .retain_catalog_scope(&token, "journal-conflict")
            .unwrap();
        {
            let _access = db.enter_catalog_scope(&token).unwrap();
            let mut internal = db.begin_transaction().unwrap();
            internal.update("catalog", seed(2)).unwrap();
            internal.commit().unwrap();
        }
        db.insert(
            "business",
            Record::new("winner").with_metadata(json!({"code":7})),
        )
        .unwrap();
        let error = owner.commit().unwrap_err();
        assert!(error.to_string().contains("duplicate"), "{error}");
        fenced(&db);
        restore(&db, &token);
        db.finish_catalog_scope(&token).unwrap();
        assert!(db.get("business", "loser").unwrap().is_none());
        assert!(db.get("business", "winner").unwrap().is_some());
        db.close().unwrap();
    }
}

#[test]
fn demotion_during_an_outer_decision_keeps_readonly_host_cleanup_access() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode).with_replication(bicdb_core::ReplicationConfig {
            enabled: true,
            mode: bicdb_core::ReplicationMode::Primary,
            cluster_id: "catalog-fence".into(),
            listen_addr: Some("127.0.0.1:0".into()),
            tls: Some(bicdb_core::ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..Default::default()
        });
        let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
        db.create_collection("catalog").unwrap();
        db.create_collection("business").unwrap();
        db.insert("catalog", seed(0)).unwrap();
        let mut owner = db.begin_transaction().unwrap();
        owner
            .insert("business", Record::new("never-acknowledged"))
            .unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        owner
            .retain_catalog_scope(&token, "journal-fenced")
            .unwrap();
        db.demote_to_standby().unwrap();
        assert!(owner.commit().is_err());
        assert!(db.get("business", "never-acknowledged").unwrap().is_none());
        fenced(&db);
        {
            // Cleanup may inspect its existing catalog prefix, while the
            // demoted node still refuses mutations and local recovery.
            let _access=db.enter_catalog_scope(&token).expect("a refused predecision role-fenced commit must not leave its host scope permanently closing");
            let mut internal = db.begin_transaction().unwrap();
            assert_eq!(
                internal.get("catalog", "seed").unwrap().unwrap().metadata["value"],
                0
            );
            assert!(internal.update("catalog", seed(9)).is_err());
            internal.rollback().unwrap();
        }
        assert!(db.resume_catalog_scope_for_recovery(&token).is_err());
        db.finish_catalog_scope(&token).unwrap();
        db.check_catalog_recovery_quiescent().unwrap();
        db.close().unwrap();
    }
}

#[test]
fn a_runtime_owner_drop_revokes_its_children_and_never_publishes_their_buffer() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    db.create_collection("catalog").unwrap();
    db.insert("catalog", seed(0)).unwrap();
    let owner = db.begin_transaction().unwrap();
    let token = owner
        .reserve_catalog_collections(&["catalog".into()])
        .unwrap();
    let mut internal = {
        let _access = db.enter_catalog_scope(&token).unwrap();
        db.begin_transaction().unwrap()
    };
    internal.update("catalog", seed(9)).unwrap();
    drop(owner);
    fenced(&db);
    assert!(internal.commit().is_err());
    let reader = db.begin_transaction().unwrap();
    assert_eq!(
        reader.get("catalog", "seed").unwrap().unwrap().metadata["value"],
        0
    );
    reader.rollback().unwrap();
    db.check_catalog_recovery_quiescent().unwrap();
    db.close().unwrap();
}

#[test]
fn host_authority_keeps_grant_checks_and_database_identity() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path().join("source")).unwrap();
    db.create_collection("protected").unwrap();
    db.set_mutation_policy("protected", MutationPolicy::grants_required())
        .unwrap();
    let owner = db.begin_transaction().unwrap();
    let token = owner
        .reserve_catalog_collections(&["protected".into()])
        .unwrap();
    let other = BicDb::open(root.path().join("other")).unwrap();
    assert!(other.enter_catalog_scope(&token).is_err());
    assert!(other.finish_catalog_scope(&token).is_err());
    let mut foreign = other.begin_transaction().unwrap();
    assert!(db.commit_buffered_transaction(&mut foreign).is_err());
    {
        let access = db.enter_catalog_scope(&token).unwrap();
        let mut internal = db.begin_transaction().unwrap();
        assert!(matches!(
            internal.insert("protected", Record::new("forbidden")),
            Err(BicDbError::MutationDenied(_))
        ));
        assert_eq!(internal.write_len(), 0);
        assert_eq!(internal.lock_len(), 0);
        internal.rollback().unwrap();
        db.finish_catalog_scope(&token).unwrap();
        assert!(
            db.begin_transaction().is_err(),
            "expired host access must not silently become ordinary authority"
        );
        drop(access);
    }
    foreign.rollback().unwrap();
    owner.rollback().unwrap();
    db.check_catalog_recovery_quiescent().unwrap();
    other.close().unwrap();
    db.close().unwrap();
}

#[test]
fn internal_catalog_writes_stay_inside_the_declared_effect_scope() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        db.create_collection("business").unwrap();
        db.insert("catalog", seed(0)).unwrap();
        db.insert("business", seed(0)).unwrap();
        let owner = db.begin_transaction().unwrap();
        let token = owner
            .reserve_catalog_collections(&["catalog".into()])
            .unwrap();
        owner
            .retain_catalog_scope(&token, "journal-bounded")
            .unwrap();
        {
            let _access = db.enter_catalog_scope(&token).unwrap();
            let mut internal = db.begin_transaction().unwrap();
            // An ordinary read may inspect an unrelated dependency. Its
            // mutation must first belong to the declared undo/effect scope.
            assert_eq!(
                internal.get("business", "seed").unwrap().unwrap().metadata["value"],
                0
            );
            assert!(matches!(
                internal.update("business", seed(9)),
                Err(BicDbError::TransactionConflict(_))
            ));
            assert!(matches!(
                internal.insert("business", Record::new("outside")),
                Err(BicDbError::TransactionConflict(_))
            ));
            assert!(matches!(
                internal.delete("business", "seed"),
                Err(BicDbError::TransactionConflict(_))
            ));
            internal.update("catalog", seed(7)).unwrap();
            internal.commit().unwrap();
        }
        assert_eq!(
            db.get("business", "seed").unwrap().unwrap().metadata["value"],
            0
        );
        assert!(db.get("business", "outside").unwrap().is_none());
        owner.rollback().unwrap();
        restore(&db, &token);
        db.finish_catalog_scope(&token).unwrap();
        db.check_catalog_recovery_quiescent().unwrap();
        db.close().unwrap();
    }
}

#[test]
fn a_sibling_scope_cannot_finalize_while_overlapping_internal_work_is_pending() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    db.create_collection("catalog").unwrap();
    db.insert("catalog", seed(0)).unwrap();
    let owner = db.begin_transaction().unwrap();
    let first = owner
        .reserve_catalog_collections(&["catalog".into(), "first".into()])
        .unwrap();
    let second = owner
        .reserve_catalog_collections(&["catalog".into(), "second".into()])
        .unwrap();
    owner.retain_catalog_scope(&first, "journal-first").unwrap();
    owner
        .retain_catalog_scope(&second, "journal-second")
        .unwrap();
    let mut internal = {
        let _access = db.enter_catalog_scope(&second).unwrap();
        db.begin_transaction().unwrap()
    };
    internal.update("catalog", seed(8)).unwrap();
    assert!(db.finish_catalog_scope(&first).is_err());
    assert!(db.finish_catalog_scope(&second).is_err());
    internal.rollback().unwrap();
    db.finish_catalog_scope(&first).unwrap();
    db.finish_catalog_scope(&second).unwrap();
    owner.rollback().unwrap();
    assert_eq!(
        db.get("catalog", "seed").unwrap().unwrap().metadata["value"],
        0
    );
    db.check_catalog_recovery_quiescent().unwrap();
    db.close().unwrap();
}

#[test]
fn internal_begin_and_finalization_have_one_atomic_winner() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.create_collection("catalog").unwrap();
        for round in 0..32 {
            let owner = db.begin_transaction().unwrap();
            let token = owner
                .reserve_catalog_collections(&["catalog".into()])
                .unwrap();
            owner
                .retain_catalog_scope(&token, &format!("journal-{round}"))
                .unwrap();
            let barrier = std::sync::Barrier::new(2);
            let (finished, begun) = std::thread::scope(|threads| {
                let (db, token, barrier) = (&db, &token, &barrier);
                let actor = threads.spawn(move || {
                    let _access = db.enter_catalog_scope(token).unwrap();
                    barrier.wait();
                    db.begin_transaction()
                });
                let finisher = threads.spawn(move || {
                    barrier.wait();
                    db.finish_catalog_scope(token)
                });
                (finisher.join().unwrap(), actor.join().unwrap())
            });
            assert_ne!(finished.is_ok(), begun.is_ok());
            if let Ok(internal) = begun {
                internal.rollback().unwrap();
                db.finish_catalog_scope(&token).unwrap();
            }
            owner.rollback().unwrap();
            db.check_catalog_recovery_quiescent().unwrap();
        }
        db.close().unwrap();
    }
}
