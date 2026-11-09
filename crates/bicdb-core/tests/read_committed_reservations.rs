use bicdb_core::{
    BicDb, BicDbError, CancellationToken, CollectionPolicy, ColumnSecurity, DbConfig,
    MutationActor, MutationGrantSpec, MutationOperation, MutationPolicy, Record, StorageMode,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{mpsc, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn actor() -> MutationActor {
    MutationActor {
        actor_id: "reader-writer".into(),
        roles: BTreeSet::from(["reader".into(), "writer".into()]),
        scopes: BTreeSet::new(),
        tenant_id: Some("t".into()),
        workspace_id: None,
        originating_plugin: "reservation-proof".into(),
        originating_resource: None,
        originating_action: None,
        trace_id: "checked-row".into(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 120000,
    }
}
fn grant(operation: MutationOperation) -> MutationGrantSpec {
    MutationGrantSpec {
        relation: "rows".into(),
        operation,
        record_id: Some("one".into()),
        record_id_prefix: None,
        expected_version: None,
        version_field: None,
        allowed_columns: BTreeSet::from(["id".into(), "v".into(), "version".into()]),
        bulk: operation == MutationOperation::Bulk,
        maximum_affected_rows: 1,
        cascade_relations: BTreeSet::new(),
        statement_budget: 1,
        tenant_field: None,
        workspace_field: None,
        audit_metadata: BTreeMap::from([("proof".into(), "checked actual action".into())]),
    }
}
fn setup(mode: StorageMode, present: bool) -> (tempfile::TempDir, Arc<BicDb>) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap();
    db.create_collection("rows").unwrap();
    if present {
        db.insert(
            "rows",
            Record::new("one").with_metadata(json!({"v":20,"version":2})),
        )
        .unwrap();
    }
    db.set_mutation_policy("rows", MutationPolicy::grants_required().with_audit())
        .unwrap();
    (dir, Arc::new(db))
}
#[test]
fn checked_invisible_winner_uses_current_preimage_and_consumes_one_actual_action() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_dir, db) = setup(mode, false);
        let mut owner = db.begin_application_transaction(actor()).unwrap();
        let g = owner
            .issue_mutation_grant(grant(MutationOperation::Insert))
            .unwrap();
        owner
            .insert_with_grant(
                g,
                "rows",
                Record::new("one").with_metadata(json!({"v":20,"version":2})),
            )
            .unwrap();
        let mut waiter = db.begin_application_transaction(actor()).unwrap();
        let mut spec = grant(MutationOperation::Update);
        spec.expected_version = Some(2);
        spec.version_field = Some("version".into());
        let g = waiter.issue_mutation_grant(spec).unwrap();
        let (done, received) = mpsc::channel();
        let keep = db.clone();
        let worker = std::thread::spawn(move || {
            let result = (|| {
                let token = waiter.reserve_authorized_read_committed_row(
                    "rows",
                    "one",
                    Some(&CancellationToken::uncancelable()),
                )?;
                let mut record = waiter
                    .read_authorized_reserved_row(&token)?
                    .unwrap()
                    .as_ref()
                    .clone();
                assert_eq!(record.metadata["v"], json!(20));
                record.metadata["v"] = json!(30);
                waiter.upsert_reserved_with_grant(g, token, record)?;
                assert_eq!(waiter.broker_publish_len(), 1);
                waiter.commit()
            })();
            done.send(result).unwrap();
            drop(keep);
        });
        let early = received.recv_timeout(Duration::from_millis(30));
        owner.commit().unwrap();
        assert!(
            early.is_err(),
            "reservation bypassed pending owner: {early:?}"
        );
        received
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        worker.join().unwrap();
        let reader = db.begin_application_transaction(actor()).unwrap();
        assert_eq!(
            reader
                .get_authorized("rows", "one")
                .unwrap()
                .unwrap()
                .metadata["v"],
            json!(30)
        );
    }
}
#[test]
fn actual_insert_and_update_authority_are_checked_after_reservation() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for present in [false, true] {
            let (_dir, db) = setup(mode.clone(), present);
            let mut tx = db.begin_application_transaction(actor()).unwrap();
            let g = tx
                .issue_mutation_grant(grant(MutationOperation::Insert))
                .unwrap();
            let token = tx
                .reserve_authorized_read_committed_row("rows", "one", None)
                .unwrap();
            let result = tx.upsert_reserved_with_grant(
                g,
                token,
                Record::new("one").with_metadata(json!({"v":10,"version":1})),
            );
            if present {
                assert!(matches!(result, Err(BicDbError::MutationDenied(_))));
                tx.rollback().unwrap();
            } else {
                result.unwrap();
                assert_eq!(tx.broker_publish_len(), 1);
                tx.commit().unwrap();
            }
        }
    }
}
#[test]
fn rollback_reacquisition_and_foreign_transactions_cannot_reuse_capabilities() {
    let (_dir, db) = setup(StorageMode::ServerPaged, true);
    let mut tx = db.begin_application_transaction(actor()).unwrap();
    let g = tx
        .issue_mutation_grant(grant(MutationOperation::Update))
        .unwrap();
    let mark = tx.rollback_mark();
    let old = tx
        .reserve_authorized_read_committed_row("rows", "one", None)
        .unwrap();
    let other = db.begin_application_transaction(actor()).unwrap();
    assert!(matches!(
        other.read_authorized_reserved_row(&old),
        Err(BicDbError::TransactionConflict(_))
    ));
    let (_other_dir, other_db) = setup(StorageMode::ServerPaged, true);
    let other_database_tx = other_db.begin_application_transaction(actor()).unwrap();
    assert_eq!(
        other_database_tx.id(),
        tx.id(),
        "fixture must test the same numeric TxID on another handle"
    );
    assert!(matches!(
        other_database_tx.read_authorized_reserved_row(&old),
        Err(BicDbError::TransactionConflict(_))
    ));
    tx.rollback_to_mark(mark).unwrap();
    let fresh = tx
        .reserve_authorized_read_committed_row("rows", "one", None)
        .unwrap();
    assert!(matches!(
        tx.read_authorized_reserved_row(&old),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert!(matches!(
        tx.upsert_reserved_with_grant(g, old, Record::new("one")),
        Err(BicDbError::TransactionConflict(_))
    ));
    let mut record = tx
        .read_authorized_reserved_row(&fresh)
        .unwrap()
        .unwrap()
        .as_ref()
        .clone();
    record.metadata["v"] = json!(25);
    tx.upsert_reserved_with_grant(g, fresh, record).unwrap();
    tx.commit().unwrap();
}
#[test]
fn retained_capabilities_report_fences_for_present_and_absent_rows() {
    for present in [false, true] {
        let (_dir, db) = setup(StorageMode::EmbeddedMemory, present);
        let mut tx = db.begin_application_transaction(actor()).unwrap();
        let token = tx
            .reserve_authorized_read_committed_row("rows", "one", None)
            .unwrap();
        assert!(!format!("{token:?}").contains("20"));
        db.durability_health_for_test()
            .record_failure("test.checked_reservation", &"uncertain outcome");
        assert!(matches!(
            tx.read_authorized_reserved_row(&token),
            Err(BicDbError::WriteFailed { .. })
        ));
    }
}
#[test]
fn masked_native_fields_are_preserved_and_foreign_tenant_cannot_be_overwritten() {
    // This integration binary owns its environment; other test binaries have
    // separate processes, and no other test here mutates protected-field keys.
    std::env::set_var("BICDB_PROTECTED_FIELD_ENCRYPTION_KEY", "11".repeat(32));
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        db.create_collection("rows").unwrap();
        db.set_collection_policy(
            "rows",
            CollectionPolicy::tenant_field("tenant_id")
                .with_read_roles(["reader"])
                .with_write_roles(["writer"])
                .with_column_security(
                    "secret",
                    ColumnSecurity::encrypted("test-secret")
                        .with_key_ref("proof:v1")
                        .with_decrypt_roles(["secret-reader"]),
                ),
        )
        .unwrap();
        db.secure(&bicdb_core::SecurityContext::new("seed", "t").with_roles(["writer"]))
            .insert(
                "rows",
                Record::new("one").with_metadata(
                    json!({"tenant_id":"t","v":20,"secret":"unpublished-sensitive-value"}),
                ),
            )
            .unwrap();
        db.set_mutation_policy(
            "rows",
            MutationPolicy::grants_required().with_tenant_field("tenant_id"),
        )
        .unwrap();
        let mut tx = db.begin_application_transaction(actor()).unwrap();
        let mut spec = grant(MutationOperation::Update);
        spec.allowed_columns = BTreeSet::from(["v".into()]);
        let g = tx.issue_mutation_grant(spec).unwrap();
        let token = tx
            .reserve_authorized_read_committed_row("rows", "one", None)
            .unwrap();
        assert!(!format!("{token:?}").contains("unpublished-sensitive-value"));
        let mut projected = tx
            .read_authorized_reserved_row(&token)
            .unwrap()
            .unwrap()
            .as_ref()
            .clone();
        assert!(projected.metadata["secret"].is_null());
        projected.metadata["v"] = json!(30);
        tx.upsert_reserved_with_grant(g, token, projected).unwrap();
        tx.commit().unwrap();
        let mut privileged = actor();
        privileged.roles.insert("secret-reader".into());
        let reader = db.begin_application_transaction(privileged).unwrap();
        let row = reader.get_authorized("rows", "one").unwrap().unwrap();
        assert_eq!(row.metadata["secret"], json!("unpublished-sensitive-value"));
        assert_eq!(row.metadata["v"], json!(30));
        reader.rollback().unwrap();
        let mut foreign = actor();
        foreign.tenant_id = Some("foreign".into());
        let mut tx = db.begin_application_transaction(foreign).unwrap();
        let g = tx
            .issue_mutation_grant(grant(MutationOperation::Bulk))
            .unwrap();
        let token = tx
            .reserve_authorized_read_committed_row("rows", "one", None)
            .unwrap();
        assert!(tx.read_authorized_reserved_row(&token).unwrap().is_none());
        assert!(matches!(
            tx.upsert_reserved_with_grant(
                g,
                token,
                Record::new("one").with_metadata(json!({"tenant_id":"foreign","v":99}))
            ),
            Err(BicDbError::Authorization(_))
        ));
        tx.rollback().unwrap();
    }
    std::env::remove_var("BICDB_PROTECTED_FIELD_ENCRYPTION_KEY");
}

#[test]
fn concurrent_fence_overrides_constructor_error_after_entry() {
    {
        let (dir, db) = setup(StorageMode::EmbeddedMemory, false);
        let mut tx = db.begin_application_transaction(actor()).unwrap();
        tx.set_snapshot_isolation(true, 0).unwrap();
        let health = db.durability_health_for_test();
        let hook = bicdb_page::test_faults::arm_io_hook(
            "read_committed.reservation.begin",
            dir.path(),
            1,
            move || {
                health.record_failure(
                    "test.reservation_constructor",
                    &"concurrent terminal failure",
                );
            },
        );
        let result = tx.reserve_authorized_read_committed_row("rows", "one", None);
        assert_eq!(hook.fired(), 1);
        assert!(
            matches!(result, Err(BicDbError::WriteFailed { .. })),
            "fence must override fixed-isolation error after entry: {result:?}"
        );
    }
}

#[test]
fn concurrent_fence_overrides_stale_token_error_after_validation_entry() {
    {
        let (dir, db) = setup(StorageMode::EmbeddedMemory, true);
        let mut tx = db.begin_application_transaction(actor()).unwrap();
        let mark = tx.rollback_mark();
        let token = tx
            .reserve_authorized_read_committed_row("rows", "one", None)
            .unwrap();
        tx.rollback_to_mark(mark).unwrap();
        let health = db.durability_health_for_test();
        let hook = bicdb_page::test_faults::arm_io_hook(
            "read_committed.reservation.validate",
            dir.path(),
            1,
            move || {
                health.record_failure("test.reservation_read", &"concurrent terminal failure");
            },
        );
        let result = tx.read_authorized_reserved_row(&token);
        assert_eq!(hook.fired(), 1);
        assert!(
            matches!(result, Err(BicDbError::WriteFailed { .. })),
            "fence must override stale-token error after validation entry: {result:?}"
        );
    }
}
