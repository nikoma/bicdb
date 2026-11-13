use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{
    BicDb, BicDbError, ConsumeOptions, IndexDefinition, IndexField, IndexKind, MutationActor,
    MutationGrantSpec, MutationOperation, MutationPolicy, NativeCommitValidator, PublishOptions,
    Record,
};
use serde_json::json;

fn actor(tenant: &str, workspace: &str) -> MutationActor {
    MutationActor {
        actor_id: "user-1".to_string(),
        roles: BTreeSet::from(["editor".to_string()]),
        scopes: BTreeSet::from(["records:write".to_string()]),
        tenant_id: Some(tenant.to_string()),
        workspace_id: Some(workspace.to_string()),
        originating_plugin: "carrier-program".to_string(),
        originating_resource: Some("documents".to_string()),
        originating_action: Some("create".to_string()),
        trace_id: "trace-1".to_string(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000,
    }
}

fn grant(
    operation: MutationOperation,
    record_id: &str,
    expected_version: Option<u64>,
) -> MutationGrantSpec {
    MutationGrantSpec {
        relation: "documents".to_string(),
        operation,
        record_id: Some(record_id.to_string()),
        record_id_prefix: None,
        expected_version,
        version_field: Some("version".to_string()),
        allowed_columns: BTreeSet::from([
            "id".to_string(),
            "tenant_id".to_string(),
            "workspace_id".to_string(),
            "version".to_string(),
            "name".to_string(),
            "created_by".to_string(),
        ]),
        bulk: false,
        maximum_affected_rows: 1,
        cascade_relations: BTreeSet::new(),
        statement_budget: 1,
        tenant_field: Some("tenant_id".to_string()),
        workspace_field: Some("workspace_id".to_string()),
        audit_metadata: BTreeMap::from([("reason".to_string(), "resource request".to_string())]),
    }
}

fn record(id: &str, tenant: &str, workspace: &str, version: u64, name: &str) -> Record {
    Record::new(id).with_metadata(json!({
        "tenant_id": tenant,
        "workspace_id": workspace,
        "version": version,
        "name": name,
        "created_by": "user-1"
    }))
}

#[test]
fn fixed_snapshot_insert_identity_preserves_signed_authority_after_a_peer_delete() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            directory.path(),
            bicdb_core::DbConfig::default().with_storage_mode(mode),
        )
        .unwrap();
        db.create_collection("documents").unwrap();
        db.insert(
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "old"),
        )
        .unwrap();
        db.set_mutation_policy(
            "documents",
            MutationPolicy::grants_required()
                .with_tenant_field("tenant_id")
                .with_workspace_field("workspace_id")
                .with_audit(),
        )
        .unwrap();

        let mut reader = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        reader.set_snapshot_isolation(true, 0).unwrap();
        reader.set_statement_write_conflict_check(true).unwrap();
        assert!(reader
            .get_for_integrity_check("documents", "doc-1")
            .unwrap()
            .is_some());
        let authority = reader
            .issue_mutation_grant(grant(MutationOperation::Insert, "doc-1", None))
            .unwrap();
        let mut peer = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        let deletion = peer
            .issue_mutation_grant(grant(MutationOperation::Delete, "doc-1", None))
            .unwrap();
        peer.delete_with_grant(deletion, "documents", "doc-1")
            .unwrap();
        peer.commit().unwrap();

        let denied = reader
            .insert_using_live_identities(
                "documents",
                "documents_pkey",
                vec![record("doc-1", "tenant-b", "workspace-a", 1, "denied")],
                Some(authority),
            )
            .unwrap_err();
        assert!(matches!(denied, BicDbError::MutationDenied(_)));
        assert_eq!(reader.write_len(), 0);
        assert_eq!(reader.lock_len(), 0);

        reader
            .insert_using_live_identities(
                "documents",
                "documents_pkey",
                vec![record("doc-1", "tenant-a", "workspace-a", 1, "replacement")],
                Some(authority),
            )
            .unwrap();
        reader.commit().unwrap();
        let stored = db.get("documents", "doc-1").unwrap().unwrap();
        assert_eq!(stored.metadata["name"], "replacement");
    }
}

#[test]
fn fixed_snapshot_insert_identity_releases_locks_on_a_failed_batch() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("documents").unwrap();
    db.insert("documents", Record::new("existing")).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.set_snapshot_isolation(true, 0).unwrap();
    let error = tx
        .insert_using_live_identities(
            "documents",
            "documents_pkey",
            vec![Record::new("vacant"), Record::new("existing")],
            None,
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::UniqueIndexConflict { .. }));
    assert_eq!(tx.lock_len(), 0);
    assert_eq!(tx.write_len(), 0);
    let mut peer = db.begin_transaction().unwrap();
    peer.insert("documents", Record::new("vacant")).unwrap();
    peer.commit().unwrap();
    tx.rollback().unwrap();
}

#[test]
fn protected_mutations_require_and_consume_exact_native_grants() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("documents").unwrap();
    db.set_mutation_policy(
        "documents",
        MutationPolicy::grants_required()
            .with_tenant_field("tenant_id")
            .with_workspace_field("workspace_id")
            .with_version_field("version")
            .with_immutable_fields(["created_by"])
            .with_audit(),
    )
    .unwrap();
    db.create_index(IndexDefinition {
        nulls_not_distinct: false,
        name: "documents_search".to_string(),
        collection: "documents".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["name".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    let direct = db
        .insert(
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "direct"),
        )
        .unwrap_err();
    assert!(matches!(direct, BicDbError::MutationDenied(_)));

    let mut ordinary = db.begin_transaction().unwrap();
    let ordinary_error = ordinary
        .insert(
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "ordinary"),
        )
        .unwrap_err();
    assert!(matches!(ordinary_error, BicDbError::MutationDenied(_)));
    ordinary.rollback().unwrap();

    let mut create = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let create_grant = create
        .issue_mutation_grant(grant(MutationOperation::Insert, "doc-1", None))
        .unwrap();
    create
        .insert_with_grant(
            create_grant,
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "created"),
        )
        .unwrap();
    let reuse = create
        .insert_with_grant(
            create_grant,
            "documents",
            record("doc-2", "tenant-a", "workspace-a", 1, "reuse"),
        )
        .unwrap_err();
    assert!(matches!(reuse, BicDbError::MutationDenied(_)));
    create.commit().unwrap();

    let mut nullable = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let nullable_grant = nullable
        .issue_mutation_grant(grant(MutationOperation::Insert, "doc-null", None))
        .unwrap();
    nullable
        .insert_with_grant(
            nullable_grant,
            "documents",
            Record::new("doc-null").with_metadata(json!({
                "tenant_id": "tenant-a",
                "workspace_id": "workspace-a",
                "version": 1,
                "name": null,
                "created_by": "user-1"
            })),
        )
        .unwrap();
    nullable.commit().unwrap();
    assert!(db.get("documents", "doc-null").unwrap().is_some());
    assert!(db
        .lookup_full_text_term("documents_search", "created", false)
        .unwrap()
        .iter()
        .all(|record_id| record_id != "doc-null"));

    let mut cross_tenant = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let cross_grant = cross_tenant
        .issue_mutation_grant(grant(MutationOperation::Insert, "doc-2", None))
        .unwrap();
    let cross_error = cross_tenant
        .insert_with_grant(
            cross_grant,
            "documents",
            record("doc-2", "tenant-b", "workspace-a", 1, "cross"),
        )
        .unwrap_err();
    assert!(matches!(cross_error, BicDbError::MutationDenied(_)));
    cross_tenant.rollback().unwrap();

    let mut update = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let update_grant = update
        .issue_mutation_grant(grant(MutationOperation::Update, "doc-1", Some(1)))
        .unwrap();
    update
        .register_commit_validator(NativeCommitValidator::OptimisticVersion {
            relation: "documents".to_string(),
            field: "version".to_string(),
        })
        .unwrap();
    update
        .update_with_grant(
            update_grant,
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 2, "updated"),
        )
        .unwrap();
    update.commit().unwrap();

    let stored = db.get("documents", "doc-1").unwrap().unwrap();
    assert_eq!(stored.metadata["name"], "updated");
    assert_eq!(stored.metadata["version"], 2);
}

#[test]
fn mutation_grants_authorize_read_committed_update_and_delete_rechecks() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("documents").unwrap();
    db.set_mutation_policy("documents", MutationPolicy::grants_required())
        .unwrap();

    let mut create = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let create_grant = create
        .issue_mutation_grant(grant(MutationOperation::Insert, "doc-1", None))
        .unwrap();
    create
        .insert_with_grant(
            create_grant,
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "created"),
        )
        .unwrap();
    create.commit().unwrap();

    let mut update = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let update_grant = update
        .issue_mutation_grant(grant(MutationOperation::Update, "doc-1", None))
        .unwrap();
    assert!(update
        .read_committed_update_record_with_grant(update_grant, "documents", "doc-1")
        .unwrap()
        .is_none());
    let wrong_record = update
        .read_committed_update_record_with_grant(update_grant, "documents", "doc-2")
        .unwrap_err();
    assert!(matches!(wrong_record, BicDbError::MutationDenied(_)));
    update
        .update_with_grant(
            update_grant,
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 2, "updated"),
        )
        .unwrap();
    update.commit().unwrap();

    let mut delete = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let delete_grant = delete
        .issue_mutation_grant(grant(MutationOperation::Delete, "doc-1", None))
        .unwrap();
    assert!(delete
        .read_committed_delete_record_with_grant(delete_grant, "documents", "doc-1")
        .unwrap()
        .is_none());
    delete
        .delete_with_grant(delete_grant, "documents", "doc-1")
        .unwrap();
    delete.commit().unwrap();

    assert!(db.get("documents", "doc-1").unwrap().is_none());
}

#[test]
fn append_only_and_commit_validators_fail_before_commit() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("documents").unwrap();
    db.set_mutation_policy(
        "documents",
        MutationPolicy::grants_required()
            .append_only()
            .with_tenant_field("tenant_id")
            .with_workspace_field("workspace_id"),
    )
    .unwrap();

    let mut create = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let create_grant = create
        .issue_mutation_grant(grant(MutationOperation::Insert, "doc-1", None))
        .unwrap();
    create
        .insert_with_grant(
            create_grant,
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "created"),
        )
        .unwrap();
    create.commit().unwrap();

    let mut update = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let update_grant = update
        .issue_mutation_grant(grant(MutationOperation::Update, "doc-1", Some(1)))
        .unwrap();
    let error = update
        .update_with_grant(
            update_grant,
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 2, "changed"),
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::CommitValidation(_)));
    update.rollback().unwrap();
}

#[test]
fn broker_outbox_publishes_only_committed_work_and_recovery_deduplicates() {
    let directory = tempfile::tempdir().unwrap();
    let message_id = {
        let db = BicDb::open(directory.path()).unwrap();
        let rolled_back = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        rolled_back
            .buffer_broker_publish(
                "resource-events",
                json!({"kind": "rolled-back"}),
                PublishOptions::default(),
            )
            .unwrap();
        rolled_back.rollback().unwrap();

        let committed = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        let message_id = committed
            .buffer_broker_publish(
                "resource-events",
                json!({"kind": "committed"}),
                PublishOptions {
                    idempotency_key: Some("resource/doc-1/v1".to_string()),
                    ..PublishOptions::default()
                },
            )
            .unwrap();
        committed.commit().unwrap();
        message_id
    };

    // Opening replays the committed WAL outbox. publish_prepared is
    // idempotent by its preallocated message id, so the message already
    // appended before shutdown is not duplicated.
    let mut reopened = BicDb::open(directory.path()).unwrap();
    let messages = reopened
        .broker()
        .consume(
            "resource-events",
            "vertical-slice",
            "consumer-1",
            ConsumeOptions {
                max_messages: 10,
                visibility_timeout_ms: 30_000,
            },
        )
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, message_id);
    assert_eq!(messages[0].payload["kind"], "committed");
}

#[test]
fn audit_required_mutations_fail_closed_and_publish_durable_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("documents").unwrap();
    db.set_mutation_policy(
        "documents",
        MutationPolicy::grants_required()
            .with_tenant_field("tenant_id")
            .with_workspace_field("workspace_id")
            .with_audit(),
    )
    .unwrap();

    let mut denied = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let mut unaudited = grant(MutationOperation::Insert, "denied", None);
    unaudited.audit_metadata.clear();
    let grant_id = denied.issue_mutation_grant(unaudited).unwrap();
    let error = denied
        .insert_with_grant(
            grant_id,
            "documents",
            record("denied", "tenant-a", "workspace-a", 1, "denied"),
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::MutationDenied(_)));
    denied.rollback().unwrap();

    let mut allowed = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let grant_id = allowed
        .issue_mutation_grant(grant(MutationOperation::Insert, "allowed", None))
        .unwrap();
    allowed
        .insert_with_grant(
            grant_id,
            "documents",
            record("allowed", "tenant-a", "workspace-a", 1, "allowed"),
        )
        .unwrap();
    allowed.commit().unwrap();

    let evidence = db
        .broker()
        .consume(
            "__bicdb_mutation_audit",
            "audit-sink",
            "consumer-1",
            ConsumeOptions {
                max_messages: 10,
                visibility_timeout_ms: 30_000,
            },
        )
        .unwrap();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].payload["relation"], "documents");
    assert_eq!(evidence[0].payload["record_ids"][0], "allowed");
    assert_eq!(evidence[0].payload["actor"]["tenant_id"], "tenant-a");
}

#[test]
fn transactional_catalog_rename_does_not_widen_native_mutation_grants() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    db.create_collection("documents").unwrap();
    db.set_mutation_policy("documents", MutationPolicy::grants_required())
        .unwrap();
    let mut tx = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    let permission = tx
        .issue_mutation_grant(grant(MutationOperation::Insert, "doc-1", None))
        .unwrap();
    tx.insert_with_grant(
        permission,
        "documents",
        record("doc-1", "tenant-a", "workspace-a", 1, "pending"),
    )
    .unwrap();
    assert!(matches!(
        db.rename_collection_preserving_pending_writes(&mut tx, "documents", "renamed"),
        Err(BicDbError::MutationDenied(_))
    ));
    assert!(db.collections().iter().any(|meta| meta.name == "documents"));
    assert!(!db.collections().iter().any(|meta| meta.name == "renamed"));
    tx.commit().unwrap();
    let reader = db
        .begin_application_transaction(actor("tenant-a", "workspace-a"))
        .unwrap();
    assert!(reader
        .get_authorized("documents", "doc-1")
        .unwrap()
        .is_some());
}

#[test]
fn identity_update_uses_one_scoped_update_and_preserves_native_policy_checks() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        for scenario in [
            "success",
            "missing_id_column",
            "wrong_operation",
            "fixed_address",
            "prefix_escape",
            "stale_version",
            "immutable_value",
            "immutable_id",
            "wrong_old_tenant",
            "occupied_destination",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let cfg = bicdb_core::DbConfig::default()
                .with_storage_mode(mode.clone())
                .with_fsync(true)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            db.create_collection("documents").unwrap();
            let old = record(
                "doc-1",
                if scenario == "wrong_old_tenant" {
                    "tenant-b"
                } else {
                    "tenant-a"
                },
                "workspace-a",
                1,
                "old",
            );
            db.insert("documents", old.clone()).unwrap();
            if scenario == "occupied_destination" {
                db.insert(
                    "documents",
                    record("doc-2", "tenant-a", "workspace-a", 1, "occupied"),
                )
                .unwrap();
            }
            let policy = MutationPolicy::grants_required()
                .with_tenant_field("tenant_id")
                .with_workspace_field("workspace_id")
                .with_version_field("version")
                .with_immutable_fields(if scenario == "immutable_id" {
                    vec!["id"]
                } else {
                    vec!["created_by"]
                })
                .with_audit();
            db.set_mutation_policy("documents", policy).unwrap();
            let mut tx = db
                .begin_application_transaction(actor("tenant-a", "workspace-a"))
                .unwrap();
            let mut spec = grant(
                MutationOperation::Update,
                "doc-1",
                Some(if scenario == "stale_version" { 9 } else { 1 }),
            );
            spec.record_id = None;
            spec.record_id_prefix = Some(
                if scenario == "prefix_escape" {
                    "doc-1"
                } else {
                    "doc-"
                }
                .into(),
            );
            if scenario == "fixed_address" {
                spec.record_id_prefix = None;
                spec.record_id = Some("doc-1".into());
            }
            if scenario == "wrong_operation" {
                spec.operation = MutationOperation::Delete;
            }
            if scenario == "missing_id_column" {
                spec.allowed_columns.remove("id");
            }
            let authority = tx.issue_mutation_grant(spec).unwrap();
            let mut next = record("doc-2", "tenant-a", "workspace-a", 2, "new");
            if scenario == "immutable_value" {
                next.metadata["created_by"] = json!("other");
            }
            let result =
                tx.update_identity_with_grant(authority, "documents", "doc-1", next.clone(), 0);
            if scenario == "success" {
                result.unwrap();
                assert_eq!(tx.broker_publish_len(), 1);
                assert_eq!(tx.write_len(), 2);
                assert!(
                    tx.update_with_grant(authority, "documents", next).is_err(),
                    "single-action capability reused"
                );
                tx.commit().unwrap();
                assert!(db.get("documents", "doc-1").unwrap().is_none());
                assert_eq!(
                    db.get("documents", "doc-2").unwrap().unwrap().metadata["name"],
                    "new"
                );
            } else {
                assert!(result.is_err(), "{scenario} unexpectedly authorized");
                assert_eq!(tx.write_len(), 0, "{scenario} staged a partial move");
                assert_eq!(tx.broker_publish_len(), 0, "{scenario} published an audit");
                assert_eq!(tx.lock_len(), 0, "{scenario} leaked row locks");
                tx.rollback().unwrap();
                assert_eq!(
                    db.get("documents", "doc-1").unwrap().unwrap().as_ref(),
                    &old
                );
                assert_eq!(
                    db.get("documents", "doc-2").unwrap().is_some(),
                    scenario == "occupied_destination"
                );
            }
            db.close().unwrap();
            for _ in 0..2 {
                let db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
                assert_eq!(
                    db.get("documents", "doc-1").unwrap().is_none(),
                    scenario == "success"
                );
                assert!(db
                    .verify_indexes_consistently()
                    .unwrap()
                    .iter()
                    .all(|r| r.valid));
                db.close().unwrap();
            }
        }
    }
}

#[test]
fn identity_update_refuses_a_destination_committed_after_its_snapshot() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            bicdb_core::DbConfig::default().with_storage_mode(mode),
        )
        .unwrap();
        db.create_collection("documents").unwrap();
        db.insert(
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "old"),
        )
        .unwrap();
        db.set_mutation_policy("documents", MutationPolicy::grants_required().with_audit())
            .unwrap();
        let mut stale = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        stale.set_snapshot_isolation(true, 0).unwrap();
        assert!(stale
            .get_for_integrity_check("documents", "doc-2")
            .unwrap()
            .is_none());
        let mut spec = grant(MutationOperation::Update, "doc-1", None);
        spec.record_id = None;
        let authority = stale.issue_mutation_grant(spec).unwrap();
        let mut peer = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        let insert = peer
            .issue_mutation_grant(grant(MutationOperation::Insert, "doc-2", None))
            .unwrap();
        peer.insert_with_grant(
            insert,
            "documents",
            record("doc-2", "tenant-a", "workspace-a", 1, "peer"),
        )
        .unwrap();
        peer.commit().unwrap();
        assert!(stale
            .update_identity_with_grant(
                authority,
                "documents",
                "doc-1",
                record("doc-2", "tenant-a", "workspace-a", 2, "forbidden"),
                0
            )
            .is_err());
        assert_eq!(stale.write_len(), 0);
        assert_eq!(stale.broker_publish_len(), 0);
        stale.rollback().unwrap();
        assert_eq!(
            db.get("documents", "doc-1").unwrap().unwrap().metadata["name"],
            "old"
        );
        assert_eq!(
            db.get("documents", "doc-2").unwrap().unwrap().metadata["name"],
            "peer"
        );
        db.close().unwrap();
    }
}

#[test]
fn failed_identity_update_restores_source_locks_audit_and_capability_budget() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            bicdb_core::DbConfig::default().with_storage_mode(mode),
        )
        .unwrap();
        db.create_collection("documents").unwrap();
        db.insert(
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "old"),
        )
        .unwrap();
        db.set_mutation_policy("documents", MutationPolicy::grants_required().with_audit())
            .unwrap();
        let mut tx = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        let mut spec = grant(MutationOperation::Update, "doc-1", None);
        spec.record_id = None;
        spec.allowed_columns.insert("vector".into());
        let authority = tx.issue_mutation_grant(spec).unwrap();
        let mut invalid = record("doc-2", "tenant-a", "workspace-a", 2, "new");
        invalid.vector = Some(vec![f32::NAN]);
        assert!(tx
            .update_identity_with_grant(authority, "documents", "doc-1", invalid, 0)
            .is_err());
        assert_eq!(tx.write_len(), 0);
        assert_eq!(tx.lock_len(), 0);
        assert_eq!(tx.broker_publish_len(), 0);
        tx.update_identity_with_grant(
            authority,
            "documents",
            "doc-1",
            record("doc-2", "tenant-a", "workspace-a", 2, "new"),
            0,
        )
        .unwrap();
        assert_eq!(tx.broker_publish_len(), 1);
        tx.commit().unwrap();
        assert!(db.get("documents", "doc-1").unwrap().is_none());
        assert_eq!(
            db.get("documents", "doc-2").unwrap().unwrap().metadata["name"],
            "new"
        );
        db.close().unwrap();
    }
}

#[test]
fn identity_update_cannot_use_a_caller_snapshot_to_override_native_preimage_versions() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            bicdb_core::DbConfig::default().with_storage_mode(mode),
        )
        .unwrap();
        db.create_collection("documents").unwrap();
        db.insert(
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 1, "original"),
        )
        .unwrap();
        db.set_mutation_policy("documents", MutationPolicy::grants_required().with_audit())
            .unwrap();
        let mut stale = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        stale.set_snapshot_isolation(true, 0).unwrap();
        assert_eq!(
            stale
                .get_for_integrity_check("documents", "doc-1")
                .unwrap()
                .unwrap()
                .metadata["version"],
            1
        );
        let mut spec = grant(MutationOperation::Update, "doc-1", Some(1));
        spec.record_id = None;
        let authority = stale.issue_mutation_grant(spec).unwrap();
        let mut peer = db
            .begin_application_transaction(actor("tenant-a", "workspace-a"))
            .unwrap();
        let peer_grant = peer
            .issue_mutation_grant(grant(MutationOperation::Update, "doc-1", Some(1)))
            .unwrap();
        peer.update_with_grant(
            peer_grant,
            "documents",
            record("doc-1", "tenant-a", "workspace-a", 2, "peer"),
        )
        .unwrap();
        peer.commit().unwrap();
        assert!(stale
            .update_identity_with_grant(
                authority,
                "documents",
                "doc-1",
                record("doc-2", "tenant-a", "workspace-a", 3, "forbidden"),
                u64::MAX
            )
            .is_err());
        assert_eq!(stale.write_len(), 0);
        assert_eq!(stale.broker_publish_len(), 0);
        stale.rollback().unwrap();
        assert_eq!(
            db.get("documents", "doc-1").unwrap().unwrap().metadata["name"],
            "peer"
        );
        assert!(db.get("documents", "doc-2").unwrap().is_none());
        db.close().unwrap();
    }
}
