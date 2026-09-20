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
