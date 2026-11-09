//! Keep signed mutation authority action-specific while the ordinary primary
//! arbiter optimization evolves. Protected races are explicitly not repaired.
use bicdb_core::{
    BicDb, DbConfig, MutationActor, MutationGrantSpec, MutationOperation, MutationPolicy,
    StorageMode,
};
use bicdb_sql::SqlSession;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

fn actor() -> MutationActor {
    MutationActor {
        actor_id: "signed-user".into(),
        roles: BTreeSet::new(),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "test".into(),
        originating_resource: None,
        originating_action: None,
        trace_id: "upsert-authority".into(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 120_000,
    }
}
fn grant(id: &str) -> MutationGrantSpec {
    MutationGrantSpec {
        relation: "signed_upsert".into(),
        operation: MutationOperation::Insert,
        record_id: Some(id.into()),
        record_id_prefix: None,
        expected_version: None,
        version_field: None,
        allowed_columns: BTreeSet::from(["id".into(), "value".into()]),
        bulk: false,
        maximum_affected_rows: 1,
        cascade_relations: BTreeSet::new(),
        statement_budget: 1,
        tenant_field: None,
        workspace_field: None,
        audit_metadata: BTreeMap::new(),
    }
}

#[test]
fn insert_only_signed_grants_preserve_upsert_action_authorization() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for (id, action, allowed) in [
            ("fresh", "DO UPDATE SET value=EXCLUDED.value", true),
            ("existing", "DO UPDATE SET value=EXCLUDED.value", false),
            ("fresh", "DO NOTHING", true),
            ("existing", "DO NOTHING", true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                dir.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap();
            SqlSession::new(&mut db).execute("CREATE TABLE signed_upsert(id TEXT PRIMARY KEY, value INT); INSERT INTO signed_upsert VALUES('existing',20)").unwrap();
            db.set_mutation_policy("signed_upsert", MutationPolicy::grants_required())
                .unwrap();
            let mut tx = db.begin_application_transaction(actor()).unwrap();
            tx.set_snapshot_isolation(false, 0).unwrap();
            let authority = tx.issue_mutation_grant(grant(id)).unwrap();
            let mut session = SqlSession::new_shared(&db)
                .with_deferred_commit()
                .with_pending_transaction(tx)
                .with_signed_mutation_grants(BTreeMap::from([("signed_upsert".into(), authority)]));
            let result = session.execute(&format!(
                "INSERT INTO signed_upsert VALUES('{id}',10) ON CONFLICT(id) {action}"
            ));
            let tx = session.take_pending_transaction().unwrap();
            if allowed {
                result.unwrap();
                tx.commit().unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(
                    error.to_string().contains("Update"),
                    "actual UPDATE must require Update authority: {error}"
                );
                tx.rollback().unwrap();
            }
            drop(session);
            let mut session = SqlSession::new_shared(&db);
            let expected = if id == "fresh" { 2 } else { 1 };
            assert_eq!(
                session
                    .execute("SELECT id FROM signed_upsert")
                    .unwrap()
                    .rows
                    .len(),
                expected
            );
        }
    }
}
