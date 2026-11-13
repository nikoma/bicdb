//! One authorized UPDATE may replace its physical address within one collection.
use super::*;

impl Transaction {
    /// Replace an existing row's Native ID using one UPDATE grant and audit.
    /// Both addresses must lie within the grant and `id` must be an allowed
    /// column. This never supplies authority for another collection or turns
    /// an INSERT/DELETE grant into UPDATE authority. The preimage is fetched
    /// under Native row locks, rather than accepted from the caller.
    pub fn update_identity_with_grant(
        &mut self,
        grant_id: MutationGrantId,
        collection: &str,
        previous_id: &str,
        record: Record,
        statement_snapshot: u64,
    ) -> Result<()> {
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        if previous_id.is_empty() || record.id.is_empty() {
            return Err(BicDbError::EmptyRecordId);
        }
        if previous_id == record.id {
            return Err(BicDbError::MutationDenied(
                "identity UPDATE requires two distinct addresses".into(),
            ));
        }
        self.validate_mutation_grant_operation(
            grant_id,
            collection,
            previous_id,
            MutationOperation::Update,
        )?;
        self.validate_mutation_grant_operation(
            grant_id,
            collection,
            &record.id,
            MutationOperation::Update,
        )?;
        let grant = self.mutation_grants[&grant_id].clone();
        if !grant.spec.allowed_columns.contains("id") {
            return Err(BicDbError::MutationDenied(
                "identity UPDATE requires explicit id column authority".into(),
            ));
        }
        let mark = self.rollback_mark();
        let result = (|| {
            self.bind_collection_incarnation(collection)?;
            let mut ids = [previous_id, record.id.as_str()];
            ids.sort_unstable();
            for id in ids {
                self.lock_row_for_write(self.id, collection, id, false)?;
            }
            let has_pending_source = self.last_pending_write(collection, previous_id).is_some();
            let native_boundary = self
                .observed_record_versions
                .lock()
                .get(collection)
                .and_then(|rows| rows.get(previous_id))
                .copied()
                .unwrap_or(self.applied_snapshot);
            let source_boundary = if statement_snapshot == 0 {
                native_boundary
            } else {
                statement_snapshot.min(native_boundary)
            };
            if !has_pending_source
                && Self::latest_committed_record_sequence(self.db_ref(), collection, previous_id)?
                    .is_some_and(|sequence| sequence > source_boundary)
            {
                return Err(BicDbError::TransactionConflict(
                    "identity UPDATE source changed beyond its Native observation".into(),
                ));
            }
            let before = if has_pending_source {
                self.get_visible_unchecked(collection, previous_id)?
            } else {
                self.db_ref().get_unchecked(collection, previous_id)?
            }
            .ok_or_else(|| {
                BicDbError::TransactionConflict(format!(
                    "identity UPDATE source `{collection}`/`{previous_id}` is absent"
                ))
            })?;
            // Check both this transaction's overlay and current committed
            // state. A stale snapshot is never proof of destination absence.
            if self
                .get_visible_unchecked(collection, &record.id)?
                .is_some()
                || (self.last_pending_write(collection, &record.id).is_none()
                    && self
                        .db_ref()
                        .get_unchecked(collection, &record.id)?
                        .is_some())
            {
                return Err(BicDbError::TransactionConflict(format!(
                    "identity UPDATE destination `{collection}`/`{}` exists",
                    record.id
                )));
            }
            let policy = self
                .db_ref()
                .mutation_policy(collection)?
                .unwrap_or_default();
            if policy.immutable_fields.contains("id") {
                return Err(BicDbError::CommitValidation(format!(
                    "Native id is immutable on `{collection}`"
                )));
            }
            for (field, expected, label) in [
                (
                    grant
                        .spec
                        .tenant_field
                        .as_ref()
                        .or(policy.tenant_field.as_ref()),
                    grant.actor.tenant_id.as_deref(),
                    "tenant",
                ),
                (
                    grant
                        .spec
                        .workspace_field
                        .as_ref()
                        .or(policy.workspace_field.as_ref()),
                    grant.actor.workspace_id.as_deref(),
                    "workspace",
                ),
            ] {
                validate_mutation_actor_field(collection, &before, field, expected, label)?;
            }
            // Bind normal UPDATE checks (changed fields, immutable values,
            // actor scope, optimistic version and budgets) to the real old
            // address. Address authority was checked separately for BOTH IDs.
            let mut authorization_row = record.clone();
            authorization_row.id = previous_id.into();
            let mut authorization = self.authorize_mutation_with_preimage(
                grant_id,
                collection,
                MutationOperation::Update,
                &[authorization_row],
                Some((previous_id, Some(before))),
            )?;
            self.delete_many_with_statement_snapshots_inner(
                collection,
                std::iter::once((previous_id, source_boundary)),
                false,
            )?;
            let next_id = record.id.clone();
            self.write_upserts_with_statement_snapshots_inner(
                collection,
                // A source observation cannot authorize a different address's
                // version. The target retains its own Native observation.
                std::iter::once((record, 0)),
                false,
            )?;
            if let Some(event) = authorization.audit.as_mut() {
                event.payload["replacement_record_ids"] = serde_json::json!([next_id.clone()]);
            }
            // Both physical writes belong to the same checked UPDATE issuer.
            // Keep the audit's original address while retaining the issuer on
            // the destination write for current-policy commit validation too.
            authorization.record_ids.push(next_id);
            self.enqueue_mutation_audit(authorization);
            Ok(())
        })();
        if result.is_err() {
            self.rollback_to_mark(mark)?;
            self.mutation_grants.insert(grant_id, grant);
        }
        result
    }
}

#[cfg(test)]
mod issuer_tests {
    use super::*;
    use crate::MutationPolicy;

    fn actor() -> MutationActor {
        MutationActor {
            actor_id: "identity-owner-control".into(),
            roles: BTreeSet::new(),
            scopes: BTreeSet::new(),
            tenant_id: None,
            workspace_id: None,
            originating_plugin: "actor-field-is-not-an-issuer".into(),
            originating_resource: None,
            originating_action: None,
            trace_id: "identity-owner-control".into(),
            deadline_unix_ms: 4_102_444_800_000,
        }
    }

    fn grant() -> MutationGrantSpec {
        MutationGrantSpec {
            relation: "documents".into(),
            operation: MutationOperation::Update,
            record_id: None,
            record_id_prefix: Some("row-".into()),
            expected_version: None,
            version_field: None,
            allowed_columns: BTreeSet::from(["id".into(), "value".into()]),
            bulk: false,
            maximum_affected_rows: 1,
            cascade_relations: BTreeSet::new(),
            statement_budget: 1,
            tenant_field: None,
            workspace_field: None,
            audit_metadata: BTreeMap::from([("reason".into(), "identity-owner-control".into())]),
        }
    }

    #[test]
    fn identity_update_retains_both_native_write_issuers_and_rollback_removes_them() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default().with_fsync(true).with_storage_mode(mode);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.create_collection("documents").unwrap();
            db.insert(
                "documents",
                Record::new("row-old").with_metadata(json!({"value":1})),
            )
            .unwrap();
            db.insert("documents", Record::new("independent-ack"))
                .unwrap();
            let ack = db
                .get("documents", "independent-ack")
                .unwrap()
                .unwrap()
                .content_hash()
                .unwrap();
            db.set_mutation_policy("documents", MutationPolicy::grants_required().with_audit())
                .unwrap();
            let mut tx = db.begin_application_transaction(actor()).unwrap();
            let mark = tx.rollback_mark();
            let authority = tx
                .issue_application_mutation_grant(grant(), "validated-package".into())
                .unwrap();
            tx.update_identity_with_grant(
                authority,
                "documents",
                "row-old",
                Record::new("row-new").with_metadata(json!({"value":2})),
                0,
            )
            .unwrap();
            assert_eq!(tx.native_write_issuers.lock().len(), 2);
            for (index, issuer) in tx.native_write_issuers.lock().iter() {
                assert_eq!(issuer.as_deref(), Some("validated-package"));
                assert!(matches!(
                    tx.writes[*index].record_id.as_str(),
                    "row-old" | "row-new"
                ));
            }
            assert_eq!(tx.pending_broker_publishes.lock().len(), 1);
            tx.rollback_to_mark(mark).unwrap();
            assert!(tx.native_write_issuers.lock().is_empty());
            assert!(tx.pending_broker_publishes.lock().is_empty());
            let authority = tx
                .issue_application_mutation_grant(grant(), "validated-package".into())
                .unwrap();
            tx.update_identity_with_grant(
                authority,
                "documents",
                "row-old",
                Record::new("row-new").with_metadata(json!({"value":2})),
                0,
            )
            .unwrap();
            tx.commit().unwrap();
            db.close().unwrap();
            for _ in 0..2 {
                let db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                assert!(db.get("documents", "row-old").unwrap().is_none());
                assert_eq!(
                    db.get("documents", "row-new").unwrap().unwrap().metadata["value"],
                    json!(2)
                );
                assert_eq!(
                    db.get("documents", "independent-ack")
                        .unwrap()
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    ack
                );
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn identity_update_never_mutates_an_immutable_native_fact_even_for_its_owner() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                root.path(),
                DbConfig::default().with_fsync(true).with_storage_mode(mode),
            )
            .unwrap();
            db.create_collection("documents").unwrap();
            db.insert("documents", Record::new("row-old")).unwrap();
            let mut policy = MutationPolicy::grants_required();
            policy.native_fact_reference_owner = Some("fact-owner".into());
            db.set_mutation_policy("documents", policy).unwrap();
            let sequence = db.last_commit_seq();
            for issuer in ["fact-owner", "other-package"] {
                let mut tx = db.begin_application_transaction(actor()).unwrap();
                let authority = tx
                    .issue_application_mutation_grant(grant(), issuer.into())
                    .unwrap();
                assert!(matches!(
                    tx.update_identity_with_grant(
                        authority,
                        "documents",
                        "row-old",
                        Record::new("row-new"),
                        0
                    ),
                    Err(BicDbError::MutationDenied(_))
                ));
                assert!(tx.writes.is_empty());
                assert!(tx.native_write_issuers.lock().is_empty());
                tx.rollback().unwrap();
            }
            assert_eq!(db.last_commit_seq(), sequence);
            assert!(db.get("documents", "row-old").unwrap().is_some());
            assert!(db.get("documents", "row-new").unwrap().is_none());
            db.close().unwrap();
        }
    }
}
