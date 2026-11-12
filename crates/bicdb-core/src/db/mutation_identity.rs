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
            let mut audit = self.authorize_mutation_with_preimage(
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
            if let Some(event) = audit.as_mut() {
                event.payload["replacement_record_ids"] = serde_json::json!([next_id]);
            }
            self.enqueue_mutation_audit(audit);
            Ok(())
        })();
        if result.is_err() {
            self.rollback_to_mark(mark)?;
            self.mutation_grants.insert(grant_id, grant);
        }
        result
    }
}
