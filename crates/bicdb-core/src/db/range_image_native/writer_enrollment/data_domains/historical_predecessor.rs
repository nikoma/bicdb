//! Original pre-close Serving provenance. This owner is neither a current
//! Serving grant nor reconstructible from persisted DATA after closure.
use super::*;
use crate::distribution_range_handoff::NativeJointHandoffPlan;

pub(crate) struct NativeWriterHistoricalPredecessor {
    writer: Arc<NativeWriterHostState>,
    plan_sha256: String,
    range: RangeDescriptor,
    profile: Profile,
}
enum Profile {
    Born,
    Serving {
        actual: Arc<VerifiedServingDomain>,
        closed: ClosedDomainRecord,
    },
}
impl NativeWriterHistoricalPredecessor {
    /// Exact original allocation only, before a retained caller may initiate
    /// constraints/freeze/closure. Equal DATA does not join a second capture.
    pub(crate) fn require_same_owner(&self, candidate: &Self) -> Result<()> {
        if !std::ptr::eq(self, candidate) {
            return Err(invalid(
                "OLD admission changed original pre-freeze predecessor owner; retry outside owner",
            ));
        }
        Ok(())
    }
    /// Full original database identity for pre/post OLD-step validation. This
    /// is not called while a caller already holds the commit-hook guard.
    pub(crate) fn require_original_database(
        &self,
        db: &BicDb,
        plan: &NativeJointHandoffPlan,
    ) -> Result<()> {
        self.writer.require_original_identity_database(db)?;
        self.writer.require_verified_registry_database(db)?;
        self.require_historical(db, &self.writer, plan)
    }
    /// Historical integrity only. The caller still supplies its genuine OLD
    /// close/promise/Frozen and independently fresh phase authority.
    pub(crate) fn require_historical(
        &self,
        db: &BicDb,
        writer: &NativeWriterHostState,
        plan: &NativeJointHandoffPlan,
    ) -> Result<()> {
        if !std::ptr::eq(self.writer.as_ref(), writer)
            || self.plan_sha256 != plan.checksum_sha256()?
            || self.range != plan.old_native_predecessor_data()?
        {
            return Err(invalid(
                "historical predecessor changed original State/plan/range",
            ));
        }
        writer.require_lifetime()?;
        match &self.profile {
            Profile::Born => writer.record.facts.require_range(&self.range),
            Profile::Serving { actual, closed } => {
                let record = &actual.record;
                record.validate(&writer.record)?;
                closed.validate(&writer.record)?;
                if record.range != self.range
                    || closed.checksum_sha256 != record.closed_sha256
                    || closed.decision.checksum_sha256() != record.decision_sha256
                    || closed.decision.activation_range() != &record.range
                    || closed.local != record.local
                {
                    return Err(invalid(
                        "historical Serving predecessor changed original CLOSED cut",
                    ));
                }
                if !crate::format::native_writer_data_domain_active(db.data_path())? {
                    return Err(invalid(
                        "historical Serving predecessor lost sticky reader fence",
                    ));
                }
                // These immutable archives were read together with their real
                // CURRENTs before OLD closure. New CLOSED/Serving CURRENTs may
                // advance later; they must never replace this retained proof.
                let persisted: ServingDomainRecord = db
                    .read_native_range_image_record(&archive(
                        writer.record.generation,
                        record.sequence,
                    ))?
                    .ok_or_else(|| invalid("historical Serving predecessor archive missing"))?;
                let prior_closed: ClosedDomainRecord = db
                    .read_native_range_image_record(&archive(
                        writer.record.generation,
                        closed.sequence,
                    ))?
                    .ok_or_else(|| {
                        invalid("historical Serving CLOSED predecessor archive missing")
                    })?;
                if persisted != *record || prior_closed != *closed {
                    return Err(invalid(
                        "historical Serving predecessor immutable archive changed",
                    ));
                }
                require_original_metadata_store(db, closed)?;
                require_serving_quorum_audit(record, &closed.decision, &writer.record)?;
                let lineage = db.current_native_range_source_lineage(record.range.id)?;
                if lineage.base.checksum_sha256 != record.local.physical_base_sha256
                    || lineage.base.local_namespace_ids != record.local.namespace_ids
                    || lineage.history_generation() != record.local.history_generation
                    || lineage.schema_sha256() != record.local.schema_sha256
                    || lineage.logical_genesis_sha256() != record.local.logical_genesis_sha256
                    || lineage.range_index() < record.local.range_index
                    || lineage.actual_local_visibility_sequence()
                        < record.local.local_visibility_sequence
                    || db
                        .read_native_history_entry(
                            record.range.id,
                            record.local.history_generation,
                            record.local.range_index,
                        )?
                        .commitment_sha256
                        != record.local.logical_history_sha256
                {
                    return Err(invalid(
                        "historical Serving predecessor native base/UID/history changed",
                    ));
                }
                Ok(())
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn test_evidence(&self) -> serde_json::Value {
        match &self.profile {
            Profile::Born => {
                serde_json::json!({"profile":"actual-preclose-born", "plan_sha256":self.plan_sha256,"range":self.range})
            }
            Profile::Serving { actual, closed } => {
                serde_json::json!({"profile":"actual-preclose-Serving", "plan_sha256":self.plan_sha256,"range":self.range,"actual_serving_record":actual.record,"actual_closed_record":closed})
            }
        }
    }
}
impl BicDb {
    /// Actual producer, invoked before the first actual range freeze or OLD promise/native put. No
    /// cold loader, serialized parts constructor or post-close fallback exists.
    pub(crate) fn capture_writer_historical_predecessor(
        &self,
        writer: Arc<NativeWriterHostState>,
        plan: &NativeJointHandoffPlan,
    ) -> Result<Arc<NativeWriterHistoricalPredecessor>> {
        let _admission = self
            .serializable_commit_admission
            .try_read()
            .ok_or_else(|| invalid("pre-close historical predecessor admission busy"))?;
        let _journal = self.try_native_image_journal()?;
        let range = plan.old_native_predecessor_data()?;
        writer.require_database(self, &range)?;
        let actual = {
            let domains = writer
                .data_domains
                .try_read()
                .ok_or_else(|| invalid("pre-close historical predecessor policy slots busy"))?;
            if domains.pending.contains(&range.id) {
                return Err(invalid(
                    "pre-close predecessor cannot start after CLOSED or uncertain publication",
                ));
            }
            domains.serving.get(&range.id).cloned()
        };
        let profile = match actual {
            None => {
                writer.record.facts.require_range(&range)?;
                Profile::Born
            }
            Some(actual) => {
                require_serving_record_current(self, &writer, &actual.record)?;
                if actual.record.range != range
                    || actual.continuity.released_generation()? != actual.record.released_generation
                {
                    return Err(invalid(
                        "pre-close predecessor changed actual Serving owner/open generation",
                    ));
                }
                let closed: ClosedDomainRecord = self
                    .read_native_range_image_record(&closed_current(
                        writer.record.generation,
                        range.id,
                    ))?
                    .ok_or_else(|| {
                        invalid("pre-close Serving predecessor CLOSED record missing")
                    })?;
                Profile::Serving { actual, closed }
            }
        };
        let proof = Arc::new(NativeWriterHistoricalPredecessor {
            writer,
            plan_sha256: plan.checksum_sha256()?,
            range,
            profile,
        });
        proof.require_historical(self, &proof.writer, plan)?;
        Ok(proof)
    }
}
