//! A private SAME-State allocation projection, never Ready or IO authority.
//! Full committed metadata (including its learner) and the effective native
//! voter descriptor remain separate facts. Nothing here is serializable.
use super::*;

pub(in crate::db::range_image_native) struct NativeAllocationServingDomain {
    writer: Arc<NativeWriterHostState>,
    audit_identity: (u32, u64, String),
    metadata: RangeDescriptor,
    effective_native: RangeDescriptor,
}
fn canonical(range: &RangeDescriptor) -> RangeDescriptor {
    crate::distribution_native_read::canonical_range(range)
}
impl NativeWriterHostState {
    fn require_allocation_projection_origin(
        self: &Arc<Self>,
        db: &BicDb,
        metadata: &RangeDescriptor,
    ) -> Result<()> {
        let original = db.commit_admission.read().native_writer().cloned();
        if original
            .as_ref()
            .is_none_or(|state| !Arc::ptr_eq(state, self))
        {
            return Err(invalid(
                "allocation serving projection requires SAME installed writer State",
            ));
        }
        // Necessary pure comparison FIRST. A malformed comparison claim must
        // not invoke inherited verbose mismatch diagnostics under a journal.
        // This cache alone grants nothing: the original full validator below
        // still checks actual DB/open/schema/registration/health/pending/audit.
        {
            let cache = self.registry_extension.read();
            if cache
                .as_ref()
                .and_then(|v| v.facts().ranges.get(&metadata.id))
                != Some(&canonical(metadata))
            {
                return Err(invalid(
                    "allocation serving projection differs from actual full metadata range",
                ));
            }
        }
        self.require_allocation_native_range(db, metadata)
    }
    pub(in crate::db::range_image_native) fn allocation_serving_domain(
        self: &Arc<Self>,
        db: &BicDb,
        metadata: &RangeDescriptor,
    ) -> Result<NativeAllocationServingDomain> {
        let audit_identity = self.allocation_projection_audit_identity()?;
        self.require_allocation_projection_origin(db, metadata)?;
        if self.allocation_projection_audit_identity()? != audit_identity {
            return Err(invalid(
                "allocation projection verified audit changed during full validation",
            ));
        }
        if metadata
            .replicas
            .iter()
            .filter(|r| r.role == crate::RangeReplicaRole::Learner)
            .count()
            != 1
            || metadata.replicas.iter().any(|r| {
                !matches!(
                    r.role,
                    crate::RangeReplicaRole::Voter | crate::RangeReplicaRole::Learner
                )
            })
        {
            return Err(invalid(
                "allocation serving projection needs its exact single learner descriptor",
            ));
        }
        let metadata = canonical(metadata);
        let mut effective_native = metadata.clone();
        effective_native
            .replicas
            .retain(|r| r.role == crate::RangeReplicaRole::Voter);
        if effective_native.replicas.is_empty() {
            return Err(invalid(
                "allocation serving projection has no native voters",
            ));
        }
        Ok(NativeAllocationServingDomain {
            writer: self.clone(),
            audit_identity,
            metadata,
            effective_native,
        })
    }
}
impl NativeAllocationServingDomain {
    // Necessary memory-only recheck for immediate use of THIS already fully
    // validated projection. Existing native/Prepared/Role scopes remain the
    // physical publication authority; this is not a standalone grant.
    fn require_current(&self, db: &BicDb) -> Result<()> {
        let installed = db
            .commit_admission
            .try_read()
            .ok_or_else(|| invalid("allocation projection installed hook busy"))?;
        let registration = self
            .writer
            .registration
            .upgrade()
            .ok_or_else(|| invalid("allocation projection registration ended"))?;
        let actual = registration
            .authority()
            .ok_or_else(|| invalid("allocation projection authority ended"))?;
        let coordinator = self
            .writer
            .coordinator
            .upgrade()
            .ok_or_else(|| invalid("allocation projection coordinator ended"))?;
        let health = self
            .writer
            .health
            .upgrade()
            .ok_or_else(|| invalid("allocation projection health ended"))?;
        if installed
            .native_writer()
            .is_none_or(|state| !Arc::ptr_eq(state, &self.writer))
            || !Arc::ptr_eq(installed.registration(), &registration)
            || !registration.is_active()
            || registration.generation() != self.writer.record.generation
            || !Arc::ptr_eq(&actual, &coordinator)
            || db.incarnation_id() != self.writer.record.database_incarnation
            || self.writer.directory.upgrade().is_none()
            || self.writer.schema.generation() != self.writer.schema_generation
            || health.is_failed()
            || self
                .writer
                .registry_publication_pending
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(invalid(
                "allocation projection SAME-State memory lifetime changed",
            ));
        }
        if self.writer.allocation_projection_audit_identity()? != self.audit_identity {
            return Err(invalid(
                "allocation projection original verified audit identity changed",
            ));
        }
        let cache = self
            .writer
            .registry_extension
            .try_read()
            .ok_or_else(|| invalid("allocation projection original audit busy"))?;
        if cache
            .as_ref()
            .and_then(|v| v.facts().ranges.get(&self.metadata.id))
            != Some(&self.metadata)
        {
            return Err(invalid(
                "allocation projection original full domain changed",
            ));
        }
        Ok(())
    }
    pub(in crate::db::range_image_native) fn effective_native_range(
        &self,
        db: &BicDb,
    ) -> Result<&RangeDescriptor> {
        self.require_current(db)?;
        Ok(&self.effective_native)
    }
    pub(in crate::db::range_image_native) fn require_native_match(
        &self,
        db: &BicDb,
        native: &RangeDescriptor,
    ) -> Result<()> {
        self.require_current(db)?;
        if canonical(native) != self.effective_native {
            return Err(invalid(
                "allocation serving projection differs from actual effective native range",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    // No Store is fabricated for activation's already-protected native read.
    // The actual installed DB State is selected privately and never exported.
    pub(in crate::db::range_image_native) fn require_actual_allocation_native_serving_match(
        &self,
        metadata: &RangeDescriptor,
        native: &RangeDescriptor,
    ) -> Result<()> {
        let writer = self
            .commit_admission
            .read()
            .native_writer()
            .cloned()
            .ok_or_else(|| {
                invalid("allocation serving projection has no actual installed writer State")
            })?;
        writer
            .allocation_serving_domain(self, metadata)?
            .require_native_match(self, native)
    }
}

#[cfg(test)]
impl InstalledNativeWriterHostGuard {
    pub(crate) fn test_allocation_activation_domain_comparison(
        &self,
        db: &BicDb,
        range: crate::RangeId,
        mode: u8,
    ) -> Result<(
        Result<()>,
        [u64; 3],
        [u64; 3],
        RangeDescriptor,
        RangeDescriptor,
        RangeDescriptor,
        RangeDescriptor,
        String,
        String,
        String,
        [u64; 2],
    )> {
        db.test_allocation_activation_domain_comparison(&self.state, range, mode)
    }
}
