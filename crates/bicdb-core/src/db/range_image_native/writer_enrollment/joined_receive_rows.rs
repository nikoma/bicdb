//! Resumable selected-row constraint WITHOUT the global native journal,
//! commit-admission or hook read guard around the actual paged scan. Selected
//! Frozen and schema/backing identity, not a global B watermark, stabilize it.
use super::*;
use crate::distribution_native_host::{
    NativeJoinedRangeReceiveOrigin, NativeJoinedRangeReceiveRequest,
};
use crate::distribution_range_consensus::RecoveryFenceScope;

pub(crate) struct NativeJoinedRowAbsenceScan {
    origin: Arc<NativeJoinedRangeReceiveOrigin>,
    scope: RecoveryFenceScope,
    frozen: Weak<FrozenRangeMutationPermit>,
    bindings: RangeImageDbBindings,
    collection_index: usize,
    after: Option<String>,
    complete: bool,
}
impl NativeJoinedRowAbsenceScan {
    /// No absence is inferred from this constructor. The caller retains this
    /// exact bounded cursor; a larger unrelated range must not restart at zero.
    pub(crate) fn begin_local(
        db: &BicDb,
        request: &NativeJoinedRangeReceiveRequest,
        frozen: &Arc<FrozenRangeMutationPermit>,
    ) -> Result<Self> {
        request.require_live()?;
        let scan = Self {
            origin: request.origin_arc().clone(),
            scope: RecoveryFenceScope::from_role(request.role().role())?,
            frozen: Arc::downgrade(frozen),
            bindings: db.capture_range_image_bindings()?,
            collection_index: 0,
            after: None,
            complete: false,
        };
        scan.require_request(db, request, frozen)?;
        Ok(scan)
    }
    fn require_request(
        &self,
        db: &BicDb,
        request: &NativeJoinedRangeReceiveRequest,
        frozen: &Arc<FrozenRangeMutationPermit>,
    ) -> Result<()> {
        request.require_live()?;
        let current = RecoveryFenceScope::from_role(request.role().role())?;
        if !Arc::ptr_eq(&self.origin, request.origin_arc())
            || !self.scope.same_session(&current)
            || !Weak::ptr_eq(&self.frozen, &Arc::downgrade(frozen))
            || self.frozen.upgrade().is_none()
        {
            return Err(invalid(
                "Joined selected scan replaced original identity/session/Frozen",
            ));
        }
        self.origin.guard().require_joined_receive_identity(db)?;
        db.validate_range_image_bindings(&self.bindings)?;
        db.require_joined_publication_storage_health()?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.scope.range_id, frozen)?;
        request.member().require_database(db)?;
        request
            .member()
            .publication_version()
            .require_current_memory()?;
        request.require_live()
    }
    /// LOCAL half only: actual MStore/runtime checks must surround the DB
    /// borrow in the Joined request wrapper, never run under this DB guard.
    /// One bounded chunk. New real Role/OWN requests may continue the SAME
    /// session/cursor; expired observations are never renewed. No metadata,
    /// service, global native journal or commit-admission/hook guard is held
    /// by this implementation while the actual scanner performs record IO.
    pub(crate) fn advance_local(
        &mut self,
        db: &BicDb,
        request: &NativeJoinedRangeReceiveRequest,
        frozen: &Arc<FrozenRangeMutationPermit>,
    ) -> Result<bool> {
        request.require_live()?;
        self.require_request(db, request, frozen)?;
        if self.complete {
            return Ok(true);
        }
        if self.collection_index == self.bindings.collections().len() {
            self.complete = true;
            return Ok(true);
        }
        let collection = &self.bindings.collections()[self.collection_index];
        let progress = db.for_each_range_snapshot_batch_with_cancellation(
            &collection.name,
            request.role().role().current_range(),
            self.after.as_deref(),
            &RangeSnapshotOptions {
                max_records_per_batch: 1,
                max_bytes_per_batch: 8 * 1024 * 1024,
                max_record_bytes: 8 * 1024 * 1024,
            },
            1024,
            request.cancellation(),
            |_batch| {
                Err(invalid(
                    "additional Joined selected range already has physical rows",
                ))
            },
        )?;
        // Do not advance this real cursor after a stale/changed postcheck.
        // Repeating the same absent chunk is safe; skipping it is not.
        self.require_request(db, request, frozen)?;
        request.require_live()?;
        if progress.completed {
            self.collection_index += 1;
            self.after = None;
            self.complete = self.collection_index == self.bindings.collections().len();
        } else {
            let after = progress.resume_after_key.ok_or_else(|| {
                invalid("Joined selected scan stopped without an actual resume cursor")
            })?;
            if self.after.as_ref().is_some_and(|old| &after <= old) {
                return Err(invalid(
                    "Joined selected absence scan made no monotonic cursor progress",
                ));
            }
            self.after = Some(after);
        }
        Ok(self.complete)
    }
    /// Only a necessary selected-row absence constraint. The fresh selected
    /// native/raw/slot obligation producer is still independently mandatory.
    pub(crate) fn require_complete_local(
        &self,
        db: &BicDb,
        request: &NativeJoinedRangeReceiveRequest,
        frozen: &Arc<FrozenRangeMutationPermit>,
    ) -> Result<()> {
        request.require_live()?;
        self.require_request(db, request, frozen)?;
        if !self.complete
            || self.collection_index != self.bindings.collections().len()
            || self.after.is_some()
        {
            return Err(invalid(
                "Joined selected row absence scan remains incomplete",
            ));
        }
        Ok(())
    }
}
