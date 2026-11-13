//! Fresh actual mutation boundary, separate from Registered B0 proposal.
use super::*;
use crate::db::NativeRegisteredRemoteMutationScope;

/// Retains the SAME actual observations, not reconstructed metadata or DTOs.
/// It proves request liveness only; actual lexical guards/history/Installing
/// remain independent mandatory transaction conditions.
pub(crate) struct NativeRegisteredRemoteTransactionRequest {
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
    role: Arc<crate::distribution_range_recovery::RecoveryRoleAdmission>,
    own: Arc<HostMetadataReadObservation>,
    member: Arc<crate::distribution_consensus::NativeOwnedMetadataMemberObservation>,
    business: Uuid,
    phase: CancellationToken,
    source_request: CancellationToken,
}
impl NativeRegisteredRemoteTransactionRequest {
    pub(crate) fn preflight(&self) -> &NativeRegisteredRemoteInstallPreflight {
        &self.preflight
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.phase.check()?;
        self.source_request.check()?;
        self.role.require_live_request()?;
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.preflight.require_original_process()?;
        self.preflight.original.database.require_database(db)?;
        self.preflight.native().require_current(db)?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        self.member.require_database_point(db)?;
        self.phase.check()?;
        self.source_request.check()
    }
}
impl NativeRegisteredRemoteProposalPoint<'_> {
    pub(crate) fn frame(&self) -> &NativeRemoteImageStep<'_> {
        self.frame
    }
    pub(crate) fn require_mutation_boundary(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeRegisteredRemoteMutationScope<'_>,
    ) -> Result<()> {
        self.require_original_mutation_identity(db, store, scope)?;
        self.preflight
            .actual
            .require_mutation_boundary(db, scope, &self.request.member)?;
        self.require_live()
    }
    /// Necessary identity checks only. The actual base publisher separately
    /// proves either the unchanged preimage or its exact owned publication.
    pub(crate) fn require_original_mutation_identity(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeRegisteredRemoteMutationScope<'_>,
    ) -> Result<()> {
        self.require_live()?;
        scope.require_database(db)?;
        self.preflight.original.database.require_database(db)?;
        if scope.attempt_id() != self.preflight.attempt_id()
            || scope.range_id() != self.preflight.scope().range_id
            || store.native_writer_store_open_id() != self.preflight.original.store.id()
        {
            return Err(invalid(
                "Registered mutation changed original actual A/range/Store",
            ));
        }
        self.preflight
            .actual
            .require_mutation_identity(db, scope, &self.request.member)?;
        db.require_native_writer_catalogue_custody(store)?;
        self.require_live()
    }
    pub(crate) fn retain_transaction_request(
        &self,
    ) -> Result<NativeRegisteredRemoteTransactionRequest> {
        self.require_live()?;
        Ok(NativeRegisteredRemoteTransactionRequest {
            preflight: self.preflight.clone(),
            role: self.request.role.retain_original(),
            own: self.request.own.clone(),
            member: self.request.member.clone(),
            business: self.request.business,
            phase: self.request.cancellation().clone(),
            source_request: self.frame.retain_original_request_cancellation()?,
        })
    }
}
impl NativeMetadataControlHost {
    /// Borrow the original host's ACTUAL retained physical receipt. This is
    /// immutable evidence only; a later adoption must independently acquire
    /// its actual A/role/OWN/journal scope and call receipt.require_current.
    pub(crate) fn registered_remote_final_image_receipt(
        &self,
        range: crate::RangeId,
    ) -> Result<&Arc<crate::db::NativeRegisteredRemoteFinalImageReceipt>> {
        self.replica
            .as_ref()
            .and_then(|r| r.remote_images.get(&range))
            .and_then(|owner| owner.final_image_receipt())
            .ok_or_else(|| invalid("original Registered protected final image receipt absent"))
    }
    #[cfg(test)]
    pub(crate) fn test_registered_remote_image_state(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.replica
            .as_ref()
            .and_then(|receiver| receiver.remote_images.get(&range))
            .map(|owner| owner.test_state())
            .ok_or_else(|| invalid("Registered actual image owner absent"))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn advance_registered_remote_image(
        &mut self,
        source: &Arc<NativeRemoteImageFact>,
        frame: &NativeRemoteImageStep<'_>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        operation: crate::db::NativeRegisteredRemoteImageOperation,
    ) -> Result<crate::db::NativeRegisteredRemoteImageProgress> {
        parent.check()?;
        if self.assigned.is_some() || self.data.is_some() {
            return Err(invalid(
                "Registered image cannot replace existing DATA owner",
            ));
        }
        let receiver = self
            .replica
            .as_mut()
            .ok_or_else(|| invalid("actual Registered image receiver absent"))?;
        let range = role.role().current_range().id;
        let attempt = receiver
            .remote_attempts
            .get(&range)
            .ok_or_else(|| invalid("actual Registered attempt absent"))?;
        if !Arc::ptr_eq(source, attempt.preflight.source()) {
            return Err(invalid("Registered image changed exact original MF Arc"));
        }
        let publication = attempt
            .installing
            .as_ref()
            .ok_or_else(|| invalid("Registered image lacks retained Installing publication"))?;
        let fence = publication
            .fence()
            .cloned()
            .ok_or_else(|| invalid("Registered image catalogue Installing not proven durable"))?;
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Registered image has no actual A lease"))?;
        lease.require_retained()?;
        let request = receiver.admission.observe_remote_installing_request(
            frame,
            role,
            own,
            business,
            lease.try_as_frozen()?,
            parent,
            publication,
            &attempt.preflight,
        )?;
        attempt.preflight.require_metadata_current(&request)?;
        let point = NativeRegisteredRemoteProposalPoint {
            preflight: &attempt.preflight,
            lease,
            request: &request,
            frame,
        };
        point.require_live()?;
        let result = {
            let actual = receiver
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Registered image actual service busy"))?;
            let database = actual.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Registered image actual DB busy"))?;
            db.with_registered_remote_mutation_exclusion(
                attempt.preflight.native(),
                &receiver.admission.original_hook,
                lease.try_as_frozen()?,
                attempt.preflight.attempt_id(),
                request.cancellation(),
                |scope| {
                    actual.with_writer_data_domain_store(&db, |store| {
                        let context = crate::db::NativeRegisteredRemoteImageContext::new(
                            &db,
                            store,
                            &point,
                            scope,
                            &fence,
                            receiver
                                .remote_images
                                .get(&range)
                                .and_then(|owner| owner.base_boundary()),
                        )?;
                        if !receiver.remote_images.contains_key(&range) {
                            // Original receiver retains exact history generation and
                            // future row/Tx outcomes before the first native write.
                            receiver.remote_images.insert(
                                range,
                                crate::db::NativeRegisteredRemoteImageInstallation::retain(
                                    &context,
                                )?,
                            );
                        }
                        receiver
                            .remote_images
                            .get_mut(&range)
                            .unwrap()
                            .advance(&db, &context, operation)
                    })
                },
            )
        };
        let post = attempt
            .preflight
            .require_metadata_current(&request)
            .and_then(|()| point.require_live());
        // Automatic checkpoint can require admission.write. Defer until all
        // actual lexical admission/hook/journal and service guards are gone.
        let database = receiver
            .backend
            .service
            .try_lock()
            .map(|service| service.database());
        if let Some(database) = database {
            if let Some(db) = database.try_read() {
                db.auto_checkpoint_if_due();
            }
        }
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(progress) => {
                post?;
                parent.check()?;
                Ok(progress)
            }
        }
    }
}
