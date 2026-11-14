//! Host-owned original additional-range attempt. Retain actual A immediately;
//! no re-enrollment, FIRST substitution, reset-on-error or Drop-to-N exists.
use super::*;
use crate::distribution_range_consensus::{
    NativeJoinedRemoteInstallingFence, NativeJoinedRemoteInstallingPublication,
};
use crate::distribution_range_image::NativeRemoteImageStep;
use crate::distribution_range_recovery::JoinedRemoteImageInstallFreeze;

pub(in super::super) struct NativeJoinedRemoteInstallAttempt {
    pub(super) preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    pub(super) lease: Option<JoinedRemoteImageInstallFreeze>,
    proposal: Option<crate::db::NativeJoinedRemoteProposalPublication>,
    pub(super) installing: Option<NativeJoinedRemoteInstallingPublication>,
    pub(super) image: Option<crate::db::NativeJoinedRemoteImageInstallation>,
    pub(super) catalogue:
        Option<crate::distribution_range_consensus::NativeJoinedRemoteCataloguePublication>,
    pub(super) materialized: Option<Arc<super::materialized::NativeJoinedRemoteMaterializedOwner>>,
    pub(super) joint: Option<Arc<super::joint_owner::NativeJoinedRemoteJointOwner>>,
}
impl NativeJoinedRangeReceiveOrigin {
    pub(super) fn require_host(&self, host: &NativeJoinedDataHost) -> Result<()> {
        host.require_writer_data_owner()?;
        self.require_live()?;
        if !std::sync::Weak::ptr_eq(&self.life, &Arc::downgrade(&host.life))
            || !std::sync::Weak::ptr_eq(&self.runtime, &Arc::downgrade(&host.parts.runtime))
            || !std::sync::Weak::ptr_eq(&self.backend, &Arc::downgrade(&host.parts.backend))
            || !Arc::ptr_eq(self.guard(), &host.enrollment.as_ref().unwrap().guard)
        {
            return Err(invalid("Joined receive changed original actual host"));
        }
        Ok(())
    }
}
impl NativeMetadataControlHost {
    pub(super) fn joined_receive_host_mut(&mut self) -> Result<&mut NativeJoinedDataHost> {
        if self.data.is_some() || self.assigned.is_some() || self.replica.is_some() {
            return Err(invalid(
                "Joined receive requires its exclusive actual DATA owner",
            ));
        }
        self.joined
            .as_mut()
            .ok_or_else(|| invalid("actual Joined DATA owner absent"))
    }
    /// Retain exact preflight before calling the genuine runtime transfer.
    /// Every error after transfer preserves both host and runtime A custody.
    pub(crate) fn prepare_joined_remote_install_attempt(
        &mut self,
        preflight: Arc<NativeJoinedRemoteInstallPreflight>,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        preflight.require_metadata_current(request)?;
        preflight.require_request(request, frame)?;
        let host = self.joined_receive_host_mut()?;
        preflight.selected().origin().require_host(host)?;
        let range = preflight.scope().range_id;
        if host
            .remote_attempts
            .get(&range)
            .is_some_and(|old| !Arc::ptr_eq(&old.preflight, &preflight))
        {
            return Err(invalid(
                "Joined retry cannot replace original selected preflight",
            ));
        }
        let runtime = host.parts.runtime.clone();
        let attempt =
            host.remote_attempts
                .entry(range)
                .or_insert_with(|| NativeJoinedRemoteInstallAttempt {
                    preflight: preflight.clone(),
                    lease: None,
                    proposal: None,
                    installing: None,
                    image: None,
                    catalogue: None,
                    materialized: None,
                    joint: None,
                });
        if attempt.joint.is_some() {
            return Err(invalid(
                "Joined original A belongs to its retained joint continuation",
            ));
        }
        if attempt.lease.is_none() {
            let lease =
                runtime.transfer_joined_remote_install(preflight, request, frame, parent)?;
            // No fallible operation between actual A return and host retention.
            attempt.lease = Some(lease);
        }
        attempt.lease.as_ref().unwrap().require_retained()?;
        attempt.preflight.require_metadata_current(request)?;
        parent.check()
    }
    pub(crate) fn publish_joined_remote_proposal(
        &mut self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJoinedRemoteProposalPointer> {
        parent.check()?;
        let host = self.joined_receive_host_mut()?;
        request.origin().require_host(host)?;
        let attempt = host
            .remote_attempts
            .get_mut(&request.origin().range_id())
            .ok_or_else(|| invalid("Joined original prepared attempt absent"))?;
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Joined actual A lease absent"))?;
        lease.require_retained()?;
        let preflight = attempt.preflight.clone();
        let result = preflight.with_attempt_scope(request, frame, lease, |db, store, scope| {
            if attempt.proposal.is_none() {
                attempt.proposal = Some(crate::db::NativeJoinedRemoteProposalPublication::begin(
                    db, store, scope,
                )?);
            }
            // Exact owner was retained before its first native put.
            attempt.proposal.as_mut().unwrap().advance(db, store, scope)
        });
        let value = result?;
        parent.check()?;
        Ok(value)
    }
    pub(crate) fn publish_joined_remote_installing(
        &mut self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJoinedRemoteInstallingFence>> {
        parent.check()?;
        let host = self.joined_receive_host_mut()?;
        request.origin().require_host(host)?;
        let backend = host.parts.backend.clone();
        let factory = host.parts.factory.clone();
        let attempt = host
            .remote_attempts
            .get_mut(&request.origin().range_id())
            .ok_or_else(|| invalid("Joined original prepared attempt absent"))?;
        let preflight = attempt.preflight.clone();
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Joined actual A lease absent"))?;
        lease.require_retained()?;
        preflight.require_metadata_current(request)?;
        preflight.require_request(request, frame)?;
        let receipt = attempt
            .proposal
            .as_ref()
            .and_then(|p| p.receipt())
            .ok_or_else(|| invalid("Joined Installing needs actual verified protected proposal"))?;
        let result = {
            // Upgrade exact originals outside any final publication guard.
            let _original = preflight.selected().origin().original()?;
            let _admission = preflight.selected().origin().admission()?;
            let mut service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined Installing original service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined Installing changed original database"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined Installing original DB busy"))?;
            db.with_joined_remote_attempt_scope(&preflight, request, frame, lease, |scope| {
                service.persist_joined_remote_installing(
                    &db,
                    scope,
                    receipt,
                    &mut attempt.installing,
                )
            })
        };
        let post = preflight
            .require_metadata_current(request)
            .and_then(|()| lease.require_retained())
            .and_then(|()| parent.check());
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(fence) => {
                post?;
                Ok(fence)
            }
        }
    }
}

/// Borrowed real host attempt, not a Manifest/DTO constructor.
pub(crate) struct NativeJoinedRemoteImagePoint<'a> {
    pub(super) preflight: &'a Arc<NativeJoinedRemoteInstallPreflight>,
    pub(super) lease: &'a JoinedRemoteImageInstallFreeze,
    pub(super) request: &'a NativeJoinedRangeReceiveRequest,
    pub(super) frame: &'a NativeRemoteImageStep<'a>,
}
impl NativeJoinedRemoteImagePoint<'_> {
    pub(crate) fn preflight(&self) -> &NativeJoinedRemoteInstallPreflight {
        self.preflight
    }
    pub(crate) fn preflight_arc(&self) -> &Arc<NativeJoinedRemoteInstallPreflight> {
        self.preflight
    }
    pub(crate) fn frame(&self) -> &NativeRemoteImageStep<'_> {
        self.frame
    }
    pub(crate) fn request(&self) -> &NativeJoinedRangeReceiveRequest {
        self.request
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.preflight.require_request(self.request, self.frame)?;
        if !Arc::ptr_eq(self.preflight, self.lease.preflight())
            || self.lease.try_as_frozen()?.owner() != self.preflight.attempt_id()
        {
            return Err(invalid("Joined image point changed exact retained A"));
        }
        Ok(())
    }
    pub(crate) fn retain_transaction_request(
        &self,
    ) -> Result<NativeJoinedRemoteTransactionRequest> {
        self.require_live()?;
        Ok(NativeJoinedRemoteTransactionRequest {
            preflight: self.preflight.clone(),
            role: self.request.role.role().retain_original(),
            own: self.request.own.clone(),
            member: self.request.member.clone(),
            business: self.request.business,
            phase: self.request.phase.clone(),
            source_request: self.frame.retain_original_request_cancellation()?,
        })
    }
}
/// Same actual finite request observations; no serializable constructor,
/// deadline renewal, ordinary writer scope or independent phase permission.
pub(crate) struct NativeJoinedRemoteTransactionRequest {
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    role: Arc<crate::distribution_range_recovery::RecoveryRoleAdmission>,
    own: Arc<HostMetadataReadObservation>,
    member: Arc<NativeOwnedMetadataMemberObservation>,
    business: Uuid,
    phase: CancellationToken,
    source_request: CancellationToken,
}
impl NativeJoinedRemoteTransactionRequest {
    pub(crate) fn preflight(&self) -> &NativeJoinedRemoteInstallPreflight {
        &self.preflight
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.phase.check()?;
        self.source_request.check()?;
        self.role.require_live_request()?;
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.preflight.require_original_process()?;
        self.preflight
            .selected()
            .origin()
            .database
            .require_database(db)?;
        self.preflight.native().require_current(db)?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        self.member.require_database_point(db)?;
        self.member.publication_version().require_current_memory()?;
        self.phase.check()?;
        self.source_request.check()
    }
}

impl NativeMetadataControlHost {
    pub(crate) fn advance_joined_remote_image(
        &mut self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
        operation: crate::db::NativeJoinedRemoteImageOperation,
    ) -> Result<crate::db::NativeJoinedRemoteImageProgress> {
        parent.check()?;
        if let crate::db::NativeJoinedRemoteImageOperation::VerifyTarget {
            max_scanned_records,
        } = &operation
        {
            return self.verify_joined_remote_target_unlocked(
                request,
                frame,
                parent,
                *max_scanned_records,
            );
        }
        parent.check()?;
        let host = self.joined_receive_host_mut()?;
        request.origin().require_host(host)?;
        let backend = host.parts.backend.clone();
        let factory = host.parts.factory.clone();
        let attempt = host
            .remote_attempts
            .get_mut(&request.origin().range_id())
            .ok_or_else(|| invalid("Joined original image attempt absent"))?;
        let preflight = attempt.preflight.clone();
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Joined image actual A absent"))?;
        lease.require_retained()?;
        preflight.require_metadata_current(request)?;
        let fence = attempt
            .installing
            .as_ref()
            .and_then(|p| p.fence())
            .cloned()
            .ok_or_else(|| invalid("Joined image requires actual durable selected Installing"))?;
        let point = NativeJoinedRemoteImagePoint {
            preflight: &preflight,
            lease,
            request,
            frame,
        };
        point.require_live()?;
        let result = {
            let _original = preflight.selected().origin().original()?;
            let _admission = preflight.selected().origin().admission()?;
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined image actual service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined image actual DB owner changed"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined image actual DB busy"))?;
            db.with_joined_remote_mutation_scope(&preflight, request, frame, lease, |scope| {
                service.with_writer_data_domain_store(&db, |store| {
                    let context = crate::db::NativeJoinedRemoteImageContext::new(
                        &db, store, &point, scope, &fence,
                    )?;
                    if attempt.image.is_none() {
                        // Exact generation/row pending owner is stored before
                        // any native/logical transaction can be issued.
                        attempt.image = Some(
                            crate::db::NativeJoinedRemoteImageInstallation::retain(&context)?,
                        );
                    }
                    attempt
                        .image
                        .as_mut()
                        .unwrap()
                        .advance(&db, &context, operation)
                })
            })
        };
        let post = preflight
            .require_metadata_current(request)
            .and_then(|()| lease.require_retained())
            .and_then(|()| point.require_live());
        let progress = match result {
            Err(primary) => {
                let _ = post;
                return Err(primary);
            }
            Ok(progress) => {
                post?;
                parent.check()?;
                point.require_live()?;
                progress
            }
        };
        // Only a successful, still-live call may start OPTIONAL checkpoint
        // IO. Admission/hook/journal/service/Controlled guards have dropped.
        // Cancellation cannot preempt IO already inside a kernel/fsync call;
        // that physical liveness limitation remains a separate test gate.
        let database = backend.service.try_lock().map(|service| service.database());
        if let Some(database) = database {
            if let Some(db) = database.try_read() {
                parent.check()?;
                point.require_live()?;
                db.auto_checkpoint_if_due();
            }
        }
        parent.check()?;
        point.require_live()?;
        Ok(progress)
    }
    #[cfg(test)]
    pub(crate) fn test_joined_remote_image_state(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.joined
            .as_ref()
            .and_then(|host| host.remote_attempts.get(&range))
            .and_then(|attempt| attempt.image.as_ref())
            .map(|image| image.test_state())
            .ok_or_else(|| invalid("Joined actual original image owner absent"))
    }
}

impl NativeMetadataControlHost {
    fn verify_joined_remote_target_unlocked(
        &mut self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
        max_scanned_records: usize,
    ) -> Result<crate::db::NativeJoinedRemoteImageProgress> {
        parent.check()?;
        if !(1..=16384).contains(&max_scanned_records) {
            return Err(invalid("Joined target raw scan budget invalid"));
        }
        parent.check()?;
        let host = self.joined_receive_host_mut()?;
        request.origin().require_host(host)?;
        let backend = host.parts.backend.clone();
        let factory = host.parts.factory.clone();
        let attempt = host
            .remote_attempts
            .get_mut(&request.origin().range_id())
            .ok_or_else(|| invalid("Joined actual target scan attempt absent"))?;
        let preflight = attempt.preflight.clone();
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Joined target scan actual A absent"))?;
        lease.require_retained()?;
        preflight.require_metadata_current(request)?;
        let fence = attempt
            .installing
            .as_ref()
            .and_then(|p| p.fence())
            .cloned()
            .ok_or_else(|| invalid("Joined target scan requires actual durable Installing"))?;
        let point = NativeJoinedRemoteImagePoint {
            preflight: &preflight,
            lease,
            request,
            frame,
        };
        point.require_live()?;
        let image = attempt.image.as_mut().ok_or_else(|| {
            invalid("Joined target scan requires actual retained rows/history owner")
        })?;
        // PREPARE: actual native/history/rows/Installing/source scope, with NO
        // physical collection scan. The read point is host-retained first.
        let prepared = {
            let _original = preflight.selected().origin().original()?;
            let _admission = preflight.selected().origin().admission()?;
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined scan prepare service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined scan prepare actual DB changed"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined scan prepare DB busy"))?;
            db.with_joined_remote_mutation_scope(&preflight, request, frame, lease, |scope| {
                service.with_writer_data_domain_store(&db, |store| {
                    let context = crate::db::NativeJoinedRemoteImageContext::new(
                        &db, store, &point, scope, &fence,
                    )?;
                    image.prepare_target_scan(&db, &context)
                })
            })
        };
        let prepare_post = preflight
            .require_metadata_current(request)
            .and_then(|()| lease.require_retained());
        prepared?;
        prepare_post?;
        parent.check()?;
        point.require_live()?;
        // READ: no service, native journal, admission or hook guard exists.
        // Only the actual DB read borrow + original selected A/identity remain.
        let scanned = {
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("Joined unlocked target DB busy"))?;
            image.scan_target_unlocked(&db, request, frame, lease, parent, max_scanned_records)
        };
        let read_post = preflight
            .require_metadata_current(request)
            .and_then(|()| lease.require_retained());
        match scanned {
            Err(primary) => {
                let _ = read_post;
                return Err(primary);
            }
            Ok(()) => read_post?,
        }
        parent.check()?;
        point.require_live()?;
        // PUBLISH: independently reacquire actual fixed native/source checks.
        // The physical cursor/hash is not counted until the short actual
        // gate/version/request boundary accepts this fresh request.
        let published = {
            let _original = preflight.selected().origin().original()?;
            let _admission = preflight.selected().origin().admission()?;
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined scan publish service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined scan publish actual DB changed"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined scan publish DB busy"))?;
            db.with_joined_remote_mutation_scope(&preflight, request, frame, lease, |scope| {
                service.with_writer_data_domain_store(&db, |store| {
                    let context = crate::db::NativeJoinedRemoteImageContext::new(
                        &db, store, &point, scope, &fence,
                    )?;
                    image.publish_target_scan(&db, &context)
                })
            })
        };
        let post = preflight
            .require_metadata_current(request)
            .and_then(|()| lease.require_retained())
            .and_then(|()| parent.check())
            .and_then(|()| point.require_live());
        match published {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(progress) => {
                post?;
                Ok(crate::db::NativeJoinedRemoteImageProgress::Verification(
                    progress,
                ))
            }
        }
    }
}
