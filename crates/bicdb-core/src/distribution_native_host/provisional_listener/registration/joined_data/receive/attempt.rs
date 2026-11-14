//! Host-owned original additional-range attempt. Retain actual A immediately;
//! no re-enrollment, FIRST substitution, reset-on-error or Drop-to-N exists.
use super::*;
use crate::distribution_range_consensus::{
    NativeJoinedRemoteInstallingFence, NativeJoinedRemoteInstallingPublication,
};
use crate::distribution_range_image::NativeRemoteImageStep;
use crate::distribution_range_recovery::JoinedRemoteImageInstallFreeze;

pub(in super::super) struct NativeJoinedRemoteInstallAttempt {
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    lease: Option<JoinedRemoteImageInstallFreeze>,
    proposal: Option<crate::db::NativeJoinedRemoteProposalPublication>,
    installing: Option<NativeJoinedRemoteInstallingPublication>,
}
impl NativeJoinedRangeReceiveOrigin {
    fn require_host(&self, host: &NativeJoinedDataHost) -> Result<()> {
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
    fn joined_receive_host_mut(&mut self) -> Result<&mut NativeJoinedDataHost> {
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
                });
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
