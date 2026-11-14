//! Retained SAME-process attempts, never a replacement writer or APP grant.
use super::*;
use crate::db::{
    NativeClosedWriterDomain, NativeWriterDataDomainPolicy, NativeWriterDataDomainTransition,
    NativeWriterServingDomainPublication,
};
use crate::distribution_data::NativeJointServingLease;
use crate::distribution_range_consensus::RangeWriteCoordinator;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

#[derive(Default)]
pub(super) struct NativeHostWriterDataDomains {
    closed: BTreeMap<crate::RangeId, NativeWriterDataDomainTransition>,
    serving: BTreeMap<crate::RangeId, NativeWriterServingDomainPublication>,
}
impl NativeHostWriterDataDomains {
    pub(super) fn require_new(&self, range: crate::RangeId) -> Result<()> {
        if self.closed.contains_key(&range) || self.serving.contains_key(&range) {
            return Err(invalid(
                "writer DATA range already has a retained continuation",
            ));
        }
        Ok(())
    }
    pub(super) fn retain(
        &mut self,
        range: crate::RangeId,
        attempt: NativeWriterDataDomainTransition,
    ) -> Result<()> {
        self.require_new(range)?;
        self.closed.insert(range, attempt);
        Ok(())
    }
    pub(super) fn unresolved(&self) -> bool {
        self.closed.values().any(|a| a.publication_unresolved())
            || self.serving.values().any(|a| a.publication_unresolved())
    }
    pub(super) fn advance_closed(
        &mut self,
        range: crate::RangeId,
        coordinator: &Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        self.closed
            .get_mut(&range)
            .ok_or_else(|| invalid("writer DATA retained CLOSED attempt absent"))?
            .advance_closed(coordinator, policy)
    }
    pub(super) fn closed(&self, range: crate::RangeId) -> Result<Arc<NativeClosedWriterDomain>> {
        self.closed
            .get(&range)
            .and_then(|attempt| attempt.verified())
            .cloned()
            .ok_or_else(|| invalid("writer DATA actual CLOSED audit not verified"))
    }
    pub(super) fn take_joint_lease(
        &mut self,
        range: crate::RangeId,
        db: &crate::BicDb,
    ) -> Result<NativeJointServingLease> {
        self.closed
            .get_mut(&range)
            .ok_or_else(|| invalid("writer DATA retained lease owner absent"))?
            .take_joint_lease(db)
    }
    pub(super) fn advance_serving(
        &mut self,
        range: crate::RangeId,
        admission: &NativeJointServingPolicyAdmission,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        if admission.data().old_range().id != range {
            return Err(invalid(
                "writer DATA Serving admission changed selected range",
            ));
        }
        if !self.serving.contains_key(&range) {
            let attempt = self.closed(range)?.begin_serving_publication()?;
            // Retained by the original host BEFORE the first native write.
            self.serving.insert(range, attempt);
        }
        self.serving
            .get_mut(&range)
            .unwrap()
            .advance(admission, policy)
    }
}

impl NativeMetadataControlHost {
    fn fresh_writer_data_policy(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeWriterDataDomainPolicy> {
        parent.check()?;
        let (leader, routing) = {
            let actual = self.backend.lock_metadata(parent)?;
            (
                actual
                    .status()
                    .leader_id
                    .ok_or_else(|| invalid("writer DATA metadata leader unknown"))?,
                actual.committed_topology().clone(),
            )
        };
        self.backend.transport.install_topology(routing)?;
        let business = Uuid::now_v7();
        // No host service, DB, native journal or MStore guard spans the RPC.
        let own = self.backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            parent,
        )?;
        NativeWriterDataDomainPolicy::from_actual(
            &self.backend.service,
            &self.backend.metadata,
            own,
            business,
            parent,
        )
    }
    pub(in crate::distribution_native_host) fn begin_native_writer_data_domain(
        &mut self,
        range: crate::RangeId,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        match (&mut self.data, &mut self.assigned, &mut self.joined) {
            (Some(owner), None, None) => owner.begin_writer_data_domain(range, lease, parent),
            (None, Some(owner), None) => owner.begin_writer_data_domain(range, lease, parent),
            (None, None, Some(owner)) => owner.begin_writer_data_domain(range, lease, parent),
            _ => Err(invalid(
                "one actual Initial, Assigned or Joined DATA host required",
            )),
        }
    }
    pub(in crate::distribution_native_host) fn advance_native_writer_closed_domain(
        &mut self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<()> {
        let policy = self.fresh_writer_data_policy(parent)?;
        match (&mut self.data, &mut self.assigned, &mut self.joined) {
            (Some(owner), None, None) => owner.advance_writer_closed_domain(range, &policy),
            (None, Some(owner), None) => owner.advance_writer_closed_domain(range, &policy),
            (None, None, Some(owner)) => owner.advance_writer_closed_domain(range, &policy),
            _ => Err(invalid("one actual retained DATA host required")),
        }
    }
    pub(in crate::distribution_native_host) fn native_closed_writer_domain(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeClosedWriterDomain>> {
        parent.check()?;
        let actual = match (&self.data, &self.assigned, &self.joined) {
            (Some(owner), None, None) => owner.closed_writer_domain(range),
            (None, Some(owner), None) => owner.closed_writer_domain(range),
            (None, None, Some(owner)) => owner.closed_writer_domain(range),
            _ => Err(invalid("one actual retained DATA host required")),
        }?;
        actual.require_lifetime()?;
        parent.check()?;
        Ok(actual)
    }
    pub(in crate::distribution_native_host) fn take_native_closed_joint_lease(
        &mut self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<NativeJointServingLease> {
        parent.check()?;
        match (&mut self.data, &mut self.assigned, &mut self.joined) {
            (Some(owner), None, None) => owner.take_closed_joint_lease(range, parent),
            (None, Some(owner), None) => owner.take_closed_joint_lease(range, parent),
            (None, None, Some(owner)) => owner.take_closed_joint_lease(range, parent),
            _ => Err(invalid("one actual retained DATA host required")),
        }
    }
    pub(in crate::distribution_native_host) fn publish_native_writer_serving_domain(
        &mut self,
        range: crate::RangeId,
        admission: &NativeJointServingPolicyAdmission,
        parent: &CancellationToken,
    ) -> Result<()> {
        // Actual OWN/MStore follows the already-completed all-released phase.
        let policy = self.fresh_writer_data_policy(parent)?;
        match (&mut self.data, &mut self.assigned, &mut self.joined) {
            (Some(owner), None, None) => {
                owner.publish_writer_serving_domain(range, admission, &policy)
            }
            (None, Some(owner), None) => {
                owner.publish_writer_serving_domain(range, admission, &policy)
            }
            (None, None, Some(owner)) => {
                owner.publish_writer_serving_domain(range, admission, &policy)
            }
            _ => Err(invalid("one actual retained DATA host required")),
        }
        // No ordinary enrolled readQQ, recovered Ready or dispatcher/APP
        // publication occurs in this necessary policy-only transition.
    }
}
