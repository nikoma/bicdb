//! Actual Assigned host custody, preserving its original installed writer.
use super::*;
use crate::db::{
    NativeClosedWriterDomain, NativeWriterDataDomainPolicy, NativeWriterDataDomainTransition,
};
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

impl NativeAssignedDataHost {
    fn require_writer_data_owner(&self) -> Result<()> {
        if self.stopping {
            return Err(invalid(
                "stopping Assigned owner cannot advance DATA policy",
            ));
        }
        self.admission
            .as_ref()
            .ok_or_else(|| invalid("Assigned actual DATA admission absent"))?
            .require_live()
    }
    pub(in super::super) fn begin_writer_data_domain(
        &mut self,
        range: crate::RangeId,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.require_writer_data_owner()?;
        self.writer_data_domains.require_new(range)?;
        let coordinator = self
            .coordinator
            .as_ref()
            .ok_or_else(|| invalid("Assigned original coordinator absent"))?;
        let guard = self
            .attempt
            .as_ref()
            .and_then(|a| a.installed_guard())
            .ok_or_else(|| invalid("Assigned original writer guard absent"))?;
        let service = self
            .parts
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("Assigned DATA transition service busy"))?;
        let db = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Assigned DATA transition DB busy"))?;
        let attempt = NativeWriterDataDomainTransition::begin(
            &db,
            &self.parts.backend.service,
            &service,
            &self.parts.runtime,
            NativeDataOwnerAdmission::Assigned(self.admission.as_ref().unwrap().clone()),
            coordinator,
            guard,
            range,
            lease,
            parent,
        )?;
        self.writer_data_domains.retain(range, attempt)
    }
    pub(in super::super) fn advance_writer_closed_domain(
        &mut self,
        range: crate::RangeId,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        self.require_writer_data_owner()?;
        self.writer_data_domains.advance_closed(
            range,
            self.coordinator
                .as_ref()
                .ok_or_else(|| invalid("Assigned original coordinator absent"))?,
            policy,
        )
    }
    pub(in super::super) fn closed_writer_domain(
        &self,
        range: crate::RangeId,
    ) -> Result<Arc<NativeClosedWriterDomain>> {
        self.require_writer_data_owner()?;
        self.writer_data_domains.closed(range)
    }
    pub(in super::super) fn take_closed_joint_lease(
        &mut self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<NativeJointServingLease> {
        parent.check()?;
        self.require_writer_data_owner()?;
        let db = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Assigned DATA custody DB busy"))?;
        self.writer_data_domains.take_joint_lease(range, &db)
    }
    pub(in super::super) fn publish_writer_serving_domain(
        &mut self,
        range: crate::RangeId,
        admission: &NativeJointServingPolicyAdmission,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        self.require_writer_data_owner()?;
        self.writer_data_domains
            .advance_serving(range, admission, policy)
    }
}
