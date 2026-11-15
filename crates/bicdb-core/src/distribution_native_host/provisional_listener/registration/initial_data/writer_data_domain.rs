//! Actual INITIAL host custody. No writer/coordinator reconstructed from DTOs.
use super::*;
use crate::db::{
    NativeClosedWriterDomain, NativeWriterDataDomainPolicy, NativeWriterDataDomainTransition,
};
use crate::distribution_data::NativeJointServingLease;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

impl NativeInitialDataHost {
    fn require_writer_data_owner(&self) -> Result<()> {
        if self.stopping {
            return Err(invalid("stopping INITIAL owner cannot advance DATA policy"));
        }
        self.admission
            .as_ref()
            .ok_or_else(|| invalid("INITIAL actual DATA admission absent"))?
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
        let enrollment = self
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("INITIAL original enrollment absent"))?;
        let service = self
            .initial
            .service
            .try_lock()
            .ok_or_else(|| invalid("INITIAL DATA transition service busy"))?;
        let db = self
            .initial
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("INITIAL DATA transition DB busy"))?;
        let attempt = NativeWriterDataDomainTransition::begin(
            &db,
            &self.initial.service,
            &service,
            self.runtime
                .as_ref()
                .ok_or_else(|| invalid("INITIAL original runtime absent"))?,
            NativeDataOwnerAdmission::Initial(self.admission.as_ref().unwrap().clone()),
            &enrollment.coordinator,
            &enrollment.guard,
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
            &self
                .enrollment
                .as_ref()
                .ok_or_else(|| invalid("INITIAL original enrollment absent"))?
                .coordinator,
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
            .initial
            .seed
            .database
            .try_read()
            .ok_or_else(|| invalid("INITIAL DATA custody DB busy"))?;
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
