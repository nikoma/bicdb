//! Selected-range SAME-State continuation on an already serving Joined host.
//! This adapter supplies identity/custody, never an image, Role, NEW vote or
//! Open grant. The original writer, coordinator, socket and dispatcher remain.
use super::*;
use crate::db::{
    NativeClosedWriterDomain, NativeWriterDataDomainPolicy, NativeWriterDataDomainTransition,
};
use crate::distribution_data::NativeJointServingLease;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

impl NativeJoinedDataHost {
    pub(super) fn require_writer_data_owner(&self) -> Result<()> {
        if self.stopping {
            return Err(invalid(
                "stopping Joined cannot advance a selected DATA domain",
            ));
        }
        let original = self.registered.as_ref().ok_or_else(|| {
            invalid("selected Joined continuation needs actual Registered origin")
        })?;
        original.require_original_lifetime()?;
        let enrollment = self
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("Joined original installed enrollment absent"))?;
        if !Arc::ptr_eq(&enrollment.coordinator, &original.coordinator)
            || !Arc::ptr_eq(&enrollment.guard, &original.guard)
        {
            return Err(invalid(
                "Joined selected continuation replaced original writer owner",
            ));
        }
        self.admission
            .as_ref()
            .ok_or_else(|| invalid("Joined original DATA admission absent"))?
            .require_listener(&self.parts.backend.service, &self.parts.runtime)
    }
    pub(in super::super) fn begin_writer_data_domain(
        &mut self,
        range: crate::RangeId,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.require_writer_data_owner()?;
        if self
            .selected_range
            .as_ref()
            .is_some_and(|old| old.id == range)
        {
            return Err(invalid(
                "additional Joined domain cannot replace its original range",
            ));
        }
        let original = self.registered.as_ref().unwrap();
        original.writer_data_domains.require_new(range)?;
        let enrollment = self.enrollment.as_ref().unwrap();
        let service = self
            .parts
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("Joined selected DATA service busy"))?;
        let db = self
            .parts
            .factory
            .database
            .try_read()
            .ok_or_else(|| invalid("Joined selected DATA database busy"))?;
        let attempt = NativeWriterDataDomainTransition::begin(
            &db,
            &self.parts.backend.service,
            &service,
            &self.parts.runtime,
            NativeDataOwnerAdmission::Joined(self.admission.as_ref().unwrap().clone()),
            &enrollment.coordinator,
            &enrollment.guard,
            range,
            lease,
            parent,
        )?;
        // The original host owns the exact transition and lease before the
        // first possibly durable audit; cleanup retains this same collection.
        self.registered
            .as_mut()
            .unwrap()
            .writer_data_domains
            .retain(range, attempt)
    }
    pub(in super::super) fn advance_writer_closed_domain(
        &mut self,
        range: crate::RangeId,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        self.require_writer_data_owner()?;
        self.registered
            .as_mut()
            .unwrap()
            .writer_data_domains
            .advance_closed(
                range,
                &self.enrollment.as_ref().unwrap().coordinator,
                policy,
            )
    }
    pub(in super::super) fn closed_writer_domain(
        &self,
        range: crate::RangeId,
    ) -> Result<Arc<NativeClosedWriterDomain>> {
        self.require_writer_data_owner()?;
        self.registered
            .as_ref()
            .unwrap()
            .writer_data_domains
            .closed(range)
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
            .ok_or_else(|| invalid("Joined selected lease custody DB busy"))?;
        self.registered
            .as_mut()
            .unwrap()
            .writer_data_domains
            .take_joint_lease(range, &db)
    }
    pub(in super::super) fn publish_writer_serving_domain(
        &mut self,
        range: crate::RangeId,
        admission: &NativeJointServingPolicyAdmission,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        self.require_writer_data_owner()?;
        self.registered
            .as_mut()
            .unwrap()
            .writer_data_domains
            .advance_serving(range, admission, policy)
    }
}
