//! Read-only normal-host observation of the SAME live process continuation.
use super::*;
impl NativeMetadataHost {
    pub(crate) fn observe_initial_data_allocation(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation observation actual control owner absent"))?
            .observe_initial_data_allocation(range, parent)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_inventory(
        &self,
    ) -> Result<serde_json::Value> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation inventory actual control absent"))?
            .test_initial_allocation_inventory()
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_reader_barrier(
        &self,
    ) -> Result<()> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation reader barrier original control absent"))?
            .test_initial_allocation_reader_barrier()
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_pause(
        &self,
        hold: bool,
    ) -> Result<()> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation pause actual original owner absent"))?
            .test_retained_initial_data_writer_guard()?
            .test_pause_allocation_recovery(hold)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_generic_recovery(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<bool> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation generic recovery original owner absent"))?
            .test_initial_allocation_generic_recovery(range, parent)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_native_point(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation actual native point owner absent"))?
            .test_initial_allocation_native_point(range)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_allocation_activation_domain_comparison(
        &self,
        owner: &Self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let actual_writer = owner
            .control
            .as_ref()
            .ok_or_else(|| invalid("actual allocation comparison owner control absent"))?
            .test_retained_initial_data_writer_guard()?;
        self.control
            .as_ref()
            .ok_or_else(|| invalid("actual allocation comparison destination control absent"))?
            .test_allocation_activation_domain_comparison(&actual_writer, range, mode, parent)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_foreign_owner(
        &self,
        foreign: &Self,
        parent: &CancellationToken,
    ) -> Result<()> {
        let other = foreign
            .control
            .as_ref()
            .ok_or_else(|| invalid("allocation actual foreign control owner absent"))?
            .test_retained_initial_data_writer_guard()?;
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation actual original control owner absent"))?
            .test_initial_allocation_foreign_owner(&other, parent)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_fault(
        &self,
        mode: u8,
    ) -> Result<()> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("allocation fault actual control owner absent"))?
            .test_retained_initial_data_writer_guard()?
            .test_arm_allocation_fault(mode)
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_initial_allocation_evidence(
        &self,
    ) -> Result<serde_json::Value> {
        Ok(self
            .control
            .as_ref()
            .ok_or_else(|| invalid("allocation evidence actual control owner absent"))?
            .test_retained_initial_data_writer_guard()?
            .test_allocation_evidence())
    }
}
