//! Existing Registered member control transition; never a fresh-voter factory.
use super::*;
impl NativeUnassignedLearnerHost {
    pub(in crate::distribution_native_host) fn metadata_control_owned(&self) -> bool {
        self.metadata_transition
            .control
            .as_ref()
            .is_some_and(|c| c.is_running())
    }
    pub(in crate::distribution_native_host) fn enable_metadata_control(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        config: crate::DistributionConfig,
        parent: &CancellationToken,
    ) -> Result<()> {
        // This STRICT installed local C proof is intentionally distinct from
        // lower-authority Registered catch-up/rebind permission.
        self.validate_owned_metadata_member(observed, business, parent)?;
        if self
            .metadata_transition
            .control
            .as_ref()
            .is_some_and(|control| control.is_running())
        {
            return Ok(());
        }
        if let Some(supervisor) = self.metadata_transition.supervisor.as_mut() {
            supervisor.stop(parent)?;
            self.metadata_transition.supervisor.take();
        }
        if self.metadata_transition.control.is_none() {
            let metadata = Arc::clone(
                self.metadata_transition
                    .metadata
                    .as_ref()
                    .ok_or_else(|| invalid("actual owned metadata absent"))?,
            );
            let service = &self._service;
            let proof = {
                let store = metadata
                    .try_lock()
                    .ok_or_else(|| invalid("control member store busy"))?;
                store.observe_owned_native_metadata_member(observed, business, parent)?
            };
            let backend = NativeMetadataControlBackend::from_registered_member(
                metadata,
                Arc::clone(service),
                Arc::clone(
                    self.metadata_transition
                        .topology
                        .as_ref()
                        .ok_or_else(|| invalid("owned member routing absent"))?,
                ),
                Arc::clone(
                    self.metadata_transition
                        .transport
                        .as_ref()
                        .ok_or_else(|| invalid("owned member transport absent"))?,
                ),
                Arc::clone(
                    self.metadata_transition
                        .provider
                        .as_ref()
                        .ok_or_else(|| invalid("owned member provider absent"))?,
                ),
                proof,
                observed,
                business,
                parent,
            )?;
            // All fallible validation precedes moving the only old handle.
            // The new owner is retained before any fallible drain/bind work.
            let listener = self
                .metadata_transition
                .listener
                .take()
                .ok_or_else(|| invalid("actual old metadata listener absent"))?;
            self.metadata_transition.control =
                Some(NativeMetadataControlHost::from_registered_member(
                    backend,
                    listener,
                    self.provider.network.clone(),
                    config,
                ));
        }
        self.metadata_transition
            .control
            .as_mut()
            .unwrap()
            .advance(parent)
    }
    pub(in crate::distribution_native_host) fn maintain_metadata_control(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataMaintenanceReport> {
        self.metadata_transition
            .control
            .as_ref()
            .ok_or_else(|| invalid("actual owned control absent"))?
            .maintain_joins(parent)
    }
    pub(in crate::distribution_native_host) fn elect_metadata_control(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.metadata_transition
            .control
            .as_ref()
            .ok_or_else(|| invalid("actual owned control absent"))?
            .elect(parent)
    }
}
