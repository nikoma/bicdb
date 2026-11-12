//! Existing Registered member control transition; never a fresh-voter factory.
use super::*;
impl NativeColdLearnerMetadataHost {
    pub(in crate::distribution_native_host) fn metadata_control_owned(&self) -> bool {
        self.control.as_ref().is_some_and(|c| c.is_running())
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
            .control
            .as_ref()
            .is_some_and(|control| control.is_running() || control.has_replica_receiver())
        {
            return Ok(());
        }
        if let Some(supervisor) = self.supervisor.as_mut() {
            supervisor.stop(parent)?;
            self.supervisor.take();
        }
        if self.control.is_none() {
            let metadata = Arc::clone(
                self.metadata
                    .as_ref()
                    .ok_or_else(|| invalid("actual owned metadata absent"))?,
            );
            let service = self
                .service
                .as_ref()
                .ok_or_else(|| invalid("cold owned service absent"))?;
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
                    self.topology
                        .as_ref()
                        .ok_or_else(|| invalid("owned member routing absent"))?,
                ),
                Arc::clone(
                    self.transport
                        .as_ref()
                        .ok_or_else(|| invalid("owned member transport absent"))?,
                ),
                Arc::clone(
                    self.provider
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
                .listener
                .take()
                .ok_or_else(|| invalid("actual old metadata listener absent"))?;
            self.control = Some(NativeMetadataControlHost::from_registered_member(
                backend,
                listener,
                self.network.clone(),
                config,
            ));
        }
        self.control.as_mut().unwrap().advance(parent)
    }
    pub(in crate::distribution_native_host) fn maintain_metadata_control(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataMaintenanceReport> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("actual owned control absent"))?
            .maintain_joins(parent)
    }
    pub(in crate::distribution_native_host) fn elect_metadata_control(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.control
            .as_ref()
            .ok_or_else(|| invalid("actual owned control absent"))?
            .elect(parent)
    }
}

impl NativeColdLearnerMetadataHost {
    pub(in crate::distribution_native_host) fn open_registered_replica_receiver(
        &mut self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.validate_owned_metadata_member(observed, business, parent)?;
        self.control
            .as_mut()
            .ok_or_else(|| {
                invalid("actual Registered metadata control required before replica receiver")
            })?
            .open_registered_replica_receiver(&self.factory, observed, business, parent)
    }
    pub(in crate::distribution_native_host) fn replica_receiver_status(
        &self,
        parent: &CancellationToken,
    ) -> Result<Option<NativeRegisteredReplicaReceiverStatus>> {
        self.control
            .as_ref()
            .map(|control| control.replica_receiver_status(parent))
            .transpose()
            .map(Option::flatten)
    }
}
