//! Same-service NEW bridge preparation. Metadata acquisition and drain happen
//! outside the latches. Neither this evidence nor its lease grants serving.
use super::*;
use crate::distribution_range_consensus::NativeJointBridgeCatalogue;
use crate::distribution_range_handoff::CommittedNativeJointDecisionObservation;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze, NativeJointImageMaterialization,
};

mod image_release;
mod release;
pub(crate) use image_release::NativeJointImageReleaseOwner;
pub(crate) use release::{ActualServiceNativeJointReleaseEvidence, NativeJointReleaseOwnerView};

/// Keep the actual owner alive, including an installer's nonextractable
/// recovery lease. No raw Frozen is created from a metadata plan or number.
#[derive(Clone)]
pub(crate) enum NativeJointServingLease {
    Survivor(Arc<FrozenRangeMutationPermit>),
    #[cfg(feature = "tls-replication")]
    Registered {
        owner: Arc<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>,
        frozen: Arc<NativeImageInstallFreeze>,
    },
    Image {
        image: Arc<NativeImageCatalogueFinalization>,
        frozen: Arc<NativeImageInstallFreeze>,
    },
}
impl NativeJointServingLease {
    pub(crate) fn frozen(&self) -> &FrozenRangeMutationPermit {
        match self {
            Self::Survivor(frozen) => frozen,
            Self::Image { frozen, .. } => frozen.as_frozen(),
            #[cfg(feature = "tls-replication")]
            Self::Registered { frozen, .. } => frozen.as_frozen(),
        }
    }
    pub(crate) fn try_frozen(&self) -> Result<&FrozenRangeMutationPermit> {
        match self {
            Self::Survivor(f) => Ok(f),
            Self::Image { frozen, .. } => frozen.try_as_frozen(),
            #[cfg(feature = "tls-replication")]
            Self::Registered { frozen, .. } => frozen.try_as_frozen(),
        }
    }
    pub(crate) fn image(&self) -> Option<&dyn NativeJointImageMaterialization> {
        match self {
            Self::Survivor(_) => None,
            Self::Image { image, .. } => Some(image),
            #[cfg(feature = "tls-replication")]
            Self::Registered { owner, .. } => Some(owner.target()),
        }
    }
}
pub(crate) struct ActualServiceNativeJointPreparedEvidence {
    pub(super) committed: CommittedNativeJointDecisionObservation,
    pub(super) prepared: crate::db::NativeJointServingPreparation,
    pub(super) catalogue: NativeJointBridgeCatalogue,
    // Captured BEFORE the native preparation producer. A later fresh MQ may
    // revalidate this object, never replace its original writer registration.
    pub(super) writer: crate::db::NativeJointPhaseOwner,
    pub(super) writer_domain: crate::db::NativeJointWriterDomain,
    pub(super) lease: NativeJointServingLease,
    pub(super) pin: parking_lot::Mutex<Option<SourceCaptureReadPin>>,
}
impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn require_original_writer_lifetime(&self) -> Result<()> {
        self.writer_domain.require_lifetime(
            self.catalogue.database_root(),
            self.catalogue.store_open_id(),
            self.committed.data().activation_range(),
        )
    }
    /// Strict legacy getter: a Registered identity cannot become a writer.
    pub(crate) fn require_original_writer(
        &self,
        database: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<Arc<crate::db::NativeWriterHostState>> {
        Ok(self
            .require_original_phase_owner(database, store)?
            .actual_writer()?
            .clone())
    }
    pub(crate) fn require_original_phase_owner(
        &self,
        database: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<crate::db::NativeJointPhaseOwner> {
        self.require_original_writer_lifetime()?;
        self.writer_domain.require_current(
            database,
            store,
            self.committed.data().activation_range(),
        )?;
        if !self.writer.same_actual(&self.writer_domain.phase_owner()) {
            return Err(data_error(
                "joint preparation changed original actual phase owner",
            ));
        }
        self.require_original_writer_lifetime()?;
        Ok(self.writer.clone())
    }
    pub(crate) fn original_phase_owner_for_continuation(
        &self,
    ) -> Result<(crate::db::NativeJointPhaseOwner, std::path::PathBuf)> {
        self.require_original_writer_lifetime()?;
        Ok((
            self.writer.clone(),
            self.catalogue.database_root().to_path_buf(),
        ))
    }
    pub(crate) fn committed(&self) -> &CommittedNativeJointDecisionObservation {
        &self.committed
    }
    pub(crate) fn prepared(&self) -> &crate::db::NativeJointServingPreparation {
        &self.prepared
    }
    pub(crate) fn require_current(&self, service: &ClusterDataNodeService) -> Result<()> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
            "prepared-service-current",
            self.prepared.local_node_id(),
            self.committed.deadline(),
        );
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        self.writer_domain.require_current(
            &database,
            &service.range_writes,
            self.committed.data().activation_range(),
        )?;
        if self.prepared.local_node_id() != &service.node_id {
            return Err(data_error(
                "joint bridge preparation belongs to another service",
            ));
        }
        self.require_original_phase_owner(&database, &service.range_writes)?;
        self.prepared.require_current(
            &database,
            &service.range_writes,
            &self.committed,
            &self.catalogue,
            self.lease.image(),
            self.lease.frozen(),
        )?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("joint bridge pin transition busy"))?;
        let gate = database.range_mutation_gate();
        if let Some(pin) = pin.as_ref() {
            gate.validate_source_capture_read_pin(self.committed.data().old_range().id, pin)?;
        } else {
            gate.validate_quiescent_frozen(
                self.committed.data().old_range().id,
                self.lease.frozen(),
            )?;
        }
        self.require_original_phase_owner(&database, &service.range_writes)?;
        #[cfg(test)]
        trace.complete();
        Ok(())
    }
    /// Revalidation of the SAME actual preparation under a DIFFERENT live
    /// recipient-bound MQ. The old committed observation is only a durable
    /// fact here; it is not modified or made live by this method.
    pub(crate) fn require_current_under_fresh_phase(
        &self,
        service: &ClusterDataNodeService,
        fresh: &CommittedNativeJointDecisionObservation,
    ) -> Result<crate::db::NativeJointPhaseOwner> {
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        self.writer_domain.require_current(
            &database,
            &service.range_writes,
            fresh.data().activation_range(),
        )?;
        self.require_original_phase_owner(&database, &service.range_writes)?;
        self.committed.require_durable_fact_current(&database)?;
        fresh.require_current(&database)?;
        let old = self.committed.metadata_anchor();
        let current = fresh.metadata_anchor();
        let preparation = self.prepared.metadata_anchor();
        if fresh.data() != self.committed.data()
            || fresh.native_receipt().checksum_sha256()
                != self.committed.native_receipt().checksum_sha256()
            || fresh.native_receipt().native_database_incarnation()
                != self.prepared.native_database_incarnation()
            || self.prepared.local_node_id() != &service.node_id
            || current.0 < old.0
            || current.1 < old.1
            || current.2 == old.2
            || current.0 < preparation.0
            || current.1 < preparation.1
            || current.2 == preparation.2
        {
            return Err(data_error(
                "fresh joint phase changed original decision/open or reused old metadata",
            ));
        }
        self.prepared.require_current(
            &database,
            &service.range_writes,
            fresh,
            &self.catalogue,
            self.lease.image(),
            self.lease.frozen(),
        )?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("fresh joint phase pin transition busy"))?;
        let gate = database.range_mutation_gate();
        if let Some(pin) = pin.as_ref() {
            gate.validate_source_capture_read_pin(fresh.data().old_range().id, pin)?;
        } else {
            gate.validate_quiescent_frozen(fresh.data().old_range().id, self.lease.frozen())?;
        }
        self.require_original_phase_owner(&database, &service.range_writes)
    }
    pub(crate) fn require_current_under_open_phase(
        &self,
        service: &ClusterDataNodeService,
        fresh: &CommittedNativeJointDecisionObservation,
        bridge: Arc<crate::db::NativeJointServingBridgeReceipt>,
    ) -> Result<crate::db::NativeJointPhaseOwner> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
            "open-actual-service-current",
            &service.node_id,
            fresh.deadline(),
        );
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        self.writer_domain.require_current(
            &database,
            &service.range_writes,
            fresh.data().activation_range(),
        )?;
        self.require_original_phase_owner(&database, &service.range_writes)?;
        #[cfg(test)]
        let mut native_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "open-native-locked-read-scope",
                &service.node_id,
                fresh.deadline(),
            );
        let scope = bridge.current_read_scope(&database)?;
        #[cfg(test)]
        {
            native_trace.complete();
            drop(native_trace);
        }
        let writer =
            self.require_local_open_scope(&scope, &service.range_writes, fresh, bridge.clone())?;
        #[cfg(test)]
        trace.complete();
        Ok(writer)
    }
    /// Reuse the SAME locked actual local proof inside the final catalogue
    /// boundary; no extra DB/service lock or network operation is acquired.
    pub(crate) fn require_local_open_scope(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        fresh: &CommittedNativeJointDecisionObservation,
        bridge: Arc<crate::db::NativeJointServingBridgeReceipt>,
    ) -> Result<crate::db::NativeJointPhaseOwner> {
        self.require_original_phase_owner(scope.database(), store)?;
        self.committed
            .require_historical_audit_with_native_read_scope(scope)?;
        fresh.require_current_with_native_read_scope(scope)?;
        let old = self.committed.metadata_anchor();
        let current = fresh.metadata_anchor();
        let preparation = self.prepared.metadata_anchor();
        if fresh.data() != self.committed.data()
            || scope.native().data() != fresh.data()
            || scope.native().native_preparation_sha256() != self.prepared.checksum_sha256()
            || scope.native().native_database_incarnation()
                != self.prepared.native_database_incarnation()
            || fresh.native_receipt().checksum_sha256()
                != self.committed.native_receipt().checksum_sha256()
            || self.prepared.local_node_id() != scope.native().local_node_id()
            || current.0 < old.0
            || current.1 < old.1
            || current.2 == old.2
            || current.0 < preparation.0
            || current.1 < preparation.1
            || current.2 == preparation.2
        {
            return Err(data_error("fresh joint open phase changed original protected preparation/bridge/open/metadata"));
        }
        let activation =
            store.observe_native_joint_activation_fact_with_scope(scope, bridge.clone())?;
        if let Some(fact) = &activation {
            // Necessary rejection BEFORE touching a potentially consumed image lease.
            store.require_joint_open_phase_not_released(fact)?;
            self.catalogue.require_after_exact_activation(
                store,
                scope,
                fresh,
                fact,
                self.lease.try_frozen()?,
            )?;
        } else {
            self.catalogue.require_current(
                store,
                scope.database(),
                fresh,
                self.lease.image(),
                self.lease.try_frozen()?,
            )?;
        }
        scope.require_prepared_frozen(&self.prepared, self.lease.try_frozen()?)?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("joint open phase pin busy"))?;
        if let Some(pin) = pin.as_ref() {
            scope
                .database()
                .range_mutation_gate()
                .validate_source_capture_read_pin(fresh.data().old_range().id, pin)?;
        } else {
            scope
                .database()
                .range_mutation_gate()
                .validate_quiescent_frozen(fresh.data().old_range().id, self.lease.try_frozen()?)?;
        }
        self.require_original_phase_owner(scope.database(), store)
    }
    pub(crate) fn require_local_publication_scope(
        &self,
        scope: &crate::db::NativeJointPreparedPublicationScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        fresh: &CommittedNativeJointDecisionObservation,
    ) -> Result<crate::db::NativeJointPhaseOwner> {
        self.require_original_phase_owner(scope.database(), store)?;
        self.committed
            .require_historical_audit_with_publication_scope(scope)?;
        fresh.require_current_with_publication_scope(scope)?;
        scope.require_owned_preparation(
            &self.prepared,
            &self.catalogue,
            self.lease.image(),
            self.lease.try_frozen()?,
            store,
        )?;
        if self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("publication pin transition busy"))?
            .is_some()
        {
            return Err(data_error(
                "publication requires original actual read pins released",
            ));
        }
        self.require_original_phase_owner(scope.database(), store)
    }
    pub(crate) fn require_phase_writer_lifetime(
        &self,
        writer: &crate::db::NativeJointPhaseOwner,
    ) -> Result<()> {
        if !writer.same_actual(&self.writer) {
            return Err(data_error(
                "joint phase substituted the original actual writer registration",
            ));
        }
        // No DB Arc lock or native journal reacquisition in this cheap check.
        self.require_original_writer_lifetime()
    }
    pub(crate) fn release_read_pin_under_fresh_phase(
        &self,
        service: &ClusterDataNodeService,
        fresh: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.require_current_under_fresh_phase(service, fresh)?;
        self.pin
            .try_lock()
            .ok_or_else(|| data_error("fresh joint phase pin busy"))?
            .take();
        self.require_current_under_fresh_phase(service, fresh)?;
        Ok(())
    }
    /// This Option is monotone: construction installs Some exactly once;
    /// release only takes it. No API can reacquire a pin into THIS evidence.
    pub(crate) fn read_pin_already_released(&self) -> Result<bool> {
        Ok(self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("joint pin transition busy"))?
            .is_none())
    }
    /// Release only the owned read pin. The underlying real freeze and all
    /// permanent promises/install refusals remain retained by this evidence.
    pub(crate) fn release_read_pin(&self, service: &ClusterDataNodeService) -> Result<()> {
        self.require_current(service)?;
        self.pin
            .try_lock()
            .ok_or_else(|| data_error("joint bridge pin busy"))?
            .take();
        self.require_current(service)
    }
}
impl ClusterDataNodeService {
    /// Necessary same-service read-only storage borrow. The scope cannot
    /// escape as a Store reference and supplies no role or DATA permission.
    pub(crate) fn with_writer_data_domain_store<R>(
        &self,
        database: &BicDb,
        operation: impl for<'scope> FnOnce(
            &'scope crate::distribution_range_consensus::RangeWriteStore,
        ) -> Result<R>,
    ) -> Result<R> {
        self.require_native_catalog_database(database)?;
        let result = operation(&self.range_writes);
        let post = self.require_native_catalog_database(database);
        match result {
            Ok(value) => {
                post?;
                Ok(value)
            }
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
        }
    }
    pub(crate) fn reconcile_native_joint_open_phase(
        &mut self,
        host: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        native: &crate::db::NativeJointServingBridgeReceipt,
        actual: &ActualServiceNativeJointPreparedEvidence,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        host.require_live_request(business)?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        actual.require_original_phase_owner(&database, &self.range_writes)?;
        self.range_writes
            .current_mut()
            .refsync_joint_activation_from_actual_host(
                &database,
                host,
                business,
                native,
                actual.lease.try_frozen()?,
                parent,
            )
    }
    pub(crate) fn persist_local_native_joint_activation(
        &mut self,
        admission: &crate::distribution_range_handoff::NativeJointOpenAdmission,
        actual: &ActualServiceNativeJointPreparedEvidence,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-service-activate-or-recover-total",
            &self.node_id,
            admission.cancellation(),
        );
        parent.check()?;
        if !admission.has_fresh_open_phase() {
            admission.require_external_publications_current()?;
            admission.require_local_open_preparation(self)?;
        }
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        actual.require_original_phase_owner(&database, &self.range_writes)?;
        self.storage_policy.require_database(&database)?;
        self.range_writes
            .current_mut()
            .refsync_native_joint_catalogue_activation(&database, admission)?;
        self.require_native_catalog_database(&database)?;
        actual.writer_domain.require_current(
            &database,
            &self.range_writes,
            admission.data().activation_range(),
        )?;
        if actual.prepared.local_node_id() != &self.node_id {
            return Err(data_error(
                "joint catalogue activation targets another actual service",
            ));
        }
        if let Some(recovered) = self
            .range_writes
            .recover_native_joint_catalogue_activation(&database, admission)?
        {
            self.range_writes
                .current_mut()
                .install_native_joint_activated_open(
                    &database,
                    &recovered,
                    actual.lease.try_frozen()?,
                )?;
            parent.check()?;
            admission.require_live()?;
            #[cfg(test)]
            {
                eprintln!("ACTUAL_JOINT_OPEN_RECOVERED exact-protected-activation=true node={} no-catalogue-rewrite=true",self.node_id);
                trace.complete();
            }
            return Ok(recovered);
        }
        let receipt = self
            .range_writes
            .current_mut()
            .persist_native_joint_catalogue_activation(
                &database,
                admission,
                admission.committed(),
                &actual.catalogue,
                actual.lease.image(),
                actual.lease.try_frozen()?,
                parent,
            )?;
        self.range_writes
            .current_mut()
            .install_native_joint_activated_open(&database, &receipt, actual.lease.try_frozen()?)?;
        #[cfg(test)]
        trace.complete();
        Ok(receipt)
    }
    pub(crate) fn require_local_native_joint_activation(
        &self,
        receipt: &crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        receipt.require_original_writer_current(&database, &self.range_writes)?;
        receipt.require_current(&database, &self.range_writes)
    }
    pub(crate) fn arm_local_native_joint_open(
        &self,
        receipt: &crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        receipt.require_original_writer_current(&database, &self.range_writes)?;
        self.range_writes
            .arm_native_joint_activated_open(&database, receipt)
    }
    pub(crate) fn require_actual_native_joint_bridge(
        &self,
        receipt: &crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        self.range_writes
            .require_native_joint_host_base(&database, receipt.data().old_range().id)?;
        if receipt.local_node_id() != &self.node_id {
            return Err(data_error(
                "protected joint bridge belongs to another actual service",
            ));
        }
        receipt.require_current(&database)
    }
    pub(crate) fn publish_local_native_joint_bridge(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointActivationAdmission,
        actual: &ActualServiceNativeJointPreparedEvidence,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJointServingBridgeReceipt> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "bridge-publish-service-total",
            &self.node_id,
            admission.test_phase_cancellation(),
        );
        parent.check()?;
        admission.require_publication_service_entry(self)?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        actual.require_original_phase_owner(&database, &self.range_writes)?;
        self.require_native_catalog_database(&database)?;
        actual.writer_domain.require_current(
            &database,
            &self.range_writes,
            admission.data().activation_range(),
        )?;
        if actual.prepared.local_node_id() != &self.node_id {
            return Err(data_error(
                "joint native bridge publishes on another actual service",
            ));
        }
        let result = database.publish_native_joint_serving_bridge(
            admission,
            admission.committed(),
            &actual.prepared,
            &self.range_writes,
            &actual.catalogue,
            actual.lease.image(),
            actual.lease.frozen(),
            parent,
        )?;
        #[cfg(test)]
        trace.complete();
        Ok(result)
    }
    pub(crate) fn recover_local_native_joint_bridge(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointActivationAdmission,
        actual: &ActualServiceNativeJointPreparedEvidence,
        parent: &CancellationToken,
    ) -> Result<Option<crate::db::NativeJointServingBridgeReceipt>> {
        parent.check()?;
        if admission.has_fresh_publication_phase() {
            // Final foreign sweep and all local proof checks are inside the
            // scoped native reader. It never recreates an absent receipt.
            let database = checked_database_read(&self.db, &self.storage_policy)?;
            self.require_native_catalog_database(&database)?;
            actual.require_original_phase_owner(&database, &self.range_writes)?;
            actual.writer_domain.require_current(
                &database,
                &self.range_writes,
                admission.data().activation_range(),
            )?;
            return database.recover_scoped_native_joint_bridge(
                admission,
                &actual.prepared,
                &self.range_writes,
                &actual.catalogue,
                actual.lease.image(),
                actual.lease.try_frozen()?,
                parent,
            );
        }
        admission.require_external_preparations_current()?;
        admission.require_local_preparation_current(self)?;
        if actual.prepared.checksum_sha256() != admission.local_preparation().checksum_sha256()
            || actual.prepared.native_database_incarnation()
                != admission.local_preparation().native_database_incarnation()
            || actual.prepared.local_node_id() != &self.node_id
        {
            return Err(data_error(
                "joint publication recovery substituted its actual local preparation/open",
            ));
        }
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        actual.require_original_phase_owner(&database, &self.range_writes)?;
        actual.writer_domain.require_current(
            &database,
            &self.range_writes,
            admission.data().activation_range(),
        )?;
        let result = database
            .read_existing_native_joint_serving_bridge(admission.committed(), &actual.prepared)?;
        parent.check()?;
        admission.require_live()?;
        Ok(result)
    }
    pub(crate) fn prepare_local_native_joint_serving(
        &self,
        committed: CommittedNativeJointDecisionObservation,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<ActualServiceNativeJointPreparedEvidence> {
        parent.check()?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        committed.require_current(&database)?;
        if matches!(&lease, NativeJointServingLease::Survivor(_))
            && lease.frozen().owner() != committed.data().promise().session_id
        {
            return Err(data_error(
                "joint survivor preparation does not own the exact permanent session",
            ));
        }

        self.require_native_writer_host_for_range(&database, committed.data().activation_range())?;
        if committed.native_receipt().local_node_id() != &self.node_id {
            return Err(data_error(
                "joint preparation uses another service's actual committed evidence",
            ));
        }
        let writer = self.range_writes.native_writer_host_state()?;
        writer.require_database(&database, committed.data().activation_range())?;
        let catalogue = self.range_writes.observe_native_joint_bridge_catalogue(
            &database,
            &committed,
            lease.image(),
            lease.frozen(),
        )?;
        if !Arc::ptr_eq(&writer, &self.range_writes.native_writer_host_state()?) {
            return Err(data_error(
                "joint writer registration changed before native preparation",
            ));
        }
        writer.require_database(&database, committed.data().activation_range())?;
        let prepared = database.prepare_native_joint_serving(
            &committed,
            &self.range_writes,
            &catalogue,
            lease.image(),
            lease.frozen(),
            parent,
        )?;
        let pin = database
            .range_mutation_gate()
            .try_source_capture_read_pin(committed.data().old_range().id, lease.frozen())?;
        drop(database);
        let evidence = ActualServiceNativeJointPreparedEvidence {
            committed,
            prepared,
            catalogue,
            writer_domain: crate::db::NativeJointWriterDomain::Installed(writer.clone()),
            writer: crate::db::NativeJointPhaseOwner::Writer(writer),
            lease,
            pin: parking_lot::Mutex::new(Some(pin)),
        };
        evidence.require_current(self)?;
        parent.check()?;
        Ok(evidence)
    }
}

impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn writer_domain(&self) -> &crate::db::NativeJointWriterDomain {
        &self.writer_domain
    }
    pub(crate) fn observe_fresh_committed(
        &self,
        service: &ClusterDataNodeService,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        range: RangeId,
    ) -> Result<CommittedNativeJointDecisionObservation> {
        service.observe_joint_decision_with_writer_domain(
            &self.writer_domain,
            metadata,
            business,
            range,
        )
    }
}
impl ClusterDataNodeService {
    pub(crate) fn observe_joint_decision_with_writer_domain(
        &self,
        domain: &crate::db::NativeJointWriterDomain,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        range: RangeId,
    ) -> Result<CommittedNativeJointDecisionObservation> {
        match domain {
            #[cfg(feature = "tls-replication")]
            crate::db::NativeJointWriterDomain::Registered(owner) => self
                .observe_registered_committed_joint_decision(
                    domain, owner, metadata, business, range,
                ),
            crate::db::NativeJointWriterDomain::Installed(_) => {
                self.observe_committed_native_joint_decision(metadata, business, range)
            }
            #[cfg(feature = "tls-replication")]
            crate::db::NativeJointWriterDomain::Closed(closed) => {
                metadata.require_live_request(business)?;
                if range != closed.range().id {
                    return Err(data_error("CLOSED joint observer selected another range"));
                }
                let database = checked_database_read(&self.db, &self.storage_policy)?;
                self.require_native_catalog_database(&database)?;
                domain.require_current(&database, &self.range_writes, closed.range())?;
                let local = metadata
                    .committed_topology(business)?
                    .nodes
                    .get(&self.node_id)
                    .ok_or_else(|| data_error("CLOSED joint actual local member missing"))?;
                let native = database.native_recovery_admission(
                    closed.data().cluster_id(),
                    closed.data().bootstrap_id(),
                    &self.node_id,
                    local.incarnation,
                )?;
                let actual = database.read_native_joint_decision(
                    &native,
                    range,
                    closed.data().checksum_sha256(),
                )?;
                if actual.data() != closed.data() {
                    return Err(data_error(
                        "CLOSED joint observer replaced actual protected decision",
                    ));
                }
                let evidence = super::ActualServiceNativeJointCommittedEvidence {
                    database_incarnation: native.database_incarnation().into(),
                    native: actual,
                    metadata,
                    business,
                };
                let result =
                    CommittedNativeJointDecisionObservation::from_actual_service(evidence)?;
                result.require_current(&database)?;
                domain.require_current(&database, &self.range_writes, closed.range())?;
                Ok(result)
            }
        }
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn prepare_local_joint_serving_with_closed_writer(
        &self,
        closed: Arc<crate::db::NativeClosedWriterDomain>,
        committed: CommittedNativeJointDecisionObservation,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<ActualServiceNativeJointPreparedEvidence> {
        parent.check()?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        let domain = crate::db::NativeJointWriterDomain::Closed(closed);
        domain.require_current(
            &database,
            &self.range_writes,
            committed.data().activation_range(),
        )?;
        committed.require_current(&database)?;
        if matches!(&lease, NativeJointServingLease::Survivor(_))
            && lease.try_frozen()?.owner() != committed.data().promise().session_id
        {
            return Err(data_error(
                "CLOSED writer joint preparation changed actual permanent OLD owner",
            ));
        }
        let writer = domain.actual_writer()?.clone();
        if committed.native_receipt().local_node_id() != &self.node_id {
            return Err(data_error(
                "CLOSED writer joint preparation changed actual node",
            ));
        }
        let catalogue = self.range_writes.observe_native_joint_bridge_catalogue(
            &database,
            &committed,
            lease.image(),
            lease.try_frozen()?,
        )?;
        domain.require_current(
            &database,
            &self.range_writes,
            committed.data().activation_range(),
        )?;
        let prepared = database.prepare_native_joint_serving(
            &committed,
            &self.range_writes,
            &catalogue,
            lease.image(),
            lease.try_frozen()?,
            parent,
        )?;
        let pin = database
            .range_mutation_gate()
            .try_source_capture_read_pin(committed.data().old_range().id, lease.try_frozen()?)?;
        drop(database);
        let evidence = ActualServiceNativeJointPreparedEvidence {
            committed,
            prepared,
            catalogue,
            writer: crate::db::NativeJointPhaseOwner::Writer(writer),
            writer_domain: domain,
            lease,
            pin: parking_lot::Mutex::new(Some(pin)),
        };
        evidence.require_current(self)?;
        parent.check()?;
        Ok(evidence)
    }
    pub(crate) fn require_joint_bridge_with_writer_domain(
        &self,
        receipt: &crate::db::NativeJointServingBridgeReceipt,
        domain: &crate::db::NativeJointWriterDomain,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        domain.require_current(
            &database,
            &self.range_writes,
            receipt.data().activation_range(),
        )?;
        if receipt.local_node_id() != &self.node_id {
            return Err(data_error(
                "joint bridge writer domain changed actual local node",
            ));
        }
        receipt.require_current(&database)
    }
}
