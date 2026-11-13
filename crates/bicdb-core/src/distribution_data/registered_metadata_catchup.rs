//! A separate lower-authority listener purpose. No READY/control/data grant.
use super::*;
use crate::distribution_consensus::NativeRegisteredMetadataCatchupObservation;
impl ClusterDataNodeService {
    pub(crate) fn open_cold_registered_metadata_catchup(
        cluster: ClusterId,
        node: ClusterNodeId,
        root: &std::path::Path,
        database: Arc<RwLock<BicDb>>,
        metadata: &crate::MetadataConsensusStore,
        proof: &NativeRegisteredMetadataCatchupObservation<'_>,
    ) -> Result<Self> {
        proof.require_current(metadata)?;
        {
            let db = database
                .try_read()
                .ok_or_else(|| data_error("Registered catch-up DB busy before service"))?;
            proof.require_database(&db)?;
        }
        let mut service = Self::open(cluster, node, root, database, true)?;
        metadata.require_listener_database(&service.listener_open_guard())?;
        if service.native_writer_enforcement_version()? != 1 {
            return Err(data_error(
                "Registered catch-up needs actual enforced native DB",
            ));
        }
        proof.require_current(metadata)?;
        service.initial_consensus_only = true;
        Ok(service)
    }
    pub(crate) fn transition_registered_metadata_catchup_listener(
        &mut self,
        actual: &Arc<Mutex<Self>>,
        drained: &crate::distribution_transport::DrainedClusterDataListener,
        database: &Arc<RwLock<BicDb>>,
        metadata: &crate::MetadataConsensusStore,
        proof: &NativeRegisteredMetadataCatchupObservation<'_>,
    ) -> Result<()> {
        drained.require_service(actual)?;
        drained.require_store(self.native_initial_listener_store_open_id())?;
        if !Arc::ptr_eq(&self.db, database)
            || self.initial_birth_provider.is_some()
            || self.initial_preparation_provider.is_some()
            || (self.learner_birth_provider.is_none() && !self.initial_consensus_only)
        {
            return Err(data_error(
                "Registered catch-up actual same-service/drain attachment differs",
            ));
        }
        metadata.require_listener_database(&self.listener_open_guard())?;
        proof.require_current(metadata)?;
        // Never modifies any old listener's captured purpose/allowlist.
        self.learner_birth_provider = None;
        self.initial_consensus_only = true;
        Ok(())
    }
}
