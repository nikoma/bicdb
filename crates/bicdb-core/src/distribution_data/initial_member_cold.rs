//! Cold service of an INITIAL member whose consensus was already exposed by
//! an earlier process. A NEW service over the strictly reopened root: it is
//! metadata-only exactly like the service the fresh launch leaves behind, and
//! carries no writer policy, leadership, readiness or data admission.
use super::*;
impl ClusterDataNodeService {
    pub(crate) fn open_cold_initial_member_metadata(
        cluster: ClusterId,
        node: ClusterNodeId,
        root: &std::path::Path,
        database: Arc<RwLock<BicDb>>,
        metadata: &crate::MetadataConsensusStore,
        seed: &Arc<crate::distribution_native_host::NativeProvisionalNode>,
    ) -> Result<Self> {
        // The reopened owned store must be the registered one of this actor.
        metadata.registered_native_metadata_member_store_id()?;
        seed.require_same_initial_database(&database, &cluster, &node)?;
        if metadata.status().node_id != node {
            return Err(data_error(
                "cold initial member metadata belongs to another actor",
            ));
        }
        let mut service = Self::open(cluster, node, root, database, true)?;
        metadata.require_listener_database(&service.listener_open_guard())?;
        if service.native_writer_enforcement_version()? != 1 {
            return Err(data_error(
                "cold initial member requires the actual enforced database",
            ));
        }
        // The same listener purpose the fresh launch transition leaves:
        // historical preparation answers and consensus, nothing else.
        service.initial_preparation_provider = Some(Arc::downgrade(seed));
        service.initial_consensus_only = true;
        Ok(service)
    }
    /// Read-only, for a reopened INITIAL owner's leader recovery: the index
    /// of a command this node started (protected writer-start) that its
    /// native prefix does not cover. Such a command needs the native abort or
    /// tail recovery; the ordinary leader recovery reads the raw log only and
    /// would not see it when the process ended before its local Prepare.
    pub(crate) fn unfinished_native_writer_start(
        &self,
        database: &BicDb,
        range: &RangeDescriptor,
    ) -> Result<Option<u64>> {
        self.require_native_catalog_database(database)?;
        let native_index = database
            .current_native_range_source_lineage(range.id)?
            .range_index();
        Ok(self
            .range_writes
            .native_abort_start_beyond(range.id, native_index))
    }
}
