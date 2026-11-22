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
    /// REOPENED INITIAL leader only, before its ordinary leader recovery.
    /// The earlier process ended inside `finish_local_application` after its
    /// native completion of its own command (durable Core WAL + certified
    /// native prefix + immutable history leaf, pending deleted, in one native
    /// page transaction) and before its raw log recorded that command
    /// Applied. The raw log then ends one below the native prefix: its first
    /// unresolved entry IS the native prefix's command, already certified
    /// (QuorumCommitted) in this node's own raw log.
    ///
    /// Only that local raw resolution is missing; nothing is decided here.
    /// The command is irrevocable (its own durable certificate), and its
    /// effect is proven by this node's own native history leaf at exactly
    /// that index naming exactly that command checksum (the same proof a
    /// follower uses, `require_native_command_materialized`, before it marks
    /// an already materialized command Applied). No native prefix, row or
    /// peer state changes; no generic replay happens. Every other shape
    /// (an uncertified entry, another command, a gap, a pending or
    /// invalidated prefix, an active recovery context) returns `None` and is
    /// left to the refusals that already guard it.
    pub(crate) fn resolve_reopened_native_completed_raw_entry(
        &mut self,
        database: &BicDb,
        range: &RangeDescriptor,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<Option<u64>> {
        self.range_mutations.validate(range.id, authority)?;
        self.require_native_catalog_database(database)?;
        self.require_catalog_storage_policy()?;
        let prefix = database.native_range_prefix_observation(range.id)?;
        if prefix.invalidated
            || prefix.pending_command_index.is_some()
            || prefix.materialized_pending_index.is_some()
        {
            return Ok(None);
        }
        let (Some(index), Some(base)) = (prefix.native_applied_index, prefix.native_base_index)
        else {
            return Ok(None);
        };
        let progress = self.range_writes.progress(range.id);
        if index <= base
            || progress.resolved_through.checked_add(1) != Some(index)
            || progress.last_index != index
        {
            return Ok(None);
        }
        let Some(entry) = self
            .probe_range_write_for_peer(range, &self.node_id, index)?
            .entry
        else {
            return Ok(None);
        };
        entry.command.validate()?;
        if entry.state != RangeWriteState::QuorumCommitted
            || entry.command.cluster_id != self.cluster_id
            || entry.command.index != index
            || entry.command.range_id != range.id
            || entry.command.range_epoch != range.epoch
            || entry.command.leader_node_id != self.node_id
        {
            return Ok(None);
        }
        // A protected writer-start slot at this index must be this command.
        self.range_writes.has_native_abort_attempt(&entry.command)?;
        self.range_writes
            .normal_native_write_allowed(&entry.command)?;
        let lineage = database.current_native_range_source_lineage(range.id)?;
        if lineage.range_index() != index {
            return Err(data_error(
                "reopened native completion changed under its raw resolution",
            ));
        }
        database.require_native_command_materialized(&lineage, &entry.command)?;
        self.range_writes
            .current_mut()
            .mark_applied(&entry.command)?;
        Ok(Some(index))
    }
}
