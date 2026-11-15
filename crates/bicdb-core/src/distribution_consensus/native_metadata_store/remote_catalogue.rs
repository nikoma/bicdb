//! Necessary actual pending-entry comparison ONLY. This neither invents a
//! dual-data proof nor retries failed metadata persistence.
use super::*;
impl MetadataConsensusStore {
    pub(crate) fn require_native_remote_catalogue_pending(
        &self,
        entry: &MetadataLogEntry,
        current: &HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        self.require_native_control_leader(current, business)?;
        if entry.leadership_barrier
            || entry.restore_activation.is_some()
            || entry.term != self.state.current_term
            || self
                .state
                .log
                .iter()
                .filter(|e| !e.leadership_barrier)
                .count()
                != 1
            || self.state.log.iter().find(|e| e.index == entry.index) != Some(entry)
        {
            return Err(metadata_error(
                "FIRST original native catalogue proposal no longer matches actual pending entry",
            ));
        }
        Ok(())
    }
}
