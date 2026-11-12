//! Schema setup only for an actual fresh, unassigned native factory. No row
//! import, range activation, metadata vote or ordinary writer grant is minted.
use super::*;
use crate::distribution::{ClusterTopology, MetadataMemberRole};

impl BicDb {
    pub(crate) fn initialize_unassigned_native_learner_schema(
        &mut self,
        factory: &FreshRangeGenesisAuthority,
        topology: &ClusterTopology,
        bundle: &ClusterSchemaBundle,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        bundle.verify()?;
        // SQL structural catalogue records need a separate scoped transaction
        // producer. Never silently omit them or enable generic native import.
        if !bundle.structural_records.is_empty() {
            return Err(BicDbError::Unsupported(
                "native learner schema bootstrap does not yet install SQL structural records"
                    .into(),
            ));
        }
        let native = factory.require_unwritten_enrollment_database(self, topology)?;
        let member = &topology.nodes[native.local_node_id()];
        if native.writer_enforcement_version() != 1
            || native.metadata_initialization_version() != 1
            || member.metadata_role != MetadataMemberRole::Learner
            || !factory.birth.spec.initial_ranges.is_empty()
            || topology.ranges.values().any(|range| {
                range
                    .replicas
                    .iter()
                    .any(|replica| &replica.node_id == native.local_node_id())
            })
            || self
                .collections()
                .iter()
                .any(|meta| !meta.name.starts_with("__bicdb_"))
            || !self.indexes.is_empty()
        {
            return Err(invalid(
                "schema bootstrap needs an actual fresh unassigned native learner",
            ));
        }
        self.require_native_metadata_uninitialized()?;
        self.ensure_writable("initialize unassigned native learner schema")?;
        let _mutation = self.range_mutations.try_enter_database()?;
        for meta in &bundle.collections {
            cancellation.check()?;
            let mut state = CollectionState::new(self.new_local_collection_metadata(meta));
            state.paged_lazy = self.paged_records.is_some();
            self.collections
                .insert(meta.name.clone(), RwLock::new(state));
        }
        self.persist_catalog()?;
        self.schema_compatibility.invalidate();
        drop(_mutation);
        for index in &bundle.indexes {
            cancellation.check()?;
            self.create_index(index.clone())?;
        }
        cancellation.check()?;
        let actual = self.schema_compatibility_fingerprint()?;
        if actual.sha256 != bundle.fingerprint.sha256 {
            return Err(invalid(
                "actual fresh learner schema differs; keep factory quarantined",
            ));
        }
        factory.require_unwritten_enrollment_database(self, topology)?;
        self.require_native_metadata_uninitialized()?;
        cancellation.check()
    }
}
