//! Protected initialization history, not a metadata quorum/term/vote proof.
//! The JSON consensus state must independently retain the SAME immutable store
//! identity. Missing state after this native receipt is never a fresh voter.
use super::*;
use crate::db::range_image_native::FreshRangeGenesisAuthority;
use crate::distribution::{ClusterId, ClusterNodeId, ClusterTopology};
use crate::distribution_consensus::MetadataRestoreActivation;
use sha2::Sha256;

const KEY: &str = "metadata-store:initialized";
const MAX_BYTES: usize = 64 * 1024;
fn invalid(message: &str) -> BicDbError {
    BicDbError::Cluster(format!("native metadata store: {message}"))
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeMetadataStoreRecord {
    version: u32,
    store_id: Uuid,
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    node_id: ClusterNodeId,
    incarnation: u64,
    birth_id: Uuid,
    birth_sha256: String,
    restore_activation: Option<MetadataRestoreActivation>,
    initial_topology_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    initial_namespace_ids: Option<Vec<(String, Uuid)>>,
    registered: bool,
    checksum_sha256: String,
}
impl NativeMetadataStoreRecord {
    fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("NativeMetadataStoreIdentity-v1", value),
                MAX_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.store_id.is_nil()
            || self.bootstrap_id.is_nil()
            || self.birth_id.is_nil()
            || self.incarnation == 0
            || self.birth_sha256.len() != 64
            || self.initial_topology_sha256.len() != 64
            || self.initial_namespace_ids.as_ref().is_some_and(|ids| {
                ids.windows(2).any(|p| p[0].0 >= p[1].0) || ids.iter().any(|(_, id)| id.is_nil())
            })
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected initialization receipt"));
        }
        Ok(())
    }
    pub(crate) fn store_id(&self) -> Uuid {
        self.store_id
    }
    pub(crate) fn initial_topology_sha256(&self) -> &str {
        &self.initial_topology_sha256
    }
    pub(crate) fn registered(&self) -> bool {
        self.registered
    }
    pub(crate) fn initial_namespace_ids(&self) -> Option<&[(String, Uuid)]> {
        self.initial_namespace_ids.as_deref()
    }
    pub(crate) fn require_namespaces_current(&self, database: &BicDb) -> Result<()> {
        if let Some(ids) = &self.initial_namespace_ids {
            if database.metadata_namespace_ids()? != *ids {
                return Err(invalid(
                    "protected metadata initial namespace identities changed",
                ));
            }
        }
        Ok(())
    }
    fn require_native(&self, native: &NativeRecoveryAdmission) -> Result<()> {
        self.validate()?;
        if self.cluster_id != *native.cluster_id()
            || self.bootstrap_id != native.bootstrap_id()
            || self.node_id != *native.local_node_id()
            || self.incarnation != native.local_member_incarnation()
            || self.birth_id != native.birth_id()
            || self.birth_sha256 != native.birth_sha256()
            || self.restore_activation.as_ref() != native.restore_activation()
            || native.writer_enforcement_version() != 1
        {
            return Err(invalid(
                "protected receipt belongs to another actual birth/domain",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    fn metadata_namespace_ids(&self) -> Result<Vec<(String, Uuid)>> {
        let mut names = self.collections.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
            .into_iter()
            .map(|name| {
                let id = self
                    .local_namespace_identity(&name)?
                    .filter(|id| !id.is_nil())
                    .ok_or_else(|| invalid("actual metadata namespace identity absent"))?;
                Ok((name, id))
            })
            .collect()
    }
    pub(super) fn metadata_native_admission(
        &self,
        topology: &ClusterTopology,
        node: &ClusterNodeId,
    ) -> Result<NativeRecoveryAdmission> {
        topology.validate()?;
        let member = topology
            .nodes
            .get(node)
            .ok_or_else(|| invalid("local member absent"))?;
        let actual = self.native_recovery_admission(
            &topology.cluster_id,
            topology
                .range_image_bootstrap_id
                .ok_or_else(|| invalid("bootstrap identity absent"))?,
            node,
            member.incarnation,
        )?;
        let expected = member
            .native_store_birth_v1
            .as_ref()
            .ok_or_else(|| invalid("local birth is not enrolled"))?;
        if expected != &crate::distribution::NativeStoreBirthV1::from_actual_native(&actual)?
            || actual.writer_enforcement_version() != 1
            || actual.metadata_initialization_version() != 1
        {
            return Err(invalid(
                "enrolled configuration does not bind the actual born-v2 store",
            ));
        }
        Ok(actual)
    }
    pub(crate) fn prepare_native_metadata_store(
        &self,
        factory: &FreshRangeGenesisAuthority,
        topology: &ClusterTopology,
        store_id: Uuid,
    ) -> Result<NativeMetadataStoreRecord> {
        let actual = factory.require_unwritten_enrollment_database(self, topology)?;
        let bound = self.metadata_native_admission(topology, actual.local_node_id())?;
        actual.require_current(self)?;
        if store_id.is_nil() {
            return Err(invalid("nil store identity"));
        }
        let initial_topology_sha256 = hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(topology, 4 * 1024 * 1024)?,
        ));
        let initial_namespace_ids = if topology.nodes[actual.local_node_id()].metadata_role
            == crate::distribution::MetadataMemberRole::Learner
        {
            Some(self.metadata_namespace_ids()?)
        } else {
            None
        };
        let _journal = self.try_native_image_journal()?;
        if self
            .read_native_range_image_record::<NativeMetadataStoreRecord>(KEY)?
            .is_some()
        {
            return Err(invalid(
                "metadata was already initialized or prepared; strict reopen required",
            ));
        }
        // Reader fencing precedes the first protected metadata receipt.
        crate::format::enable_native_metadata_store(&self.path)?;
        let mut record = NativeMetadataStoreRecord {
            version: 1,
            store_id,
            cluster_id: bound.cluster_id().clone(),
            bootstrap_id: bound.bootstrap_id(),
            node_id: bound.local_node_id().clone(),
            incarnation: bound.local_member_incarnation(),
            birth_id: bound.birth_id(),
            birth_sha256: bound.birth_sha256().to_owned(),
            restore_activation: bound.restore_activation().cloned(),
            initial_topology_sha256,
            initial_namespace_ids,
            registered: false,
            checksum_sha256: String::new(),
        };
        record.checksum_sha256 = record.checksum()?;
        self.write_native_range_image_record(KEY, &record)?;
        let stored: NativeMetadataStoreRecord = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("native prepare readback absent"))?;
        if stored != record {
            return Err(invalid("native prepare readback changed"));
        }
        Ok(record)
    }
    pub(crate) fn finish_native_metadata_store(
        &self,
        expected: &NativeMetadataStoreRecord,
        topology: &ClusterTopology,
    ) -> Result<()> {
        let actual = self.metadata_native_admission(topology, &expected.node_id)?;
        expected.require_native(&actual)?;
        expected.require_namespaces_current(self)?;
        let _journal = self.try_native_image_journal()?;
        let stored: NativeMetadataStoreRecord = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("prepared record missing"))?;
        if stored != *expected || stored.registered {
            return Err(invalid("prepared identity changed"));
        }
        let mut registered = stored;
        registered.registered = true;
        registered.checksum_sha256 = registered.checksum()?;
        self.write_native_range_image_record(KEY, &registered)?;
        let actual: NativeMetadataStoreRecord = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("registration readback absent"))?;
        if actual != registered {
            return Err(invalid("native registration changed"));
        }
        Ok(())
    }
    pub(crate) fn native_metadata_initialization_record(
        &self,
        cluster: &ClusterId,
        bootstrap: Uuid,
        node: &ClusterNodeId,
        incarnation: u64,
    ) -> Result<Option<NativeMetadataStoreRecord>> {
        let native = self.native_recovery_admission(cluster, bootstrap, node, incarnation)?;
        if native.metadata_initialization_version() != 1
            || !crate::format::native_metadata_store_active(&self.path)?
        {
            return Err(invalid(
                "metadata initialization was not fenced from actual birth",
            ));
        }
        let record = self.read_native_range_image_record::<NativeMetadataStoreRecord>(KEY)?;
        if let Some(r) = &record {
            r.require_native(&native)?;
        }
        Ok(record)
    }
    pub(crate) fn read_native_metadata_store(
        &self,
        topology: &ClusterTopology,
        node: &ClusterNodeId,
    ) -> Result<NativeMetadataStoreRecord> {
        let actual = self.metadata_native_admission(topology, node)?;
        if !crate::format::native_metadata_store_active(&self.path)? {
            return Err(invalid("metadata ownership reader fence absent"));
        }
        let _journal = self.try_native_image_journal()?;
        let record: NativeMetadataStoreRecord = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("protected store initialization missing"))?;
        record.require_native(&actual)?;
        if topology.nodes[node].metadata_role == crate::distribution::MetadataMemberRole::Learner
            && !topology.ranges.values().any(|range| {
                range
                    .replicas
                    .iter()
                    .any(|replica| &replica.node_id == node)
            })
        {
            record.require_namespaces_current(self)?;
        }
        Ok(record)
    }
    pub(crate) fn require_native_metadata_uninitialized(&self) -> Result<()> {
        // The marker is an irreversible refusal, including a Prepared orphan.
        if self
            .read_native_range_image_record::<NativeMetadataStoreRecord>(KEY)?
            .is_some()
        {
            return Err(invalid("protected metadata initialization already exists"));
        }
        Ok(())
    }
    pub(crate) fn native_metadata_owner_open_lifetime(&self) -> std::sync::Weak<()> {
        Arc::downgrade(&self.native_metadata_open_lifetime)
    }
    #[cfg(test)]
    pub(crate) fn test_native_metadata_record_bytes(&self) -> Result<Vec<u8>> {
        self.check_recovery_state()?;
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("native page store absent"))?;
        paged
            .store()
            .get_as_of(
                &paged.latest_snapshot(),
                &super::range_image_native::native_key(KEY),
            )
            .map_err(|e| BicDbError::PagedStorage(e.to_string()))?
            .ok_or_else(|| invalid("actual stored metadata record absent"))
    }
    pub(crate) fn native_metadata_owner_health(&self) -> Result<Arc<bicdb_page::DurabilityHealth>> {
        self.check_recovery_state()?;
        Ok(Arc::clone(&self.durability))
    }
}
