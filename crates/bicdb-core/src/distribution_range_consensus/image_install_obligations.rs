//! An actual local range-log observation for history compatibility checks.
//! It is not a source quorum proof, role, mutation permission or floor setter.
use super::*;

const MAX_OBLIGATIONS: usize = 65_536;
const MAX_OBSERVATION_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct TargetImageCatalogHistoryObligations {
    store_open_id: Uuid,
    cluster_id: ClusterId,
    node_id: ClusterNodeId,
    root: PathBuf,
    range_id: RangeId,
    current_epoch: u64,
    last_index: u64,
    resolved_through: u64,
    compacted_through: u64,
    irrevocable_commands: BTreeMap<u64, String>,
    range_log_sha256: String,
}
impl TargetImageCatalogHistoryObligations {
    pub(crate) fn range_id(&self) -> RangeId {
        self.range_id
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.node_id
    }
    pub(crate) fn database_root(&self) -> &Path {
        &self.root
    }
    pub(crate) fn current_epoch(&self) -> u64 {
        self.current_epoch
    }
    pub(crate) fn last_index(&self) -> u64 {
        self.last_index
    }
    pub(crate) fn resolved_through(&self) -> u64 {
        self.resolved_through
    }
    pub(crate) fn compacted_through(&self) -> u64 {
        self.compacted_through
    }
    pub(crate) fn range_log_sha256(&self) -> &str {
        &self.range_log_sha256
    }
    /// All retained CQ/Applied obligations, including at/below native head.
    /// A caller cannot filter this map or reconstruct the enclosing token.
    pub(crate) fn irrevocable_commands(&self) -> &BTreeMap<u64, String> {
        &self.irrevocable_commands
    }

    pub(crate) fn require_current(&self, store: &RangeWriteStore) -> Result<()> {
        let current = store.image_install_catalog_history_obligations(self.range_id)?;
        if current.store_open_id != self.store_open_id
            || current.cluster_id != self.cluster_id
            || current.node_id != self.node_id
            || current.root != self.root
            || current.range_log_sha256 != self.range_log_sha256
        {
            return Err(range_write_error(
                "actual image catalog history obligations changed/reopened",
            ));
        }
        Ok(())
    }
}

impl RangeWriteStore {
    /// Read every retained actual command state. There is no caller list,
    /// selected suffix, claimed watermark or deserializable token constructor.
    pub(crate) fn image_install_catalog_history_obligations(
        &self,
        range_id: RangeId,
    ) -> Result<TargetImageCatalogHistoryObligations> {
        let progress = self.progress(range_id);
        let mut observations = Vec::new();
        let mut irrevocable_commands = BTreeMap::new();
        if let Some(log) = self.catalog.ranges.get(&range_id) {
            if log.entries.len() > MAX_OBLIGATIONS {
                return Err(range_write_error("image catalog obligation bound exceeded"));
            }
            for (index, entry) in &log.entries {
                // Entries were checked at store open/prepare; only their state
                // changes afterward. Retain identity AND state in the digest,
                // so even an unresolved tail transition invalidates this view.
                observations.push((
                    *index,
                    &entry.command.command_id,
                    &entry.command.checksum_sha256,
                    entry.state,
                ));
                if matches!(
                    entry.state,
                    RangeWriteState::QuorumCommitted | RangeWriteState::Applied
                ) {
                    irrevocable_commands.insert(*index, entry.command.checksum_sha256.clone());
                }
            }
        }
        let range_log_sha256 = hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &(
                    &self.catalog.cluster_id,
                    &self.catalog.node_id,
                    &progress,
                    observations,
                ),
                MAX_OBSERVATION_BYTES,
            )?,
        ));
        Ok(TargetImageCatalogHistoryObligations {
            store_open_id: self.open_id,
            cluster_id: self.catalog.cluster_id.clone(),
            node_id: self.catalog.node_id.clone(),
            root: self.root.canonicalize()?,
            range_id,
            current_epoch: progress.current_epoch,
            last_index: progress.last_index,
            resolved_through: progress.resolved_through,
            compacted_through: progress.compacted_through,
            irrevocable_commands,
            range_log_sha256,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn command(index: u64) -> RangeWriteCommand {
        let mut value = RangeWriteCommand {
            protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
            cluster_id: ClusterId::new("actual-catalog-obligations-test").unwrap(),
            range_id: RangeId::new(1).unwrap(),
            range_epoch: 1,
            index,
            command_id: format!("command-{index}"),
            leader_node_id: ClusterNodeId::new("n1").unwrap(),
            transaction_id: index,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".into(),
                record_id: format!("k{index}"),
                record: Some(crate::Record::new(format!("k{index}"))),
            }],
            checksum_sha256: String::new(),
        };
        value.checksum_sha256 = value.calculate_checksum().unwrap();
        value
    }
    #[test]
    fn actual_catalog_token_keeps_all_certified_obligations_and_detects_tail_transition_and_reopen()
    {
        let root = tempfile::tempdir().unwrap();
        let mut store = RangeWriteStore::open(
            root.path(),
            command(1).cluster_id,
            ClusterNodeId::new("n1").unwrap(),
            true,
        )
        .unwrap();
        let one = command(1);
        store.prepare(one.clone()).unwrap();
        store.commit(&one).unwrap();
        store.certify(&one).unwrap();
        store.mark_applied(&one).unwrap();
        let two = command(2);
        store.prepare(two.clone()).unwrap();
        store.commit(&two).unwrap();
        store.certify(&two).unwrap();
        let token = store
            .image_install_catalog_history_obligations(one.range_id)
            .unwrap();
        assert_eq!(
            token.irrevocable_commands(),
            &BTreeMap::from([
                (1, one.checksum_sha256.clone()),
                (2, two.checksum_sha256.clone())
            ])
        );
        token.require_current(&store).unwrap();
        store.mark_applied(&two).unwrap();
        assert!(
            token.require_current(&store).is_err(),
            "CQ->Applied invalidates the exact captured state"
        );
        let token = store
            .image_install_catalog_history_obligations(one.range_id)
            .unwrap();
        let three = command(3);
        store.prepare(three).unwrap();
        assert!(
            token.require_current(&store).is_err(),
            "new unresolved tail cannot hide behind unchanged CQ map"
        );
        let token = store
            .image_install_catalog_history_obligations(one.range_id)
            .unwrap();
        drop(store);
        let reopened = RangeWriteStore::open(
            root.path(),
            one.cluster_id,
            ClusterNodeId::new("n1").unwrap(),
            true,
        )
        .unwrap();
        assert!(
            token.require_current(&reopened).is_err(),
            "same bytes on a new store open need a new actual token"
        );
    }
}
