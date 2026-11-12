//! An actual local range-log observation for history compatibility checks.
//! It is not a source quorum proof, role, mutation permission or floor setter.
use super::*;

const MAX_OBLIGATIONS: usize = 65_536;
const MAX_OBSERVATION_BYTES: usize = 16 * 1024 * 1024;
// The sparse catalogue itself is bounded at 32 MiB. Hash its full bodies and
// audit provenance without allocating another full encoded catalogue; leave a
// bounded allowance for the current certification/fence provenance as well.
const MAX_EXTENDED_OBSERVATION_BYTES: usize = 34 * 1024 * 1024;

fn insert_irrevocable(
    commands: &mut BTreeMap<u64, String>,
    index: u64,
    checksum: &str,
) -> Result<()> {
    if index == 0 || checksum.len() != 64 || !checksum.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(range_write_error("invalid image history obligation"));
    }
    if let Some(previous) = commands.get(&index) {
        if previous != checksum {
            return Err(range_write_error(
                "image history obligations conflict at one command index",
            ));
        }
    } else {
        if commands.len() >= MAX_OBLIGATIONS {
            return Err(range_write_error(
                "image history obligation union exceeds bound",
            ));
        }
        commands.insert(index, checksum.to_owned());
    }
    Ok(())
}

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
    /// All retained CQ/Applied, current-certification and permanent sparse
    /// obligations, including at/below native head.
    /// A caller cannot filter this map or reconstruct the enclosing token.
    pub(crate) fn irrevocable_commands(&self) -> &BTreeMap<u64, String> {
        &self.irrevocable_commands
    }

    /// Exact protected activation is the ONLY allowed epoch-only transition.
    /// All retained command states, watermarks and full provenance remain bound.
    pub(super) fn require_after_native_joint_activation(
        &self,
        store: &RangeWriteStore,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        fact: &NativeJointCatalogueActivationFact,
    ) -> Result<()> {
        fact.require_current_with_scope(scope, store)?;
        let native = fact.native();
        let actual = store.progress(self.range_id);
        if self.store_open_id != fact.store_open_id()
            || self.range_id != native.data().old_range().id
            || self.range_log_sha256 != native.preparation_catalogue_sha256()
            || actual.current_epoch != native.data().activation_range().epoch
            || self.current_epoch > actual.current_epoch
        {
            return Err(range_write_error(
                "joint catalogue epoch transition has no exact protected preparation/activation",
            ));
        }
        let normalized = store.observe_image_obligations_at_original_joint_epoch(
            self.range_id,
            Some(self.current_epoch),
        )?;
        if normalized.store_open_id != self.store_open_id
            || normalized.cluster_id != self.cluster_id
            || normalized.node_id != self.node_id
            || normalized.root != self.root
            || normalized.range_log_sha256 != self.range_log_sha256
        {
            return Err(range_write_error(
                "joint activation changed catalogue beyond its exact protected epoch transition",
            ));
        }
        Ok(())
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
        self.observe_image_obligations_at_original_joint_epoch(range_id, None)
    }
    fn observe_image_obligations_at_original_joint_epoch(
        &self,
        range_id: RangeId,
        protected_original_epoch: Option<u64>,
    ) -> Result<TargetImageCatalogHistoryObligations> {
        self.require_durable_recovery_catalog()?;
        let mut progress = self.progress(range_id);
        if let Some(epoch) = protected_original_epoch {
            progress.current_epoch = epoch;
        }
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
                    insert_irrevocable(
                        &mut irrevocable_commands,
                        *index,
                        &entry.command.checksum_sha256,
                    )?;
                }
            }
        }
        let certifications = self
            .catalog
            .recovery_fences
            .get(&range_id)
            .filter(|fence| !fence.historical_certifications.is_empty());
        if let Some(fence) = certifications {
            fence.validate()?;
            for (index, marker) in &fence.historical_certifications {
                insert_irrevocable(&mut irrevocable_commands, *index, &marker.command_sha256)?;
            }
        }
        // This validates every complete sparse record and its actual ordered
        // history conflicts before exporting its checksum as an obligation.
        for (index, checksum) in self.sparse_irreversible_commands(range_id)? {
            insert_irrevocable(&mut irrevocable_commands, index, &checksum)?;
        }
        for (index, checksum) in self.installing_certification_obligations(range_id)? {
            insert_irrevocable(&mut irrevocable_commands, index, &checksum)?;
        }
        let sparse = self
            .catalog
            .sparse_committed_decisions
            .as_ref()
            .and_then(|ranges| ranges.get(&range_id))
            .filter(|entries| !entries.is_empty());
        let mut digest = Sha256::new();
        digest.update(crate::distribution_range_image::bounded_json(
            &(
                &self.catalog.cluster_id,
                &self.catalog.node_id,
                &progress,
                observations,
            ),
            MAX_OBSERVATION_BYTES,
        )?);
        if certifications.is_some() || sparse.is_some() {
            struct HashWriter<'a> {
                digest: &'a mut Sha256,
                bytes: usize,
            }
            impl std::io::Write for HashWriter<'_> {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    let next = self.bytes.checked_add(bytes.len()).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "image history digest overflow",
                        )
                    })?;
                    if next > MAX_EXTENDED_OBSERVATION_BYTES {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "complete image history provenance exceeds bounded digest size",
                        ));
                    }
                    self.bytes = next;
                    self.digest.update(bytes);
                    Ok(bytes.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            // Preserve the exact legacy digest when both additions are absent.
            // Otherwise commit FULL saved certificate and sparse provenance,
            // including command bodies and original audit/open/birth bindings.
            serde_json::to_writer(
                HashWriter {
                    digest: &mut digest,
                    bytes: 0,
                },
                &("bicdb-image-history-extended-v1", certifications, sparse),
            )?;
        }
        if let Some(current) = self
            .catalog
            .installing_committed_certifications
            .as_ref()
            .and_then(|m| m.get(&range_id))
            .filter(|m| !m.is_empty())
        {
            digest.update(crate::distribution_range_image::bounded_json(
                &("Installing-existing-committed-provenance-v1", current),
                MAX_EXTENDED_OBSERVATION_BYTES,
            )?);
        }
        if let Some(h) = self
            .catalog
            .installing_recovery_handovers
            .as_ref()
            .and_then(|m| m.get(&range_id))
            .filter(|m| !m.is_empty())
        {
            digest.update(crate::distribution_range_image::bounded_json(
                &("Installing-refusal-handover-history-v1", h),
                MAX_EXTENDED_OBSERVATION_BYTES,
            )?);
        }
        let range_log_sha256 = hex::encode(digest.finalize());
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
