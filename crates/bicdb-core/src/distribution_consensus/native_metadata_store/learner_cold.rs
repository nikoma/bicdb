//! Original configuration is a protected initialization FACT, never a fresh
//! quorum or a snapshot at today's read index. No term-zero reset is provided.
use super::*;
use std::io::Read;

pub(crate) const ORIGINAL_CONFIGURATION_FILE: &str = "native-learner-metadata-original-c.json";
const MAX_ORIGINAL_BYTES: usize = 4 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalConfiguration {
    version: u32,
    topology: ClusterTopology,
    checksum_sha256: String,
}
impl OriginalConfiguration {
    fn checksum(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &(
                    "NativeLearnerMetadataOriginalConfiguration-v1",
                    self.version,
                    &self.topology,
                ),
                MAX_ORIGINAL_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        self.topology.validate()?;
        if self.version != 1 || self.checksum_sha256 != self.checksum()? {
            return Err(metadata_error(
                "original learner C sidecar checksum/version invalid",
            ));
        }
        Ok(())
    }
}
fn load_original(root: &Path) -> Result<OriginalConfiguration> {
    let mut bytes = Vec::new();
    fs::File::open(root.join(ORIGINAL_CONFIGURATION_FILE))?
        .take((MAX_ORIGINAL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_ORIGINAL_BYTES {
        return Err(metadata_error("original learner C sidecar bound exceeded"));
    }
    let result: OriginalConfiguration = serde_json::from_slice(&bytes)?;
    result.validate()?;
    Ok(result)
}
pub(super) fn persist_original_configuration(
    root: &Path,
    topology: &ClusterTopology,
) -> Result<()> {
    // Called only under actual DB/metadata-directory ownership, before any
    // protected UUID exists. An orphan cannot authorize a metadata endpoint.
    let mut value = OriginalConfiguration {
        version: 1,
        topology: topology.clone(),
        checksum_sha256: String::new(),
    };
    value.checksum_sha256 = value.checksum()?;
    let bytes = crate::distribution_range_image::bounded_json(&value, MAX_ORIGINAL_BYTES)?;
    crate::storage::write_atomic(&root.join(ORIGINAL_CONFIGURATION_FILE), &bytes, true)?;
    // The metadata directory may itself be new. Its link in the actual DB
    // parent must be durable BEFORE native WAL can reference the sidecar.
    if let Some(parent) = root.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
pub(super) fn require_never_voted_state(state: &MetadataConsensusState) -> Result<()> {
    if state.format_version != NATIVE_METADATA_CONSENSUS_FORMAT
        || state.native_store_id.is_none()
        || state.current_term != 0
        || state.voted_for.is_some()
        || state.role != MetadataConsensusRole::Follower
        || state.leader_id.is_some()
        || state.base_index != 0
        || state.base_term != 0
        || state.commit_index != 0
        || !state.log.is_empty()
        || state.committed_restore_activation.is_some()
    {
        return Err(metadata_error(
            "Prepared learner state is not exact never-exposed term-zero state; quarantined",
        ));
    }
    Ok(())
}

#[cfg(feature = "tls-replication")]
impl MetadataConsensusStore {
    /// No authority is reconstructed from sidecar data. OWN current MQ admits
    /// the actual unassigned learner; only the native receipt names the UUID/C.
    /// Caller retains this slot on EVERY error and exposes no Prepared store.
    pub(crate) fn recover_native_learner_owned_retaining(
        root: &Path,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        retained: &mut Option<Self>,
    ) -> Result<()> {
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        observed.require_live_request(business)?;
        let current = observed.committed_topology(business)?;
        let node = factory.node_id().clone();
        let member = current
            .nodes
            .get(&node)
            .ok_or_else(|| metadata_error("cold learner current identity absent"))?;
        if observed.requester_node_id() != &node
            || observed.requester_incarnation() != member.incarnation
            || member.lifecycle != ClusterNodeLifecycle::Active
            || member.metadata_role != MetadataMemberRole::Learner
            || current
                .ranges
                .values()
                .any(|r| r.replicas.iter().any(|replica| replica.node_id == node))
        {
            return Err(metadata_error(
                "cold metadata needs actual OWN current unassigned learner MQ",
            ));
        }
        let record = {
            let db = database
                .try_read()
                .ok_or_else(|| metadata_error("cold actual DB busy"))?;
            let native = factory.require_unwritten_enrollment_database(&db, current)?;
            if native.restore_activation() != observed.restore_activation(business)? {
                return Err(metadata_error("cold learner restore identity changed"));
            }
            let record = db.native_metadata_initialization_record(
                &current.cluster_id,
                current
                    .range_image_bootstrap_id
                    .ok_or_else(|| metadata_error("cold bootstrap absent"))?,
                &node,
                member.incarnation,
            )?;
            if let Some(record) = &record {
                record.require_namespaces_current(&db)?;
            }
            record
        };
        let Some(record) = record else {
            if retained.is_some() {
                return Err(metadata_error(
                    "retained metadata owner has no protected UUID; quarantined",
                ));
            }
            // Existing state without a protected record refuses in initializer.
            // A sidecar-only cut may be replaced BEFORE the first UUID/Prepare.
            Self::initialize_native_owned_into(
                root,
                database,
                factory,
                current.clone(),
                &token,
                retained,
                |_| {
                    observed.require_live_request(business)?;
                    token.check()
                },
            )?;
            observed.require_live_request(business)?;
            return token.check();
        };
        if record.registered() && retained.is_none() {
            Self::reopen_native_owned_retaining(
                root,
                Arc::clone(&database),
                node.clone(),
                current.clone(),
                &token,
                retained,
            )?;
        } else if !record.registered() {
            if record.initial_namespace_ids().is_none() {
                return Err(metadata_error(
                    "old Prepared learner lacks protected namespace identity; quarantined",
                ));
            }
            // Acquire actual owner claims BEFORE inspecting historical files.
            let mut owner = if retained.is_none() {
                Some(NativeMetadataStoreOwner::acquire(
                    root,
                    Arc::clone(&database),
                    false,
                )?)
            } else {
                None
            };
            let original = load_original(root).map_err(|e| {
                metadata_error(format!(
                    "Prepared original full C unavailable/invalid; quarantined: {e}"
                ))
            })?;
            let original_sha = hex::encode(Sha256::digest(
                crate::distribution_range_image::bounded_json(
                    &original.topology,
                    MAX_ORIGINAL_BYTES,
                )?,
            ));
            if original_sha != record.initial_topology_sha256() {
                return Err(metadata_error(
                    "Prepared original C does not match protected SHA; quarantined",
                ));
            }
            let old_member = original
                .topology
                .nodes
                .get(&node)
                .ok_or_else(|| metadata_error("original learner identity absent"))?;
            if original.topology.cluster_id != current.cluster_id
                || original.topology.range_image_bootstrap_id != current.range_image_bootstrap_id
                || old_member.lifecycle != ClusterNodeLifecycle::Active
                || old_member.metadata_role != MetadataMemberRole::Learner
                || old_member.incarnation != member.incarnation
                || old_member.native_store_birth_v1 != member.native_store_birth_v1
                || old_member.tls_certificate_sha256 != member.tls_certificate_sha256
                || old_member.pending_tls_certificate_sha256
                    != member.pending_tls_certificate_sha256
                || original
                    .topology
                    .ranges
                    .values()
                    .any(|r| r.replicas.iter().any(|r| r.node_id == node))
                || !original
                    .topology
                    .is_metadata_voter(observed.metadata_leader_id())
            {
                return Err(metadata_error("Prepared cold continuation needs unchanged actual learner and an original-C voter leader; new leader snapshot admission unsupported"));
            }
            {
                let db = database.try_read().ok_or_else(|| {
                    metadata_error("cold DB busy validating protected original C")
                })?;
                if db.read_native_metadata_store(&original.topology, &node)? != record {
                    return Err(metadata_error("cold protected original identity changed"));
                }
                if let Some(owner) = owner.as_mut() {
                    owner.bind_actual_actor(&db, &original.topology, &node)?;
                }
            }
            let expected = MetadataConsensusState {
                format_version: NATIVE_METADATA_CONSENSUS_FORMAT,
                native_store_id: Some(record.store_id()),
                cluster_id: current.cluster_id.clone(),
                node_id: node.clone(),
                current_term: 0,
                voted_for: None,
                role: MetadataConsensusRole::Follower,
                leader_id: None,
                base_index: 0,
                base_term: 0,
                commit_index: 0,
                committed_topology: original.topology.clone(),
                log: Vec::new(),
                committed_restore_activation: None,
            };
            let path = root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
            match fs::read(&path) {
                Ok(bytes) => {
                    let state = decode_state(&bytes).map_err(|e| {
                        metadata_error(format!("Prepared state invalid; quarantined: {e}"))
                    })?;
                    validate_state(&state).map_err(|e| {
                        metadata_error(format!("Prepared state invalid; quarantined: {e}"))
                    })?;
                    require_never_voted_state(&state)?;
                    if state != expected {
                        return Err(metadata_error(
                            "Prepared state/UUID/original C differs; never reset",
                        ));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
            if retained
                .as_ref()
                .is_some_and(|store| store.state != expected || store.root != root)
            {
                return Err(metadata_error(
                    "retained Prepared state differs from exact protected original; quarantined",
                ));
            }
            if retained.is_none() {
                *retained = Some(Self {
                    root: root.to_path_buf(),
                    fsync: true,
                    state: expected,
                    match_index: BTreeMap::new(),
                    last_leader_contact_ms: unix_time_ms(),
                    native_owner: owner,
                });
            }
            let store = retained.as_mut().unwrap();
            store.reset_match_indexes();
            observed.require_live_request(business)?;
            token.check()?;
            store.persist()?;
            if decode_state(&fs::read(&path)?)? != store.state {
                return Err(metadata_error("cold Prepared state readback changed"));
            }
            observed.require_live_request(business)?;
            token.check()?;
            database
                .try_read()
                .ok_or_else(|| metadata_error("cold DB busy finishing SAME Prepared UUID"))?
                .finish_native_metadata_store(&record, &original.topology)?;
            store.native_owner.as_mut().unwrap().registered_store_id = Some(record.store_id());
        } else {
            // Exact retry after protected Registered succeeded but the original
            // response/cancellation failed. Never recreate or regress a state.
            let store = retained.as_mut().unwrap();
            let mut disk = decode_state(&fs::read(
                root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
            )?)?;
            validate_state(&disk)?;
            // Strict reopen only clears these volatile Raft labels. Retrying
            // its failed fsync may preserve that change, never term/vote/log/C.
            disk.role = MetadataConsensusRole::Follower;
            disk.leader_id = None;
            if disk != store.state
                || disk.native_store_id != Some(record.store_id())
                || store.root != root
            {
                return Err(metadata_error(
                    "Registered retry state/UUID changed; strict reopen required",
                ));
            }
            store
                .native_owner
                .as_mut()
                .ok_or_else(|| metadata_error("cold actual owner absent"))?
                .registered_store_id = Some(record.store_id());
        }
        let store = retained.as_mut().unwrap();
        store.registered_native_learner_store_id()?;
        if !store
            .state
            .committed_topology
            .is_metadata_voter(observed.metadata_leader_id())
        {
            return Err(metadata_error("cold learner current leader is not a voter in retained committed C; exact fresh-snapshot admission unsupported"));
        }
        // Restrictive same-state refsync on retry; preserve term/vote/log/index.
        store.persist()?;
        observed.require_live_request(business)?;
        token.check()
    }
}

#[cfg(test)]
impl MetadataConsensusStore {
    /// Deliberate offline corruption of a REAL persisted envelope. This cannot
    /// construct a registered owner, receipt, vote or authority observation.
    pub(crate) fn test_corrupt_cold_learner_state(root: &Path, wrong_uuid: bool) -> Result<()> {
        let path = root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        let mut state = decode_state(&fs::read(&path)?)?;
        if wrong_uuid {
            state.native_store_id = Some(Uuid::now_v7());
        } else {
            require_never_voted_state(&state)?;
            state.current_term = 1;
            state.voted_for = Some(
                state
                    .committed_topology
                    .nodes
                    .values()
                    .find(|n| n.metadata_role == MetadataMemberRole::Voter)
                    .ok_or_else(|| metadata_error("test real voter absent"))?
                    .id
                    .clone(),
            );
        }
        let envelope = MetadataConsensusEnvelope {
            format_version: state.format_version,
            checksum_sha256: state_checksum(&state)?,
            state,
        };
        crate::storage::write_atomic(&path, &serde_json::to_vec(&envelope)?, true)
    }
    pub(crate) fn test_corrupt_cold_original_configuration(root: &Path) -> Result<()> {
        let mut original = load_original(root)?;
        original.topology.generation += 1;
        original.checksum_sha256 = original.checksum()?;
        crate::storage::write_atomic(
            &root.join(ORIGINAL_CONFIGURATION_FILE),
            &serde_json::to_vec(&original)?,
            true,
        )
    }
}
