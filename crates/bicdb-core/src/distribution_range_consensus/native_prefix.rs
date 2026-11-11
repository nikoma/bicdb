//! Private native-prefix admission ingredients. These are not metadata/read
//! authorities, portable signatures, or source visibility by themselves.
use super::*;
use crate::db::{RangeImageBirthMember, SourceRangeImageLineage};

const MAX_NATIVE_DATA_VOTERS: usize = 64;
const MAX_NATIVE_SCOPE_BYTES: usize = 64 * 1024;

/// A bounded command/configuration snapshot. Only this parent coordinator
/// module can construct it; a decoded command DTO cannot supply its authority.
#[derive(Clone, Debug)]
pub(crate) struct NativeRangeCommandIntent {
    command_id: String,
    command_checksum_sha256: String,
    origin_transaction_id: u64,
    mutations_sha256: String,
    range_index: u64,
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    range: RangeDescriptor,
    expected_base: RangeDescriptor,
    members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    schema_sha256: String,
    scope_sha256: String,
}

/// Actual distinct certificate ACKs collected by the coordinator. This opaque
/// value is not serializable and cannot be recreated from an ACK count.
#[derive(Debug)]
pub(crate) struct NativeRangeCommitCertificate {
    intent: NativeRangeCommandIntent,
    certified_voters: BTreeSet<ClusterNodeId>,
    required_quorum: usize,
}

fn replicas(
    range: &RangeDescriptor,
) -> BTreeMap<crate::ReplicaId, (ClusterNodeId, RangeReplicaRole)> {
    range
        .replicas
        .iter()
        .map(|replica| (replica.id, (replica.node_id.clone(), replica.role)))
        .collect()
}
fn same_range_configuration(left: &RangeDescriptor, right: &RangeDescriptor) -> bool {
    left.id == right.id
        && left.start_token == right.start_token
        && left.end_token == right.end_token
        && left.epoch == right.epoch
        && left.leader == right.leader
        && left.placement == right.placement
        && left.replicas.len() == right.replicas.len()
        && replicas(left) == replicas(right)
}

impl NativeRangeCommandIntent {
    pub(super) fn plan(
        command: &RangeWriteCommand,
        range: &RangeDescriptor,
        topology: &ClusterTopology,
    ) -> Result<Self> {
        let bootstrap_id = topology
            .range_image_bootstrap_id
            .filter(|id| !id.is_nil())
            .ok_or_else(|| {
                range_write_error("native write has no committed image-aware bootstrap domain")
            })?;
        let current = topology
            .range_by_id(range.id)
            .ok_or_else(|| range_write_error("native write range is absent"))?;
        if !same_range_configuration(current, range)
            || command.cluster_id != topology.cluster_id
            || command.range_id != range.id
            || command.range_epoch != range.epoch
            || command.leader_node_id != range.leader
        {
            return Err(range_write_error(
                "native command does not match its committed-local configuration snapshot",
            ));
        }
        command.validate()?;
        let mut members = BTreeMap::new();
        for replica in &range.replicas {
            if replica.role != RangeReplicaRole::Voter {
                continue;
            }
            let member = topology
                .nodes
                .get(&replica.node_id)
                .ok_or_else(|| range_write_error("native data voter is absent"))?;
            if members
                .insert(
                    member.id.clone(),
                    RangeImageBirthMember {
                        incarnation: member.incarnation,
                        tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: member
                            .pending_tls_certificate_sha256
                            .clone(),
                    },
                )
                .is_some()
            {
                return Err(range_write_error("duplicate native data voter"));
            }
        }
        if members.is_empty() || members.len() > MAX_NATIVE_DATA_VOTERS {
            return Err(range_write_error(
                "native data voter count is outside 1..=64",
            ));
        }
        let schema_sha256 = range
            .required_schema_sha256(&topology.nodes)
            .ok_or_else(|| range_write_error("native write has no committed schema fence"))?
            .to_string();
        let mut expected_base = range.clone();
        if let Some(relocation) = topology.active_relocation_for_range(range.id) {
            if relocation.learner_epoch == range.epoch {
                if relocation.expected_epoch.checked_add(1) != Some(range.epoch) {
                    return Err(range_write_error(
                        "native write learner allocation has no exact predecessor",
                    ));
                }
                let matches = range
                    .replicas
                    .iter()
                    .filter(|replica| {
                        replica.id == relocation.target_replica_id
                            && replica.node_id == relocation.target
                            && replica.role == RangeReplicaRole::Learner
                    })
                    .count();
                if matches != 1 {
                    return Err(range_write_error(
                        "native write learner allocation differs from committed plan",
                    ));
                }
                expected_base.epoch = relocation.expected_epoch;
                expected_base
                    .replicas
                    .retain(|replica| replica.id != relocation.target_replica_id);
            }
        }
        let scope = crate::distribution_range_image::bounded_json(
            &(
                &topology.cluster_id,
                bootstrap_id,
                &range.id,
                range.start_token,
                range.end_token,
                range.epoch,
                &range.leader,
                &members,
                &schema_sha256,
            ),
            MAX_NATIVE_SCOPE_BYTES,
        )?;
        Ok(Self {
            command_id: command.command_id.clone(),
            command_checksum_sha256: command.checksum_sha256.clone(),
            origin_transaction_id: command.transaction_id,
            mutations_sha256: crate::db::admission_mutations_sha256(&command.mutations)?,
            range_index: command.index,
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id,
            range: range.clone(),
            expected_base,
            members,
            schema_sha256,
            scope_sha256: hex::encode(Sha256::digest(scope)),
        })
    }
    pub(crate) fn validate_native_base(&self, lineage: &SourceRangeImageLineage) -> Result<()> {
        if lineage.cluster_id() != &self.cluster_id
            || lineage.bootstrap_id() != self.bootstrap_id
            || lineage.range_index().checked_add(1) != Some(self.range_index)
            || lineage.schema_sha256() != self.schema_sha256
            || !same_range_configuration(lineage.base_range(), &self.expected_base)
            || !self.members.contains_key(lineage.local_node_id())
            || self
                .members
                .get(lineage.local_node_id())
                .map(|member| member.incarnation)
                != Some(lineage.local_member_incarnation())
        {
            return Err(range_write_error(
                "native command cannot extend this actual native base/configuration",
            ));
        }
        for (id, member) in &self.members {
            if lineage.base_members().get(id) != Some(member) {
                return Err(range_write_error(
                    "native command voter credentials differ from its base",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn certify(
        &self,
        certified_voters: &BTreeSet<ClusterNodeId>,
    ) -> Result<NativeRangeCommitCertificate> {
        let required_quorum = self.members.len() / 2 + 1;
        if certified_voters.len() < required_quorum
            || !certified_voters.contains(&self.range.leader)
            || !certified_voters
                .iter()
                .all(|member| self.members.contains_key(member))
        {
            return Err(range_write_error(
                "native command has no distinct leader-inclusive certificate quorum",
            ));
        }
        Ok(NativeRangeCommitCertificate {
            intent: self.clone(),
            certified_voters: certified_voters.clone(),
            required_quorum,
        })
    }
    pub(crate) fn command_id(&self) -> &str {
        &self.command_id
    }
    pub(crate) fn command_checksum_sha256(&self) -> &str {
        &self.command_checksum_sha256
    }
    pub(crate) fn origin_transaction_id(&self) -> u64 {
        self.origin_transaction_id
    }
    pub(crate) fn mutations_sha256(&self) -> &str {
        &self.mutations_sha256
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.bootstrap_id
    }
    pub(crate) fn scope_sha256(&self) -> &str {
        &self.scope_sha256
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        &self.schema_sha256
    }
}
impl NativeRangeCommitCertificate {
    pub(crate) fn intent(&self) -> &NativeRangeCommandIntent {
        &self.intent
    }
    pub(crate) fn certified_voters(&self) -> &BTreeSet<ClusterNodeId> {
        &self.certified_voters
    }
    pub(crate) fn required_quorum(&self) -> usize {
        self.required_quorum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{BicDb, DbConfig, RangeImageBirthSpec};
    use crate::distribution_range_mutation::RangeMutationAuthority;
    use crate::{CancellationToken, DistributionConfig, DistributionStore, Record, StorageMode};
    use serde_json::json;

    type NativeCoordinator = RangeWriteCoordinator<super::super::tests::TestTransport>;

    fn actual_coordinator(
        db: &Arc<RwLock<BicDb>>,
        parent: &Path,
        topology: ClusterTopology,
        spec: &RangeImageBirthSpec,
    ) -> (Arc<NativeCoordinator>, Arc<Mutex<ClusterDataNodeService>>) {
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                topology.cluster_id.clone(),
                spec.node_id.clone(),
                parent.join("database"),
                Arc::clone(db),
                true,
            )
            .unwrap(),
        ));
        let coordinator = Arc::new(
            RangeWriteCoordinator::new_schema_fenced(
                topology.cluster_id.clone(),
                spec.node_id.clone(),
                Arc::new(RwLock::new(topology)),
                Arc::clone(&service),
                Arc::new(super::super::tests::TestTransport { available: false }),
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(1024).unwrap();
        db.read().install_commit_admission(coordinator.clone());
        (coordinator, service)
    }

    fn fixture() -> (
        tempfile::TempDir,
        Arc<RwLock<BicDb>>,
        ClusterTopology,
        RangeImageBirthSpec,
    ) {
        let parent = tempfile::tempdir().unwrap();
        let bootstrap_id = Uuid::now_v7();
        let mut topology = DistributionStore::initialize_at(
            parent.path().join("metadata"),
            DistributionConfig {
                enabled: true,
                initial_ranges: 2,
                replication_factor: 1,
                range_image_bootstrap_id: Some(bootstrap_id),
                ..Default::default()
            },
            true,
            1,
        )
        .unwrap()
        .topology()
        .clone();
        let node = topology.nodes.values().next().unwrap().clone();
        let spec = RangeImageBirthSpec {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id,
            node_id: node.id.clone(),
            member_incarnation: node.incarnation,
            restore_activation: None,
            initial_members: topology
                .nodes
                .values()
                .map(|member| {
                    (
                        member.id.clone(),
                        RangeImageBirthMember {
                            incarnation: member.incarnation,
                            tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: member
                                .pending_tls_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect(),
            initial_ranges: topology.ranges.values().cloned().collect(),
        };
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let root = parent.path().join("database");
        let (mut db, _) =
            BicDb::create_new_range_image_database(&root, config.clone(), spec.clone()).unwrap();
        db.create_collection("items").unwrap();
        drop(db);
        let (db, authority) = BicDb::open_range_image_database(&root, config, &spec).unwrap();
        let schema = db.schema_compatibility_fingerprint().unwrap();
        topology.nodes.get_mut(&node.id).unwrap().labels.insert(
            crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
            schema.sha256,
        );
        let db = Arc::new(RwLock::new(db));
        let gate = db.read().range_mutation_gate();
        for range in &spec.initial_ranges {
            let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
            BicDb::initialize_range_image_genesis(
                &db,
                &authority,
                range,
                &frozen,
                Duration::from_secs(3),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        }
        (parent, db, topology, spec)
    }
    fn command(topology: &ClusterTopology, key: &str, index: u64) -> RangeWriteCommand {
        let range = topology.range_for_key("items", key).unwrap();
        RangeWriteCommand::new(
            topology.cluster_id.clone(),
            range,
            index,
            range.leader.clone(),
            &CommitAdmissionIntent {
                transaction_id: 100 + index,
                mutations: vec![CommitAdmissionMutation {
                    collection: "items".into(),
                    record_id: key.into(),
                    record: Some(Record::new(key).with_metadata(json!({"index":index}))),
                }],
            },
        )
        .unwrap()
    }
    #[test]
    fn native_prefix_actual_wal_and_quorum_extend_and_reopen_with_database_seq_distinct_from_range_index(
    ) {
        let (parent, db, topology, spec) = fixture();
        let first = "a";
        let range = topology.range_for_key("items", first).unwrap();
        let other = (0..1000)
            .map(|id| format!("other-{id}"))
            .find(|key| topology.range_for_key("items", key).unwrap().id != range.id)
            .unwrap();
        for (key, index) in [(first, 1), (other.as_str(), 1), (first, 2)] {
            let command = command(&topology, key, index);
            let range = topology.range_by_id(command.range_id).unwrap();
            let intent = NativeRangeCommandIntent::plan(&command, range, &topology).unwrap();
            let certificate = intent
                .certify(&BTreeSet::from([range.leader.clone()]))
                .unwrap();
            let gate = db.read().range_mutation_gate();
            let permit = gate.try_enter(range.id).unwrap();
            db.read()
                .apply_native_locally_certified_command(
                    &certificate,
                    &command.mutations,
                    &RangeMutationAuthority::Mutation(&permit),
                )
                .unwrap();
            let before_duplicate = db.read().current_commit_seq();
            db.read()
                .apply_native_locally_certified_command(
                    &certificate,
                    &command.mutations,
                    &RangeMutationAuthority::Mutation(&permit),
                )
                .unwrap();
            assert_eq!(db.read().current_commit_seq(), before_duplicate);
            assert_eq!(
                db.read()
                    .native_range_prefix_observation(range.id)
                    .unwrap()
                    .native_applied_index,
                Some(index)
            );
        }
        assert!(db.read().current_commit_seq() > 2);
        assert_eq!(
            db.read().get("items", first).unwrap().unwrap().metadata,
            json!({"index":2})
        );
        assert_eq!(
            db.read()
                .current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            2
        );
        drop(db);
        let (reopened, _) = BicDb::open_range_image_database(
            &parent.path().join("database"),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
            &spec,
        )
        .unwrap();
        reopened.schema_compatibility_fingerprint().unwrap();
        assert_eq!(
            reopened
                .current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            2
        );
        assert_eq!(
            reopened
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            Some(2)
        );
    }
    #[test]
    fn native_pending_never_raises_source_floor_and_survives_reopen_without_forged_quorum() {
        let (parent, db, topology, spec) = fixture();
        let command = command(&topology, "pending", 1);
        let range = topology.range_by_id(command.range_id).unwrap();
        let intent = NativeRangeCommandIntent::plan(&command, range, &topology).unwrap();
        assert!(intent.certify(&BTreeSet::new()).is_err());
        let gate = db.read().range_mutation_gate();
        let permit = gate.try_enter(range.id).unwrap();
        let _pending = db
            .read()
            .prepare_native_range_command(&intent, 77, &RangeMutationAuthority::Mutation(&permit))
            .unwrap();
        let observation = db.read().native_range_prefix_observation(range.id).unwrap();
        assert_eq!(observation.native_applied_index, Some(0));
        assert_eq!(observation.pending_command_index, Some(1));
        assert_eq!(observation.materialized_pending_index, None);
        assert!(db
            .read()
            .current_native_range_source_lineage(range.id)
            .is_err());
        drop(permit);
        drop(db);
        let (reopened, _) = BicDb::open_range_image_database(
            &parent.path().join("database"),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
            &spec,
        )
        .unwrap();
        reopened.schema_compatibility_fingerprint().unwrap();
        let observation = reopened.native_range_prefix_observation(range.id).unwrap();
        assert_eq!(observation.native_applied_index, Some(0));
        assert_eq!(observation.pending_command_index, Some(1));
        assert!(reopened
            .current_native_range_source_lineage(range.id)
            .is_err());
    }

    #[test]
    fn native_origin_actual_coordinator_commits_under_outer_db_write_guard_without_reacquire() {
        let (parent, db, topology, spec) = fixture();
        let range = topology.range_for_key("items", "origin").unwrap().clone();
        let (coordinator, service) = actual_coordinator(&db, parent.path(), topology, &spec);
        let plain = CommitAdmissionIntent {
            transaction_id: 77,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".into(),
                record_id: "origin".into(),
                record: Some(Record::new("origin")),
            }],
        };
        assert!(coordinator.admit(&plain).is_err());
        let (sender, receiver) = std::sync::mpsc::channel();
        let owned = Arc::clone(&db);
        let worker = std::thread::spawn(move || {
            let database = owned.write();
            let result = (|| {
                let mut tx = database.begin_transaction()?;
                tx.insert(
                    "items",
                    Record::new("origin").with_metadata(json!({"value":1})),
                )?;
                tx.commit()
            })();
            sender
                .send(result.map_err(|error: BicDbError| error.to_string()))
                .unwrap();
        });
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("native callback must not reacquire its caller-owned DB write guard")
            .unwrap();
        worker.join().unwrap();
        assert_eq!(
            service
                .lock()
                .range_write_progress(range.id)
                .resolved_through,
            1
        );
        assert_eq!(
            db.read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            Some(1)
        );
        assert_eq!(
            db.read().get("items", "origin").unwrap().unwrap().metadata,
            json!({"value":1})
        );
        assert!(coordinator.pending_acknowledgements.lock().is_empty());
        let gate = db.read().range_mutation_gate();
        let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
        drop(frozen);
        db.read().clear_commit_admission();
    }

    #[test]
    fn native_origin_public_ticket_without_actual_pending_scope_cannot_apply_rows() {
        #[derive(Debug)]
        struct BareTicket;
        impl CommitAdmission for BareTicket {
            fn admit(&self, _: &CommitAdmissionIntent) -> Result<CommitAdmissionTicket> {
                Ok(CommitAdmissionTicket {
                    authority: "plain-public-ticket".into(),
                    command_id: Uuid::now_v7().to_string(),
                })
            }
            fn local_applied(&self, _: &CommitAdmissionTicket, _: u64) -> Result<()> {
                panic!("public ticket without native scope must not reach application callback")
            }
        }
        let (_parent, db, topology, _spec) = fixture();
        let range = topology.range_for_key("items", "unscoped").unwrap();
        let database = db.write();
        database.install_commit_admission(Arc::new(BareTicket));
        let mut tx = database.begin_transaction().unwrap();
        tx.insert("items", Record::new("unscoped")).unwrap();
        assert!(matches!(
            tx.commit(),
            Err(BicDbError::CommitOutcomeUnknown { .. })
        ));
        assert!(database.get("items", "unscoped").unwrap().is_none());
        let observation = database.native_range_prefix_observation(range.id).unwrap();
        assert_eq!(observation.native_applied_index, Some(0));
        assert_eq!(observation.pending_command_index, None);
        database.clear_commit_admission();
    }

    #[test]
    fn native_origin_post_admission_pre_wal_unknown_cleans_local_attempt_before_recovery_freeze() {
        let (parent, db, topology, spec) = fixture();
        let range = topology.range_for_key("items", "cut").unwrap().clone();
        let (coordinator, service) = actual_coordinator(&db, parent.path(), topology, &spec);
        let (sender, receiver) = std::sync::mpsc::channel();
        let owned = Arc::clone(&db);
        let worker = std::thread::spawn(move || {
            let database = owned.write();
            let mut tx = database.begin_transaction().unwrap();
            tx.insert("items", Record::new("cut")).unwrap();
            BicDb::test_reject_next_native_after_admission();
            let error = tx.commit().unwrap_err();
            sender
                .send(matches!(error, BicDbError::CommitOutcomeUnknown { .. }))
                .unwrap();
        });
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        worker.join().unwrap();
        assert!(db.read().get("items", "cut").unwrap().is_none());
        assert!(coordinator.pending_acknowledgements.lock().is_empty());
        assert!(!coordinator.recovered_epochs.lock().contains_key(&range.id));
        let observation = db.read().native_range_prefix_observation(range.id).unwrap();
        assert_eq!(observation.native_applied_index, Some(0));
        assert_eq!(observation.pending_command_index, Some(1));
        let gate = db.read().range_mutation_gate();
        let frozen = gate
            .freeze(
                range.id,
                Uuid::now_v7(),
                Duration::from_millis(200),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        drop(frozen);
        assert_eq!(service.lock().range_write_progress(range.id).last_index, 1);
        assert_eq!(
            service
                .lock()
                .range_write_progress(range.id)
                .resolved_through,
            0
        );
        db.read().clear_commit_admission();
        drop(coordinator);
        drop(service);
        drop(db);
        let (reopened, _) = BicDb::open_range_image_database(
            &parent.path().join("database"),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
            &spec,
        )
        .unwrap();
        reopened.schema_compatibility_fingerprint().unwrap();
        let reopened = Arc::new(RwLock::new(reopened));
        let service = ClusterDataNodeService::open(
            spec.cluster_id.clone(),
            spec.node_id.clone(),
            parent.path().join("database"),
            Arc::clone(&reopened),
            true,
        )
        .unwrap();
        assert_eq!(service.range_write_progress(range.id).resolved_through, 0);
        assert_eq!(
            reopened
                .read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .pending_command_index,
            Some(1)
        );
        assert!(reopened.read().get("items", "cut").unwrap().is_none());
        assert!(reopened
            .read()
            .current_native_range_source_lineage(range.id)
            .is_err());
    }

    #[test]
    fn native_install_safety_preserves_floor_when_raw_repair_invalidates_source() {
        let (_parent, db, topology, spec) = fixture();
        let command = command(&topology, "guard", 1);
        let range = topology.range_by_id(command.range_id).unwrap();
        let intent = NativeRangeCommandIntent::plan(&command, range, &topology).unwrap();
        let certificate = intent
            .certify(&BTreeSet::from([range.leader.clone()]))
            .unwrap();
        let gate = db.read().range_mutation_gate();
        {
            let permit = gate.try_enter(range.id).unwrap();
            let authority = RangeMutationAuthority::Mutation(&permit);
            db.read()
                .apply_native_locally_certified_command(
                    &certificate,
                    &command.mutations,
                    &authority,
                )
                .unwrap();
            db.read()
                .apply_admitted_mutations_with_mutation(range.id, &command.mutations, &authority)
                .unwrap();
        }
        assert_eq!(
            db.read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            None
        );
        let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
        let admission = db
            .read()
            .native_recovery_admission(
                &spec.cluster_id,
                spec.bootstrap_id,
                &spec.node_id,
                spec.member_incarnation,
            )
            .unwrap();
        let safety = db
            .read()
            .native_range_install_safety(&admission, range.id, &frozen)
            .unwrap();
        assert_eq!(safety.applied_index(), Some(1));
        assert_eq!(safety.maximum_irreversible_index(), Some(1));
        assert!(safety.source_invalidated());
        safety
            .require_current(&db.read(), &admission, &frozen)
            .unwrap();
    }
}
