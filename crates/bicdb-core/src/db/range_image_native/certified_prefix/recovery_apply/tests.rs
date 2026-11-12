use super::*;
use crate::db::ActualNativeRf3Fixture;
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRecoveryRolePurpose};
use crate::distribution_data::ClusterDataNodeService;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_recovery::{
    test_recovery_metadata_cluster, CurrentHistoricalCertificationRound,
    FreshRangeRecoveryAuthority, HistoricalCommandDecisionRound, RangeRecoveryRuntime,
    RecoveryEpochFenceRequest, RecoveryFenceQuorumRound, RecoveryFencedProbeRequest,
};

/// Initial/current metadata configurations are explicit test preconditions.
/// Metadata witnesses and every data history/fence/certification response are
/// obtained through actual providers/runtimes/services, never caller ACK sets.
struct ActualHistoricalContext {
    root: tempfile::TempDir,
    topology: crate::ClusterTopology,
    databases: BTreeMap<ClusterNodeId, Arc<RwLock<BicDb>>>,
    services: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    providers: BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    runtimes: BTreeMap<ClusterNodeId, RangeRecoveryRuntime>,
    parent: CancellationToken,
}
impl ActualHistoricalContext {
    fn from_live(fixture: &ActualNativeRf3Fixture) -> Self {
        Self::new(
            fixture.topology.read().clone(),
            fixture.databases.clone(),
            fixture.services.clone(),
        )
    }
    fn new(
        topology: crate::ClusterTopology,
        databases: BTreeMap<ClusterNodeId, Arc<RwLock<BicDb>>>,
        services: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let metadata_leader = topology.nodes.keys().last().unwrap();
        let providers =
            test_recovery_metadata_cluster(root.path(), &topology, metadata_leader).unwrap();
        let runtimes = services
            .iter()
            .map(|(node, service)| {
                (
                    node.clone(),
                    RangeRecoveryRuntime::new(Arc::clone(service), Arc::clone(&providers[node])),
                )
            })
            .collect();
        Self {
            root,
            topology,
            databases,
            services,
            providers,
            runtimes,
            parent: CancellationToken::uncancelable()
                .child_with_deadline(Instant::now() + Duration::from_secs(30)),
        }
    }
    fn initiator(&self) -> MetadataReadParticipant {
        self.providers[&self.topology.ranges.values().next().unwrap().leader].local_participant()
    }
    fn role(&self, node: &ClusterNodeId, nonce: Uuid) -> RecoveryRoleAdmission {
        let provider = &self.providers[node];
        let actor = self.initiator();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let range = self.topology.ranges.values().next().unwrap();
        let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
        let witness = provider
            .witness_recovery_role(
                &actor,
                business,
                range.id,
                range.epoch,
                nonce,
                challenge,
                purpose.clone(),
                &self.parent,
            )
            .unwrap();
        RecoveryRoleAdmission::from_witness(
            witness,
            &actor,
            node,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            &purpose,
        )
        .unwrap()
    }
    fn current_apply(
        &self,
        recipient: &ClusterNodeId,
        certifiers: &[ClusterNodeId],
        command: &crate::RangeWriteCommand,
    ) -> (Arc<FreshRangeRecoveryAuthority>, RecoveryApplyAdmission) {
        let nonce = Uuid::now_v7();
        let mut rounds = BTreeMap::new();
        for node in self.topology.nodes.keys() {
            rounds.insert(
                node.clone(),
                RecoveryFenceQuorumRound::begin(self.role(node, nonce), &self.parent).unwrap(),
            );
        }
        let scope = rounds[recipient].scope().clone();
        let actor = self.initiator();
        let mut fences = Vec::new();
        for node in self.topology.nodes.keys() {
            fences.push(Arc::new(
                self.runtimes[node]
                    .fence_epoch(
                        &actor,
                        &RecoveryEpochFenceRequest {
                            scope: scope.clone(),
                            budget_ms: 5000,
                        },
                        &self.parent,
                    )
                    .unwrap(),
            ));
        }
        let mut authorities = BTreeMap::new();
        for (node, mut round) in rounds {
            // All responses are actual fsynced service observations; no count
            // or reconstructed voter list creates the authority.
            for fence in &fences {
                round.record(Arc::clone(fence)).unwrap();
            }
            authorities.insert(node, Arc::new(round.finish().unwrap()));
        }
        let mut decisions = BTreeMap::new();
        for (node, authority) in &authorities {
            let mut round =
                HistoricalCommandDecisionRound::begin(Arc::clone(authority), command.clone())
                    .unwrap();
            for source in self.topology.nodes.keys() {
                round
                    .record(
                        self.runtimes[source]
                            .observe_history(
                                &actor,
                                &RecoveryFencedProbeRequest {
                                    scope: authority.scope().clone(),
                                    index: Some(command.index),
                                    budget_ms: 5000,
                                },
                                &self.parent,
                            )
                            .unwrap(),
                    )
                    .unwrap();
            }
            decisions.insert(node.clone(), Arc::new(round.finish().unwrap()));
        }
        // Actual current-session marker writes, independent from old raw CQ.
        for node in certifiers {
            let authority = &authorities[node];
            let frozen = self.runtimes[node]
                .source_capture_freeze(authority.scope())
                .unwrap();
            self.services[node]
                .lock()
                .certify_native_historical_command(
                    authority.role(),
                    &decisions[node],
                    &frozen,
                    &self.parent,
                )
                .unwrap();
        }
        let authority = Arc::clone(&authorities[recipient]);
        let mut round =
            CurrentHistoricalCertificationRound::begin(Arc::clone(&decisions[recipient])).unwrap();
        for node in certifiers {
            round
                .record(
                    self.runtimes[node]
                        .observe_history(
                            &actor,
                            &RecoveryFencedProbeRequest {
                                scope: authority.scope().clone(),
                                index: Some(command.index),
                                budget_ms: 5000,
                            },
                            &self.parent,
                        )
                        .unwrap(),
                )
                .unwrap();
        }
        (authority, round.finish().unwrap())
    }
    fn apply(
        &self,
        recipient: &ClusterNodeId,
        authority: &FreshRangeRecoveryAuthority,
        admission: &RecoveryApplyAdmission,
    ) -> NativeRecoveryAppliedReceipt {
        let frozen = self.runtimes[recipient]
            .source_capture_freeze(authority.scope())
            .unwrap();
        self.services[recipient]
            .lock()
            .apply_native_historical_command(authority.role(), admission, &frozen, &self.parent)
            .unwrap()
    }
}
fn original_command(fixture: &ActualNativeRf3Fixture) -> crate::RangeWriteCommand {
    let topology = fixture.topology.read();
    let range = topology.ranges.values().next().unwrap();
    fixture.services[&range.leader]
        .lock()
        .probe_range_write_for_peer(range, &range.leader, 1)
        .unwrap()
        .entry
        .unwrap()
        .command
}

#[test]
fn actual_native_historical_replica_uses_current_marker_quorum_without_data_leader_certifier() {
    let fixture = ActualNativeRf3Fixture::new();
    fixture.append("historical-replica").unwrap();
    let command = original_command(&fixture);
    let context = ActualHistoricalContext::from_live(&fixture);
    let recipient = ClusterNodeId::new("n1").unwrap();
    let certifiers = vec![recipient.clone(), ClusterNodeId::new("n2").unwrap()];
    assert!(!certifiers.contains(&fixture.leader()));
    let (authority, admission) = context.current_apply(&recipient, &certifiers, &command);
    let receipt = context.apply(&recipient, &authority, &admission);
    assert_eq!(receipt.range_index(), 1);
    assert_eq!(receipt.command_sha256(), command.checksum_sha256);
    let db = context.databases[&recipient].read();
    assert!(db.get("items", "historical-replica").unwrap().is_some());
    assert_eq!(
        db.current_native_range_source_lineage(command.range_id)
            .unwrap()
            .range_index(),
        1
    );
    // Scope remains actively fenced until the SEPARATE actual ReadyQQ/
    // RecoveryComplete protocol authorizes normal admission. No manual release.
    assert!(db
        .range_mutation_gate()
        .try_enter(command.range_id)
        .is_err());
}

#[test]
fn actual_post_wal_unknown_checkpoint_reopen_uses_new_local_attempt_and_original_command() {
    let mut fixture = ActualNativeRf3Fixture::new();
    BicDb::test_reject_next_native_after_local_wal();
    let error = fixture.append("post-wal-adoption").unwrap_err();
    assert!(matches!(error, BicDbError::CommitOutcomeUnknown { .. }));
    let command = original_command(&fixture);
    let original_leader = fixture.leader();
    let (old_pending, old_open, wal_after_cut) = {
        let mut db = fixture.databases[&original_leader].write();
        let pending: PendingNativeRangeCommand = db
            .read_native_range_image_record(&pending_key(command.range_id))
            .unwrap()
            .unwrap();
        assert_eq!(pending.command_checksum_sha256, command.checksum_sha256);
        assert!(pending.materialized.is_none());
        assert_eq!(
            db.native_range_prefix_observation(command.range_id)
                .unwrap()
                .native_applied_index,
            Some(0)
        );
        assert!(db.get("items", "post-wal-adoption").unwrap().is_some());
        let before = std::fs::metadata(&db.tx_log.path).unwrap().len();
        assert!(before > 0);
        db.checkpoint_for_resume().unwrap();
        let after = std::fs::metadata(&db.tx_log.path).unwrap().len();
        assert!(
            after < before,
            "actual checkpoint must cut the old AppWAL; otherwise this cut is not exercised"
        );
        (pending, db.incarnation.clone(), after)
    };
    let data_root = std::mem::replace(&mut fixture.root, tempfile::tempdir().unwrap());
    let specs = fixture.specs.clone();
    let mut topology = fixture.topology.read().clone();
    drop(fixture); // ALL actual service/store/DB owners close before reopen.
                   // Explicit new current metadata configuration precondition, not a claim
                   // that this test exercised online metadata leadership election.
    let new_leader = ClusterNodeId::new("n1").unwrap();
    topology.generation += 1;
    let range = topology.ranges.values_mut().next().unwrap();
    range.epoch += 1;
    range.leader = new_leader.clone();
    topology.validate().unwrap();
    assert_ne!(command.leader_node_id, new_leader);
    assert!(command.range_epoch < topology.ranges.values().next().unwrap().epoch);
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let mut databases = BTreeMap::new();
    let mut services = BTreeMap::new();
    for (node, spec) in specs {
        let path = data_root.path().join(node.as_str());
        let (db, _) = BicDb::open_range_image_database(&path, config.clone(), &spec).unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        let db = Arc::new(RwLock::new(db));
        let service = ClusterDataNodeService::open(
            topology.cluster_id.clone(),
            node.clone(),
            &path,
            Arc::clone(&db),
            true,
        )
        .unwrap();
        databases.insert(node.clone(), db);
        services.insert(node, Arc::new(Mutex::new(service)));
    }
    let context = ActualHistoricalContext::new(topology, databases, services);
    assert_ne!(
        context.databases[&original_leader].read().incarnation,
        old_open
    );
    let certifiers = vec![original_leader.clone(), ClusterNodeId::new("n2").unwrap()];
    assert!(!certifiers.contains(&new_leader));
    let (authority, admission) = context.current_apply(&original_leader, &certifiers, &command);
    let receipt = context.apply(&original_leader, &authority, &admission);
    assert_eq!(receipt.command_sha256(), command.checksum_sha256);
    assert_eq!(admission.command().leader_node_id, original_leader);
    assert_eq!(admission.command().range_epoch, command.range_epoch);
    assert!(admission.current_range().epoch > command.range_epoch);
    let db = context.databases[&original_leader].read();
    assert!(db.get("items", "post-wal-adoption").unwrap().is_some());
    let archived: PendingNativeRangeCommand = db
        .read_native_range_image_record(&format!(
            "pending-attempt:{}:{}",
            command.range_id.get(),
            old_pending.checksum_sha256
        ))
        .unwrap()
        .unwrap();
    assert_eq!(archived, old_pending);
    let birth = db.read_native_birth().unwrap().unwrap();
    let base = db
        .actual_native_range_integrity_binding(&birth, command.range_id)
        .unwrap()
        .unwrap();
    let prefix = db
        .read_native_certified_range_prefix(&birth, &base)
        .unwrap()
        .unwrap();
    assert_eq!(prefix.command_checksum_sha256, command.checksum_sha256);
    assert_ne!(
        prefix.materialized.as_ref().unwrap().database_incarnation,
        old_open
    );
    assert_eq!(
        prefix.materialized.as_ref().unwrap().database_incarnation,
        db.incarnation
    );
    assert_eq!(
        prefix.materialized.as_ref().unwrap().local_wal_sequence,
        receipt.local_visibility_db_sequence()
    );
    assert!(
        std::fs::metadata(&db.tx_log.path).unwrap().len() > wal_after_cut,
        "the already-matching row must still receive an actual NEW local AppWAL frame"
    );
    assert_eq!(
        prefix
            .recovery_attempt
            .as_ref()
            .unwrap()
            .previous_pending_sha256
            .as_deref(),
        Some(old_pending.checksum_sha256.as_str())
    );
    assert!(
        matches!(prefix.certificate_evidence,NativeRangeCertificateEvidence::HistoricalCurrentCertificateQuorum {current_range_epoch,..}
        if current_range_epoch==admission.current_range().epoch)
    );
    // Config overlay/ReadyQQ/durable RecoveryComplete remain SEPARATE gates.
    // Do not clear Active or release its freeze to manufacture a new-write pass.
    assert!(db
        .range_mutation_gate()
        .try_enter(command.range_id)
        .is_err());
}
