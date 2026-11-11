//! Distinct current-voter epoch-fence acknowledgments. A completed round proves
//! that a data majority has stopped ordinary old-session mutations; it grants
//! neither native visibility nor a committed per-command decision or read permit.
use std::collections::BTreeMap;
use std::time::Instant;

use super::{invalid, RecoveryFenceObservation, RecoveryMemberIdentity, RecoveryRoleAdmission};
use crate::distribution::{ClusterNodeId, RangeDescriptor, RangeReplicaRole};
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::{RangeWriteCommand, RecoveryFenceScope};
use crate::{CancellationToken, Result};

const MAX_VOTERS: usize = 64;
const MAX_OBSERVATION_TEXT: usize = 16 * 1024;

/// No caller-supplied counters, duplicate-voter counting or decoded DTOs can
/// construct this round's observations. Each observation has already traversed
/// an authenticated transport or the actual local receiver's drain/persist path.
#[derive(Debug)]
pub(crate) struct RecoveryFenceQuorumRound {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    observations: BTreeMap<ClusterNodeId, RecoveryFenceObservation>,
}

impl RecoveryFenceQuorumRound {
    pub(crate) fn begin(role: RecoveryRoleAdmission, parent: &CancellationToken) -> Result<Self> {
        let cancellation = parent.child_with_deadline(role.deadline());
        cancellation.check()?;
        role.require_live_request()?;
        let members = role.member_credentials();
        if members.is_empty()
            || members.len() > MAX_VOTERS
            || members.len()
                != role
                    .current_range()
                    .replicas
                    .iter()
                    .filter(|replica| replica.role == RangeReplicaRole::Voter)
                    .count()
        {
            return Err(invalid("invalid or oversized current recovery voter set"));
        }
        let scope = RecoveryFenceScope::from_role(&role)?;
        Ok(Self {
            role,
            scope,
            cancellation,
            observations: BTreeMap::new(),
        })
    }

    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn required_quorum(&self) -> usize {
        self.role.member_credentials().len() / 2 + 1
    }
    pub(crate) fn has_observation(&self, node: &ClusterNodeId) -> bool {
        self.observations.contains_key(node)
    }

    pub(crate) fn record(&mut self, observation: RecoveryFenceObservation) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        observation.require_live()?;
        // Recipients obtain independent fresh metadata contexts. Their durable
        // recovery session must match; their context UUID need not equal ours.
        observation.scope().require_role(&self.role)?;
        if !self.scope.same_session(observation.scope()) {
            return Err(invalid(
                "epoch-fence reply belongs to another recovery session",
            ));
        }
        if self.observations.contains_key(observation.node_id()) {
            return Err(invalid("duplicate recovery fence voter"));
        }
        let member = self
            .role
            .member_credentials()
            .get(observation.node_id())
            .ok_or_else(|| invalid("recovery fence observer is not a current data voter"))?;
        let peer = observation.authenticated_peer();
        if peer.node_id() != observation.node_id()
            || peer.member_incarnation() != member.incarnation
            || observation.member_incarnation() != member.incarnation
        {
            return Err(invalid(
                "recovery fence peer incarnation differs from fresh membership",
            ));
        }
        let has_pins = member.active_certificate_sha256.is_some()
            || member.pending_certificate_sha256.is_some();
        if has_pins {
            let actual = peer
                .certificate_sha256()
                .ok_or_else(|| invalid("recovery fence peer has no actual certificate"))?;
            if member.active_certificate_sha256.as_deref() != Some(actual)
                && member.pending_certificate_sha256.as_deref() != Some(actual)
            {
                return Err(invalid(
                    "recovery fence peer certificate is no longer authorized",
                ));
            }
        } else if !peer.plaintext_loopback() {
            return Err(invalid(
                "unpinned recovery peer requires explicit loopback development mode",
            ));
        }
        let progress = observation.progress();
        let extra_handoff_epoch = match self.role.purpose() {
            MetadataRecoveryRolePurpose::NormalRecovery => None,
            MetadataRecoveryRolePurpose::CommittedHandoff { .. } => {
                self.role.current_range().epoch.checked_add(2)
            }
        };
        if progress.node_id != *observation.node_id()
            || progress.range_id != self.scope.range_id
            || (progress.current_epoch != self.scope.range_epoch
                && Some(progress.current_epoch) != extra_handoff_epoch)
            || progress.resolved_through > progress.last_index
            || progress.compacted_through > progress.resolved_through
            || observation.store_open_id().is_nil()
            || observation.native_database_incarnation().is_empty()
            || observation.native_database_incarnation().len() > MAX_OBSERVATION_TEXT
        {
            return Err(invalid(
                "recovery fence log identity, epoch or watermarks are invalid",
            ));
        }
        let native = observation.native();
        if native.range_id != self.scope.range_id
            || native
                .native_base_index
                .zip(native.native_applied_index)
                .is_some_and(|(base, applied)| base > applied)
            || native.pending_command_index.is_some_and(|index| index == 0)
            || native
                .materialized_pending_index
                .is_some_and(|index| native.pending_command_index != Some(index))
            || native
                .pending_command_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.len() > MAX_OBSERVATION_TEXT)
            || native
                .diagnostic
                .as_ref()
                .is_some_and(|message| message.len() > MAX_OBSERVATION_TEXT)
            || native.native_receipt_sha256.as_ref().is_some_and(|sha| {
                sha.len() != 64 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Err(invalid("recovery fence native observation is malformed"));
        }
        // Unknown/invalidated native floors can join a logical fence quorum.
        // They must be repaired before native-ready certification; None is
        // never converted to verified empty genesis or a snapshot floor.
        self.observations
            .insert(observation.node_id().clone(), observation);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<FreshRangeRecoveryAuthority> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        if self.observations.len() < self.required_quorum() {
            return Err(invalid(
                "no distinct current-data-voter epoch-fence majority",
            ));
        }
        // A consumed successful reply proves a DURABLE refusal. Its short RPC
        // timeout is not a lease expiry and cannot shorten this original role
        // deadline. New operations still require independent fresh role checks.
        Ok(FreshRangeRecoveryAuthority {
            role: self.role,
            scope: self.scope,
            cancellation: self.cancellation,
            observations: self.observations,
        })
    }
}

/// Actual current-data majority fencing, not native-ready or strong-read
/// authority. Receiver-side mutation still requires its independently fresh
/// role, owned freeze and an actual distinct per-command certificate quorum.
#[derive(Debug)]
pub(crate) struct FreshRangeRecoveryAuthority {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    observations: BTreeMap<ClusterNodeId, RecoveryFenceObservation>,
}
impl FreshRangeRecoveryAuthority {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.role
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        self.role.current_range()
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        self.role.member_credentials()
    }
    pub(crate) fn observations(&self) -> &BTreeMap<ClusterNodeId, RecoveryFenceObservation> {
        &self.observations
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.cancellation
            .deadline()
            .expect("role supplies a finite deadline")
    }
    pub(crate) fn require_historical_scope(&self, command: &RangeWriteCommand) -> Result<()> {
        self.require_live()?;
        self.role.require_historical_command_scope(command)
    }
    /// A conservative observation bound, NOT a materialized/certified cut.
    /// Native images can cover more history than a relocated raw log catalog.
    pub(crate) fn highest_observed_index(&self) -> u64 {
        self.observations
            .values()
            .flat_map(|observation| {
                let native = observation.native();
                [
                    Some(observation.progress().last_index),
                    native.native_base_index,
                    native.native_applied_index,
                    native.pending_command_index,
                    native.materialized_pending_index,
                ]
                .into_iter()
                .flatten()
            })
            .max()
            .unwrap_or(0)
    }
    /// Candidates for subsequent ACTUAL lineage/history verification. These
    /// status observations alone cannot authorize source capture or erase data.
    pub(crate) fn clean_source_candidates(&self) -> Vec<&RecoveryFenceObservation> {
        let mut candidates: Vec<_> = self
            .observations
            .values()
            .filter(|observation| {
                let native = observation.native();
                native.factory_initialization_complete
                    && !native.invalidated
                    && native.native_applied_index.is_some()
                    && native.native_receipt_sha256.is_some()
                    && native.pending_command_index.is_none()
                    && native.materialized_pending_index.is_none()
            })
            .collect();
        candidates.sort_by(|left, right| {
            right
                .native()
                .native_applied_index
                .cmp(&left.native().native_applied_index)
                .then_with(|| left.node_id().cmp(right.node_id()))
        });
        candidates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{RangeImageBirthMember, RangeImageBirthSpec};
    use crate::distribution::{DistributionConfig, DistributionStore, RangeReplica, ReplicaId};
    use crate::distribution_data::ClusterDataNodeService;
    use crate::distribution_metadata_read_provider::MetadataReadProvider;
    use crate::distribution_range_recovery::{
        test_recovery_metadata_cluster, RangeRecoveryRuntime, RecoveryEpochFenceRequest,
    };
    use crate::{BicDb, ClusterTopology, DbConfig, StorageMode};
    use parking_lot::{Mutex, RwLock};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::Duration;
    use uuid::Uuid;

    struct Fixture {
        _root: tempfile::TempDir,
        topology: ClusterTopology,
        nodes: Vec<ClusterNodeId>,
        providers: BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
        runtimes: BTreeMap<ClusterNodeId, RangeRecoveryRuntime>,
        databases: BTreeMap<ClusterNodeId, Arc<RwLock<BicDb>>>,
        parent: CancellationToken,
    }
    impl Fixture {
        fn new(count: usize) -> Self {
            let root = tempfile::tempdir().unwrap();
            let bootstrap = Uuid::now_v7();
            let first = ClusterNodeId::new("n1").unwrap();
            let config = DistributionConfig {
                enabled: true,
                cluster_id: crate::ClusterId::new("actual-fence-quorum").unwrap(),
                node_id: first.clone(),
                node_address: "127.0.0.1:10001".into(),
                initial_ranges: 1,
                replication_factor: 1,
                range_image_bootstrap_id: Some(bootstrap),
                ..Default::default()
            };
            let mut topology = DistributionStore::initialize_at(
                root.path().join("initial-metadata"),
                config,
                true,
                1,
            )
            .unwrap()
            .topology()
            .clone();
            let nodes: Vec<_> = (1..=count)
                .map(|i| ClusterNodeId::new(format!("n{i}")).unwrap())
                .collect();
            let prototype = topology.nodes[&first].clone();
            for (i, id) in nodes.iter().enumerate().skip(1) {
                let mut node = prototype.clone();
                node.id = id.clone();
                node.address = format!("127.0.0.1:{}", 10001 + i);
                topology.nodes.insert(id.clone(), node);
            }
            topology.replication_factor = count as u8;
            topology.next_replica_id = count as u64 + 1;
            topology.ranges.values_mut().next().unwrap().replicas = nodes
                .iter()
                .enumerate()
                .map(|(i, node)| RangeReplica {
                    id: ReplicaId::new(i as u64 + 1).unwrap(),
                    node_id: node.clone(),
                    role: RangeReplicaRole::Voter,
                })
                .collect();
            topology.validate().unwrap();
            let mut databases = BTreeMap::new();
            let mut services = BTreeMap::new();
            for id in &nodes {
                let path = root.path().join(format!("db-{id}"));
                let spec = RangeImageBirthSpec {
                    cluster_id: topology.cluster_id.clone(),
                    bootstrap_id: bootstrap,
                    node_id: id.clone(),
                    member_incarnation: topology.nodes[id].incarnation,
                    initial_members: topology
                        .nodes
                        .iter()
                        .map(|(id, node)| {
                            (
                                id.clone(),
                                RangeImageBirthMember {
                                    incarnation: node.incarnation,
                                    tls_certificate_sha256: node.tls_certificate_sha256.clone(),
                                    pending_tls_certificate_sha256: node
                                        .pending_tls_certificate_sha256
                                        .clone(),
                                },
                            )
                        })
                        .collect(),
                    // Actual completed birth, deliberately NO range genesis.
                    // A current member can be behind and require an image;
                    // neither metadata nor an empty log can manufacture R=0.
                    initial_ranges: vec![],
                    restore_activation: None,
                };
                let (db, _) = BicDb::create_new_range_image_database(
                    &path,
                    DbConfig::default()
                        .with_fsync(true)
                        .with_storage_mode(StorageMode::ServerPaged)
                        .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
                    spec,
                )
                .unwrap();
                let fingerprint = db.schema_compatibility_fingerprint().unwrap().sha256;
                topology.nodes.get_mut(id).unwrap().labels.insert(
                    crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
                    fingerprint,
                );
                let db = Arc::new(RwLock::new(db));
                let service = Arc::new(Mutex::new(
                    ClusterDataNodeService::open(
                        topology.cluster_id.clone(),
                        id.clone(),
                        &path,
                        Arc::clone(&db),
                        true,
                    )
                    .unwrap(),
                ));
                databases.insert(id.clone(), db);
                services.insert(id.clone(), service);
            }
            topology.validate().unwrap();
            // Deliberately different metadata and data leaders for RF3.
            let metadata_leader = nodes.get(1).unwrap_or(&nodes[0]);
            let providers = test_recovery_metadata_cluster(
                &root.path().join("shared-metadata"),
                &topology,
                metadata_leader,
            )
            .unwrap();
            let runtimes = services
                .into_iter()
                .map(|(node, service)| {
                    let runtime = RangeRecoveryRuntime::new(service, Arc::clone(&providers[&node]));
                    (node, runtime)
                })
                .collect();
            Self {
                _root: root,
                topology,
                nodes,
                providers,
                runtimes,
                databases,
                parent: CancellationToken::uncancelable()
                    .child_with_deadline(Instant::now() + Duration::from_secs(5)),
            }
        }
        fn role(&self, nonce: Uuid) -> RecoveryRoleAdmission {
            let id = &self.nodes[0];
            let provider = &self.providers[id];
            let actual = provider.local_participant();
            let business = Uuid::now_v7();
            let challenge = Uuid::now_v7();
            let range = self.topology.ranges.values().next().unwrap();
            let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
            let witness = provider
                .witness_recovery_role(
                    &actual,
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
                &actual,
                id,
                business,
                range.id,
                range.epoch,
                nonce,
                challenge,
                &purpose,
            )
            .unwrap()
        }
        fn observe(
            &self,
            node: &ClusterNodeId,
            scope: &RecoveryFenceScope,
        ) -> RecoveryFenceObservation {
            let actual = self.providers[&self.nodes[0]].local_participant();
            self.runtimes[node]
                .fence_epoch(
                    &actual,
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 3000,
                    },
                    &self.parent,
                )
                .unwrap()
        }
    }

    #[test]
    fn actual_rf1_fence_is_durable_but_unknown_native_floor_never_becomes_zero_ready() {
        let fixture = Fixture::new(1);
        let mut round =
            RecoveryFenceQuorumRound::begin(fixture.role(Uuid::now_v7()), &fixture.parent).unwrap();
        let scope = round.scope().clone();
        let observation = fixture.observe(&fixture.nodes[0], &scope);
        assert_eq!(observation.native().native_applied_index, None);
        round.record(observation).unwrap();
        let authority = round.finish().unwrap();
        authority.require_live().unwrap();
        assert_eq!(authority.observations().len(), 1);
        assert_eq!(authority.highest_observed_index(), 0); // diagnostic ONLY
        assert!(authority.clean_source_candidates().is_empty());
        assert!(fixture.databases[&fixture.nodes[0]]
            .read()
            .range_mutation_gate()
            .try_enter(scope.range_id)
            .is_err());
    }
    #[test]
    fn actual_rf3_distinct_fences_succeed_without_waiting_for_data_leader_ack() {
        let fixture = Fixture::new(3);
        let mut round =
            RecoveryFenceQuorumRound::begin(fixture.role(Uuid::now_v7()), &fixture.parent).unwrap();
        assert_eq!(round.required_quorum(), 2);
        let scope = round.scope().clone();
        for id in &fixture.nodes[1..] {
            round.record(fixture.observe(id, &scope)).unwrap();
        }
        let authority = round.finish().unwrap();
        assert_eq!(authority.observations().len(), 2);
        assert!(!authority.observations().contains_key(&fixture.nodes[0]));
        assert!(authority.clean_source_candidates().is_empty());
    }
    #[test]
    fn actual_duplicate_and_insufficient_fences_cannot_create_majority() {
        let fixture = Fixture::new(3);
        let mut round =
            RecoveryFenceQuorumRound::begin(fixture.role(Uuid::now_v7()), &fixture.parent).unwrap();
        let scope = round.scope().clone();
        round
            .record(fixture.observe(&fixture.nodes[1], &scope))
            .unwrap();
        assert!(round
            .record(fixture.observe(&fixture.nodes[1], &scope))
            .is_err());
        assert!(round.finish().is_err());
    }
    #[test]
    fn independently_real_but_different_session_fence_is_rejected() {
        let fixture = Fixture::new(3);
        let mut round =
            RecoveryFenceQuorumRound::begin(fixture.role(Uuid::now_v7()), &fixture.parent).unwrap();
        let other = RecoveryFenceScope::from_role(&fixture.role(Uuid::now_v7())).unwrap();
        assert!(round
            .record(fixture.observe(&fixture.nodes[1], &other))
            .is_err());
        assert!(!round.has_observation(&fixture.nodes[1]));
    }
    #[test]
    fn original_parent_cancellation_prevents_consuming_a_real_fence_quorum() {
        let fixture = Fixture::new(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let parent = CancellationToken::new(
            Arc::clone(&cancelled),
            Some(fixture.parent.deadline().unwrap()),
        );
        let mut round =
            RecoveryFenceQuorumRound::begin(fixture.role(Uuid::now_v7()), &parent).unwrap();
        round
            .record(fixture.observe(&fixture.nodes[0], round.scope()))
            .unwrap();
        cancelled.store(true, Ordering::SeqCst);
        assert!(round.finish().is_err());
    }
}
