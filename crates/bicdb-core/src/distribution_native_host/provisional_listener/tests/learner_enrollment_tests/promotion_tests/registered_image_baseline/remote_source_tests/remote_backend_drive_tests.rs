//! The remote joined-handoff backend drives one handoff from the committed
//! learner allocation to the completed relocation: the final membership
//! commit, the activation (every new member's bridge preparation,
//! publication, Open, custody retirement, release and writer policy), the
//! target's serving, the activation record and the source retirement. The
//! backend is given the coordinator's transport, node ids and the
//! coordinator's own data service only: every other member is reached
//! through the product requests of that one transport. In-process handles
//! are read here only to compare outcomes, and the test hosts commit through
//! the real metadata consensus of the metadata leader.
//!
//! The target's two host commands (its writer policy and its serving) are
//! performed by the target's own host loop, which this fixture does not run.
//! The tests stand in for that loop and for nothing else: either this thread
//! calls the target host's loop entry while the coordinator drives on its
//! own thread (`pumped`), or it calls that entry once between two
//! coordinator steps (the controller walk).
use super::*;
use crate::distribution::{NativeJointHandoffStageV1, RangeReplicaRole, RelocationPhase};
use crate::distribution_native_host::joined_data_handoff::{
    JoinedHandoffBackend, JoinedHandoffMetadataHosts, NativeJoinedDataHandoff,
    NativeJoinedDataHandoffStage as Stage, NativeJoinedDataPlanningView,
    RemoteJoinedHandoffBackend, RemoteTestStop,
};
use crate::distribution_native_host::range_move::{
    NativeRangeMoveController, NativeRangeMoveHosts,
};
use crate::distribution_range_handoff::{
    NativeJointHandoffStepOperationV1, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepRequestV1,
};
use std::collections::BTreeMap;

type Handoff = NativeJoinedDataHandoff<RemoteJoinedHandoffBackend>;

pub(in super::super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::RemoteBackendFullDrive
            | SourceCase::RemoteBackendRedrive
            | SourceCase::RemoteBackendOneDrive
            | SourceCase::RemoteBackendDecisionMissingNewMember
            | SourceCase::RemoteBackendDecisionMissingSource
            | SourceCase::RemoteBackendControllerWalk
            | SourceCase::RemoteBackendNotRangeLeader
    )
}
/// One new member misses the publication of the joint decision: nothing is
/// certified and the missing member is named; once it can be reached the
/// same handoff certifies and the whole move completes.
#[test]
fn native_remote_backend_decision_missing_on_a_new_member_is_not_certified_then_completes() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorLeadsRange,
        Some(SourceCase::RemoteBackendDecisionMissingNewMember),
    );
}
/// The same for the retiring source, which needs the decision to record its
/// retirement.
#[test]
fn native_remote_backend_decision_missing_on_the_retiring_source_is_not_certified_then_completes() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorLeadsRange,
        Some(SourceCase::RemoteBackendDecisionMissingSource),
    );
}
/// One coordinator object, one `drive()`: the range is moved, the handoff is
/// `Cleaned`, the relocation `Completed`, and the original client reads its
/// acknowledged rows from the new membership and appends.
#[test]
fn native_remote_backend_one_drive_moves_the_range_and_the_target_serves_clients() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorLeadsRange,
        Some(SourceCase::RemoteBackendOneDrive),
    );
}
/// The production controller with the remote backend walks `MoveRange` to
/// `completed`; the target's pending host commands are reported as a wait,
/// never as an error.
#[test]
fn native_range_move_remote_backend_walks_move_range_to_completed_and_reports_host_wait() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorLeadsRange,
        Some(SourceCase::RemoteBackendControllerWalk),
    );
}
#[test]
fn native_remote_backend_fresh_backends_after_promotion_release_policy_serve_and_record_complete() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorLeadsRange,
        Some(SourceCase::RemoteBackendFullDrive),
    );
}
#[test]
fn native_remote_backend_fresh_backends_after_certified_commit_and_after_open_complete() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorLeadsRange,
        Some(SourceCase::RemoteBackendRedrive),
    );
}
#[test]
fn native_remote_backend_refuses_coordinator_that_is_not_both_leaders_before_any_request() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorNotRangeLeader,
        Some(SourceCase::RemoteBackendNotRangeLeader),
    );
}

/// The metadata surface of the real metadata leader (node 0): committed
/// reads, real consensus commits and a planning view of the fresh committed
/// configuration. It holds no member service.
struct CommitHosts {
    /// The metadata leader's own heartbeat.
    heartbeat: Box<dyn Fn(&CancellationToken) -> Result<()> + Send + Sync>,
    metadata: Vec<Arc<Mutex<crate::MetadataConsensusStore>>>,
    /// The four original transports and the target's: each routes by the
    /// committed configuration once a commit returns.
    clients: Vec<Arc<TcpClusterRelocationTransport>>,
    template: crate::DistributionConfig,
    planning_roots: Vec<tempfile::TempDir>,
    commits: usize,
}
impl JoinedHandoffMetadataHosts for CommitHosts {
    fn metadata_leader(&self) -> Result<ClusterNodeId> {
        Ok(node(0))
    }
    fn committed_topology(&self) -> Result<ClusterTopology> {
        Ok(self.metadata[0].lock().committed_topology().clone())
    }
    fn commit_topology(
        &mut self,
        candidate: ClusterTopology,
        _parent: &CancellationToken,
    ) -> Result<()> {
        let entry = self.metadata[0]
            .lock()
            .propose_topology(candidate.clone())?;
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            (self.heartbeat)(&budget(5000))?;
            let applied = self.metadata.iter().all(|actual| {
                let store = actual.lock();
                store.status().commit_index >= entry.index
                    && store.committed_topology() == &candidate
            });
            if applied {
                break;
            }
            if Instant::now() >= until {
                return Err(BicDbError::Cluster(
                    "committed configuration did not reach every metadata store".into(),
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        for client in &self.clients {
            client.install_topology(candidate.clone())?;
        }
        self.commits += 1;
        Ok(())
    }
    fn planning_view(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<NativeJoinedDataPlanningView> {
        let business = Uuid::now_v7();
        let fresh = self.clients[0].acquire_host_metadata_observation(
            &node(0),
            business,
            Duration::from_secs(5),
            parent,
        )?;
        let snapshot = crate::ClusterBootstrapSnapshot {
            topology: fresh.committed_topology(business)?.clone(),
            config_template: self.template.clone(),
            metadata_leader_id: Some(node(0)),
        };
        // A planning view only; the real consensus commit publishes.
        let root = tempfile::tempdir()?;
        let config = crate::provision_cluster_member_directory(
            root.path(),
            &snapshot,
            &node(0),
            self.template.transport.clone(),
            true,
        )?;
        let view = crate::DistributionStore::open(root.path(), config, true)?.fork_ephemeral();
        self.planning_roots.push(root);
        Ok(NativeJoinedDataPlanningView {
            view,
            fresh,
            business,
        })
    }
}

impl NativeRangeMoveHosts for CommitHosts {
    fn metadata(&mut self) -> &mut (dyn JoinedHandoffMetadataHosts + 'static) {
        self
    }
}

/// Run `drive` on its own thread, as the coordinator's step worker does,
/// while this thread is the target's host loop: it calls the loop's entry
/// for the joint handoff host commands until the drive has returned.
/// Returns the drive's result and how many host commands that entry
/// performed. A failed attempt is retained by the host and repeated, as in
/// the owning process loop.
fn pumped<T: Send>(
    target_control: &mut NativeMetadataControlHost,
    drive: impl FnOnce() -> T + Send,
) -> (T, usize) {
    std::thread::scope(|scope| {
        let worker = scope.spawn(drive);
        let mut performed = 0;
        while !worker.is_finished() {
            match target_control.advance_joint_bridge_host_commands(&budget(5000)) {
                Ok(true) => performed += 1,
                Ok(false) => {}
                Err(error) => eprintln!("REMOTE_BACKEND_HOST_COMMAND_REPEATED {error}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        (worker.join().expect("the drive panicked"), performed)
    })
}
/// The target host's loop entry, once, between two coordinator steps.
fn perform_host_command(target_control: &mut NativeMetadataControlHost) {
    for _ in 0..20 {
        match target_control.advance_joint_bridge_host_commands(&budget(5000)) {
            Ok(true) => return,
            Ok(false) => panic!("the target holds no pending host command"),
            Err(error) => eprintln!("REMOTE_BACKEND_HOST_COMMAND_REPEATED {error}"),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the target host did not complete its pending command");
}

/// What the members retain, read in-process to compare with the answers the
/// backend kept.
struct Members<'a> {
    controls: &'a [NativeMetadataControlHost],
    target_control: &'a NativeMetadataControlHost,
    range: crate::RangeId,
    new: &'a [ClusterNodeId],
    /// The target first, then the survivors in node order.
    order: &'a [ClusterNodeId],
    decision_sha256: &'a str,
}
impl Members<'_> {
    fn evidence(&self, member: &ClusterNodeId) -> Option<serde_json::Value> {
        let i = index_of(member);
        if i == 4 {
            self.target_control
                .test_registered_bridge_publication(self.range)
                .unwrap()
        } else {
            self.controls[i]
                .test_initial_bridge_publication(self.range)
                .unwrap()
        }
    }
    /// What the backend retained of the members' answers to preparation,
    /// publication and Open, and what every member retains of them.
    fn opened(
        &self,
        handoff: &Handoff,
        before_release: bool,
    ) -> BTreeMap<ClusterNodeId, (String, String)> {
        let backend = handoff.backend();
        let publication = backend.bridge_publication().unwrap();
        let receipts = publication.receipts().unwrap();
        assert_eq!(receipts.keys().cloned().collect::<Vec<_>>(), self.new);
        let opened = backend.bridge_opened();
        assert_eq!(
            opened.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
            self.order,
            "the target opens first, then the survivors in node order"
        );
        let mut held = BTreeMap::new();
        for (member, outcome) in opened {
            let evidence = self.evidence(member).expect("retained bridge custody");
            assert_eq!(evidence["bridge_confirmed"], true);
            assert_eq!(evidence["publication_pending"], false);
            assert_eq!(evidence["activation_confirmed"], true);
            assert_eq!(evidence["activation_pending"], false);
            assert!(outcome.complete);
            assert_eq!(
                outcome.decision_sha256.as_deref(),
                Some(self.decision_sha256)
            );
            assert_eq!(
                outcome.bridge_receipt_sha256.as_deref(),
                Some(receipts[member].as_str())
            );
            assert_eq!(
                evidence["retained_bridge_sha256"],
                receipts[member].as_str()
            );
            let activation = outcome.activation_sha256.clone().unwrap();
            assert_eq!(evidence["retained_activation_sha256"], activation.as_str());
            if before_release {
                // The member still holds its preparation custody, through
                // which its durable bridge and activation are read.
                publication.require_current().unwrap();
                let bridge = &evidence["durable_bridge"];
                assert!(bridge.is_object(), "{member}: {evidence}");
                assert_eq!(bridge["decision_sha256"], self.decision_sha256);
                assert_eq!(bridge["local_node_id"], serde_json::json!(member));
                assert_eq!(bridge["archive_equals_pointer"], true);
                assert_eq!(bridge["checksum_sha256"], receipts[member].as_str());
                assert_eq!(
                    evidence["durable_activation_sha256"],
                    activation.as_str(),
                    "{member}: durable catalogue activation"
                );
                assert_eq!(evidence["release_confirmed"], false);
                assert_eq!(evidence["old_custody_retired"], false);
            }
            held.insert(member.clone(), (receipts[member].clone(), activation));
        }
        held
    }
    /// What every member in `asked` answered for its completed release and
    /// retains of it: the bridge and activation it released, and its armed
    /// generation. A target that was observed released is not asked.
    fn released(
        &self,
        handoff: &Handoff,
        asked: &[ClusterNodeId],
    ) -> BTreeMap<ClusterNodeId, (String, String, Uuid)> {
        let outcomes = handoff.backend().bridge_released();
        assert_eq!(
            outcomes
                .iter()
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>(),
            asked,
            "the target is released first, then the survivors in node order"
        );
        let mut held = BTreeMap::new();
        for (member, outcome) in outcomes {
            let evidence = self.evidence(member).expect("retained bridge custody");
            assert!(outcome.complete);
            assert_eq!(outcome.released, Some(true));
            assert_eq!(
                outcome.decision_sha256.as_deref(),
                Some(self.decision_sha256)
            );
            let generation = outcome.released_generation.expect("armed generation");
            assert_eq!(evidence["release_confirmed"], true, "{member}: {evidence}");
            assert_eq!(evidence["release_pending"], false);
            assert_eq!(evidence["old_custody_retired"], true);
            assert_eq!(evidence["preparation_custody_retained"], false);
            assert_eq!(
                evidence["released_generation"],
                serde_json::json!(generation)
            );
            assert_eq!(
                evidence["retained_bridge_sha256"],
                serde_json::json!(outcome.bridge_receipt_sha256)
            );
            assert_eq!(
                evidence["retained_activation_sha256"],
                serde_json::json!(outcome.activation_sha256)
            );
            held.insert(
                member.clone(),
                (
                    outcome.bridge_receipt_sha256.clone().unwrap(),
                    outcome.activation_sha256.clone().unwrap(),
                    generation,
                ),
            );
        }
        held
    }
    /// What every member answered for its completed writer policy: the
    /// survivors in node order, then the target, each for the bridge,
    /// activation and generation it released.
    fn policies(
        &self,
        handoff: &Handoff,
        released: &BTreeMap<ClusterNodeId, (String, String, Uuid)>,
    ) -> Vec<(ClusterNodeId, NativeJointHandoffStepOutcomeV1)> {
        let outcomes = handoff.backend().bridge_policies();
        let mut expected = self
            .new
            .iter()
            .filter(|id| **id != node(4))
            .cloned()
            .collect::<Vec<_>>();
        expected.push(node(4));
        assert_eq!(
            outcomes
                .iter()
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>(),
            expected,
            "the survivors publish their policy first, in node order, then the target"
        );
        for (member, outcome) in outcomes {
            assert!(outcome.complete);
            assert_eq!(outcome.writer_policy_installed, Some(true));
            assert!(outcome.target_recovered_ranges.is_none());
            assert_eq!(
                outcome.decision_sha256.as_deref(),
                Some(self.decision_sha256)
            );
            let (bridge, activation, generation) = &released[member];
            assert_eq!(outcome.released, Some(true));
            assert_eq!(outcome.released_generation, Some(*generation));
            assert_eq!(outcome.bridge_receipt_sha256.as_ref(), Some(bridge));
            assert_eq!(outcome.activation_sha256.as_ref(), Some(activation));
            if *member != node(4) {
                let state = self.controls[index_of(member)]
                    .test_initial_bridge_writer_policy(self.range)
                    .unwrap();
                assert_eq!(state["policy_confirmed"], true, "{member}: {state}");
                assert_eq!(state["policy_pending"], false, "{member}: {state}");
            }
        }
        outcomes.to_vec()
    }
    /// The successful host commands the target's host retains, in order.
    fn host_commands(&self) -> Vec<serde_json::Value> {
        let evidence = self
            .target_control
            .test_registered_bridge_host_commands()
            .unwrap();
        assert!(evidence["pending"].is_null(), "{evidence}");
        evidence["done"].as_array().unwrap().clone()
    }
}

/// The committed completion of the relocation and what the target serves.
#[allow(clippy::too_many_arguments)]
fn assert_moved(
    hosts: &CommitHosts,
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    target_control: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    range: &crate::RangeDescriptor,
    relocation: &crate::distribution::RangeRelocation,
    new: &[ClusterNodeId],
    acknowledged: &[serde_json::Value],
) -> ClusterTopology {
    let cleaned = hosts.committed_topology().unwrap();
    let row = &cleaned.relocations[&relocation.id];
    let record = row.native_joint_handoff_v1.as_ref().unwrap();
    assert_eq!(row.phase, RelocationPhase::Completed);
    assert_eq!(record.stage(), NativeJointHandoffStageV1::Cleaned);
    assert!(record.activated_quorum_sha256().is_some());
    assert!(record.source_cleanup_sha256().is_some());
    let moved = cleaned.range_by_id(range.id).unwrap();
    assert_eq!(moved.epoch, range.epoch + 2);
    assert_eq!(
        moved.leader,
        node(4),
        "the target leads the final membership"
    );
    let mut voters = moved
        .replicas
        .iter()
        .map(|replica| {
            assert_eq!(replica.role, RangeReplicaRole::Voter);
            replica.node_id.clone()
        })
        .collect::<Vec<_>>();
    voters.sort();
    assert_eq!(voters, new);
    for actual in metadata {
        assert_eq!(actual.lock().committed_topology(), &cleaned);
    }
    // The retired source confirmed the decision into its own catalogue
    // before it recorded its retirement.
    let (decision, native) = services[0]
        .lock()
        .test_catalogue_joint_decision(range.id)
        .expect("the retired source confirmed the decision");
    assert_eq!(decision.as_deref(), Some(record.decision_sha256()));
    assert!(native.is_some());
    // The target admits commits, its ordinary DATA listener is published,
    // and it holds the rows that were acknowledged before the move.
    assert!(factory.database.read().commit_admission_ready());
    assert!(
        target_control
            .joined_data_status()
            .expect("the target is a joined DATA owner")
            .dispatcher_published
    );
    for acknowledged in acknowledged {
        let record_id = acknowledged["record_id"].as_str().unwrap();
        let owner = cleaned
            .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, record_id)
            .unwrap();
        if owner.id != range.id {
            continue;
        }
        let actual = factory
            .database
            .read()
            .get(crate::CLUSTER_EVENTS_COLLECTION, record_id)
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual.as_ref().unwrap()).unwrap(),
            acknowledged["actual_full_record"]
        );
    }
    cleaned
}

fn index_of(id: &ClusterNodeId) -> usize {
    (0..5).find(|i| node(*i) == *id).unwrap()
}
fn native_records(
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    old: &[ClusterNodeId],
    range: crate::RangeId,
) -> Vec<Option<serde_json::Value>> {
    old.iter()
        .map(|id| {
            let database = services[index_of(id)].lock().database();
            let record = database.read().test_native_old_close_record(range).unwrap();
            record
        })
        .collect()
}
fn recorded(
    topology: &ClusterTopology,
    range: crate::RangeId,
) -> Option<NativeJointHandoffStageV1> {
    topology
        .relocations
        .values()
        .find(|plan| plan.range_id == range)
        .and_then(|plan| plan.native_joint_handoff_v1.as_ref())
        .map(|record| record.stage())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &mut [NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    target_control: &mut NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    template: &crate::DistributionConfig,
    original_gateway: &crate::ClusterEventClient,
    process: &CancellationToken,
) {
    assert!(is_case(case));
    // A member's process binds its image-source owner when its host starts
    // (`start_hot_process`); this fixture starts no such process.
    for control in controls.iter() {
        control.bind_initial_image_source_process(process).unwrap();
    }
    let range = pending.range_by_id(ranges[0].id).unwrap().clone();
    let relocation = pending
        .relocations
        .values()
        .find(|plan| plan.range_id == range.id)
        .unwrap()
        .clone();
    let old = range
        .replicas
        .iter()
        .filter(|replica| replica.role == RangeReplicaRole::Voter)
        .map(|replica| replica.node_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(old.len(), 3);
    let mut all_clients = clients.to_vec();
    all_clients.push(target.clone());
    let mut hosts = CommitHosts {
        heartbeat: controls[0].test_heartbeat_handle(),
        metadata: metadata.to_vec(),
        clients: all_clients,
        template: template.clone(),
        planning_roots: Vec::new(),
        commits: 0,
    };
    assert_eq!(&hosts.committed_topology().unwrap(), pending);
    let target_before = state(&factory.database, pending, &node(4), ranges).unwrap();
    // The backend is constructed from the coordinator's transport and node
    // ids alone: it cannot hold a member's service.
    let coordinated = |hosts_range: crate::RangeId, coordinator: usize| {
        NativeJoinedDataHandoff::with_backend(
            hosts_range,
            node(4),
            RemoteJoinedHandoffBackend::new(node(4), clients[coordinator].clone()),
        )
    };

    if matches!(case, SourceCase::RemoteBackendNotRangeLeader) {
        assert_ne!(range.leader, node(0));
        let untouched = |hosts: &CommitHosts| {
            assert!(native_records(services, &old, range.id)
                .iter()
                .all(Option::is_none));
            assert!(target_control
                .test_joint_handoff_step_state(range.id)
                .unwrap()
                .is_null());
            assert_eq!(
                state(&factory.database, pending, &node(4), ranges).unwrap(),
                target_before
            );
            assert_eq!(hosts.commits, 0);
            assert_eq!(&hosts.committed_topology().unwrap(), pending);
        };
        // The metadata leader that does not lead the range.
        let mut handoff = coordinated(range.id, 0);
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::LearnerAllocated), &budget(20_000))
                .unwrap(),
            Stage::LearnerAllocated,
            "the stages before the first member request need committed metadata only"
        );
        for until in [
            Some(Stage::ImageVerified),
            Some(Stage::MembershipFinal),
            None,
        ] {
            let error = handoff
                .drive_until(&mut hosts, until, &budget(20_000))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!(
                    "coordinator {} is the metadata leader but not the leader {} of range {}",
                    node(0),
                    range.leader,
                    range.id
                )),
                "{error}"
            );
        }
        assert!(!handoff.backend().cut_started());
        untouched(&hosts);
        // The range leader that is not the metadata leader.
        let mut handoff = coordinated(range.id, index_of(&range.leader));
        for until in [
            Some(Stage::TargetAdmitted),
            Some(Stage::ImageVerified),
            None,
        ] {
            let error = handoff
                .drive_until(&mut hosts, until, &budget(20_000))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!(
                    "coordinator {} is not the metadata leader {}",
                    range.leader,
                    node(0)
                )),
                "{error}"
            );
        }
        assert!(!handoff.backend().cut_started());
        untouched(&hosts);
        return;
    }

    assert_eq!(range.leader, node(0), "the coordinator leads the range");
    if matches!(
        case,
        SourceCase::RemoteBackendOneDrive
            | SourceCase::RemoteBackendDecisionMissingNewMember
            | SourceCase::RemoteBackendDecisionMissingSource
            | SourceCase::RemoteBackendControllerWalk
    ) {
        whole_move(
            case,
            &mut hosts,
            controls,
            clients,
            services,
            metadata,
            target_control,
            factory,
            &range,
            &relocation,
            &old,
            acknowledged,
            output,
            original_gateway,
        );
        return;
    }
    let mut handoff = coordinated(range.id, 0);
    let started = Instant::now();
    assert_eq!(
        handoff
            .drive_until(&mut hosts, Some(Stage::ImageVerified), &budget(120_000))
            .unwrap(),
        Stage::ImageVerified
    );
    let image_ms = started.elapsed().as_millis();
    {
        let backend = handoff.backend();
        assert!(backend.cut_started() && !backend.cut_required());
        assert_eq!(backend.closed_frames().len(), old.len());
        let plan = backend.retained_plan().unwrap();
        let donor = backend.donor().unwrap();
        assert!(old.contains(donor));
        assert_ne!(donor, &range.leader);
        assert_ne!(Some(donor), relocation.source.as_ref());
        let outcome = backend.install_outcome().unwrap();
        let manifest = backend.source_manifest().unwrap();
        assert!(outcome.complete);
        assert_eq!(
            outcome.manifest_sha256.as_ref(),
            Some(&manifest.checksum_sha256)
        );
        assert_eq!(&manifest.source_id, donor);
        assert_eq!(manifest.target_id, node(4));
        let attempt = RemoteJoinedHandoffBackend::target_attempt(plan).unwrap();
        assert_eq!(manifest.target_attempt, attempt);
        // The same completed state the step-request test reaches.
        assert!(native_records(services, &old, range.id)
            .iter()
            .all(Option::is_some));
        let step = target_control
            .test_joint_handoff_step_state(range.id)
            .unwrap();
        assert_eq!(step["target_attempt"], attempt.to_string());
        assert_eq!(step["completed"]["complete"], true);
        target_control
            .registered_remote_joint_owner(range.id)
            .unwrap();
        let donor_state = controls[index_of(donor)]
            .test_source_discovery_state(range.id)
            .unwrap();
        assert_eq!(
            donor_state["binding"]["target_attempt"],
            attempt.to_string()
        );
        assert_eq!(donor_state["source"]["capture_retained"], false);
        assert!(!donor_state["source"]["export_retirement"]["completed"].is_null());
        // The export retirement step never starts or replaces an install:
        // another attempt than the completed one is refused, changing nothing.
        let parent = budget(5000);
        let business = Uuid::now_v7();
        let own = clients[0]
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
            .unwrap();
        let request = NativeJointHandoffStepRequestV1::from_plan(
            plan,
            NativeJointHandoffStepOperationV1::SourceExportRetire {
                donor: donor.clone(),
                target_attempt: Uuid::now_v7(),
            },
            business,
            4000,
        )
        .unwrap();
        let error = clients[0]
            .request_native_joint_handoff_step(&node(4), plan, &request, &own, &parent)
            .err()
            .expect("an export retirement of another attempt must be refused")
            .to_string();
        assert!(
            error.contains("names another donor/attempt than the retained target install"),
            "{error}"
        );
        assert_eq!(
            target_control
                .test_joint_handoff_step_state(range.id)
                .unwrap(),
            step
        );
        assert_eq!(
            controls[index_of(donor)]
                .test_source_discovery_state(range.id)
                .unwrap(),
            donor_state
        );
    }
    assert_eq!(hosts.commits, 0);
    assert_eq!(
        handoff
            .drive_until(&mut hosts, Some(Stage::DecisionPublished), &budget(120_000))
            .unwrap(),
        Stage::DecisionPublished
    );
    let certificate_sha256 = {
        let certificate = handoff.backend().decision_certificate().unwrap();
        certificate.require_current().unwrap();
        for id in old.iter().chain(std::iter::once(&node(4))) {
            assert!(certificate.publishers().contains(id), "{id}");
        }
        certificate.checksum_sha256().unwrap()
    };
    assert_eq!(hosts.commits, 0);
    // Before the certification is committed no member confirms anything:
    // its own committed metadata records no handoff of this relocation.
    {
        let survivor = old
            .iter()
            .find(|id| Some(*id) != relocation.source.as_ref())
            .unwrap();
        let i = index_of(survivor);
        let token = budget(5000);
        let business = Uuid::now_v7();
        let own = clients[i]
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
            .unwrap();
        let before = services[i].lock().test_catalogue_joint_decision(range.id);
        let error = crate::ClusterDataNodeService::confirm_committed_native_joint_decision(
            &services[i],
            &own,
            business,
            relocation.id,
            range.id,
            &token,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("joint decision confirmation has no committed handoff record"),
            "{error}"
        );
        assert_eq!(
            services[i].lock().test_catalogue_joint_decision(range.id),
            before
        );
    }
    let activation = handoff
        .backend()
        .decision_certificate()
        .unwrap()
        .data()
        .activation_range()
        .clone();
    eprintln!(
        "REMOTE_BACKEND_DRIVE {}",
        serde_json::json!({"case":format!("{case:?}"),"image_ms":image_ms,
            "decision_ms":started.elapsed().as_millis(),"certificate":certificate_sha256,
            "donor":handoff.backend().donor(),"activation":activation})
    );

    if matches!(case, SourceCase::RemoteBackendRedrive) {
        // First loss of memory, before anything is committed: the committed
        // relocation is still the bare learner allocation, so the new
        // backend repeats the source stage. Every part confirms what is
        // durable: the same close records, the same install attempt and its
        // retained outcome, the donor's export terminal, the same decision.
        let records = native_records(services, &old, range.id);
        let step = target_control
            .test_joint_handoff_step_state(range.id)
            .unwrap();
        let manifest_sha256 = handoff
            .backend()
            .source_manifest()
            .unwrap()
            .checksum_sha256
            .clone();
        handoff = coordinated(range.id, 0);
        assert!(handoff.backend().cut_required());
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::DecisionPublished), &budget(120_000))
                .unwrap(),
            Stage::DecisionPublished
        );
        assert_eq!(hosts.commits, 0);
        assert_eq!(native_records(services, &old, range.id), records);
        assert_eq!(
            target_control
                .test_joint_handoff_step_state(range.id)
                .unwrap(),
            step
        );
        assert_eq!(
            handoff.backend().source_manifest().unwrap().checksum_sha256,
            manifest_sha256
        );
        assert_eq!(
            handoff
                .backend()
                .decision_certificate()
                .unwrap()
                .checksum_sha256()
                .unwrap(),
            certificate_sha256
        );
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::Certified), &budget(60_000))
                .unwrap(),
            Stage::Certified
        );
        assert_eq!(hosts.commits, 1);
        assert_eq!(
            recorded(&hosts.committed_topology().unwrap(), range.id),
            Some(NativeJointHandoffStageV1::Certified)
        );
        // The coordinator loses its memory: a new backend object holds
        // nothing and re-derives the decision from the members.
        handoff = coordinated(range.id, 0);
        let records = native_records(services, &old, range.id);
        let step = target_control
            .test_joint_handoff_step_state(range.id)
            .unwrap();
        assert!(handoff.backend().decision_certificate().is_none());
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::Certified), &budget(60_000))
                .unwrap(),
            Stage::Certified
        );
        assert_eq!(
            hosts.commits, 1,
            "the certified stage is not committed twice"
        );
        let again = handoff.backend().decision_certificate().unwrap();
        assert_eq!(again.checksum_sha256().unwrap(), certificate_sha256);
        assert!(handoff.backend().cut_started());
        // The re-derivation was read-only towards every member.
        assert_eq!(native_records(services, &old, range.id), records);
        assert_eq!(
            target_control
                .test_joint_handoff_step_state(range.id)
                .unwrap(),
            step
        );
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::MembershipFinal), &budget(60_000))
                .unwrap(),
            Stage::MembershipFinal
        );
        assert_eq!(hosts.commits, 3);
    } else {
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::PromotedFrozen), &budget(60_000))
                .unwrap(),
            Stage::PromotedFrozen
        );
        assert_eq!(hosts.commits, 2);
        // The coordinator loses its memory between the promotion commit and
        // the final membership commit. The certificate cannot be collected
        // again (its scope is the pending plan, which only the pre-promotion
        // range leader can read), and it is not needed: the certified record
        // of the committed metadata carries the decision's scope, and the
        // final membership is built from that record alone.
        let frozen = hosts.committed_topology().unwrap();
        assert_eq!(
            recorded(&frozen, range.id),
            Some(NativeJointHandoffStageV1::PromotedFrozen)
        );
        let scope = frozen.relocations[&relocation.id]
            .native_joint_handoff_v1
            .as_ref()
            .unwrap()
            .certified_scope()
            .expect("the certification wrote the decision's scope")
            .clone();
        assert_eq!(scope.activation_range(), &activation);
        assert_eq!(
            scope,
            handoff
                .backend()
                .decision_certificate()
                .unwrap()
                .data()
                .certified_scope()
        );
        handoff = coordinated(range.id, 0);
        assert!(handoff.backend().decision_certificate().is_none());
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::MembershipFinal), &budget(60_000))
                .unwrap(),
            Stage::MembershipFinal
        );
        assert_eq!(hosts.commits, 3);
        assert!(
            handoff.backend().decision_certificate().is_none(),
            "nothing was collected again"
        );
        assert_eq!(
            handoff.backend().committed_decision().unwrap().scope(),
            &scope
        );
    }

    // The committed metadata shows the final membership.
    let committed = hosts.committed_topology().unwrap();
    let final_range = committed.range_by_id(range.id).unwrap();
    assert_eq!(final_range, &activation);
    assert_eq!(final_range.epoch, range.epoch + 2);
    let source = relocation.source.clone().unwrap();
    let mut voters = final_range
        .replicas
        .iter()
        .map(|replica| {
            assert_eq!(replica.role, RangeReplicaRole::Voter);
            replica.node_id.clone()
        })
        .collect::<Vec<_>>();
    voters.sort();
    let mut expected = old
        .iter()
        .filter(|id| **id != source)
        .cloned()
        .chain(std::iter::once(node(4)))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(voters, expected);
    let row = &committed.relocations[&relocation.id];
    assert_eq!(row.phase, RelocationPhase::CleaningUp);
    assert_eq!(
        row.native_joint_handoff_v1.as_ref().unwrap().stage(),
        NativeJointHandoffStageV1::AwaitingActivation
    );
    for actual in metadata {
        assert_eq!(actual.lock().committed_topology(), &committed);
    }
    // A repeated drive commits nothing more, with or without memory.
    assert_eq!(
        handoff
            .drive_until(&mut hosts, Some(Stage::MembershipFinal), &budget(60_000))
            .unwrap(),
        Stage::MembershipFinal
    );
    assert_eq!(
        coordinated(range.id, 0)
            .drive_until(&mut hosts, Some(Stage::MembershipFinal), &budget(60_000))
            .unwrap(),
        Stage::MembershipFinal
    );
    assert_eq!(hosts.commits, 3);
    // The activation, first part: every new member prepares, publishes its
    // bridge and persists its catalogue activation, driven by step requests
    // of the coordinator only. The coordinator's replica retires, so the
    // coordinator is not a new member and steps no bridge of its own.
    assert_eq!(source, node(0), "the coordinator's replica retires");
    let new = expected;
    assert!(!new.contains(&node(0)) && new.contains(&node(4)));
    let decision_sha256 = row
        .native_joint_handoff_v1
        .as_ref()
        .unwrap()
        .decision_sha256()
        .to_owned();
    let mut order = vec![node(4)];
    order.extend(new.iter().filter(|id| **id != node(4)).cloned());
    macro_rules! members {
        () => {
            Members {
                controls: &*controls,
                target_control: &*target_control,
                range: range.id,
                new: &new,
                order: &order,
                decision_sha256: &decision_sha256,
            }
        };
    }
    for member in &new {
        assert!(
            members!().evidence(member).is_none(),
            "{member} holds bridge custody"
        );
    }
    // A member confirms the committed decision into its own catalogue only
    // from its own protected native decision record and its own committed
    // metadata. The remote decision round wrote the former and no catalogue
    // acknowledgement.
    {
        let survivor = new.iter().find(|id| **id != node(4)).unwrap();
        let i = index_of(survivor);
        let other = new.iter().find(|id| *id != survivor && **id != node(4));
        let confirm = |reader: usize, named: Option<&str>| {
            let token = budget(5000);
            let business = Uuid::now_v7();
            let own = clients[reader]
                .acquire_host_metadata_observation(
                    &node(0),
                    business,
                    Duration::from_secs(5),
                    &token,
                )
                .unwrap();
            match named {
                Some(digest) => {
                    crate::ClusterDataNodeService::test_confirm_native_joint_decision_named(
                        &services[i],
                        &own,
                        business,
                        relocation.id,
                        range.id,
                        digest,
                        &token,
                    )
                }
                None => crate::ClusterDataNodeService::confirm_committed_native_joint_decision(
                    &services[i],
                    &own,
                    business,
                    relocation.id,
                    range.id,
                    &token,
                ),
            }
        };
        let state = || services[i].lock().test_catalogue_joint_decision(range.id);
        let before = state();
        assert_eq!(
            before.clone().and_then(|(decision, _)| decision),
            None,
            "the remote decision round acknowledged nothing in the catalogue"
        );
        // A committed digest that is not this member's native record.
        let error = confirm(i, Some(&"a".repeat(64))).unwrap_err().to_string();
        assert!(
            error.contains("holds no protected native decision record")
                && error.contains("differs from committed correlation"),
            "{error}"
        );
        assert_eq!(state(), before);
        // Another member's metadata read is not this member's own.
        if let Some(other) = other {
            let error = confirm(index_of(other), None).unwrap_err().to_string();
            assert!(
                error.contains("not this member's own")
                    || error
                        .contains("owned metadata member requires actual independently fresh OWN"),
                "{error}"
            );
            assert_eq!(state(), before);
        }
        assert!(
            confirm(i, None).unwrap(),
            "the confirmation is written once"
        );
        let confirmed = state().unwrap();
        assert_eq!(confirmed.0.as_deref(), Some(&*decision_sha256));
        assert!(confirmed.1.is_some());
        assert!(
            !confirm(i, None).unwrap(),
            "a confirmed decision is a no-op"
        );
        assert_eq!(state().unwrap(), confirmed);
    }
    const STOPPED_OPEN: &str = "remote joined handoff backend: test stop after Open";
    const STOPPED_RELEASE: &str = "remote joined handoff backend: test stop after Release";
    const STOPPED_POLICY: &str = "remote joined handoff backend: test stop after WriterPolicy";
    let survivors = order[1..].to_vec();
    // A coordinator object as the production controller builds it: the
    // coordinator's transport and this node's own data service.
    let completing = |stop: Option<RemoteTestStop>| {
        let mut backend = RemoteJoinedHandoffBackend::new(node(4), clients[0].clone())
            .with_local_service(services[0].clone());
        if let Some(stop) = stop {
            backend = backend.test_stop_after(stop);
        }
        NativeJoinedDataHandoff::with_backend(range.id, node(4), backend)
    };
    let confirmed = || {
        for member in &new {
            let (decision, native) = services[index_of(member)]
                .lock()
                .test_catalogue_joint_decision(range.id)
                .expect("confirmed catalogue decision");
            assert_eq!(decision.as_deref(), Some(&*decision_sha256), "{member}");
            assert!(native.is_some(), "{member}");
        }
    };
    let observed = |handoff: &Handoff| {
        handoff
            .backend()
            .bridge_observed()
            .iter()
            .map(|(_, answer)| *answer)
            .collect::<Vec<_>>()
    };
    let completion = |topology: &ClusterTopology| {
        let row = &topology.relocations[&relocation.id];
        let record = row.native_joint_handoff_v1.as_ref().unwrap();
        assert_eq!(record.decision_sha256(), decision_sha256);
        assert_eq!(topology.range_by_id(range.id).unwrap(), &activation);
        (
            row.phase,
            record.stage(),
            record.activated_quorum_sha256().map(str::to_owned),
            record.source_cleanup_sha256().map(str::to_owned),
        )
    };
    assert!(!factory.database.read().commit_admission_ready());
    let started = Instant::now();
    let first_opened;
    let first_released;
    if matches!(case, SourceCase::RemoteBackendRedrive) {
        // The coordinator stops after every member's Open. Every new member
        // confirmed the remotely published decision into its own catalogue
        // as the first part of its own preparation.
        let mut stopping = completing(Some(RemoteTestStop::Open));
        let error = stopping
            .drive_until(&mut hosts, Some(Stage::Serving), &budget(300_000))
            .unwrap_err()
            .to_string();
        assert!(error.contains(STOPPED_OPEN), "{error}");
        confirmed();
        assert_eq!(observed(&stopping), vec![None; new.len()]);
        first_opened = members!().opened(&stopping, true);
        assert!(stopping.backend().bridge_released().is_empty());
        assert_eq!(hosts.commits, 3);
        // RESTART AFTER OPEN. A new object holds no member answer. Every
        // member answers the read-only release observation as open and not
        // released, so no preparation, publication or Open is sent again.
        // One drive completes the move: custody retirement, release, writer
        // policy, serving, activation record and source retirement.
        let mut amnesic = completing(None);
        let (stage, performed) = pumped(target_control, || {
            amnesic.drive(&mut hosts, &budget(600_000))
        });
        assert_eq!(stage.unwrap(), Stage::SourceRetired);
        assert_eq!(
            performed, 2,
            "the target host performed its writer policy and its serving, once each"
        );
        assert_eq!(observed(&amnesic), vec![Some(false); new.len()]);
        assert!(amnesic.backend().bridge_publication().is_none());
        assert!(amnesic.backend().bridge_opened().is_empty());
        assert_eq!(amnesic.backend().bridge_retired(), &new[..]);
        first_released = members!().released(&amnesic, &order);
        members!().policies(&amnesic, &first_released);
        let served = amnesic.backend().served().expect("the target served");
        assert!(served.complete);
        assert!(served
            .target_recovered_ranges
            .is_some_and(|ranges| ranges >= 1));
    } else {
        // One object drives preparation, publication, Open, custody
        // retirement and release, and stops there.
        let mut stopping = completing(Some(RemoteTestStop::Release));
        let error = stopping
            .drive_until(&mut hosts, Some(Stage::Serving), &budget(300_000))
            .unwrap_err()
            .to_string();
        assert!(error.contains(STOPPED_RELEASE), "{error}");
        confirmed();
        assert_eq!(observed(&stopping), vec![None; new.len()]);
        first_opened = members!().opened(&stopping, false);
        assert_eq!(stopping.backend().bridge_retired(), &new[..]);
        first_released = members!().released(&stopping, &order);
        assert!(stopping.backend().bridge_policies().is_empty());
        // The same object again: every member answers from what it holds,
        // and the released target is not sent its release again.
        let error = stopping
            .drive_until(&mut hosts, None, &budget(300_000))
            .unwrap_err()
            .to_string();
        assert!(error.contains(STOPPED_RELEASE), "{error}");
        assert_eq!(observed(&stopping), vec![Some(true); new.len()]);
        assert!(stopping.backend().bridge_retired().is_empty());
        for (member, again) in members!().released(&stopping, &survivors) {
            assert_eq!(again, first_released[&member], "{member}");
        }
        // Nothing of the writer policy exists yet, and nothing is committed.
        assert!(members!().host_commands().is_empty());
        assert!(!factory.database.read().commit_admission_ready());
        assert_eq!(hosts.commits, 3);
        assert_eq!(hosts.committed_topology().unwrap(), committed);

        // RESTART AFTER RELEASE. A new object holds no certificate and no
        // member answer. Every member answers as released: no preparation,
        // publication, Open, custody retirement or target release is sent
        // again. The survivors publish their writer policy, then the target
        // host performs its own; the object stops before the serving.
        let mut amnesic = completing(Some(RemoteTestStop::WriterPolicy));
        let (result, performed) = pumped(target_control, || {
            amnesic.drive_until(&mut hosts, Some(Stage::Serving), &budget(300_000))
        });
        let error = result.unwrap_err().to_string();
        assert!(error.contains(STOPPED_POLICY), "{error}");
        assert_eq!(performed, 1, "the target host performed its writer policy");
        assert!(amnesic.backend().decision_certificate().is_none());
        assert!(amnesic.backend().committed_decision().is_some());
        assert_eq!(observed(&amnesic), vec![Some(true); new.len()]);
        assert!(amnesic.backend().bridge_publication().is_none());
        assert!(amnesic.backend().bridge_opened().is_empty());
        assert!(amnesic.backend().bridge_retired().is_empty());
        for (member, again) in members!().released(&amnesic, &survivors) {
            assert_eq!(again, first_released[&member], "{member}");
        }
        let policies = members!().policies(&amnesic, &first_released);
        assert!(amnesic.backend().served().is_none());
        // The target holds its first writer policy and does not serve yet.
        assert!(factory.database.read().commit_admission_ready());
        assert_eq!(
            target_control
                .test_registered_first_policy_evidence()
                .unwrap()["stage"],
            "Installed"
        );
        assert_eq!(members!().host_commands().len(), 1);
        assert_eq!(hosts.commits, 3);
        assert_eq!(hosts.committed_topology().unwrap(), committed);

        // RESTART AFTER THE WRITER POLICY. The target's host is a DATA owner
        // now and would refuse a repeated release; none is sent. Every
        // member answers the repeated writer policy with the outcome it
        // retained, and the target host performs its serving.
        let mut amnesic = completing(None);
        let (stage, performed) = pumped(target_control, || {
            amnesic.drive_until(&mut hosts, Some(Stage::Serving), &budget(300_000))
        });
        assert_eq!(stage.unwrap(), Stage::Serving);
        assert_eq!(performed, 1, "the target host performed its serving only");
        assert_eq!(observed(&amnesic), vec![Some(true); new.len()]);
        assert!(amnesic.backend().bridge_opened().is_empty());
        assert!(amnesic.backend().bridge_retired().is_empty());
        for (member, again) in members!().released(&amnesic, &survivors) {
            assert_eq!(again, first_released[&member], "{member}");
        }
        assert_eq!(members!().policies(&amnesic, &first_released), policies);
        let served = amnesic
            .backend()
            .served()
            .expect("the target served")
            .clone();
        assert!(served.complete);
        assert!(served
            .target_recovered_ranges
            .is_some_and(|ranges| ranges >= 1));
        assert_eq!(members!().host_commands().len(), 2);
        assert_eq!(hosts.commits, 3);
        assert_eq!(hosts.committed_topology().unwrap(), committed);

        // RESTART AFTER THE TARGET SERVES. Nothing durable is repeated: the
        // target host has no command to perform, every member answers with
        // its retained outcome, and the activation record is committed.
        let mut amnesic = completing(None);
        let (stage, performed) = pumped(target_control, || {
            amnesic.drive_until(
                &mut hosts,
                Some(Stage::ActivationRecorded),
                &budget(300_000),
            )
        });
        assert_eq!(stage.unwrap(), Stage::ActivationRecorded);
        assert_eq!(performed, 0, "a serving target repeats no host command");
        assert_eq!(observed(&amnesic), vec![Some(true); new.len()]);
        assert!(amnesic.backend().bridge_retired().is_empty());
        assert_eq!(members!().policies(&amnesic, &first_released), policies);
        assert_eq!(amnesic.backend().served(), Some(&served));
        assert_eq!(members!().host_commands().len(), 2);
        assert_eq!(hosts.commits, 4);
        let recorded = hosts.committed_topology().unwrap();
        let (phase, stage, quorum, cleanup) = completion(&recorded);
        assert_eq!(phase, RelocationPhase::CleaningUp);
        assert_eq!(stage, NativeJointHandoffStageV1::AwaitingCleanup);
        assert!(quorum.is_some() && cleanup.is_none());

        // RESTART AFTER THE ACTIVATION RECORD. The committed metadata alone
        // says that the handoff is past Serving: a new object sends no
        // bridge step and finishes the retirement. A coordinator that is
        // the retired source and was not given its own data service cannot
        // ask itself, and commits nothing.
        let error = coordinated(range.id, 0)
            .drive(&mut hosts, &budget(60_000))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("is the retired source and was not given its own data service"),
            "{error}"
        );
        assert_eq!(hosts.commits, 4);
        assert_eq!(hosts.committed_topology().unwrap(), recorded);
        let mut amnesic = completing(None);
        assert_eq!(
            amnesic.drive(&mut hosts, &budget(300_000)).unwrap(),
            Stage::SourceRetired
        );
        assert!(amnesic.backend().bridge_observed().is_empty());
        assert!(amnesic.backend().bridge_released().is_empty());
        assert!(amnesic.backend().bridge_policies().is_empty());
        assert!(amnesic.backend().served().is_none());
        assert_eq!(completion(&hosts.committed_topology().unwrap()).2, quorum);
    }
    eprintln!(
        "REMOTE_BACKEND_COMPLETE {}",
        serde_json::json!({"case":format!("{case:?}"),"ms":started.elapsed().as_millis()})
    );
    for (member, (bridge, activation, _)) in &first_released {
        assert_eq!(
            &first_opened[member],
            &(bridge.clone(), activation.clone()),
            "{member} released the bridge and activation it opened"
        );
    }
    assert_eq!(hosts.commits, 5);
    let cleaned = assert_moved(
        &hosts,
        metadata,
        services,
        &*target_control,
        factory,
        &range,
        &relocation,
        &new,
        acknowledged,
    );
    let (phase, stage, quorum, cleanup) = completion(&cleaned);
    assert_eq!(phase, RelocationPhase::Completed);
    assert_eq!(stage, NativeJointHandoffStageV1::Cleaned);
    assert!(quorum.is_some() && cleanup.is_some());
    // The members are as released, and the target host performed exactly
    // its two commands.
    for (member, (_, _, generation)) in &first_released {
        let evidence = members!().evidence(member).unwrap();
        assert_eq!(evidence["release_confirmed"], true);
        assert_eq!(
            evidence["released_generation"],
            serde_json::json!(generation)
        );
    }
    let commands = members!().host_commands();
    assert_eq!(commands.len(), 2, "{commands:?}");
    // A completed handoff is not driven again, with or without memory.
    assert_eq!(
        completing(None).drive(&mut hosts, &budget(60_000)).unwrap(),
        Stage::SourceRetired
    );
    assert_eq!(
        coordinated(range.id, 0)
            .drive(&mut hosts, &budget(60_000))
            .unwrap(),
        Stage::SourceRetired
    );
    assert_eq!(hosts.commits, 5);
    assert_eq!(hosts.committed_topology().unwrap(), cleaned);
    assert_eq!(members!().host_commands(), commands);
}

/// The whole move from the committed learner allocation, by one coordinator:
/// either one `drive()` of one handoff object, or the production controller
/// stepping it. The original client's acceptance runs around it.
#[allow(clippy::too_many_arguments)]
fn whole_move(
    case: SourceCase,
    hosts: &mut CommitHosts,
    controls: &mut [NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target_control: &mut NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    range: &crate::RangeDescriptor,
    relocation: &crate::RangeRelocation,
    old: &[ClusterNodeId],
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    original_gateway: &crate::ClusterEventClient,
) {
    let source = relocation.source.clone().unwrap();
    assert_eq!(source, node(0), "the coordinator's replica retires");
    let mut new = old
        .iter()
        .filter(|id| **id != source)
        .cloned()
        .chain(std::iter::once(node(4)))
        .collect::<Vec<_>>();
    new.sort();
    let mut order = vec![node(4)];
    order.extend(new.iter().filter(|id| **id != node(4)).cloned());
    let pending = hosts.committed_topology().unwrap();
    assert!(!factory.database.read().commit_admission_ready());
    // The backend exactly as the production move builds it: the
    // coordinator's transport and this node's own data service.
    let transport = clients[0].clone();
    let service = services[0].clone();
    let build = move || {
        RemoteJoinedHandoffBackend::new(node(4), transport.clone())
            .with_local_service(service.clone())
    };
    let started = Instant::now();
    if matches!(case, SourceCase::RemoteBackendControllerWalk) {
        // The fifth-member controller fixture starts no member image-source
        // process, so the production controller is walked here, on the
        // fixture whose members can complete the remote route. The target's
        // host loop is not running: each pending host command ends a step
        // as a wait, and this thread then calls the loop's entry once.
        let wait = Duration::from_millis(1500);
        let mut controller = NativeRangeMoveController::new(
            node(0),
            range.id,
            node(4),
            &*hosts,
            Box::new(move || build().test_host_command_wait(wait)),
        )
        .unwrap()
        .with_backoff(Duration::ZERO, Duration::ZERO);
        let mut waits = Vec::new();
        let mut stages = Vec::new();
        super::hot_policy_tests::data_tests::exercise_range_with_serve(
            "RemoteBackendControllerWalk",
            None,
            range.id,
            target_control,
            controls,
            original_gateway,
            services,
            &pending,
            acknowledged,
            output,
            &mut |control| {
                for _ in 0..24 {
                    if controller.finished() {
                        break;
                    }
                    controller.tick(&mut *hosts, &budget(120_000));
                    let status = controller.status();
                    eprintln!("REMOTE_BACKEND_CONTROLLER status={status:?}");
                    assert_eq!(status.last_error, None, "{status:?}");
                    assert!(status.paused.is_none() && !status.aborted, "{status:?}");
                    match &status.waiting {
                        Some(waiting) => {
                            // A normal wait: no error, the stage unchanged.
                            assert_eq!(
                                (status.stage, status.next_stage, status.completed),
                                (Some("membership_final"), Some("serving"), false),
                                "{status:?}"
                            );
                            waits.push(waiting.clone());
                            perform_host_command(control);
                        }
                        None => stages.push(status.stage.expect("a completed stage")),
                    }
                }
            },
        );
        let status = controller.status();
        assert!(status.completed && !status.aborted, "{status:?}");
        assert_eq!(
            (
                status.stage,
                status.next_stage,
                status.last_error.clone(),
                status.waiting.clone(),
                status.paused.clone()
            ),
            (Some("source_retired"), None, None, None, None)
        );
        assert_eq!(
            stages,
            [
                "target_admitted",
                "learner_allocated",
                "image_verified",
                "decision_published",
                "certified",
                "promoted_frozen",
                "membership_final",
                "serving",
                "activation_recorded",
                "source_retired"
            ]
        );
        assert_eq!(
            waits,
            [
                format!(
                    "waiting at serving: waiting for the host of target {} to run its writer policy command",
                    node(4)
                ),
                format!(
                    "waiting at serving: waiting for the host of target {} to run its serve command",
                    node(4)
                ),
            ]
        );
        assert_eq!(status.attempts, 12);
        // A completed move does nothing on later steps.
        controller.tick(&mut *hosts, &budget(60_000));
        assert_eq!(controller.status(), status);
    } else {
        // The member whose decision publication is lost, when the case has one.
        let missing = match case {
            SourceCase::RemoteBackendDecisionMissingNewMember => {
                Some(new.iter().find(|id| **id != node(4)).unwrap().clone())
            }
            SourceCase::RemoteBackendDecisionMissingSource => Some(source.clone()),
            _ => None,
        };
        let fault = Arc::new(Mutex::new(None::<ClusterNodeId>));
        let faulty = || build().test_decision_publish_fault(fault.clone());
        let mut handoff = NativeJoinedDataHandoff::with_backend(range.id, node(4), faulty());
        let mut driven = None;
        super::hot_policy_tests::data_tests::exercise_range_with_serve(
            "RemoteBackendOneDrive",
            None,
            range.id,
            target_control,
            controls,
            original_gateway,
            services,
            &pending,
            acknowledged,
            output,
            &mut |control| {
                if let Some(missing) = &missing {
                    *fault.lock() = Some(missing.clone());
                    // The same coordinator object, then one without memory:
                    // neither certifies, both name the missing member, and
                    // the second computes the same fact set again (the
                    // member accepts its repeated preparation).
                    for attempt in 0..2 {
                        if attempt == 1 {
                            handoff =
                                NativeJoinedDataHandoff::with_backend(range.id, node(4), faulty());
                        }
                        let error = handoff
                            .drive_until(&mut *hosts, Some(Stage::Certified), &budget(300_000))
                            .unwrap_err()
                            .to_string();
                        eprintln!("REMOTE_BACKEND_DECISION_MISSING attempt={attempt} {error}");
                        assert!(
                            error.contains(
                                "the joint decision is not durable on every member that needs it"
                            ) && error.contains(&format!(
                                "missing: {missing} (publish: test fault: the request was not sent; observe decision: "
                            )) && error.contains("Nothing was certified"),
                            "{error}"
                        );
                        assert_eq!(hosts.commits, 0, "nothing is committed");
                        assert_eq!(
                            recorded(&hosts.committed_topology().unwrap(), range.id),
                            None
                        );
                        assert!(handoff.backend().decision_certificate().is_none());
                        assert!(!handoff.backend().decision_durable());
                    }
                    // The member can be reached again: the same stage
                    // publishes to it alone and certifies.
                    *fault.lock() = None;
                    assert_eq!(
                        handoff
                            .drive_until(&mut *hosts, Some(Stage::Certified), &budget(300_000))
                            .unwrap(),
                        Stage::Certified
                    );
                    assert_eq!(hosts.commits, 1);
                    let publishers = handoff
                        .backend()
                        .decision_certificate()
                        .unwrap()
                        .publishers();
                    for id in old.iter().chain(std::iter::once(&node(4))) {
                        assert!(publishers.contains(id), "{id}: {publishers:?}");
                    }
                }
                driven = Some(pumped(control, || {
                    handoff.drive(&mut *hosts, &budget(600_000))
                }));
            },
        );
        let (stage, performed) = driven.expect("the drive ran");
        assert_eq!(stage.unwrap(), Stage::SourceRetired);
        assert_eq!(
            performed, 2,
            "the target host performed its writer policy and its serving, once each"
        );
        let committed = hosts.committed_topology().unwrap();
        let decision_sha256 = committed.relocations[&relocation.id]
            .native_joint_handoff_v1
            .as_ref()
            .unwrap()
            .decision_sha256()
            .to_owned();
        let view = Members {
            controls: &*controls,
            target_control: &*target_control,
            range: range.id,
            new: &new,
            order: &order,
            decision_sha256: &decision_sha256,
        };
        let backend = handoff.backend();
        assert!(backend
            .bridge_observed()
            .iter()
            .all(|(_, answer)| answer.is_none()));
        let opened = view.opened(&handoff, false);
        assert_eq!(backend.bridge_retired(), &new[..]);
        let released = view.released(&handoff, &order);
        for (member, (bridge, activation, _)) in &released {
            assert_eq!(&opened[member], &(bridge.clone(), activation.clone()));
        }
        view.policies(&handoff, &released);
        let served = backend.served().expect("the target served");
        assert!(served.complete);
        assert_eq!(served.writer_policy_installed, Some(true));
        assert!(served
            .target_recovered_ranges
            .is_some_and(|ranges| ranges >= 1));
        assert!(backend.waiting().is_none());
        // A completed handoff is not driven again, with or without memory.
        assert_eq!(
            handoff.drive(&mut *hosts, &budget(60_000)).unwrap(),
            Stage::SourceRetired
        );
        assert_eq!(
            NativeJoinedDataHandoff::with_backend(
                range.id,
                node(4),
                RemoteJoinedHandoffBackend::new(node(4), clients[0].clone())
            )
            .drive(&mut *hosts, &budget(60_000))
            .unwrap(),
            Stage::SourceRetired
        );
    }
    eprintln!(
        "REMOTE_BACKEND_WHOLE_MOVE {}",
        serde_json::json!({"case":format!("{case:?}"),"ms":started.elapsed().as_millis()})
    );
    assert_eq!(hosts.commits, 5);
    assert_moved(
        hosts,
        metadata,
        services,
        &*target_control,
        factory,
        range,
        relocation,
        &new,
        acknowledged,
    );
    let commands = target_control
        .test_registered_bridge_host_commands()
        .unwrap();
    assert!(commands["pending"].is_null(), "{commands}");
    assert_eq!(commands["done"].as_array().unwrap().len(), 2, "{commands}");
    // The finished handoff no longer occupies a member's step table: what a
    // member's own committed metadata shows finished is released, and what
    // it shows unfinished (the metadata before the move) is kept.
    let cleaned = hosts.committed_topology().unwrap();
    for member in &new {
        let i = index_of(member);
        let (unfinished, finished) = if i == 4 {
            assert!(target_control
                .test_registered_bridge_publication(range.id)
                .unwrap()
                .is_some());
            (
                target_control
                    .test_registered_release_finished_bridge_steps(&pending)
                    .unwrap(),
                target_control
                    .test_registered_release_finished_bridge_steps(&cleaned)
                    .unwrap(),
            )
        } else {
            assert!(controls[i]
                .test_initial_bridge_publication(range.id)
                .unwrap()
                .is_some());
            (
                controls[i]
                    .test_initial_release_finished_bridge_steps(&pending)
                    .unwrap(),
                controls[i]
                    .test_initial_release_finished_bridge_steps(&cleaned)
                    .unwrap(),
            )
        };
        assert_eq!(
            unfinished,
            [range.id],
            "{member} released an unfinished handoff"
        );
        assert!(finished.is_empty(), "{member} kept a finished handoff");
    }
}
