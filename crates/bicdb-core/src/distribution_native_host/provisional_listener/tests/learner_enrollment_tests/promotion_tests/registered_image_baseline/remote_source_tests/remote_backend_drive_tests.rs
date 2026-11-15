//! The remote joined-handoff backend drives one handoff from the committed
//! learner allocation through the final membership commit. The backend is
//! given the coordinator's transport and node ids only: every member is
//! reached through the product requests of that one transport. In-process
//! handles are read here only to compare outcomes, and the test hosts commit
//! through the real metadata consensus of the metadata leader.
use super::*;
use crate::distribution::{NativeJointHandoffStageV1, RangeReplicaRole, RelocationPhase};
use crate::distribution_native_host::joined_data_handoff::{
    JoinedHandoffBackend, JoinedHandoffMetadataHosts, NativeJoinedDataHandoff,
    NativeJoinedDataHandoffStage as Stage, NativeJoinedDataPlanningView,
    RemoteJoinedHandoffBackend,
};

pub(in super::super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::RemoteBackendFullDrive
            | SourceCase::RemoteBackendRedrive
            | SourceCase::RemoteBackendNotRangeLeader
    )
}
#[test]
fn native_remote_backend_drives_handoff_to_final_membership_over_coordinator_transport() {
    run_registered_image_baseline_with_placement_and_source(
        Case::Required,
        AssignmentMode::CoordinatorLeadsRange,
        Some(SourceCase::RemoteBackendFullDrive),
    );
}
#[test]
fn native_remote_backend_fresh_backend_after_certified_commit_redrives_to_final_membership() {
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
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    target_control: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    template: &crate::DistributionConfig,
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
        // A coordinator that loses its memory in this window cannot collect
        // the certificate again; it refuses and commits nothing.
        let frozen = hosts.committed_topology().unwrap();
        let mut amnesic = coordinated(range.id, 0);
        let error = amnesic
            .drive_until(&mut hosts, Some(Stage::MembershipFinal), &budget(60_000))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("does not retain its decision certificate")
                && error.contains("cannot be collected again"),
            "{error}"
        );
        assert_eq!(hosts.commits, 2);
        assert_eq!(hosts.committed_topology().unwrap(), frozen);
        drop(amnesic);
        // The coordinator that kept its certificate finishes.
        assert_eq!(
            handoff
                .drive_until(&mut hosts, Some(Stage::MembershipFinal), &budget(60_000))
                .unwrap(),
            Stage::MembershipFinal
        );
        assert_eq!(hosts.commits, 3);
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
    // The bridge phases are not this backend's yet: it names the next one.
    let error = handoff
        .drive_until(&mut hosts, None, &budget(60_000))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("remote joined handoff backend: activation and release is not implemented"),
        "{error}"
    );
    assert_eq!(hosts.committed_topology().unwrap(), committed);
}
