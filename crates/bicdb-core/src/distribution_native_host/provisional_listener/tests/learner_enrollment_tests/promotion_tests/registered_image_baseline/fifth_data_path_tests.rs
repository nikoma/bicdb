//! Positive four-to-five DATA path for ONE range, with a genuine Registered
//! fifth member (no INITIAL ranges, learner-born store, promoted metadata
//! voter) as the image target and the new range leader.
//!
//! Scope, stated exactly: five actual native hosts with real mutual-TLS
//! metadata and DATA sockets in ONE process. Application writes, Strong reads,
//! metadata consensus, recovery roles and range replication use those sockets.
//! The handoff itself is orchestrated here through the existing in-process
//! joint objects (source closure, local image capture, target install, joint
//! decision, staged metadata membership, Open and release). There is no
//! cross-process image stream and no automatic controller in this test.
use super::*;
use crate::distribution_range_consensus::{ImageInstallPurpose, NativeImageInstallRole};
use crate::distribution_range_handoff::{
    NativeJointDurableDecision, NativeJointHandoffPlan, NativeJointHandoffRuntime,
    NativeJointNewObservation, NativeJointNewRound, NativeJointOldRound,
    NativeJointPreparedServingObservation, NativeJointPreparedServingRound,
};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze, NativeRangeImageInstaller,
};
use crate::RangeSnapshotOptions;

type Transport = Arc<TcpClusterRelocationTransport>;

struct Five {
    f: EnrollmentFixture,
    host: crate::distribution_native_host::NativeUnassignedLearnerHost,
    controls: Vec<NativeMetadataControlHost>,
    databases: Vec<Arc<RwLock<BicDb>>>,
    metadata: Vec<Arc<Mutex<crate::MetadataConsensusStore>>>,
    services: Vec<Arc<Mutex<crate::ClusterDataNodeService>>>,
    clients: Vec<Transport>,
    configs: Vec<crate::DistributionConfig>,
}

fn index_of(id: &ClusterNodeId) -> usize {
    (0..5).find(|i| node(*i) == *id).unwrap()
}

fn key_for(topology: &ClusterTopology, range: crate::RangeId, tag: &str) -> String {
    (0..100_000)
        .map(|i| format!("fifth-data-path-{tag}-{i}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .id
                == range
        })
        .unwrap()
}

/// Four INITIAL owners with open DATA, and a fifth member that registered,
/// caught up, was promoted to metadata voter and opened its replica receiver.
fn start_five(name: &str) -> Five {
    // Same per-request network budget as the three-member joint fixture: a
    // joint phase revalidates every final member inside one metadata window.
    let mut f = EnrollmentFixture::with_spares_and_io_timeout(1, 4000);
    let (factory, mut host) = f.factory(name);
    assert!(factory.spec.initial_ranges.is_empty());
    let token = CancellationToken::uncancelable();
    let template = config(&f);
    start_owned(&f, &mut host);
    let readiness = ready(&f, &token);
    let (post, business) = f.current();
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&readiness, &post, business, &template, 71, &token)
        .unwrap()
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(entry.index, 16)
        .unwrap();
    let ack = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(ack.success && ack.match_index >= entry.index);
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &ack)
        .unwrap();
    let own_business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), own_business, Duration::from_secs(5), &token)
        .unwrap();
    let promoted = own.committed_topology(own_business).unwrap().clone();
    assert_eq!(
        promoted.nodes[&node(4)].metadata_role,
        MetadataMemberRole::Voter
    );
    let own_config = configuration(&template, &promoted, &node(4), &f.networks[4]);
    host.enable_metadata_control(&own, own_business, own_config, &token)
        .unwrap();
    host.open_registered_replica_receiver(&own, own_business, &token)
        .unwrap();
    let receiver = host.replica_receiver_status(&token).unwrap().unwrap();
    assert!(receiver.receiver_ready && !receiver.data_serving);
    assert!(!factory.database.read().commit_admission_ready());
    let databases = f
        .seeds
        .iter()
        .map(|seed| seed.database.clone())
        .chain(std::iter::once(factory.database.clone()))
        .collect::<Vec<_>>();
    let metadata = f
        .launches
        .iter()
        .map(|launch| launch.metadata.clone())
        .chain(std::iter::once(host.test_owned_metadata().unwrap()))
        .collect::<Vec<_>>();
    let services = f
        .launches
        .iter()
        .map(|launch| launch.service.clone())
        .chain(std::iter::once(host.test_service()))
        .collect::<Vec<_>>();
    let clients = f
        .launches
        .iter()
        .map(|launch| launch.transport.clone())
        .collect::<Vec<_>>();
    let configs = (0..4)
        .map(|i| configuration(&template, &promoted, &node(i), &f.networks[i]))
        .collect::<Vec<_>>();
    let launches = std::mem::take(&mut f.launches);
    let listeners = std::mem::take(&mut f.listeners);
    let mut controls = launches
        .into_iter()
        .zip(listeners)
        .enumerate()
        .map(|(i, (launch, listener))| {
            NativeMetadataControlHost::new(
                launch,
                listener,
                f.networks[i].clone(),
                configs[i].clone(),
            )
        })
        .collect::<Vec<_>>();
    for control in &mut controls {
        control.advance(&budget(5000)).unwrap();
    }
    let until = Instant::now() + Duration::from_secs(30);
    let mut opened = [false; 4];
    while !opened.iter().all(|ok| *ok) {
        for (i, control) in controls.iter_mut().enumerate() {
            if !opened[i] {
                opened[i] = control.open_initial_data(&budget(1500)).is_ok();
            }
        }
        assert!(
            Instant::now() < until,
            "four actual INITIAL DATA receivers not ready: {opened:?}"
        );
    }
    Five {
        f,
        host,
        controls,
        databases,
        metadata,
        services,
        clients,
        configs,
    }
}

fn stop_five(mut five: Five) -> Vec<String> {
    let mut errors = Vec::new();
    for control in &mut five.controls {
        if let Err(error) = control.stop(&budget(10_000)) {
            errors.push(error.to_string());
        }
    }
    if let Err(error) = five.host.stop_owned_metadata(&budget(10_000)) {
        errors.push(error.to_string());
    }
    if let Err(error) = five.host.shutdown() {
        errors.push(error.to_string());
    }
    five.f.shutdown();
    errors
}

/// Propose one committed metadata configuration through the actual metadata
/// leader and wait until every one of the five owned stores has applied it.
fn commit_topology(five: &Five, candidate: ClusterTopology) {
    let entry = five.metadata[0]
        .lock()
        .propose_topology(candidate.clone())
        .unwrap();
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        five.controls[0].heartbeat(&budget(5000)).unwrap();
        let applied = five.metadata.iter().all(|actual| {
            let store = actual.lock();
            store.status().commit_index >= entry.index && store.committed_topology() == &candidate
        });
        if applied {
            break;
        }
        assert!(
            Instant::now() < until,
            "committed configuration did not reach all five metadata stores"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    for client in &five.clients {
        client.install_topology(candidate.clone()).unwrap();
    }
}

/// One committed learner allocation of the fifth member on `original`,
/// retiring the current range leader.
fn assign_fifth(current: &ClusterTopology, original: &crate::RangeDescriptor) -> ClusterTopology {
    let mut proposed = current.clone();
    assert!(proposed.nodes[&node(4)].native_store_birth_v1.is_some());
    assert!(proposed.ranges.values().all(|range| range
        .replicas
        .iter()
        .all(|replica| replica.node_id != node(4))));
    let id = RelocationId::new(proposed.next_relocation_id).unwrap();
    proposed.next_relocation_id += 1;
    let target_replica = ReplicaId::new(proposed.next_replica_id).unwrap();
    proposed.next_replica_id += 1;
    let range = proposed
        .ranges
        .values_mut()
        .find(|r| r.id == original.id)
        .unwrap();
    assert_eq!(&*range, original);
    range.epoch += 1;
    range.replicas.push(RangeReplica {
        id: target_replica,
        node_id: node(4),
        role: RangeReplicaRole::Learner,
    });
    proposed.relocations.insert(
        id,
        RangeRelocation {
            id,
            plan_id: format!("fifth-data-path-{}", original.id),
            handoff_session: Some(RangeHandoffSession {
                session_id: Uuid::now_v7(),
                nonce: Uuid::now_v7(),
            }),
            native_joint_handoff_v1: None,
            range_id: original.id,
            expected_epoch: original.epoch,
            learner_epoch: original.epoch + 1,
            promoted_epoch: None,
            source: Some(original.leader.clone()),
            target: node(4),
            target_replica_id: target_replica,
            reason: ReplicaMoveReason::LoadBalance,
            phase: RelocationPhase::LearnerAllocated,
            resume_phase: None,
            snapshot_id: None,
            snapshot_sha256: None,
            snapshot_bytes_copied: 0,
            snapshot_resume_after_key: None,
            snapshot_commit_sequence: None,
            destination_durable_commit_sequence: None,
            source_commit_sequence: None,
            cleanup_resume_after_key: None,
            cleanup_records_deleted: 0,
            created_at_ms: 100,
            updated_at_ms: 100,
            last_error: None,
        },
    );
    proposed.generation += 1;
    proposed.validate().unwrap();
    proposed
}

/// Actual per-member handles for the joint orchestration. Index 4 is the
/// fifth member; its transport is the one its own DATA owner uses.
struct Members {
    clients: Vec<Transport>,
    providers: Vec<MetadataReadProvider>,
    services: Vec<Arc<Mutex<crate::ClusterDataNodeService>>>,
    actor: crate::distribution_consensus::MetadataReadParticipant,
}
impl Members {
    fn role(&self, plan: &NativeJointHandoffPlan, recipient: usize) -> RecoveryRoleAdmission {
        let promise = plan.promise();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let purpose =
            crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id: promise.relocation_id,
                plan_id: promise.plan_id.clone(),
                session_id: promise.session_id,
                nonce: promise.nonce,
            };
        let token = budget(5000);
        let observed = self.providers[recipient]
            .witness_recovery_role(
                &self.actor,
                business,
                plan.old_range().id,
                plan.old_range().epoch,
                promise.nonce,
                challenge,
                purpose.clone(),
                &token,
            )
            .unwrap();
        RecoveryRoleAdmission::from_witness(
            observed,
            &self.actor,
            &node(recipient),
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            &purpose,
        )
        .unwrap()
    }
}

/// Ship the frozen source image and its complete history into the fifth
/// member's Installing state, verify it there and finalize its catalogue.
fn install_on_fifth(
    members: &Members,
    runtime: &Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    plan: &NativeJointHandoffPlan,
    mut capture: RangeImageCapture,
    fail_after_first_copy_step: bool,
) -> std::result::Result<(NativeImageInstallFreeze, NativeImageCatalogueFinalization), String> {
    let target = 4usize;
    let parent = CancellationToken::uncancelable();
    let initial = members.role(plan, target);
    let scope = RecoveryFenceScope::from_role(&initial).unwrap();
    runtime
        .fence_epoch(
            &members.actor,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    let handoff = members.role(plan, target).into_handoff().unwrap();
    let image_role = NativeImageInstallRole::CommittedHandoff(&handoff);
    let attempt = Uuid::now_v7();
    let source = capture.authorize_target_install().unwrap();
    let lease = runtime
        .transfer_image_install(
            &scope,
            &source,
            &image_role,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &parent,
        )
        .unwrap();
    let mut install: NativeRangeImageInstaller = members.services[target]
        .lock()
        .begin_native_range_image_install(
            &source,
            &image_role,
            lease,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &parent,
        )
        .unwrap();
    drop(source);
    loop {
        let cursor = capture.history_cursor().clone();
        let step = capture.export_history_step(&cursor, 256, &parent).unwrap();
        let handoff = members.role(plan, target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        let stage_id = install.history_stage_id();
        let progress = members.services[target]
            .lock()
            .stage_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                stage_id,
                &step,
                &parent,
            )
            .unwrap();
        if progress.cursor.complete() {
            break;
        }
    }
    loop {
        let handoff = members.role(plan, target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        if members.services[target]
            .lock()
            .audit_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                256,
                &parent,
            )
            .unwrap()
            .audit_complete
        {
            break;
        }
    }
    loop {
        let handoff = members.role(plan, target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        if members.services[target]
            .lock()
            .clear_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                &parent,
            )
            .unwrap()
        {
            break;
        }
    }
    let options = RangeSnapshotOptions {
        max_records_per_batch: 1,
        max_bytes_per_batch: 1024 * 1024,
        max_record_bytes: 1024 * 1024,
    };
    let mut copied = 0usize;
    loop {
        let cursor = capture.cursor().clone();
        let step = capture.export_step(&cursor, &options, &parent).unwrap();
        let handoff = members.role(plan, target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        members.services[target]
            .lock()
            .copy_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                attempt,
                &step,
                &parent,
            )
            .unwrap();
        copied += 1;
        if fail_after_first_copy_step && copied == 1 {
            return Err("injected transfer failure after the first copied image step".into());
        }
        if step.complete {
            break;
        }
    }
    let completed = capture.complete().unwrap();
    let handoff = members.role(plan, target).into_handoff().unwrap();
    let mut verification = members.services[target]
        .lock()
        .begin_native_range_image_verification(
            install,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            completed,
            &parent,
        )
        .unwrap();
    let mut complete = false;
    for _ in 0..128 {
        let handoff = members.role(plan, target).into_handoff().unwrap();
        if members.services[target]
            .lock()
            .advance_native_range_image_verification(
                &mut verification,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &parent,
            )
            .unwrap()
            == crate::db::NativeTargetImageVerificationProgress::Complete
        {
            complete = true;
            break;
        }
    }
    assert!(complete, "actual bounded target verification must finish");
    let handoff = members.role(plan, target).into_handoff().unwrap();
    let verified = members.services[target]
        .lock()
        .finish_native_range_image_verification(
            verification,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &parent,
        )
        .unwrap();
    let handoff = members.role(plan, target).into_handoff().unwrap();
    let adopted = members.services[target]
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &parent,
        )
        .unwrap();
    let handoff = members.role(plan, target).into_handoff().unwrap();
    Ok(members.services[target]
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &parent,
        )
        .unwrap())
}

/// One staged metadata membership step, built from the durable dual-majority
/// decision and a fresh host metadata observation, then committed by the
/// actual metadata consensus.
fn commit_joint_stage(five: &Five, decision: &NativeJointDurableDecision, stage: usize) {
    let token = budget(5000);
    let business = Uuid::now_v7();
    let fresh = five.clients[0]
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    let (controller, _) = five.controls[0].test_initial_data_routing_inputs().unwrap();
    let snapshot = controller.bootstrap(&token).unwrap();
    assert_eq!(
        &snapshot.topology,
        fresh.committed_topology(business).unwrap()
    );
    // A planning view only: it holds the fresh committed configuration and is
    // never a metadata store. Only the real consensus commit publishes it.
    let root = tempfile::tempdir().unwrap();
    let config = crate::provision_cluster_member_directory(
        root.path(),
        &snapshot,
        &node(0),
        snapshot.config_template.transport.clone(),
        true,
    )
    .unwrap();
    let mut view = crate::DistributionStore::open(root.path(), config, true)
        .unwrap()
        .fork_ephemeral();
    match stage {
        0 => {
            assert!(view
                .certify_native_joint_handoff(decision, &fresh, business, &node(0), 110)
                .unwrap());
        }
        1 => {
            view.promote_native_joint_handoff_frozen(decision, &fresh, business, &node(0), 111)
                .unwrap();
        }
        2 => {
            view.finalize_native_joint_membership_frozen(decision, &fresh, business, &node(0), 112)
                .unwrap();
        }
        _ => panic!("unknown joint stage"),
    }
    commit_topology(five, view.topology().clone());
}

fn retry_timed_out<T>(what: &str, mut attempt: impl FnMut() -> Result<T>) -> T {
    for round in 0..4 {
        match attempt() {
            Ok(value) => return value,
            Err(BicDbError::QueryTimedOut) if round < 3 => {
                eprintln!("FIFTH_DATA_PATH bounded retry of {what} after a timed-out phase");
            }
            Err(error) => panic!("{what} failed: {error}"),
        }
    }
    unreachable!()
}

fn append_until_acknowledged(
    gateway: &crate::ClusterEventClient,
    key: &str,
    value: &serde_json::Value,
) {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        match gateway.append("items", key, value.clone()) {
            Ok(_) => return,
            Err(error) => {
                assert!(
                    Instant::now() < until,
                    "write {key} was never acknowledged: {error}"
                );
                eprintln!("FIFTH_DATA_PATH write {key} retried after: {error}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn strong_until_served(
    gateway: &crate::ClusterEventClient,
    key: &str,
) -> Option<serde_json::Value> {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        match gateway.get_strong("items", key) {
            Ok(value) => return value,
            Err(error) => {
                assert!(
                    Instant::now() < until,
                    "Strong read of {key} was never served: {error}"
                );
                eprintln!("FIFTH_DATA_PATH Strong read {key} retried after: {error}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn stored(five: &Five, member: usize, key: &str) -> Option<serde_json::Value> {
    let id = crate::cluster_event_record_id("items", key).unwrap();
    five.databases[member]
        .read()
        .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
        .unwrap()
        .map(|record| record.metadata.clone())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    /// The joining member is unavailable before any range is touched.
    TargetUnavailable,
    /// The image transfer fails after the source cut and the first copied
    /// step, while the range is frozen on its owners.
    TransferAfterSourceCut,
}

#[test]
fn native_fifth_data_path_receives_image_becomes_voter_and_serves_after_one_original_stops() {
    run_fifth_data_path(Fault::None);
}

#[test]
fn native_fifth_data_path_unavailable_target_leaves_range_writable_on_original_four() {
    run_fifth_data_path(Fault::TargetUnavailable);
}

#[test]
fn native_fifth_data_path_transfer_failure_after_source_cut_characterization() {
    run_fifth_data_path(Fault::TransferAfterSourceCut);
}

fn run_fifth_data_path(fault: Fault) {
    let mut five = start_five("fifth-data-path-target");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exercise(&mut five, fault);
    }));
    let cleanup = stop_five(five);
    eprintln!("FIFTH_DATA_PATH cleanup errors: {cleanup:?}");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn exercise(five: &mut Five, fault: Fault) {
    let parent = CancellationToken::uncancelable();
    let started = Instant::now();
    let mark = |phase: &str| {
        eprintln!(
            "FIFTH_DATA_PATH phase={phase} t_ms={}",
            started.elapsed().as_millis()
        )
    };
    let current = five.metadata[0].lock().committed_topology().clone();
    let original = current.ranges.values().next().unwrap().clone();
    assert_eq!(
        original
            .replicas
            .iter()
            .filter(|r| r.role == RangeReplicaRole::Voter)
            .count(),
        3
    );
    assert!(original.replicas.iter().all(|r| r.node_id != node(4)));

    // Rows acknowledged by the four-node cluster, before any change.
    let gateway = crate::ClusterEventClient::new(&five.configs[0], current.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(5))
        .unwrap();
    let mut acknowledged = Vec::new();
    for n in 0..3 {
        let key = key_for(&current, original.id, &format!("before-{n}"));
        let value = serde_json::json!({"fifth_data_path":"acknowledged by four","n":n,"nested":[null,17,{"full":"payload"}]});
        assert!(gateway.append("items", &key, value.clone()).unwrap());
        assert_eq!(
            gateway.get_strong("items", &key).unwrap(),
            Some(value.clone())
        );
        acknowledged.push((key, value));
    }
    for (key, _) in &acknowledged {
        assert_eq!(
            stored(five, 4, key),
            None,
            "the fifth member holds no rows before the change"
        );
    }

    // Step 1: the joining member must hold an installed host writer policy
    // and a DATA socket BEFORE any range configuration is changed.
    if fault == Fault::TargetUnavailable {
        five.host.stop_owned_metadata(&budget(10_000)).unwrap();
        let refused = five.host.open_joined_data(&budget(10_000)).unwrap_err();
        eprintln!("FIFTH_DATA_PATH unavailable target refused before any change: {refused}");
        // No learner allocation was proposed, so the range is untouched.
        assert_eq!(
            five.metadata[0].lock().committed_topology(),
            &current,
            "a refused join must not change the committed configuration"
        );
        let key = key_for(&current, original.id, "after-refused-join");
        let value = serde_json::json!({"fifth_data_path":"still writable on four"});
        assert!(gateway.append("items", &key, value.clone()).unwrap());
        assert_eq!(gateway.get_strong("items", &key).unwrap(), Some(value));
        for (key, value) in &acknowledged {
            assert_eq!(
                gateway.get_strong("items", key).unwrap(),
                Some(value.clone())
            );
        }
        return;
    }
    mark("four-node rows acknowledged");
    five.host.open_joined_data(&budget(10_000)).unwrap();
    let status = five.host.joined_data_status().unwrap();
    assert!(status.receiver_listening && status.writer_policy_installed);
    assert!(!status.dispatcher_published);
    let (_, _, fifth_runtime, fifth_client) = five.host.test_joined_data_bindings().unwrap();

    mark("fifth joined DATA owner open");
    // Step 2: commit the learner allocation of the fifth member.
    let pending = assign_fifth(&current, &original);
    commit_topology(five, pending.clone());
    let range = pending.range_by_id(original.id).unwrap().clone();
    let relocation = pending
        .relocations
        .values()
        .find(|plan| plan.range_id == range.id)
        .unwrap()
        .clone();
    let leader_index = index_of(&range.leader);
    let mut clients = five.clients.clone();
    clients.push(fifth_client);
    clients[4].install_topology(pending.clone()).unwrap();
    let members = Members {
        providers: (0..5)
            .map(|i| {
                MetadataReadProvider::new(five.metadata[i].clone(), clients[i].clone()).unwrap()
            })
            .collect(),
        actor: clients[leader_index].local_metadata_read_participant(),
        services: five.services.clone(),
        clients,
    };

    mark("learner allocation committed");
    // Step 3: freeze the range on its current owners (source cut).
    let round_parent = budget(5000);
    let business = Uuid::now_v7();
    let observation = members.clients[leader_index]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            range.id,
            range.epoch,
            Duration::from_secs(5),
            &round_parent,
        )
        .unwrap();
    mark("source round metadata observed");
    let mut round = NativeJointOldRound::begin(observation, business, relocation.id).unwrap();
    let plan = round.plan();
    assert_eq!(plan.activation_range().leader, node(4));
    let old_indices = plan.old_members().keys().map(index_of).collect::<Vec<_>>();
    let new_indices = plan.new_members().keys().map(index_of).collect::<Vec<_>>();
    assert!(new_indices.contains(&4) && !new_indices.contains(&leader_index));
    let survivors = new_indices
        .iter()
        .copied()
        .filter(|i| *i != 4)
        .collect::<Vec<_>>();
    assert_eq!(survivors.len(), 2);
    let runtimes = old_indices
        .iter()
        .map(|i| {
            (
                *i,
                Arc::new(NativeJointHandoffRuntime::new(members.services[*i].clone()).unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for &i in &old_indices {
        let operation = budget(5000);
        let constraints = runtimes[&i]
            .observe_constraints(plan.clone(), members.role(&plan, i), &operation)
            .unwrap();
        round
            .record_constraints(constraints, &members.services[i])
            .unwrap();
    }
    mark("source constraints observed");
    for &i in &old_indices {
        let operation = budget(5000);
        let role = members.role(&plan, i);
        let own_business = Uuid::now_v7();
        let own = members.clients[i]
            .acquire_host_metadata_observation(
                &node(0),
                own_business,
                Duration::from_secs(5),
                &operation,
            )
            .unwrap();
        let closed = five.controls[i]
            .close_initial_old_step(
                runtimes[&i].clone(),
                Arc::new(role),
                own,
                own_business,
                &operation,
            )
            .unwrap();
        round.record_retained_closed_actual(closed).unwrap();
    }
    mark("source owners closed");
    let mut closed = round.finish().unwrap();
    assert!(closed.range_index() >= 1);
    let capture = closed
        .begin_capture(Duration::from_secs(2), &parent)
        .unwrap();
    let closed = Arc::new(closed);

    mark("source cut and capture started");
    // Step 4: ship image + history into the fifth member and verify there.
    let installed = install_on_fifth(
        &members,
        &fifth_runtime,
        &plan,
        capture,
        fault == Fault::TransferAfterSourceCut,
    );
    if fault == Fault::TransferAfterSourceCut {
        let error = installed.err().expect("injected transfer failure");
        eprintln!("FIFTH_DATA_PATH transfer failed after source cut: {error}");
        // Characterization, not a success claim. The source cut records a
        // permanent promise on the old owners; there is no abort that returns
        // the range to its old configuration. Dropping every owner releases
        // the in-memory freezes; what the old configuration then accepts is
        // observed and printed, and acknowledged rows must be intact.
        drop(closed);
        drop(runtimes);
        for &i in &old_indices {
            five.controls[i]
                .forget_initial_old_closure(range.id)
                .unwrap();
        }
        for (key, value) in &acknowledged {
            for &i in &old_indices {
                assert_eq!(
                    stored(five, i, key).as_ref(),
                    Some(value),
                    "acknowledged row lost on an original owner after a failed transfer"
                );
            }
        }
        let key = key_for(&current, original.id, "after-failed-transfer");
        let value = serde_json::json!({"fifth_data_path":"after failed transfer"});
        let gateway = crate::ClusterEventClient::new(&five.configs[0], pending.clone())
            .unwrap()
            .with_request_timeout(Duration::from_secs(3))
            .unwrap();
        let outcome = gateway.append("items", &key, value.clone());
        eprintln!(
            "FIFTH_DATA_PATH_CHARACTERIZATION write on the old configuration after a post-cut transfer failure: {:?}",
            outcome.as_ref().map_err(|error| error.to_string())
        );
        // Safety only: an acknowledgement, if one is ever returned here, must
        // be a real durable row on a majority of the old owners. Whether the
        // write is accepted is reported, not asserted, by this test.
        if matches!(outcome, Ok(true)) {
            let holders = old_indices
                .iter()
                .filter(|i| stored(five, **i, &key).as_ref() == Some(&value))
                .count();
            assert!(holders >= 2, "acknowledged write is not on an old majority");
        }
        return;
    }
    mark("image transfer finished");
    let (frozen, image) = installed.unwrap();
    assert_eq!(image.base().range_index(), closed.range_index());
    assert_eq!(
        image.base().logical_history_sha256(),
        closed.logical_history_sha256()
    );
    for (key, value) in &acknowledged {
        assert_eq!(
            stored(five, 4, key).as_ref(),
            Some(value),
            "verified image row missing on the fifth member"
        );
    }

    mark("image verified on the fifth");
    // Step 5: joint decision over the old and the new DATA majority, then the
    // staged metadata membership change (learner -> voter, source retired).
    let handoff = members.role(&plan, 4).into_handoff().unwrap();
    let target = members.services[4]
        .lock()
        .observe_native_joint_new_target(&closed, handoff, image, frozen, &parent)
        .unwrap();
    let mut new = NativeJointNewRound::begin(closed.clone()).unwrap();
    for &i in &survivors {
        new.record_actual_survivor(&node(i), members.role(&plan, i))
            .unwrap();
    }
    new.record(
        NativeJointNewObservation::from_finalized_actual_target(
            target,
            members.services[4].clone(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut publication = new
        .finish()
        .unwrap()
        .prepare_decision()
        .unwrap()
        .publication_round();
    for &i in old_indices.iter().chain(std::iter::once(&4)) {
        publication
            .publish_actual_member(members.role(&plan, i), &parent)
            .unwrap();
    }
    mark("joint decision published");
    let decision = publication.finish().unwrap();
    for stage in 0..3 {
        commit_joint_stage(five, &decision, stage);
    }
    let committed = five.metadata[0].lock().committed_topology().clone();
    let activation = committed.range_by_id(range.id).unwrap().clone();
    assert_eq!(&activation, plan.activation_range());
    assert_eq!(activation.leader, node(4));
    assert!(activation
        .replicas
        .iter()
        .any(|r| r.node_id == node(4) && r.role == RangeReplicaRole::Voter));
    assert!(activation
        .replicas
        .iter()
        .all(|r| r.node_id != node(leader_index)));
    members.clients[4]
        .install_topology(committed.clone())
        .unwrap();

    mark("membership stages committed");
    // Step 6: every owner installs the host policy of the new configuration.
    for &i in &old_indices {
        five.controls[i]
            .forget_initial_old_closure(range.id)
            .unwrap();
    }
    for control in &mut five.controls {
        control
            .refresh_initial_data_policy(&budget(10_000))
            .unwrap();
    }
    five.host
        .refresh_joined_data_policy(&budget(10_000))
        .unwrap();

    mark("new-configuration policies installed");
    // Step 7: joint serving preparation, publication, Open and release
    // (unfreeze) on the complete new member set, including the fifth.
    let mut prepared_round: Option<NativeJointPreparedServingRound> = None;
    for (local, (service, lease)) in decision.into_new_serving_owners().unwrap() {
        let business = Uuid::now_v7();
        let mq = members.clients[index_of(&local)]
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
            .unwrap();
        let actual = {
            let actual = service.lock();
            let committed = actual
                .observe_committed_native_joint_decision(mq, business, range.id)
                .unwrap();
            actual
                .prepare_local_native_joint_serving(committed, lease, &parent)
                .unwrap()
        };
        let observation =
            NativeJointPreparedServingObservation::from_actual_service(actual, service).unwrap();
        if let Some(round) = &mut prepared_round {
            round.record(observation).unwrap();
        } else {
            prepared_round =
                Some(NativeJointPreparedServingRound::begin(observation, &parent).unwrap());
        }
    }
    mark("serving prepared");
    let prepared = Arc::new(prepared_round.unwrap().finish().unwrap());
    let leader = node(0);
    let mut publication = prepared.resume_native_publication().unwrap();
    for &local in &new_indices {
        retry_timed_out("joint serving publication", || {
            let mut fresh = prepared.begin_fresh_preparation_phase(Duration::from_secs(4))?;
            for &i in &new_indices {
                fresh.acquire_actual_member(&members.clients[i], &leader)?;
            }
            let fresh = Arc::new(fresh.finish()?);
            let admission = fresh.acquire_local_activation(&members.clients[local], &leader)?;
            publication.publish_actual_local_member(&admission, &parent)
        });
    }
    let current_phase = retry_timed_out("joint serving publication aggregate", || {
        let mut fresh = prepared.begin_fresh_preparation_phase(Duration::from_secs(4))?;
        for &i in &new_indices {
            fresh.acquire_actual_member(&members.clients[i], &leader)?;
        }
        Ok(Arc::new(fresh.finish()?))
    });
    let published = Arc::new(publication.finish_with_fresh_phase(current_phase).unwrap());
    mark("serving published");
    let mut activations = BTreeMap::new();
    let mut image_owner = None;
    let mut open_order = vec![4usize];
    open_order.extend(survivors.iter().copied());
    for &local in &open_order {
        let receipt = retry_timed_out("joint Open", || {
            let mut round = published.begin_fresh_open_phase(Duration::from_secs(4))?;
            for &i in &new_indices {
                round.acquire_actual_member(&members.clients[i], &leader)?;
            }
            let round = Arc::new(round.finish()?);
            let open = round.acquire_local_open(&members.clients[local], &leader)?;
            let receipt = Arc::new(open.persist_local_catalogue_activation(&parent)?);
            if local == 4 && image_owner.is_none() {
                image_owner = Some(open.retain_image_release_owner(receipt.clone())?);
            }
            Ok(receipt)
        });
        activations.insert(node(local), receipt);
    }
    mark("joint Open persisted");
    let continuation = Arc::new(
        published
            .retain_release_continuation(
                activations,
                survivors
                    .iter()
                    .map(|i| (node(*i), runtimes[i].clone()))
                    .collect(),
                image_owner.unwrap(),
            )
            .unwrap(),
    );
    // Real freeze owners must drain before release.
    drop(published);
    drop(prepared);
    drop(closed);
    for &local in &open_order {
        retry_timed_out("joint release", || {
            let mut round = continuation.begin_fresh_release_phase(Duration::from_secs(4))?;
            for &i in &new_indices {
                round.acquire_actual_member(&members.clients[i], &leader)?;
            }
            Arc::new(round.finish()?)
                .acquire_local_release(&members.clients[local], &leader)?
                .release_actual_local()
        });
    }

    mark("released");
    // Step 8: the fifth member recovers leadership of its range and opens
    // application and Strong ingress.
    let recovered = five.host.publish_joined_data(&budget(20_000)).unwrap();
    assert!(recovered >= 1, "the fifth member must lead its new range");
    assert!(five.host.joined_data_status().unwrap().dispatcher_published);

    mark("fifth leadership recovered and ingress published");
    let gateway = crate::ClusterEventClient::new(&five.configs[0], committed.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(5))
        .unwrap();
    // (a) Strong reads of the pre-change rows are served by the fifth member:
    // it is the only leader of this range in the committed configuration.
    for (key, value) in &acknowledged {
        assert_eq!(strong_until_served(&gateway, key).as_ref(), Some(value));
        assert_eq!(stored(five, 4, key).as_ref(), Some(value));
    }
    mark("(a) Strong reads served by the fifth");
    // (b) A new write is acknowledged with the fifth member as a voter (and
    // leader) and is readable from it.
    let key = key_for(&committed, range.id, "after-change");
    let value = serde_json::json!({"fifth_data_path":"acknowledged with the fifth as voter"});
    append_until_acknowledged(&gateway, &key, &value);
    assert_eq!(strong_until_served(&gateway, &key).as_ref(), Some(&value));
    assert_eq!(stored(five, 4, &key).as_ref(), Some(&value));
    for &i in &survivors {
        assert_eq!(stored(five, i, &key).as_ref(), Some(&value));
    }
    assert_eq!(
        stored(five, leader_index, &key),
        None,
        "the retired source must not receive post-change rows"
    );
    acknowledged.push((key, value));

    mark("(b) post-change write acknowledged");
    // (c) Stop one ORIGINAL member that is a voter of the new configuration.
    // The remaining DATA majority is the fifth member plus one original.
    let stopped = *survivors.iter().find(|i| **i != 0).unwrap();
    let remaining = *survivors.iter().find(|i| **i != stopped).unwrap();
    five.controls[stopped].stop(&budget(10_000)).unwrap();
    let key = key_for(&committed, range.id, "after-one-original-stopped");
    let value = serde_json::json!({"fifth_data_path":"acknowledged with one original stopped"});
    append_until_acknowledged(&gateway, &key, &value);
    assert_eq!(strong_until_served(&gateway, &key).as_ref(), Some(&value));
    assert_eq!(stored(five, 4, &key).as_ref(), Some(&value));
    assert_eq!(stored(five, remaining, &key).as_ref(), Some(&value));
    assert_eq!(
        stored(five, stopped, &key),
        None,
        "the stopped original cannot have received this row"
    );
    acknowledged.push((key, value));
    for (key, value) in &acknowledged {
        assert_eq!(strong_until_served(&gateway, key).as_ref(), Some(value));
    }
    eprintln!(
        "FIFTH_DATA_PATH_ACCEPTANCE range={} old_leader={} new_leader={} survivors={survivors:?} stopped_original={} rows={}",
        range.id,
        node(leader_index),
        activation.leader,
        node(stopped),
        acknowledged.len()
    );
}
