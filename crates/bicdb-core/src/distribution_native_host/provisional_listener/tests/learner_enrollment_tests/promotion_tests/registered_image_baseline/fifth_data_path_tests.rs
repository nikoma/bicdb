//! Four-to-five DATA path for ONE range, with a genuine Registered fifth
//! member (no INITIAL ranges, learner-born store, promoted metadata voter) as
//! the image target and the new range leader, driven by the product driver
//! `NativeJoinedDataHandoff`.
//!
//! Scope, stated exactly: five actual native hosts with real mutual-TLS
//! metadata and DATA sockets in ONE process. Application writes, Strong reads,
//! metadata consensus, recovery roles and range replication use those sockets.
//! The driver sequences the existing in-process joint objects through the
//! host actions implemented for this fixture below. There is no cross-process
//! image stream and no automatic controller in these tests.
//!
//! Besides the positive path, the tests cover: an abort back to the original
//! voters after the learner allocation and before any source cut; a transfer
//! that fails after the source cut and is re-driven to completion, including
//! the replacement of the target's partial install; and a driver that returns
//! between stages and is run again.
use super::*;
use crate::distribution_native_host::joined_data_handoff::{
    NativeJoinedDataHandoff, NativeJoinedDataHandoffHosts, NativeJoinedDataHandoffStage as Stage,
    NativeJoinedDataMember, NativeJoinedDataPlanningView,
};
use crate::distribution_range_handoff::{NativeJointHandoffRuntime, NativeJointOldObservation};

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
    /// Directories of the ephemeral planning views; never a metadata store.
    planning_roots: Vec<tempfile::TempDir>,
    /// Fail the image transfer once, after this many copied steps.
    fail_transfer_after: Option<usize>,
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
        planning_roots: Vec::new(),
        fail_transfer_after: None,
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

/// The fixture's host actions. Each one is the member's existing host method;
/// the sequencing and the abort/resume decisions are the driver's.
impl NativeJoinedDataHandoffHosts for Five {
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
        commit_topology(self, candidate.clone());
        if let Ok((_, client)) = self.host.joined_data_handoff_parts() {
            client.install_topology(candidate)?;
        }
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
        let (controller, _) = self.controls[0].test_initial_data_routing_inputs()?;
        let snapshot = controller.bootstrap(parent)?;
        if &snapshot.topology != fresh.committed_topology(business)? {
            return Err(BicDbError::Cluster(
                "planning snapshot is not the fresh committed configuration".into(),
            ));
        }
        // A planning view only: it holds the fresh committed configuration and
        // is never a metadata store. Only the real consensus commit publishes.
        let root = tempfile::tempdir()?;
        let config = crate::provision_cluster_member_directory(
            root.path(),
            &snapshot,
            &node(0),
            snapshot.config_template.transport.clone(),
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
    fn member(&self, id: &ClusterNodeId) -> Result<NativeJoinedDataMember> {
        let i = index_of(id);
        // The fifth member's transport is the one its own DATA owner uses.
        let client = if i == 4 {
            self.host.joined_data_handoff_parts()?.1
        } else {
            self.clients[i].clone()
        };
        Ok(NativeJoinedDataMember {
            provider: Arc::new(MetadataReadProvider::new(
                self.metadata[i].clone(),
                client.clone(),
            )?),
            service: self.services[i].clone(),
            client,
        })
    }
    fn data_owners(&self) -> Result<Vec<ClusterNodeId>> {
        Ok((0..4).map(node).collect())
    }
    fn open_target(&mut self, parent: &CancellationToken) -> Result<()> {
        if self
            .host
            .joined_data_status()
            .is_some_and(|status| status.receiver_listening && status.writer_policy_installed)
        {
            return Ok(());
        }
        self.host.open_joined_data(parent)
    }
    fn target_recovery_runtime(
        &self,
    ) -> Result<Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>> {
        Ok(self.host.joined_data_handoff_parts()?.0)
    }
    fn refresh_target_policy(&mut self, parent: &CancellationToken) -> Result<()> {
        self.host.refresh_joined_data_policy(parent)
    }
    fn publish_target(&mut self, parent: &CancellationToken) -> Result<usize> {
        self.host.publish_joined_data(parent)
    }
    fn close_old(
        &self,
        id: &ClusterNodeId,
        runtime: Arc<NativeJointHandoffRuntime>,
        role: Arc<RecoveryRoleAdmission>,
        own: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJointOldObservation>> {
        self.controls[index_of(id)].close_initial_old_step(runtime, role, own, business, parent)
    }
    fn forget_old_closure(&self, id: &ClusterNodeId, range: crate::RangeId) -> Result<bool> {
        self.controls[index_of(id)].forget_initial_old_closure(range)
    }
    fn refresh_owner_policy(
        &mut self,
        id: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.controls[index_of(id)].refresh_initial_data_policy(parent)
    }
    fn recover_owner_leadership(
        &mut self,
        id: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<usize> {
        self.controls[index_of(id)].recover_initial_data_leadership(parent)
    }
    fn transfer_progress(&mut self, copied_steps: usize) -> Result<()> {
        if self.fail_transfer_after == Some(copied_steps) {
            self.fail_transfer_after = None;
            return Err(BicDbError::Cluster(
                "injected transfer failure after a copied image step".into(),
            ));
        }
        Ok(())
    }
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

fn gateway(five: &Five, topology: &ClusterTopology, seconds: u64) -> crate::ClusterEventClient {
    crate::ClusterEventClient::new(&five.configs[0], topology.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(seconds))
        .unwrap()
}

fn committed(five: &Five) -> ClusterTopology {
    five.metadata[0].lock().committed_topology().clone()
}

fn run(name: &str, body: impl FnOnce(&mut Five)) {
    let mut five = start_five(name);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&mut five)));
    let cleanup = stop_five(five);
    eprintln!("FIFTH_DATA_PATH cleanup errors: {cleanup:?}");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

struct Scenario {
    current: ClusterTopology,
    original: crate::RangeDescriptor,
    old_voters: Vec<usize>,
    old_leader: usize,
    acknowledged: Vec<(String, serde_json::Value)>,
    handoff: NativeJoinedDataHandoff,
    started: Instant,
}
impl Scenario {
    fn mark(&self, phase: &str) {
        eprintln!(
            "FIFTH_DATA_PATH phase={phase} t_ms={}",
            self.started.elapsed().as_millis()
        );
    }
}

/// Rows acknowledged by the four-node cluster before any change, and one
/// handoff of the first range onto the fifth member.
fn prepare(five: &mut Five) -> Scenario {
    let started = Instant::now();
    let current = committed(five);
    let original = current.ranges.values().next().unwrap().clone();
    let old_voters = original
        .replicas
        .iter()
        .filter(|r| r.role == RangeReplicaRole::Voter)
        .map(|r| index_of(&r.node_id))
        .collect::<Vec<_>>();
    assert_eq!(old_voters.len(), 3);
    assert!(original.replicas.iter().all(|r| r.node_id != node(4)));
    let client = gateway(five, &current, 5);
    let mut acknowledged = Vec::new();
    for n in 0..3 {
        let key = key_for(&current, original.id, &format!("before-{n}"));
        let value = serde_json::json!({"fifth_data_path":"acknowledged by four","n":n,"nested":[null,17,{"full":"payload"}]});
        assert!(client.append("items", &key, value.clone()).unwrap());
        assert_eq!(
            client.get_strong("items", &key).unwrap(),
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
    let scenario = Scenario {
        old_leader: index_of(&original.leader),
        handoff: NativeJoinedDataHandoff::new(original.id, node(4)).with_image_batch_records(1),
        current,
        original,
        old_voters,
        acknowledged,
        started,
    };
    scenario.mark("four-node rows acknowledged");
    scenario
}

/// The committed relocation of the scenario's range onto the fifth member.
fn relocation(five: &Five, scenario: &Scenario) -> RangeRelocation {
    committed(five)
        .relocations
        .values()
        .find(|plan| plan.range_id == scenario.original.id && plan.target == node(4))
        .unwrap()
        .clone()
}

/// The range is frozen or outside every installed policy: a write on the
/// committed configuration must not be acknowledged, and every acknowledged
/// row is still on every old voter.
fn assert_unwritable_and_intact(five: &Five, scenario: &Scenario, tag: &str) {
    let pending = committed(five);
    for (key, value) in &scenario.acknowledged {
        for &i in &scenario.old_voters {
            assert_eq!(
                stored(five, i, key).as_ref(),
                Some(value),
                "acknowledged row lost on an original owner during the handoff"
            );
        }
    }
    let key = key_for(&pending, scenario.original.id, tag);
    let value = serde_json::json!({"fifth_data_path":"must not be acknowledged","tag":tag});
    let outcome = gateway(five, &pending, 3).append("items", &key, value.clone());
    eprintln!(
        "FIFTH_DATA_PATH write during the handoff ({tag}): {:?}",
        outcome.as_ref().map_err(|error| error.to_string())
    );
    // Safety: an acknowledgement, if one were ever returned here, must be a
    // real durable row on a majority of the old owners.
    if matches!(outcome, Ok(true)) {
        let holders = scenario
            .old_voters
            .iter()
            .filter(|i| stored(five, **i, &key).as_ref() == Some(&value))
            .count();
        assert!(holders >= 2, "acknowledged write is not on an old majority");
    }
    assert!(
        outcome.is_err(),
        "a range inside an unfinished handoff acknowledged a write"
    );
}

/// Acceptance of the completed path: the fifth member is a voter and the only
/// leader of the range, serves Strong reads of every earlier row, acknowledges
/// new writes, and keeps doing so with one original member stopped.
fn accept(five: &mut Five, scenario: &mut Scenario) {
    let committed = committed(five);
    let range = scenario.original.id;
    let activation = committed.range_by_id(range).unwrap().clone();
    assert_eq!(activation.leader, node(4));
    assert!(activation
        .replicas
        .iter()
        .any(|r| r.node_id == node(4) && r.role == RangeReplicaRole::Voter));
    assert!(activation
        .replicas
        .iter()
        .all(|r| r.node_id != node(scenario.old_leader)));
    let survivors = activation
        .replicas
        .iter()
        .map(|r| index_of(&r.node_id))
        .filter(|i| *i != 4)
        .collect::<Vec<_>>();
    assert_eq!(survivors.len(), 2);
    assert!(five.host.joined_data_status().unwrap().dispatcher_published);
    let client = gateway(five, &committed, 5);
    // (a) Strong reads of the earlier rows are served by the fifth member:
    // it is the only leader of this range in the committed configuration.
    for (key, value) in &scenario.acknowledged {
        assert_eq!(strong_until_served(&client, key).as_ref(), Some(value));
        assert_eq!(stored(five, 4, key).as_ref(), Some(value));
    }
    scenario.mark("(a) Strong reads served by the fifth");
    // (b) A new write is acknowledged with the fifth member as a voter (and
    // leader) and is readable from it.
    let key = key_for(&committed, range, "after-change");
    let value = serde_json::json!({"fifth_data_path":"acknowledged with the fifth as voter"});
    append_until_acknowledged(&client, &key, &value);
    assert_eq!(strong_until_served(&client, &key).as_ref(), Some(&value));
    assert_eq!(stored(five, 4, &key).as_ref(), Some(&value));
    for &i in &survivors {
        assert_eq!(stored(five, i, &key).as_ref(), Some(&value));
    }
    assert_eq!(
        stored(five, scenario.old_leader, &key),
        None,
        "the retired source must not receive post-change rows"
    );
    scenario.acknowledged.push((key, value));
    scenario.mark("(b) post-change write acknowledged");
    // (c) Stop one ORIGINAL member that is a voter of the new configuration.
    // The remaining DATA majority is the fifth member plus one original.
    let stopped = *survivors.iter().find(|i| **i != 0).unwrap();
    let remaining = *survivors.iter().find(|i| **i != stopped).unwrap();
    five.controls[stopped].stop(&budget(10_000)).unwrap();
    let key = key_for(&committed, range, "after-one-original-stopped");
    let value = serde_json::json!({"fifth_data_path":"acknowledged with one original stopped"});
    append_until_acknowledged(&client, &key, &value);
    assert_eq!(strong_until_served(&client, &key).as_ref(), Some(&value));
    assert_eq!(stored(five, 4, &key).as_ref(), Some(&value));
    assert_eq!(stored(five, remaining, &key).as_ref(), Some(&value));
    assert_eq!(
        stored(five, stopped, &key),
        None,
        "the stopped original cannot have received this row"
    );
    scenario.acknowledged.push((key, value));
    for (key, value) in &scenario.acknowledged {
        assert_eq!(strong_until_served(&client, key).as_ref(), Some(value));
    }
    eprintln!(
        "FIFTH_DATA_PATH_ACCEPTANCE range={} old_leader={} new_leader={} survivors={survivors:?} stopped_original={} rows={}",
        range,
        node(scenario.old_leader),
        activation.leader,
        node(stopped),
        scenario.acknowledged.len()
    );
}

#[test]
fn native_fifth_data_path_receives_image_becomes_voter_and_serves_after_one_original_stops() {
    run("fifth-data-path-target", |five| {
        let parent = CancellationToken::uncancelable();
        let mut scenario = prepare(five);
        // Step 1: the joining member holds an installed host writer policy and
        // a DATA socket BEFORE any range configuration is changed.
        assert_eq!(
            scenario
                .handoff
                .drive_until(&mut *five, Some(Stage::TargetAdmitted), &parent)
                .unwrap(),
            Stage::TargetAdmitted
        );
        let status = five.host.joined_data_status().unwrap();
        assert!(status.receiver_listening && status.writer_policy_installed);
        assert!(!status.dispatcher_published);
        assert_eq!(committed(five), scenario.current);
        scenario.mark("fifth joined DATA owner open");
        // Steps 2 to 4: allocation, source cut, transfer and verification.
        assert_eq!(
            scenario
                .handoff
                .drive_until(&mut *five, Some(Stage::ImageVerified), &parent)
                .unwrap(),
            Stage::ImageVerified
        );
        for (key, value) in &scenario.acknowledged {
            assert_eq!(
                stored(five, 4, key).as_ref(),
                Some(value),
                "verified image row missing on the fifth member"
            );
        }
        let pending = relocation(five, &scenario);
        assert_eq!(pending.phase, RelocationPhase::LearnerAllocated);
        assert!(pending.native_joint_handoff_v1.is_none());
        scenario.mark("image verified on the fifth");
        // Steps 5 to 9.
        assert_eq!(
            scenario.handoff.drive(&mut *five, &parent).unwrap(),
            Stage::Serving
        );
        scenario.mark("fifth leadership recovered and ingress published");
        accept(five, &mut scenario);
    });
}

#[test]
fn native_fifth_data_path_unavailable_target_leaves_range_writable_on_original_four() {
    run("fifth-data-path-unavailable", |five| {
        let parent = CancellationToken::uncancelable();
        let mut scenario = prepare(five);
        five.host.stop_owned_metadata(&budget(10_000)).unwrap();
        let refused = scenario.handoff.drive(&mut *five, &parent).unwrap_err();
        eprintln!("FIFTH_DATA_PATH unavailable target refused before any change: {refused}");
        assert!(!scenario.handoff.cut_attempted());
        // No learner allocation was proposed, so the range is untouched.
        assert_eq!(
            committed(five),
            scenario.current,
            "a refused join must not change the committed configuration"
        );
        let client = gateway(five, &scenario.current, 5);
        let key = key_for(
            &scenario.current,
            scenario.original.id,
            "after-refused-join",
        );
        let value = serde_json::json!({"fifth_data_path":"still writable on four"});
        assert!(client.append("items", &key, value.clone()).unwrap());
        assert_eq!(client.get_strong("items", &key).unwrap(), Some(value));
        for (key, value) in &scenario.acknowledged {
            assert_eq!(
                client.get_strong("items", key).unwrap(),
                Some(value.clone())
            );
        }
    });
}

/// The target fails after the learner allocation committed and before any
/// source cut. The abort returns the range to its original voters: a new
/// write is acknowledged by them and is Strong-readable.
#[test]
fn native_fifth_data_path_target_stopped_after_allocation_aborts_to_original_voters() {
    run("fifth-data-path-abort", |five| {
        let parent = CancellationToken::uncancelable();
        let mut scenario = prepare(five);
        assert_eq!(
            scenario
                .handoff
                .drive_until(&mut *five, Some(Stage::LearnerAllocated), &parent)
                .unwrap(),
            Stage::LearnerAllocated
        );
        let pending = committed(five);
        let range = pending.range_by_id(scenario.original.id).unwrap().clone();
        assert_eq!(range.epoch, scenario.original.epoch + 1);
        assert!(range
            .replicas
            .iter()
            .any(|r| r.node_id == node(4) && r.role == RangeReplicaRole::Learner));
        assert_eq!(
            relocation(five, &scenario).phase,
            RelocationPhase::LearnerAllocated
        );
        scenario.mark("learner allocation committed");
        // The gap this closes: the committed allocation alone leaves the
        // range outside every original owner's installed policy.
        assert_unwritable_and_intact(five, &scenario, "after-allocation");
        five.host.stop_owned_metadata(&budget(10_000)).unwrap();
        scenario.mark("target stopped");
        assert!(!scenario.handoff.cut_attempted());
        let recovered = scenario
            .handoff
            .abort_before_cut(&mut *five, &parent)
            .unwrap();
        assert!(recovered >= 1, "the original leader must recover its range");
        scenario.mark("aborted to the original voters");
        // The abort changes no committed configuration.
        assert_eq!(committed(five), pending);
        let client = gateway(five, &pending, 5);
        for n in 0..2 {
            let key = key_for(&pending, range.id, &format!("after-abort-{n}"));
            let value = serde_json::json!({"fifth_data_path":"acknowledged after abort","n":n});
            append_until_acknowledged(&client, &key, &value);
            assert_eq!(strong_until_served(&client, &key).as_ref(), Some(&value));
            let holders = scenario
                .old_voters
                .iter()
                .filter(|i| stored(five, **i, &key).as_ref() == Some(&value))
                .count();
            assert!(holders >= 2, "abort write is not on an original majority");
            assert_eq!(
                stored(five, 4, &key),
                None,
                "the stopped fifth member cannot hold a post-abort row"
            );
            scenario.acknowledged.push((key, value));
        }
        for (key, value) in &scenario.acknowledged {
            assert_eq!(strong_until_served(&client, key).as_ref(), Some(value));
        }
        scenario.mark("writes acknowledged by the original voters");
    });
}

/// The transfer fails after the source cut and the first copied step. The
/// old voters hold a permanent promise, so there is no abort: the same
/// handoff is driven again, the target's partial install is replaced by a new
/// attempt, and the path completes with the acceptance of the positive test.
#[test]
fn native_fifth_data_path_transfer_failure_after_source_cut_resumes_to_completion() {
    run("fifth-data-path-resume", |five| {
        let parent = CancellationToken::uncancelable();
        let mut scenario = prepare(five);
        five.fail_transfer_after = Some(1);
        let failed = scenario.handoff.drive(&mut *five, &parent).unwrap_err();
        eprintln!("FIFTH_DATA_PATH transfer failed after source cut: {failed}");
        assert!(failed.to_string().contains("injected transfer failure"));
        assert!(scenario.handoff.cut_attempted());
        let pending = relocation(five, &scenario);
        assert_eq!(pending.phase, RelocationPhase::LearnerAllocated);
        assert!(pending.native_joint_handoff_v1.is_none());
        scenario.mark("transfer failed after the source cut");
        // Safety of the interrupted state.
        assert_unwritable_and_intact(five, &scenario, "after-failed-transfer");
        // A permanent promise exists: the abort is refused, not attempted.
        let refused = scenario
            .handoff
            .abort_before_cut(&mut *five, &parent)
            .unwrap_err();
        assert!(refused.to_string().contains("can only be resumed"));
        assert_unwritable_and_intact(five, &scenario, "after-refused-abort");
        // Resume: the same call again.
        assert_eq!(
            scenario.handoff.drive(&mut *five, &parent).unwrap(),
            Stage::Serving
        );
        scenario.mark("resumed to completion");
        accept(five, &mut scenario);
    });
}

/// The driver returns between stages (after the joint decision, after the
/// certification and after the union membership) and is run again each time.
/// Repeating the call after completion changes nothing.
#[test]
fn native_fifth_data_path_interrupted_driver_reruns_to_completion() {
    run("fifth-data-path-rerun", |five| {
        let parent = CancellationToken::uncancelable();
        let mut scenario = prepare(five);
        for stage in [
            Stage::DecisionPublished,
            Stage::Certified,
            Stage::PromotedFrozen,
        ] {
            assert_eq!(
                scenario
                    .handoff
                    .drive_until(&mut *five, Some(stage), &parent)
                    .unwrap(),
                stage
            );
            let pending = relocation(five, &scenario);
            let recorded = pending
                .native_joint_handoff_v1
                .as_ref()
                .map(|record| record.stage());
            match stage {
                Stage::DecisionPublished => {
                    assert_eq!(pending.phase, RelocationPhase::LearnerAllocated);
                    assert_eq!(recorded, None);
                }
                Stage::Certified => assert_eq!(
                    recorded,
                    Some(crate::distribution::NativeJointHandoffStageV1::Certified)
                ),
                _ => assert_eq!(
                    recorded,
                    Some(crate::distribution::NativeJointHandoffStageV1::PromotedFrozen)
                ),
            }
            assert!(!scenario.handoff.completed());
            scenario.mark(&format!("driver returned after {stage:?}"));
            assert_unwritable_and_intact(five, &scenario, &format!("after-{stage:?}"));
        }
        assert_eq!(
            scenario.handoff.drive(&mut *five, &parent).unwrap(),
            Stage::Serving
        );
        assert!(scenario.handoff.completed());
        scenario.mark("driver completed after three interruptions");
        // Repeating the call after completion is a no-op.
        let after = committed(five);
        assert_eq!(
            scenario.handoff.drive(&mut *five, &parent).unwrap(),
            Stage::Serving
        );
        assert_eq!(committed(five), after);
        assert!(five.host.joined_data_status().unwrap().dispatcher_published);
        accept(five, &mut scenario);
    });
}
