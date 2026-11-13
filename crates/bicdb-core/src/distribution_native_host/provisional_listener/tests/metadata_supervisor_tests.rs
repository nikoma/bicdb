//! Genuine born-enforced stores, actual protected UUIDs and pinned TLS. Core
//! outages here stop/drain endpoints/workers, not SIGKILL/power-cut a process.
use super::*;
use crate::distribution_consensus::{MetadataConsensusRole, MetadataConsensusStore};
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use std::time::Instant;

fn budget() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
struct Network {
    workers: Vec<Option<NativeMetadataSupervisor>>,
    listeners: Vec<Option<ClusterDataServerHandle>>,
    launches: Vec<Option<NativeInitialConsensusLaunch>>,
    fixture: Fixture,
}
impl Network {
    fn new() -> Self {
        let mut fixture = Fixture::new();
        let c = fixture.hosts[0]
            .observe_initial_configuration(&budget())
            .unwrap();
        let permits = fixture
            .hosts
            .iter()
            .map(|host| {
                host.prepare_common_initial_configuration(&c, &budget())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let launches = fixture
            .hosts
            .iter_mut()
            .zip(&permits)
            .map(|(host, permit)| Some(host.finish_initial_consensus(permit, &budget()).unwrap()))
            .collect::<Vec<_>>();
        let listeners = launches
            .iter()
            .enumerate()
            .map(|(i, launch)| {
                Some(
                    launch
                        .as_ref()
                        .unwrap()
                        .start_metadata_listener(fixture.networks[i].clone(), &budget())
                        .unwrap(),
                )
            })
            .collect();
        Self {
            workers: (0..3).map(|_| None).collect(),
            listeners,
            launches,
            fixture,
        }
    }
    fn launch(&self, i: usize) -> &NativeInitialConsensusLaunch {
        self.launches[i].as_ref().unwrap()
    }
    fn start(&mut self, i: usize) {
        let launch = self.launch(i);
        self.workers[i] = Some(
            launch
                .start_automatic_metadata(
                    self.fixture.networks[i].clone(),
                    &CancellationToken::uncancelable(),
                )
                .unwrap(),
        );
    }
    fn stop(&mut self, i: usize) {
        if let Some(worker) = self.workers[i].as_mut() {
            worker.stop(&budget()).unwrap();
        }
        self.workers[i].take();
        if let Some(listener) = self.listeners[i].as_mut() {
            listener.stop_and_drain(&budget()).unwrap();
        }
        self.listeners[i].take();
    }
    fn restart(&mut self, i: usize) {
        self.listeners[i] = Some(
            self.launch(i)
                .start_metadata_listener(self.fixture.networks[i].clone(), &budget())
                .unwrap(),
        );
        self.start(i);
    }
    fn anchored_leader(&self, candidates: &[usize]) -> Option<usize> {
        candidates.iter().copied().find(|i| {
            let launch = self.launch(*i);
            let actual = launch.service.lock().listener_open_guard();
            let store = launch.metadata.lock();
            let snapshot = store.native_supervisor_snapshot(&actual, None).unwrap().1;
            snapshot.status.role == MetadataConsensusRole::Leader
                && snapshot.anchored
                && snapshot.voting_actor
        })
    }
    fn wait_leader(&self, candidates: &[usize]) -> usize {
        let until = Instant::now() + Duration::from_secs(12);
        loop {
            if let Some(leader) = self.anchored_leader(candidates) {
                return leader;
            }
            assert!(
                Instant::now() < until,
                "actual automatic TLS election never committed its current-term anchor: {:?}",
                candidates
                    .iter()
                    .map(|i| self.launch(*i).metadata.lock().status())
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn fresh_mq(&self, caller: usize, leader: usize) {
        let business = Uuid::now_v7();
        let observed = self
            .launch(caller)
            .transport
            .acquire_host_metadata_observation(
                &node(leader),
                business,
                Duration::from_secs(5),
                &budget(),
            )
            .unwrap();
        observed.require_live_request(business).unwrap();
        assert_eq!(observed.metadata_leader_id(), &node(leader));
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        for worker in self.workers.iter_mut().flatten() {
            let _ = worker.stop(&budget());
        }
        for listener in self.listeners.iter_mut().flatten() {
            let _ = listener.stop_and_drain(&budget());
        }
        if std::thread::panicking() {
            self.fixture._root.disable_cleanup(true);
            eprintln!(
                "NATIVE_METADATA_SUPERVISOR_PRIVATE {}",
                self.fixture._root.path().display()
            );
        }
    }
}

#[test]
fn native_owned_metadata_supervisor_closed_ingress_cannot_hold_leadership_same_worker_resumes() {
    let mut f = Network::new();
    // Elect n2 through actual votes first. No role/term/cache setter is used.
    f.start(2);
    assert_eq!(f.wait_leader(&[2]), 2);
    f.start(0);
    f.start(1);
    f.fresh_mq(0, 2);
    let original_metadata = f.launch(2).metadata.clone();
    let original_service = f.launch(2).service.clone();
    let original_process = f.workers[2].as_ref().unwrap().lifecycle_cancellation();
    let original_worker = f.workers[2].as_ref().unwrap() as *const _ as usize;

    // ONLY incoming ingress is stopped. The original actual metadata worker
    // and all database/service owners remain; outgoing RPC is still possible.
    f.listeners[2]
        .as_mut()
        .unwrap()
        .stop_and_drain(&budget())
        .unwrap();
    f.listeners[2].take();
    original_process.check().unwrap();
    let healthy = f.wait_leader(&[0, 1]);
    f.fresh_mq(0, healthy);
    f.fresh_mq(1, healthy);
    assert_eq!(
        f.launch(2).metadata.lock().status().role,
        MetadataConsensusRole::Follower
    );

    // Rebind SAME service/open and leave the original supervisor untouched.
    f.listeners[2] = Some(
        f.launch(2)
            .start_metadata_listener(f.fixture.networks[2].clone(), &budget())
            .unwrap(),
    );
    assert!(Arc::ptr_eq(&original_metadata, &f.launch(2).metadata));
    assert!(Arc::ptr_eq(&original_service, &f.launch(2).service));
    assert_eq!(
        original_worker,
        f.workers[2].as_ref().unwrap() as *const _ as usize
    );
    original_process.check().unwrap();
    f.fresh_mq(2, healthy);

    // Make ONLY that original worker responsible for the next election.
    // The remaining follower's actual endpoint still supplies its real vote.
    let other = if healthy == 0 { 1 } else { 0 };
    f.workers[other].as_mut().unwrap().stop(&budget()).unwrap();
    f.workers[other].take();
    f.stop(healthy);
    assert_eq!(f.wait_leader(&[2]), 2);
    original_process.check().unwrap();
    f.fresh_mq(other, 2);
}
#[test]
fn native_owned_metadata_supervisor_actual_tls_majority_minority_stationary_leader_and_heal() {
    let mut f = Network::new();
    // A real isolated candidate has no fabricated votes or anchor. Two actual
    // TLS endpoints are absent, so its own persisted vote cannot become MQ.
    f.stop(1);
    f.stop(2);
    f.start(0);
    std::thread::sleep(Duration::from_millis(2200));
    let isolated = f.launch(0).metadata.lock().status();
    assert_ne!(isolated.role, MetadataConsensusRole::Leader);
    assert_eq!(isolated.commit_index, 0);
    let business = Uuid::now_v7();
    assert!(f
        .launch(0)
        .transport
        .acquire_host_metadata_observation(
            &node(0),
            business,
            Duration::from_millis(250),
            &budget()
        )
        .is_err());
    f.restart(1);
    f.restart(2);
    let first = f.wait_leader(&[0, 1, 2]);
    f.fresh_mq((first + 1) % 3, first);
    let term = f.launch(first).metadata.lock().status().current_term;
    // Valid stationary Append/read-probe traffic must keep followers from
    // campaigning after an otherwise idle, healthy leader's election timeout.
    std::thread::sleep(Duration::from_millis(3300));
    assert_eq!(f.launch(first).metadata.lock().status().current_term, term);
    assert_eq!(f.anchored_leader(&[0, 1, 2]), Some(first));
    f.stop(first);
    let majority = (0..3).filter(|i| *i != first).collect::<Vec<_>>();
    let next = f.wait_leader(&majority);
    assert!(f.launch(next).metadata.lock().status().current_term > term);
    f.fresh_mq(majority.iter().copied().find(|i| *i != next).unwrap(), next);
    // The isolated old leader may still have an elected label and historical
    // anchor. Neither counts as a fresh quorum in the minority.
    {
        let launch = f.launch(first);
        let store = launch.metadata.lock();
        let participant = launch.transport.local_metadata_read_participant();
        let round = store
            .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &participant)
            .unwrap();
        assert!(store.finish_read_authority(round).is_err());
    }
    f.restart(first);
    let until = Instant::now() + Duration::from_secs(8);
    loop {
        let old = f.launch(first).metadata.lock().status();
        if old.current_term >= f.launch(next).metadata.lock().status().current_term
            && old.role == MetadataConsensusRole::Follower
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "healed stale leader never learned actual newer term"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    f.fresh_mq(first, next);
}
#[test]
fn native_owned_metadata_supervisor_actual_tls_invalid_and_stale_messages_do_not_renew_contact() {
    let mut f = Network::new();
    // Use the genuine finite election path, then stop all scheduling while
    // exercising actual TLS request validation deterministically.
    f.launch(0).elect_initial_metadata(&budget()).unwrap();
    let before = f.launch(1).metadata.lock().last_leader_contact_ms();
    let mut stale = f
        .launch(0)
        .metadata
        .lock()
        .append_request_from(1, 16)
        .unwrap();
    stale.term = 0;
    assert!(
        !f.launch(0)
            .transport
            .append_metadata_cancellable(&node(1), stale, &budget())
            .unwrap()
            .success
    );
    assert_eq!(f.launch(1).metadata.lock().last_leader_contact_ms(), before);
    // The REAL committed current-term barrier has compacted to a snapshot.
    // Corrupt a copy of that actual builder output, never the healthy FSM.
    let mut malformed_snapshot = f
        .launch(0)
        .metadata
        .lock()
        .append_request_from(1, 16)
        .unwrap();
    let snapshot = malformed_snapshot
        .snapshot
        .as_mut()
        .expect("committed barrier must supply its actual compacted snapshot");
    assert!(snapshot.last_included_index > 0);
    snapshot.topology.generation = 0;
    let snapshot_error = f
        .launch(0)
        .transport
        .append_metadata_cancellable(&node(1), malformed_snapshot, &budget())
        .unwrap_err();
    assert!(
        snapshot_error.to_string().contains("generation"),
        "{snapshot_error}"
    );
    assert_eq!(f.launch(1).metadata.lock().last_leader_contact_ms(), before);
    // Exercise the entry path with an actual persisted, not-yet-replicated
    // heartbeat proposal. This is legitimate current-leader CONFIG input; no
    // compaction setting, term, vote or accepted position is changed by hand.
    let entry = {
        let mut store = f.launch(0).metadata.lock();
        let mut topology = store.committed_topology().clone();
        topology.generation += 1;
        topology.nodes.get_mut(&node(0)).unwrap().last_heartbeat_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();
        let entry = store.propose_topology(topology).unwrap();
        assert!(store.status().commit_index < entry.index);
        entry
    };
    let mut malformed_entry = f
        .launch(0)
        .metadata
        .lock()
        .append_request_from(entry.index, 16)
        .unwrap();
    assert!(malformed_entry.snapshot.is_none());
    assert_eq!(malformed_entry.entries, vec![entry]);
    malformed_entry.entries[0].topology.generation = 0;
    let entry_error = f
        .launch(0)
        .transport
        .append_metadata_cancellable(&node(1), malformed_entry, &budget())
        .unwrap_err();
    assert!(
        entry_error.to_string().contains("generation"),
        "{entry_error}"
    );
    assert_eq!(f.launch(1).metadata.lock().last_leader_contact_ms(), before);
    f.launch(0).heartbeat_initial_metadata(&budget()).unwrap();
    assert!(f.launch(1).metadata.lock().last_leader_contact_ms() > before);
    let probe_before = [
        f.launch(1).metadata.lock().last_leader_contact_ms(),
        f.launch(2).metadata.lock().last_leader_contact_ms(),
    ];
    f.fresh_mq(0, 0);
    assert!(
        f.launch(1).metadata.lock().last_leader_contact_ms() > probe_before[0]
            || f.launch(2).metadata.lock().last_leader_contact_ms() > probe_before[1],
        "actual valid read-probe must renew a contacted follower's leader timer"
    );
    // Exactly one worker owns an actual store, including while stopping.
    f.start(0);
    assert!(f
        .launch(0)
        .start_automatic_metadata(f.fixture.networks[0].clone(), &budget())
        .is_err());
}
#[test]
fn native_owned_metadata_supervisor_strict_metadata_reopen_preserves_protected_uuid_and_durable_state(
) {
    let mut f = Network::new();
    f.launch(0).elect_initial_metadata(&budget()).unwrap();
    f.stop(1);
    let old = f.launches[1].take().unwrap();
    let NativeInitialConsensusLaunch {
        metadata,
        service,
        topology,
        seed,
        transport,
        provider,
    } = old;
    let actual = service.lock().listener_open_guard();
    let (id, snapshot) = metadata
        .lock()
        .native_supervisor_snapshot(&actual, None)
        .unwrap();
    let metadata_root = seed
        .root
        .join(crate::distribution_native_host::initial_preparation::INITIAL_METADATA_DIRECTORY);
    let state_path =
        metadata_root.join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    let before: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    drop(provider);
    drop(
        Arc::try_unwrap(metadata)
            .ok()
            .expect("all actual metadata owners must end before strict reopen")
            .into_inner(),
    );
    let reopened = MetadataConsensusStore::reopen_native_owned(
        &metadata_root,
        Arc::clone(&seed.database),
        node(1),
        snapshot.topology,
        &budget(),
    )
    .unwrap();
    let (after_id, after) = reopened
        .native_supervisor_snapshot(&actual, Some(id))
        .unwrap();
    assert_eq!(after_id, id);
    assert_eq!(after.status.current_term, snapshot.status.current_term);
    assert_eq!(after.status.voted_for, snapshot.status.voted_for);
    assert_eq!(after.status.commit_index, snapshot.status.commit_index);
    assert_eq!(after.status.last_log_index, snapshot.status.last_log_index);
    let after_bytes: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    let durable = |value: &serde_json::Value| {
        let mut state = value["state"].clone();
        state.as_object_mut().unwrap().remove("role");
        state.as_object_mut().unwrap().remove("leader_id");
        state
    };
    assert_eq!(durable(&before), durable(&after_bytes));
    let metadata = Arc::new(Mutex::new(reopened));
    let provider =
        Arc::new(MetadataReadProvider::new(Arc::clone(&metadata), transport.clone()).unwrap());
    f.launches[1] = Some(NativeInitialConsensusLaunch {
        metadata,
        service,
        topology,
        seed,
        transport,
        provider,
    });
    f.restart(1);
    f.start(0);
    f.start(2);
    let leader = f.wait_leader(&[0, 1, 2]);
    f.fresh_mq(1, leader);
    assert_eq!(
        f.launch(1)
            .metadata
            .lock()
            .native_supervisor_snapshot(&actual, Some(id))
            .unwrap()
            .0,
        id
    );
}
