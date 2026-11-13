//! One actual-owned metadata scheduler. All RPCs are actual pinned TLS, outside
//! service/DB/metadata latches. Its diagnostic phase is never a read/write lease.
use super::*;
use crate::distribution_consensus::{
    MetadataConsensusRole, MetadataConsensusStore, NativeMetadataSupervisorSnapshot,
};
use crate::distribution_data::ClusterServiceOpenGuard;
use crate::{ClusterDataNodeService, ClusterNetworkTransportConfig, TcpClusterRelocationTransport};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::{OnceLock, Weak};
use std::thread::{self, JoinHandle};

static ACTIVE: OnceLock<Mutex<BTreeMap<usize, Weak<()>>>> = OnceLock::new();
fn claim_worker(metadata: &Arc<Mutex<MetadataConsensusStore>>) -> Result<Arc<()>> {
    let mut active = ACTIVE
        .get_or_init(Mutex::default)
        .try_lock()
        .ok_or_else(|| invalid("native metadata supervisor ownership registry busy"))?;
    active.retain(|_, claim| claim.strong_count() > 0);
    let key = Arc::as_ptr(metadata) as usize;
    if active.get(&key).and_then(Weak::upgrade).is_some() {
        return Err(invalid(
            "actual metadata store already has a live or stopping supervisor",
        ));
    }
    let claim = Arc::new(());
    active.insert(key, Arc::downgrade(&claim));
    Ok(claim)
}
use std::time::Instant;

#[derive(Clone, Copy)]
pub(crate) struct NativeMetadataSupervisorConfig {
    pub(crate) heartbeat: Duration,
    pub(crate) election: Duration,
    pub(crate) round: Duration,
}
impl Default for NativeMetadataSupervisorConfig {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_millis(200),
            election: Duration::from_millis(1200),
            round: Duration::from_millis(400),
        }
    }
}
impl NativeMetadataSupervisorConfig {
    fn validate(self) -> Result<()> {
        if self.heartbeat < Duration::from_millis(20)
            || self.heartbeat > Duration::from_secs(5)
            || self.round < self.heartbeat
            || self.election < self.round.saturating_mul(3)
            || self.election > Duration::from_secs(120)
        {
            return Err(invalid("invalid bounded native metadata supervisor timing"));
        }
        Ok(())
    }
}

pub(crate) struct NativeMetadataSupervisor {
    cancellation: CancellationToken,
    worker: Option<JoinHandle<()>>,
}
struct Driver {
    _claim: Arc<()>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    actual: ClusterServiceOpenGuard,
    store_id: Uuid,
    transport: Arc<TcpClusterRelocationTransport>,
    config: NativeMetadataSupervisorConfig,
    contact: u64,
    observed_term: u64,
    election_at: Instant,
    peer_cursor: usize,
}
impl NativeMetadataSupervisor {
    /// Scheduling lifetime of this ACTUAL owned supervisor, never MQ/data authority.
    pub(in crate::distribution_native_host) fn lifecycle_cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    pub(crate) fn start(
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        topology: Arc<RwLock<ClusterTopology>>,
        network: ClusterNetworkTransportConfig,
        config: NativeMetadataSupervisorConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        config.validate()?;
        network.validate()?;
        if network.dev_localhost_plaintext
            || !network
                .tls
                .as_ref()
                .is_some_and(|tls| tls.require_client_cert)
        {
            return Err(invalid(
                "native metadata supervisor requires pinned mutual TLS",
            ));
        }
        // The listener is already serving: one in-flight handler may hold the
        // service or the metadata store for a moment. Starting the owned
        // worker waits a bounded time for that instead of refusing the whole
        // start on a single lost race; an owner that stays busy still refuses.
        let busy_until = Instant::now() + Duration::from_secs(2);
        let actual = loop {
            parent.check()?;
            if let Some(service) = service.try_lock_for(Duration::from_millis(5)) {
                break service.listener_open_guard();
            }
            if Instant::now() >= busy_until {
                return Err(invalid("native metadata supervisor service busy"));
            }
        };
        let (id, snapshot) = loop {
            parent.check()?;
            if let Some(store) = metadata.try_lock_for(Duration::from_millis(5)) {
                break store.native_supervisor_snapshot(&actual, None)?;
            }
            if Instant::now() >= busy_until {
                return Err(invalid("native metadata supervisor store busy"));
            }
        };
        let transport = Arc::new(TcpClusterRelocationTransport::new(
            snapshot.status.cluster_id.clone(),
            snapshot.status.node_id.clone(),
            topology,
            network,
        )?);
        let claim = claim_worker(&metadata)?;
        let mut driver = Driver {
            _claim: claim,
            metadata,
            actual,
            store_id: id,
            transport,
            config,
            contact: snapshot.contact,
            observed_term: snapshot.status.current_term,
            election_at: Instant::now() + election_delay(&snapshot, config),
            peer_cursor: 0,
        };
        let cancellation = parent.child();
        let active = cancellation.clone();
        let worker = thread::Builder::new()
            .name("bicdb-native-metadata".into())
            .spawn(move || {
                let mut last_error = None;
                while active.check().is_ok() && driver.actual.require_live().is_ok() {
                    let start = Instant::now();
                    let round = active.child_with_deadline(start + config.round);
                    // Failure is not a successful phase or a fresh authority. A
                    // later bounded retry always rechecks the actual protected
                    // owner and current persisted role/C, never cached status.
                    match driver.tick(&round) {
                        Err(error) if active.check().is_ok() => {
                            let error = error.to_string();
                            if last_error.as_ref() != Some(&error) {
                                eprintln!("bicdb native metadata supervisor: {error}");
                            }
                            last_error = Some(error);
                        }
                        Ok(()) => last_error = None,
                        Err(_) => {}
                    }
                    while active.check().is_ok() && start.elapsed() < config.heartbeat {
                        thread::sleep(Duration::from_millis(5));
                    }
                }
            })
            .map_err(|error| {
                invalid(&format!("native metadata supervisor spawn failed: {error}"))
            })?;
        Ok(Self {
            cancellation,
            worker: Some(worker),
        })
    }
    /// Cancellation does not surrender an in-flight worker's actual owners.
    /// Keep this stopping handle on timeout; only join after real exit.
    pub(crate) fn stop(&mut self, parent: &CancellationToken) -> Result<()> {
        self.cancellation.cancel();
        while self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            parent.check()?;
            thread::sleep(Duration::from_millis(5));
        }
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| invalid("native metadata supervisor panicked"))?;
        }
        Ok(())
    }
}
impl Drop for NativeMetadataSupervisor {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
fn election_delay(
    snapshot: &NativeMetadataSupervisorSnapshot,
    config: NativeMetadataSupervisorConfig,
) -> Duration {
    // Stable per-actor/per-term jitter avoids synchronized startup candidates;
    // monotonic local time, rather than wall-clock subtraction, measures silence.
    let digest = Sha256::digest(
        format!(
            "{}:{}",
            snapshot.status.node_id, snapshot.status.current_term
        )
        .as_bytes(),
    );
    let hash = u64::from_be_bytes(digest[..8].try_into().expect("fixed SHA-256 prefix"));
    config.election + Duration::from_millis(hash % (config.election.as_millis() as u64 / 2).max(1))
}
impl Driver {
    fn snapshot(&self) -> Result<NativeMetadataSupervisorSnapshot> {
        Ok(self
            .metadata
            .try_lock()
            .ok_or_else(|| invalid("native metadata tick store busy"))?
            .native_supervisor_snapshot(&self.actual, Some(self.store_id))?
            .1)
    }
    fn tick(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        let snapshot = self.snapshot()?;
        // Republish a successfully durable backend C even after restart or a
        // previous cache-publication failure. This is configuration DATA only.
        self.transport.install_topology(snapshot.topology.clone())?;
        if !self.actual.has_live_metadata_ingress(&self.metadata)? {
            // Outgoing votes/heartbeats can still succeed after our incoming
            // socket closes. They must not keep an unreachable node elected.
            // Do not cancel this process: SAME-owner rebind resumes this same
            // worker without replacing or reviving any lifecycle token.
            self.election_at = Instant::now() + election_delay(&snapshot, self.config);
            let mut store = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("native metadata ingress relinquish store busy"))?;
            store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
            store.relinquish_leadership()?;
            return Err(invalid(
                "native metadata scheduler paused: actual owned ingress unavailable",
            ));
        }
        if snapshot.contact != self.contact || snapshot.status.current_term != self.observed_term {
            self.contact = snapshot.contact;
            self.observed_term = snapshot.status.current_term;
            self.election_at = Instant::now() + election_delay(&snapshot, self.config);
        }
        if !snapshot.voting_actor {
            return Ok(());
        }
        if snapshot.status.role == MetadataConsensusRole::Leader {
            let _phase = if snapshot.anchored {
                "leader_anchor_committed"
            } else {
                "leader_awaiting_anchor"
            };
            // Awaiting-anchor is still a leader: keep replicating the original
            // current-term barrier instead of starting another term each tick.
            return self.replicate(&snapshot, parent);
        }
        if !snapshot.status.voters.contains(&snapshot.status.node_id)
            || Instant::now() < self.election_at
        {
            return Ok(());
        }
        let request = {
            let mut store = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("native election store busy"))?;
            store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
            store.start_election_if_unchanged(&snapshot.status, snapshot.contact)?
        };
        let Some(request) = request else {
            return Ok(());
        };
        self.election_at = Instant::now() + election_delay(&snapshot, self.config);
        let mut votes = BTreeSet::from([request.candidate_id.clone()]);
        let peers = snapshot
            .election_voters
            .iter()
            .filter(|id| **id != request.candidate_id)
            .cloned()
            .collect::<Vec<_>>();
        for (position, peer) in peers.iter().enumerate() {
            parent.check()?;
            let token = rpc_budget(parent, peers.len() - position);
            let reply = match self.transport.request_metadata_vote_cancellable(
                peer,
                request.clone(),
                &token,
            ) {
                Ok(reply) => reply,
                Err(_) => {
                    parent.check()?;
                    continue;
                }
            };
            let mut store = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("native election response store busy"))?;
            store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
            if reply.term > request.term {
                store.observe_remote_term(reply.term)?;
                return Ok(());
            }
            if store.status().current_term != request.term
                || store.status().role != MetadataConsensusRole::Candidate
            {
                return Ok(());
            }
            if reply.vote_granted && reply.term == request.term {
                votes.insert(reply.voter_id);
            }
        }
        parent.check()?;
        {
            let mut store = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("native election finish store busy"))?;
            store.finish_native_supervisor_election(
                &self.actual,
                self.store_id,
                &request,
                &votes,
            )?;
        }
        let elected = self.snapshot()?;
        self.replicate(&elected, parent)
    }
    fn replicate(
        &mut self,
        snapshot: &NativeMetadataSupervisorSnapshot,
        parent: &CancellationToken,
    ) -> Result<()> {
        let mut peers = snapshot
            .topology
            .nodes
            .values()
            .filter(|member| {
                member.id != snapshot.status.node_id
                    && member.lifecycle == ClusterNodeLifecycle::Active
                    && member.native_store_birth_v1.is_some()
            })
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        if !peers.is_empty() {
            let rotate = self.peer_cursor % peers.len();
            peers.rotate_left(rotate);
            self.peer_cursor = self.peer_cursor.wrapping_add(1);
        }
        for (position, peer) in peers.iter().enumerate() {
            parent.check()?;
            let request = {
                let store = self
                    .metadata
                    .try_lock()
                    .ok_or_else(|| invalid("native heartbeat builder busy"))?;
                let (_, current) =
                    store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
                if current.status.role != MetadataConsensusRole::Leader
                    || current.status.current_term != snapshot.status.current_term
                {
                    return Ok(());
                }
                if !current.topology.nodes.get(peer).is_some_and(|member| {
                    member.lifecycle == ClusterNodeLifecycle::Active
                        && member.native_store_birth_v1.is_some()
                }) {
                    continue;
                }
                store.append_request_for_peer(peer, 16)?
            };
            let token = rpc_budget(parent, peers.len() - position);
            let reply = match self
                .transport
                .append_metadata_cancellable(peer, request, &token)
            {
                Ok(reply) => reply,
                Err(_) => {
                    parent.check()?;
                    continue;
                }
            };
            parent.check()?;
            {
                let mut store = self
                    .metadata
                    .try_lock()
                    .ok_or_else(|| invalid("native heartbeat response store busy"))?;
                store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
                store.record_append_response(peer, &reply)?;
            }
            let current = self.snapshot()?;
            self.transport.install_topology(current.topology)?;
        }
        Ok(())
    }
}
fn rpc_budget(parent: &CancellationToken, remaining: usize) -> CancellationToken {
    let now = Instant::now();
    let left = parent.deadline().map_or(Duration::from_millis(200), |end| {
        end.saturating_duration_since(now)
    });
    parent.child_with_deadline(now + left / remaining.max(1).min(u32::MAX as usize) as u32)
}
