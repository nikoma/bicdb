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

#[cfg(test)]
mod observation;
#[cfg(test)]
use observation::Observation;

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
    #[cfg(test)]
    observation: Arc<Observation>,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<()>>,
}
struct Driver {
    #[cfg(test)]
    observation: Arc<Observation>,
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
        #[cfg(test)]
        let observation = Arc::new(Observation::new());
        let mut driver = Driver {
            #[cfg(test)]
            observation: observation.clone(),
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
                            #[cfg(test)]
                            driver.observation.record(
                                "tick_error",
                                None,
                                0,
                                0,
                                None,
                                None,
                                start,
                                &round,
                                Some(&error),
                            );
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
            #[cfg(test)]
            observation,
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
#[cfg(test)]
impl NativeMetadataSupervisor {
    pub(crate) fn test_observation(&self) -> Arc<Observation> {
        self.observation.clone()
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
    /// Only local serialization admission changes: the fast probe remains,
    /// then waits within this SAME original round. No network, service/DB,
    /// journal, version or other metadata guard is held at any call site.
    fn lock_metadata<'a>(
        &'a self,
        parent: &CancellationToken,
        busy: &'static str,
    ) -> Result<parking_lot::MutexGuard<'a, MetadataConsensusStore>> {
        parent.check()?;
        #[cfg(test)]
        let entered = Instant::now();
        if let Some(held) = self.metadata.try_lock() {
            let current = parent.check();
            #[cfg(test)]
            if let Err(error) = &current {
                self.observation.observe_admission_denial(error);
            }
            current?;
            return Ok(held);
        }
        #[cfg(test)]
        self.observation
            .initial_busy
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let remaining = parent
            .deadline()
            .map(|end| end.saturating_duration_since(Instant::now()))
            .unwrap_or_default();
        let Some(held) = self.metadata.try_lock_for(remaining) else {
            #[cfg(test)]
            if let Err(error) = parent.check() {
                self.observation.observe_admission_denial(&error);
            }
            return Err(invalid(busy));
        };
        // Acquiring a mutex never makes an expired/canceled round current.
        let current = parent.check();
        #[cfg(test)]
        if let Err(error) = &current {
            self.observation.observe_admission_denial(error);
        }
        current?;
        #[cfg(test)]
        self.observation.max_lock_wait_us.fetch_max(
            entered.elapsed().as_micros().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(held)
    }
    fn snapshot(&self, parent: &CancellationToken) -> Result<NativeMetadataSupervisorSnapshot> {
        #[cfg(test)]
        let entered = Instant::now();
        #[cfg(test)]
        self.observation.before_snapshot(parent)?;
        let result = {
            let held = self.lock_metadata(parent, "native metadata tick store busy")?;
            held.native_supervisor_snapshot(&self.actual, Some(self.store_id))
        };
        // Original native/owner error wins over a later diagnostic/deadline.
        let snapshot = result?.1;
        parent.check()?;
        #[cfg(test)]
        {
            self.observation
                .completed_snapshots
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.observation.record_snapshot(&snapshot, entered, parent);
        }
        Ok(snapshot)
    }
    fn tick(&mut self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        let snapshot = self.snapshot(parent)?;
        // Republish a successfully durable backend C even after restart or a
        // previous cache-publication failure. This is configuration DATA only.
        self.transport.install_topology(snapshot.topology.clone())?;
        if !self.actual.has_live_metadata_ingress(&self.metadata)? {
            // Outgoing votes/heartbeats can still succeed after our incoming
            // socket closes. They must not keep an unreachable node elected.
            // Do not cancel this process: SAME-owner rebind resumes this same
            // worker without replacing or reviving any lifecycle token.
            self.election_at = Instant::now() + election_delay(&snapshot, self.config);
            let mut store =
                self.lock_metadata(parent, "native metadata ingress relinquish store busy")?;
            store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
            parent.check()?;
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
            let mut store = self.lock_metadata(parent, "native election store busy")?;
            store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
            parent.check()?;
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
            #[cfg(test)]
            let sent = Instant::now();
            #[cfg(test)]
            self.observation.record(
                "vote_dispatch",
                Some(peer),
                request.term,
                request.last_log_index,
                None,
                None,
                sent,
                parent,
                None,
            );
            let reply = match self.transport.request_metadata_vote_cancellable(
                peer,
                request.clone(),
                &token,
            ) {
                Ok(reply) => {
                    #[cfg(test)]
                    self.observation.record(
                        "vote_returned",
                        Some(peer),
                        request.term,
                        request.last_log_index,
                        Some((reply.term, reply.vote_granted)),
                        None,
                        sent,
                        parent,
                        None,
                    );
                    reply
                }
                Err(error) => {
                    #[cfg(test)]
                    self.observation.record(
                        "vote_error",
                        Some(peer),
                        request.term,
                        request.last_log_index,
                        None,
                        None,
                        sent,
                        parent,
                        Some(&error),
                    );
                    #[cfg(not(test))]
                    let _ = error;
                    parent.check()?;
                    continue;
                }
            };
            let mut store = self.lock_metadata(parent, "native election response store busy")?;
            store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
            parent.check()?;
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
            let mut store = self.lock_metadata(parent, "native election finish store busy")?;
            parent.check()?;
            store.finish_native_supervisor_election(
                &self.actual,
                self.store_id,
                &request,
                &votes,
            )?;
        }
        let elected = self.snapshot(parent)?;
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
                let store = self.lock_metadata(parent, "native heartbeat builder busy")?;
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
                parent.check()?;
                store.append_request_for_peer(peer, 16)?
            };
            let token = rpc_budget(parent, peers.len() - position);
            #[cfg(test)]
            let sent = Instant::now();
            #[cfg(test)]
            let header = (request.term, request.prev_log_index);
            #[cfg(test)]
            self.observation
                .record_append_dispatch(peer, &request, sent, &token);
            let reply = match self
                .transport
                .append_metadata_cancellable(peer, request, &token)
            {
                Ok(reply) => {
                    #[cfg(test)]
                    self.observation.record(
                        "append_returned",
                        Some(peer),
                        header.0,
                        reply.match_index,
                        Some((reply.term, reply.success)),
                        None,
                        sent,
                        &token,
                        None,
                    );
                    reply
                }
                Err(error) => {
                    #[cfg(test)]
                    self.observation.record(
                        "append_error",
                        Some(peer),
                        header.0,
                        header.1,
                        None,
                        None,
                        sent,
                        &token,
                        Some(&error),
                    );
                    #[cfg(not(test))]
                    let _ = error;
                    parent.check()?;
                    continue;
                }
            };
            parent.check()?;
            let counted = {
                let mut store =
                    self.lock_metadata(parent, "native heartbeat response store busy")?;
                store.native_supervisor_snapshot(&self.actual, Some(self.store_id))?;
                parent.check()?;
                store.record_append_response(peer, &reply)
            };
            #[cfg(test)]
            self.observation.record(
                "append_record_returned",
                Some(peer),
                header.0,
                reply.match_index,
                Some((reply.term, reply.success)),
                counted.as_ref().ok().copied(),
                sent,
                parent,
                counted.as_ref().err(),
            );
            counted?;
            let current = self.snapshot(parent)?;
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
