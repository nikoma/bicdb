//! Fresh metadata-leader acquisition, separate from range-image/read visibility.
//! No caller-supplied DTO can mint a local MetadataReadAuthority. Network work
//! runs without consensus/database/service latches. Limits bound outstanding
//! contexts, worker threads, queues, voter count, retries and serialized state.
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard};

use crate::distribution::{ClusterNodeId, RangeDescriptor};
use crate::distribution_consensus::{
    MetadataConsensusRole, MetadataConsensusStore, MetadataReadAck, MetadataReadAcquireRequest,
    MetadataReadAcquireResponse, MetadataReadAuthority, MetadataReadContext,
    MetadataReadParticipant, MetadataReadProbeRequest, MetadataRecoveryRoleObservation,
    MetadataRecoveryRolePurpose, MetadataRecoveryRoleRequest, MetadataRecoveryRoleResponse,
};
use crate::error::{BicDbError, Result};
use crate::CancellationToken;
mod host_read;

fn acquisition_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("metadata read acquisition: {}", message.into()))
}

pub trait MetadataReadProbeTransport: Send + Sync + std::fmt::Debug + 'static {
    fn local_participant(&self) -> MetadataReadParticipant;
    fn acquire_recovery_role(
        &self,
        _destination: &ClusterNodeId,
        _request: MetadataRecoveryRoleRequest,
        _parent: &CancellationToken,
    ) -> Result<MetadataRecoveryRoleObservation> {
        Err(acquisition_error(
            "fresh recovery-role transport is unavailable",
        ))
    }
    fn probe(
        &self,
        destination: &ClusterNodeId,
        request: MetadataReadProbeRequest,
        cancellation: &CancellationToken,
    ) -> Result<MetadataReadAck>;
}

#[derive(Clone, Debug)]
pub struct MetadataReadProviderLimits {
    pub max_inflight_contexts: usize,
    pub workers_per_context: usize,
    pub max_voters: usize,
    pub max_attempts_per_voter: usize,
    pub max_payload_bytes: usize,
}

impl Default for MetadataReadProviderLimits {
    fn default() -> Self {
        Self {
            max_inflight_contexts: 4,
            workers_per_context: 4,
            max_voters: 64,
            max_attempts_per_voter: 4,
            max_payload_bytes: 2 * 1024 * 1024,
        }
    }
}

impl MetadataReadProviderLimits {
    fn validate(&self) -> Result<()> {
        if !(1..=4).contains(&self.max_inflight_contexts)
            || !(1..=4).contains(&self.workers_per_context)
            || !(1..=64).contains(&self.max_voters)
            || !(1..=8).contains(&self.max_attempts_per_voter)
            || !(256 * 1024..=2 * 1024 * 1024).contains(&self.max_payload_bytes)
        {
            return Err(acquisition_error("provider limits exceed hard bounds"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MetadataReadProvider {
    consensus: Arc<Mutex<MetadataConsensusStore>>,
    transport: Arc<dyn MetadataReadProbeTransport>,
    limits: MetadataReadProviderLimits,
    inflight: Arc<AtomicUsize>,
    listener_database: std::sync::OnceLock<crate::distribution_data::ClusterServiceOpenGuard>,
    #[cfg(test)]
    passive_capture: Option<crate::native_passive_diagnostics::Handle>,
}

struct ContextPermit(Arc<AtomicUsize>);
impl Drop for ContextPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
struct CancelOnReturn(CancellationToken);
impl Drop for CancelOnReturn {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
enum ReadAdmission<'a> {
    Leader,
    RecoveryWitness(&'a MetadataRecoveryRoleRequest),
    HostMember(&'a crate::distribution_consensus::HostMetadataReadAcquireRequest),
}
impl ReadAdmission<'_> {
    fn validate(
        &self,
        store: &MetadataConsensusStore,
        request: &MetadataReadAcquireRequest,
        caller: &MetadataReadParticipant,
    ) -> Result<RangeDescriptor> {
        match self {
            Self::Leader => store.validate_read_acquisition_caller(request, caller),
            Self::RecoveryWitness(role) => store.validate_recovery_role_caller(role, caller),
            Self::HostMember(host) => store.validate_host_metadata_caller(host, request, caller),
        }
    }
}

struct ProbeJob {
    destination: ClusterNodeId,
    request: MetadataReadProbeRequest,
    deadline: Instant,
    #[cfg(test)]
    passive: Option<PassiveProbe>,
}
struct Candidate {
    destination: ClusterNodeId,
    next_index: u64,
    attempts: usize,
}

#[cfg(test)]
#[derive(Clone)]
struct PassiveAcquisition {
    capture: crate::native_passive_diagnostics::Handle,
    acquisition_id: uuid::Uuid,
    read_request_id: uuid::Uuid,
    started: Instant,
}
#[cfg(test)]
impl PassiveAcquisition {
    fn record(&self, stage: &str, data: serde_json::Value) {
        self.capture.record(stage, serde_json::json!({"acquisition_id":self.acquisition_id,
            "read_request_id":self.read_request_id,"acquisition_elapsed_us":self.started.elapsed().as_micros(),
            "observation":data}));
    }
}
#[cfg(test)]
struct PassiveProbe {
    acquisition: PassiveAcquisition,
    voter: ClusterNodeId,
    attempt: usize,
    next_index: u64,
    queued: Instant,
    deadline: Instant,
}
#[cfg(test)]
impl PassiveProbe {
    fn record(&self, stage: &str, data: serde_json::Value) {
        self.acquisition.record(stage, serde_json::json!({"voter":self.voter,
            "attempt":self.attempt,"next_index":self.next_index,"queue_elapsed_us":self.queued.elapsed().as_micros(),
            "actual_attempt_deadline":self.acquisition.capture.deadline(Some(self.deadline)),"observation":data}));
    }
}
impl MetadataReadProvider {
    pub(crate) fn bind_listener_database(
        &self,
        consensus: &Arc<Mutex<MetadataConsensusStore>>,
        actual: &crate::distribution_data::ClusterServiceOpenGuard,
    ) -> Result<()> {
        if !Arc::ptr_eq(consensus, &self.consensus) {
            return Err(BicDbError::Cluster(
                "metadata provider is attached to another consensus store".into(),
            ));
        }
        self.consensus
            .try_lock()
            .ok_or_else(|| {
                BicDbError::Cluster("metadata provider busy before listener attachment".into())
            })?
            .require_listener_database(actual)?;
        if let Some(previous) = self.listener_database.get() {
            // Validate the already-bound provider against the SAME actual open.
            let store = self.consensus.try_lock().ok_or_else(|| {
                BicDbError::Cluster("metadata provider busy during listener attachment".into())
            })?;
            store.require_listener_database(previous)?;
            store.require_listener_database(actual)?;
        } else {
            self.listener_database
                .set(actual.clone())
                .map_err(|_| BicDbError::Cluster("metadata provider attachment raced".into()))?;
        }
        Ok(())
    }
    /// Actual locally configured transport identity, never a caller-selected
    /// participant DTO. Recovery uses it for local fence observations.
    /// Routing hint only. A caller MUST still acquire actual fresh HostMQ or
    /// generic range authority; this status cannot authorize a row or absence.
    pub(crate) fn current_metadata_leader_hint(
        &self,
        parent: &CancellationToken,
    ) -> Result<Option<ClusterNodeId>> {
        let consensus = self.lock_consensus(parent)?;
        let status = consensus.status();
        Ok(if status.role == MetadataConsensusRole::Leader {
            Some(self.local_participant().node_id().clone())
        } else {
            status.leader_id
        })
    }
    pub(crate) fn local_participant(&self) -> MetadataReadParticipant {
        self.transport.local_participant()
    }
    pub fn new(
        consensus: Arc<Mutex<MetadataConsensusStore>>,
        transport: Arc<dyn MetadataReadProbeTransport>,
    ) -> Result<Self> {
        Self::with_limits(consensus, transport, MetadataReadProviderLimits::default())
    }

    pub fn with_limits(
        consensus: Arc<Mutex<MetadataConsensusStore>>,
        transport: Arc<dyn MetadataReadProbeTransport>,
        limits: MetadataReadProviderLimits,
    ) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            consensus,
            transport,
            limits,
            inflight: Arc::new(AtomicUsize::new(0)),
            listener_database: std::sync::OnceLock::new(),
            #[cfg(test)]
            passive_capture: crate::native_passive_diagnostics::current(),
        })
    }

    pub fn inflight_contexts(&self) -> usize {
        self.inflight.load(Ordering::Acquire)
    }

    fn lock_consensus(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<MutexGuard<'_, MetadataConsensusStore>> {
        loop {
            cancellation.check()?;
            if let Some(guard) = self.consensus.try_lock_for(Duration::from_millis(10)) {
                cancellation.check()?;
                if let Some(actual) = self.listener_database.get() {
                    guard.require_listener_database(actual)?;
                }
                return Ok(guard);
            }
        }
    }

    fn prepare_job(
        &self,
        context: &MetadataReadContext,
        candidate: &Candidate,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<ProbeJob> {
        let consensus = self.lock_consensus(cancellation)?;
        let request = consensus.bounded_read_authority_probe(
            context,
            &candidate.destination,
            candidate.next_index,
            self.limits.max_payload_bytes,
        )?;
        Ok(ProbeJob {
            destination: candidate.destination.clone(),
            request,
            deadline,
            #[cfg(test)]
            passive: None,
        })
    }

    /// Called only with the actual inbound authenticated member participant.
    /// Finite network deadlines do not make an OS metadata fsync preemptible.
    pub(crate) fn acquire(
        &self,
        caller: &MetadataReadParticipant,
        request: MetadataReadAcquireRequest,
        parent: &CancellationToken,
    ) -> Result<MetadataReadAcquireResponse> {
        let authority =
            self.acquire_authority(caller, request.clone(), ReadAdmission::Leader, parent)?;
        let response = authority.into_acquisition_response(request)?;
        self.bound_response(&response)?;
        Ok(response)
    }

    fn bound_response(&self, response: &impl serde::Serialize) -> Result<()> {
        serde_json::to_writer(&mut SizeBound(self.limits.max_payload_bytes), response)
            .map_err(|_| acquisition_error("completed proof exceeds byte bound"))?;
        Ok(())
    }

    pub(crate) fn acquire_recovery_role(
        &self,
        caller: &MetadataReadParticipant,
        request: MetadataRecoveryRoleRequest,
        parent: &CancellationToken,
    ) -> Result<MetadataRecoveryRoleResponse> {
        request.validate()?;
        let authority = self.acquire_authority(
            caller,
            request.subject_request(),
            ReadAdmission::RecoveryWitness(&request),
            parent,
        )?;
        let response = authority.into_recovery_role_response(request)?;
        self.bound_response(&response)?;
        Ok(response)
    }

    /// The fresh query runs before callers acquire any data/service/DB latch.
    /// A cached leader ID is only a routing hint; it can never grant a role.
    pub(crate) fn witness_recovery_role(
        &self,
        initiator: &MetadataReadParticipant,
        read_request_id: uuid::Uuid,
        range_id: crate::RangeId,
        range_epoch: u64,
        recovery_nonce: uuid::Uuid,
        recipient_challenge: uuid::Uuid,
        purpose: MetadataRecoveryRolePurpose,
        parent: &CancellationToken,
    ) -> Result<MetadataRecoveryRoleObservation> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        let local = self.transport.local_participant();
        let budget = token
            .deadline()
            .unwrap()
            .checked_duration_since(Instant::now())
            .ok_or_else(|| acquisition_error("recovery-role budget expired"))?;
        let request = MetadataRecoveryRoleRequest {
            protocol_version: crate::METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
            acquisition_id: uuid::Uuid::now_v7(),
            read_request_id,
            initiator_node_id: initiator.node_id().clone(),
            initiator_incarnation: initiator.member_incarnation(),
            initiator_certificate_sha256: initiator.certificate_sha256().map(str::to_string),
            initiator_plaintext_loopback: initiator.plaintext_loopback(),
            recipient_node_id: local.node_id().clone(),
            recipient_incarnation: local.member_incarnation(),
            range_id,
            range_epoch,
            recovery_nonce,
            recipient_challenge,
            purpose,
            budget_ms: budget
                .as_millis()
                .try_into()
                .map_err(|_| acquisition_error("recovery-role duration overflow"))?,
        };
        request.validate()?;
        let status = self.lock_consensus(&token)?.status();
        if status.role == MetadataConsensusRole::Leader {
            let authority = self.acquire_authority(
                &local,
                request.subject_request(),
                ReadAdmission::RecoveryWitness(&request),
                &token,
            )?;
            return MetadataRecoveryRoleObservation::from_local_authority(
                authority, request, &local, &token,
            );
        }
        let leader = status.leader_id.ok_or_else(|| {
            acquisition_error("metadata leader hint unavailable; retry fresh recovery-role query")
        })?;
        let observation = self
            .transport
            .acquire_recovery_role(&leader, request, &token)?;
        observation.require_initiator(initiator)?;
        token.check()?;
        Ok(observation)
    }

    fn acquire_authority(
        &self,
        caller: &MetadataReadParticipant,
        request: MetadataReadAcquireRequest,
        admission: ReadAdmission<'_>,
        parent: &CancellationToken,
    ) -> Result<MetadataReadAuthority> {
        #[cfg(test)]
        let diagnostic = self
            .passive_capture
            .clone()
            .map(|capture| PassiveAcquisition {
                capture,
                acquisition_id: request.acquisition_id,
                read_request_id: request.read_request_id,
                started: Instant::now(),
            });
        #[cfg(test)]
        if let Some(d) = &diagnostic {
            d.record("metadata_acquire_begin", serde_json::json!({"caller":caller.node_id(),
                "range_id":request.range_id,"range_epoch":request.range_epoch,"requested_budget_ms":request.budget_ms,
                "original_parent_deadline":d.capture.deadline(parent.deadline()),
                "max_attempts_per_voter":self.limits.max_attempts_per_voter,"workers":self.limits.workers_per_context}));
        }
        #[cfg(test)]
        let result =
            self.acquire_authority_inner(caller, request, admission, parent, diagnostic.as_ref());
        #[cfg(not(test))]
        let result = self.acquire_authority_inner(caller, request, admission, parent);
        #[cfg(test)]
        if let Some(d) = &diagnostic {
            d.record("metadata_acquire_return", serde_json::json!({"result_ok":result.is_ok(),
                "actual_error_chain":result.as_ref().err().map(|e|crate::native_passive_diagnostics::error_chain(e)),
                "returned_term":result.as_ref().ok().map(|a|a.term()),
                "returned_read_index":result.as_ref().ok().map(|a|a.read_index()),
                "returned_deadline":result.as_ref().ok().map(|a|d.capture.deadline(Some(a.deadline()))),
                "terminal_is_original_result":true}));
        }
        result
    }
    // Original protocol body. Diagnostic observations never supply a result,
    // retry, deadline, acknowledgement or acceptance decision to this method.
    fn acquire_authority_inner(
        &self,
        caller: &MetadataReadParticipant,
        request: MetadataReadAcquireRequest,
        admission: ReadAdmission<'_>,
        parent: &CancellationToken,
        #[cfg(test)] diagnostic: Option<&PassiveAcquisition>,
    ) -> Result<MetadataReadAuthority> {
        request.validate()?;
        parent.check()?;
        self.inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limits.max_inflight_contexts).then_some(active + 1)
            })
            .map_err(|_| acquisition_error("bounded acquisition capacity is busy"))?;
        let permit = Arc::new(ContextPermit(Arc::clone(&self.inflight)));
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        let _cancel_on_return = CancelOnReturn(token.clone());
        let (mut context, range) = {
            let consensus = self.lock_consensus(&token)?;
            consensus.read_authority_payload_fits(self.limits.max_payload_bytes - 128 * 1024)?;
            let range = admission.validate(&consensus, &request, caller)?;
            let budget = token
                .deadline()
                .unwrap()
                .checked_duration_since(Instant::now())
                .ok_or_else(|| acquisition_error("deadline expired before begin"))?;
            let mut context = consensus.begin_read_authority(
                request.read_request_id,
                budget,
                &self.transport.local_participant(),
            )?;
            context.restrict_deadline(token.deadline().unwrap());
            (context, range)
        };
        #[cfg(test)]
        if let Some(d) = diagnostic {
            d.record(
                "metadata_context_begun",
                serde_json::json!({"actual_context":context.test_passive_data(),
                "actual_context_deadline":d.capture.deadline(Some(context.deadline())),
                "effective_parent_deadline":d.capture.deadline(token.deadline())}),
            );
        }
        let voters = context.remote_voters();
        if voters.len() + 1 > self.limits.max_voters {
            return Err(acquisition_error(
                "metadata voter set exceeds bounded fanout",
            ));
        }
        let token = token.child_with_deadline(context.deadline());
        let mut candidates = voters
            .into_iter()
            .map(|destination| Candidate {
                destination,
                next_index: context.read_index().saturating_add(1),
                attempts: 0,
            })
            .collect::<VecDeque<_>>();
        let workers = self.limits.workers_per_context.min(candidates.len());
        let (jobs_tx, jobs_rx) = mpsc::sync_channel::<ProbeJob>(workers.max(1));
        let jobs_rx = Arc::new(Mutex::new(jobs_rx));
        let (results_tx, results_rx) = mpsc::sync_channel(workers.max(1));
        for _ in 0..workers {
            let jobs = Arc::clone(&jobs_rx);
            let results = results_tx.clone();
            let transport = Arc::clone(&self.transport);
            let token = token.clone();
            let permit = Arc::clone(&permit);
            thread::Builder::new()
                .name("metadata-read-probe".to_string())
                .spawn(move || {
                    let _permit = permit;
                    while token.check().is_ok() {
                        let next = jobs.lock().recv_timeout(Duration::from_millis(10));
                        match next {
                            Ok(job) => {
                                if token.check().is_err() {
                                    break;
                                }
                                let attempt = token.child_with_deadline(job.deadline);
                                #[cfg(test)]
                                if let Some(d) = &job.passive {
                                    d.record("metadata_probe_worker_start", serde_json::json!({
                                        "original_worker_parent_deadline":d.acquisition.capture.deadline(token.deadline()),
                                        "effective_deadline":d.acquisition.capture.deadline(attempt.deadline())}));
                                }
                                #[cfg(test)]
                                let passive_transport_started = Instant::now();
                                let result =
                                    transport.probe(&job.destination, job.request, &attempt);
                                #[cfg(test)]
                                if let Some(d) = &job.passive {
                                    d.record("metadata_probe_actual_return", serde_json::json!({"result_ok":result.is_ok(),
                                        "actual_transport_elapsed_us":passive_transport_started.elapsed().as_micros(),
                                        "actual_error_chain":result.as_ref().err().map(|e|crate::native_passive_diagnostics::error_chain(e)),
                                        "actual_ack":result.as_ref().ok().map(|a|a.test_passive_data())}));
                                }
                                if results.send((job.destination, result)).is_err() {
                                    break;
                                }
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                })?;
        }
        drop(results_tx);
        let mut outstanding = BTreeMap::new();
        loop {
            token.check()?;
            {
                let consensus = self.lock_consensus(&token)?;
                if consensus.read_authority_ready(&context)? {
                    let current_subject = admission.validate(&consensus, &request, caller)?;
                    if !matches!(&admission, ReadAdmission::HostMember(_))
                        && !same_range_read_authority(&current_subject, &range)
                    {
                        return Err(acquisition_error(
                            "range configuration changed during fresh quorum round",
                        ));
                    }
                    consensus
                        .read_authority_payload_fits(self.limits.max_payload_bytes - 128 * 1024)?;
                    let authority = consensus.finish_read_authority(context)?;
                    token.check()?;
                    // Sibling cancellation happens on return, without joining a
                    // silent voter and allowing its timeout to expire this proof.
                    return Ok(authority);
                }
            }
            while outstanding.len() < workers {
                let Some(mut candidate) = candidates.pop_front() else {
                    break;
                };
                // Reserve time for every queued voter instead of allowing
                // the first sorted silent peers to occupy all workers until
                // the context expires. Each attempt is a child of the SAME
                // original deadline; retrying never renews the round.
                let queued = candidates.len() + outstanding.len() + 1;
                let waves = queued.div_ceil(workers.max(1));
                let remaining = context
                    .deadline()
                    .checked_duration_since(Instant::now())
                    .ok_or_else(|| acquisition_error("deadline expired before probe admission"))?;
                let slice = remaining / u32::try_from(waves + 1).unwrap_or(u32::MAX);
                if slice.is_zero() {
                    return Err(acquisition_error("probe deadline slice exhausted"));
                }
                let attempt_deadline = (Instant::now() + slice).min(context.deadline());
                let job = self.prepare_job(&context, &candidate, &token, attempt_deadline)?;
                #[cfg(test)]
                let job = {
                    let mut job = job;
                    if let Some(d) = diagnostic {
                        let probe = PassiveProbe {
                            acquisition: d.clone(),
                            voter: candidate.destination.clone(),
                            attempt: candidate.attempts + 1,
                            next_index: candidate.next_index,
                            queued: Instant::now(),
                            deadline: attempt_deadline,
                        };
                        probe.record("metadata_probe_dispatch", serde_json::json!({"dispatch_scope":"bounded_worker_queue_intent_not_wire",
                            "slice_us":slice.as_micros(),
                            "context_remaining_us":remaining.as_micros(),"queued_voters":queued,"waves":waves,
                            "actual_context":context.test_passive_data()}));
                        job.passive = Some(probe);
                    }
                    job
                };
                candidate.attempts += 1;
                jobs_tx
                    .try_send(job)
                    .map_err(|_| acquisition_error("bounded probe queue invariant failed"))?;
                outstanding.insert(candidate.destination.clone(), candidate);
            }
            if outstanding.is_empty() {
                #[cfg(test)]
                if let Some(d) = diagnostic {
                    d.record(
                        "metadata_probe_attempts_exhausted",
                        serde_json::json!({"actual_context":context.test_passive_data(),
                        "remaining_candidates":candidates.len(),"outstanding":outstanding.len(),
                        "context_deadline":d.capture.deadline(Some(context.deadline()))}),
                    );
                }
                return Err(acquisition_error(
                    "fresh joint quorum unavailable within bounded probe attempts",
                ));
            }
            match results_rx.recv_timeout(Duration::from_millis(10)) {
                Ok((voter, result)) => {
                    let mut candidate = outstanding
                        .remove(&voter)
                        .ok_or_else(|| acquisition_error("unexpected probe worker result"))?;
                    let retry = if let Ok(ack) = result {
                        let accepted = self
                            .lock_consensus(&token)?
                            .record_read_authority_ack(&mut context, &ack);
                        #[cfg(test)]
                        if let Some(d) = diagnostic {
                            d.record("metadata_probe_ack_acceptance", serde_json::json!({"voter":voter,
                                "attempt":candidate.attempts,"acceptance_ok":accepted.is_ok(),
                                "accepted":accepted.as_ref().ok(),
                                "actual_error_chain":accepted.as_ref().err().map(|e|crate::native_passive_diagnostics::error_chain(e)),
                                "actual_ack":ack.test_passive_data(),"actual_context":context.test_passive_data()}));
                        }
                        let accepted = accepted?;
                        if !accepted {
                            candidate.next_index = ack
                                .conflict_index()
                                .unwrap_or(1)
                                .max(1)
                                .min(context.read_index().saturating_add(1));
                        }
                        !accepted
                    } else {
                        true
                    };
                    #[cfg(test)]
                    if let Some(d) = diagnostic {
                        d.record("metadata_probe_collector_result", serde_json::json!({"voter":voter,
                            "attempt":candidate.attempts,"next_index":candidate.next_index,"retry_required":retry,
                            "requeued":retry && candidate.attempts<self.limits.max_attempts_per_voter,
                            "context_deadline":d.capture.deadline(Some(context.deadline()))}));
                    }
                    if retry && candidate.attempts < self.limits.max_attempts_per_voter {
                        // Retries join the tail behind all not-yet-tried voters.
                        candidates.push_back(candidate);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(acquisition_error("probe workers disconnected"))
                }
            }
        }
    }
}

// Load estimates are placement inputs, not ownership authority. Preserve
// every other descriptor field, including replica identities/roles and policy.
fn same_range_read_authority(left: &RangeDescriptor, right: &RangeDescriptor) -> bool {
    left.id == right.id
        && left.start_token == right.start_token
        && left.end_token == right.end_token
        && left.epoch == right.epoch
        && left.leader == right.leader
        && left.replicas.len() == right.replicas.len()
        && left
            .replicas
            .iter()
            .map(|r| (r.id, (&r.node_id, r.role)))
            .collect::<BTreeMap<_, _>>()
            == right
                .replicas
                .iter()
                .map(|r| (r.id, (&r.node_id, r.role)))
                .collect::<BTreeMap<_, _>>()
        && left.placement == right.placement
}

struct SizeBound(usize);
impl Write for SizeBound {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metadata proof byte bound",
            ));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_consensus::METADATA_READ_ACQUISITION_PROTOCOL_VERSION;
    use crate::{
        ClusterId, ClusterNode, ClusterTopology, DistributionConfig, DistributionStore, RangeId,
    };
    use std::collections::BTreeSet;
    use uuid::Uuid;

    fn node(name: &str) -> ClusterNodeId {
        ClusterNodeId::new(name).unwrap()
    }
    fn participant(topology: &ClusterTopology, name: &str) -> MetadataReadParticipant {
        let id = node(name);
        MetadataReadParticipant::from_transport(
            id.clone(),
            topology.nodes[&id].incarnation,
            None,
            true,
        )
    }
    fn topology_with_count(count: u64) -> ClusterTopology {
        let root = tempfile::tempdir().unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("metadata-provider-test").unwrap(),
            node_id: node("n1"),
            node_address: "127.0.0.1:10001".into(),
            replication_factor: 1,
            initial_ranges: 1,
            ..DistributionConfig::default()
        };
        let mut store = DistributionStore::initialize_at(root.path(), config, false, 10).unwrap();
        for i in 2..=count {
            store
                .join_node(
                    ClusterNode::new(
                        node(&format!("n{i}")),
                        format!("127.0.0.1:{}", 10000 + i),
                        1,
                        1_000_000,
                        10 + i,
                    )
                    .unwrap(),
                    &node("n1"),
                    10 + i,
                )
                .unwrap();
        }
        let mut topology = store.topology().clone();
        let range = topology.ranges.values_mut().next().unwrap();
        range.leader = node("n2");
        range.replicas[0].node_id = node("n2");
        topology.validate().unwrap();
        topology
    }
    fn open(
        root: &std::path::Path,
        name: &str,
        topology: &ClusterTopology,
    ) -> MetadataConsensusStore {
        MetadataConsensusStore::open(
            root,
            topology.cluster_id.clone(),
            node(name),
            topology.clone(),
            topology.range_image_bootstrap_id.is_some(),
        )
        .unwrap()
    }
    #[derive(Debug)]
    struct FakeTransport {
        topology: ClusterTopology,
        follower: Arc<Mutex<MetadataConsensusStore>>,
        leader: Arc<Mutex<MetadataConsensusStore>>,
        mode: AtomicUsize,
        stalled_started: AtomicUsize,
        gate: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    }
    impl MetadataReadProbeTransport for FakeTransport {
        fn local_participant(&self) -> MetadataReadParticipant {
            participant(&self.topology, "n1")
        }
        fn probe(
            &self,
            destination: &ClusterNodeId,
            request: MetadataReadProbeRequest,
            token: &CancellationToken,
        ) -> Result<MetadataReadAck> {
            if *destination == node("n3") {
                self.stalled_started.fetch_add(1, Ordering::SeqCst);
                loop {
                    token.check()?;
                    thread::sleep(Duration::from_millis(2));
                }
            }
            if let Some((entered, released)) = self.gate.lock().take() {
                entered.send(()).unwrap();
                loop {
                    token.check()?;
                    match released.recv_timeout(Duration::from_millis(2)) {
                        Ok(()) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            return Err(acquisition_error("test probe gate disconnected"))
                        }
                    }
                }
            }
            match self.mode.load(Ordering::SeqCst) {
                1 => {
                    self.leader
                        .lock()
                        .observe_remote_term(request.append.term + 1)?;
                }
                2 => {
                    let mut leader = self.leader.lock();
                    let mut changed = leader.committed_topology().clone();
                    changed.generation += 1;
                    changed.nodes.get_mut(&node("n3")).unwrap().incarnation += 1;
                    leader.propose_topology(changed)?;
                }
                _ => {}
            }
            let response = self
                .follower
                .lock()
                .handle_read_authority_probe(request.clone())?;
            MetadataReadAck::from_transport(
                &request,
                destination,
                participant(&self.topology, "n2"),
                response,
            )
        }
    }
    struct Fixture {
        _root: tempfile::TempDir,
        topology: ClusterTopology,
        transport: Arc<FakeTransport>,
        provider: MetadataReadProvider,
    }
    impl Fixture {
        fn new() -> Self {
            Self::with_topology(topology_with_count(3))
        }
        fn with_topology(topology: ClusterTopology) -> Self {
            let root = tempfile::tempdir().unwrap();
            let mut leader = open(&root.path().join("n1"), "n1", &topology);
            let mut follower = open(&root.path().join("n2"), "n2", &topology);
            leader.start_election().unwrap();
            leader
                .become_leader(&BTreeSet::from([node("n1"), node("n2")]))
                .unwrap();
            let response = follower
                .handle_append_request(leader.append_request_from(1, 16).unwrap())
                .unwrap();
            leader
                .record_append_response(&node("n2"), &response)
                .unwrap();
            let leader = Arc::new(Mutex::new(leader));
            let transport = Arc::new(FakeTransport {
                topology: topology.clone(),
                follower: Arc::new(Mutex::new(follower)),
                leader: leader.clone(),
                mode: AtomicUsize::new(0),
                stalled_started: AtomicUsize::new(0),
                gate: Mutex::new(None),
            });
            let provider = MetadataReadProvider::new(leader, transport.clone()).unwrap();
            Self {
                _root: root,
                topology,
                transport,
                provider,
            }
        }
        fn request(&self) -> MetadataReadAcquireRequest {
            MetadataReadAcquireRequest {
                protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
                acquisition_id: Uuid::now_v7(),
                read_request_id: Uuid::now_v7(),
                requester_node_id: node("n2"),
                requester_incarnation: self.topology.nodes[&node("n2")].incarnation,
                range_id: RangeId::new(1).unwrap(),
                range_epoch: self
                    .topology
                    .range_by_id(RangeId::new(1).unwrap())
                    .unwrap()
                    .epoch,
                budget_ms: 1000,
            }
        }
        fn acquire(
            &self,
            request: MetadataReadAcquireRequest,
        ) -> Result<MetadataReadAcquireResponse> {
            self.provider.acquire(
                &participant(&self.topology, "n2"),
                request,
                &CancellationToken::uncancelable(),
            )
        }
    }
    fn role_request(
        fixture: &Fixture,
        recipient: &str,
        initiator: &str,
        purpose: MetadataRecoveryRolePurpose,
    ) -> MetadataRecoveryRoleRequest {
        MetadataRecoveryRoleRequest {
            protocol_version: crate::METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
            acquisition_id: Uuid::now_v7(),
            read_request_id: Uuid::now_v7(),
            initiator_node_id: node(initiator),
            initiator_incarnation: fixture.topology.nodes[&node(initiator)].incarnation,
            initiator_certificate_sha256: None,
            initiator_plaintext_loopback: true,
            recipient_node_id: node(recipient),
            recipient_incarnation: fixture.topology.nodes[&node(recipient)].incarnation,
            range_id: crate::RangeId::new(1).unwrap(),
            range_epoch: fixture.topology.ranges.values().next().unwrap().epoch,
            recovery_nonce: Uuid::now_v7(),
            recipient_challenge: Uuid::now_v7(),
            purpose,
            budget_ms: 1000,
        }
    }
    #[test]
    fn recovery_role_observations_retain_local_and_transport_acquisition_cancellation() {
        for local_mint in [true, false] {
            let mut topology = topology_with_count(3);
            // Make the actual metadata leader also a current data voter, so
            // local_mint exercises the real provider's sealed local path.
            topology.replication_factor = 2;
            topology.ranges.values_mut().next().unwrap().replicas.push(
                crate::distribution::RangeReplica {
                    id: crate::distribution::ReplicaId::new(topology.next_replica_id).unwrap(),
                    node_id: node("n1"),
                    role: crate::distribution::RangeReplicaRole::Voter,
                },
            );
            topology.next_replica_id += 1;
            topology.validate().unwrap();
            let fixture = Fixture::with_topology(topology);
            let parent = CancellationToken::uncancelable()
                .child_with_deadline(Instant::now() + Duration::from_secs(5));
            let request = role_request(
                &fixture,
                "n1",
                "n2",
                MetadataRecoveryRolePurpose::NormalRecovery,
            );
            let initiator = participant(&fixture.topology, "n2");
            let observation = if local_mint {
                fixture
                    .provider
                    .witness_recovery_role(
                        &initiator,
                        request.read_request_id,
                        request.range_id,
                        request.range_epoch,
                        request.recovery_nonce,
                        request.recipient_challenge,
                        request.purpose.clone(),
                        &parent,
                    )
                    .unwrap()
            } else {
                let started = Instant::now();
                let response = fixture
                    .provider
                    .acquire_recovery_role(
                        &participant(&fixture.topology, "n1"),
                        request.clone(),
                        &parent,
                    )
                    .unwrap();
                // This test transport wraps only evidence returned by the
                // actual stores' begin/probe/record/finish. No DTO mints QQ.
                MetadataRecoveryRoleObservation::from_transport(
                    &request,
                    &fixture.topology.cluster_id,
                    &node("n1"),
                    participant(&fixture.topology, "n1"),
                    response,
                    parent.deadline().unwrap(),
                    started,
                    &parent,
                )
                .unwrap()
            };
            observation
                .require_live_request(request.read_request_id, request.recipient_challenge)
                .unwrap();
            let original_deadline = observation.deadline();
            parent.cancel();
            assert!(
                Instant::now() < original_deadline,
                "original scope must not expire first"
            );
            assert!(matches!(
                observation
                    .require_live_request(request.read_request_id, request.recipient_challenge,),
                Err(BicDbError::QueryCanceled)
            ));
            assert_eq!(observation.deadline(), original_deadline);
        }
    }
    #[test]
    fn fenced_observer_is_data_voter_scoped_and_never_mutation_admission() {
        use crate::distribution_range_consensus::RecoveryFenceScope;
        use crate::distribution_range_recovery::{
            RecoveryObservationRoleAdmission, RecoveryRoleAdmission,
        };
        let mut topology = topology_with_count(3);
        topology.range_image_bootstrap_id = Some(Uuid::now_v7());
        topology.replication_factor = 2;
        topology.ranges.values_mut().next().unwrap().replicas.push(
            crate::distribution::RangeReplica {
                id: crate::distribution::ReplicaId::new(topology.next_replica_id).unwrap(),
                node_id: node("n1"),
                role: crate::distribution::RangeReplicaRole::Voter,
            },
        );
        topology.next_replica_id += 1;
        topology.validate().unwrap();
        let fixture = Fixture::with_topology(topology);
        let parent = CancellationToken::uncancelable();
        let mut normal = role_request(
            &fixture,
            "n2",
            "n2",
            MetadataRecoveryRolePurpose::NormalRecovery,
        );
        let started = Instant::now();
        let response = fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                normal.clone(),
                &parent,
            )
            .unwrap();
        let witness = MetadataRecoveryRoleObservation::from_transport(
            &normal,
            &fixture.topology.cluster_id,
            &node("n1"),
            participant(&fixture.topology, "n1"),
            response,
            started + Duration::from_secs(1),
            started,
            &parent,
        )
        .unwrap();
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            &participant(&fixture.topology, "n2"),
            &node("n2"),
            normal.read_request_id,
            normal.range_id,
            normal.range_epoch,
            normal.recovery_nonce,
            normal.recipient_challenge,
            &normal.purpose,
        )
        .unwrap();
        let scope = RecoveryFenceScope::from_role(&role).unwrap();
        let purpose = RecoveryObservationRoleAdmission::purpose(&scope).unwrap();
        let mut request = role_request(&fixture, "n2", "n1", purpose.clone());
        request.recovery_nonce = normal.recovery_nonce;
        let started = Instant::now();
        let response = fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                request.clone(),
                &parent,
            )
            .unwrap();
        let witness = MetadataRecoveryRoleObservation::from_transport(
            &request,
            &fixture.topology.cluster_id,
            &node("n1"),
            participant(&fixture.topology, "n1"),
            response,
            started + Duration::from_secs(1),
            started,
            &parent,
        )
        .unwrap();
        let readonly = RecoveryObservationRoleAdmission::from_witness(
            witness,
            &participant(&fixture.topology, "n1"),
            &node("n2"),
            request.read_request_id,
            request.recipient_challenge,
            &scope,
        )
        .unwrap();
        readonly.require_scope(&scope).unwrap();
        let mut wrong = scope.clone();
        wrong.recovery_nonce = Uuid::now_v7();
        assert!(readonly.require_scope(&wrong).is_err());
        let started = Instant::now();
        let response = fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                request.clone(),
                &parent,
            )
            .unwrap();
        let witness = MetadataRecoveryRoleObservation::from_transport(
            &request,
            &fixture.topology.cluster_id,
            &node("n1"),
            participant(&fixture.topology, "n1"),
            response,
            started + Duration::from_secs(1),
            started,
            &parent,
        )
        .unwrap();
        assert!(RecoveryRoleAdmission::from_witness(
            witness,
            &participant(&fixture.topology, "n1"),
            &node("n2"),
            request.read_request_id,
            request.range_id,
            request.range_epoch,
            request.recovery_nonce,
            request.recipient_challenge,
            &purpose
        )
        .is_err());
        for (observer, coordinator, incarnation) in [
            ("n3", "n2", fixture.topology.nodes[&node("n2")].incarnation),
            ("n1", "n1", fixture.topology.nodes[&node("n1")].incarnation),
            ("n1", "n2", u64::MAX),
        ] {
            let mut invalid = role_request(
                &fixture,
                "n2",
                observer,
                MetadataRecoveryRolePurpose::ObserveFencedRecovery {
                    coordinator_node_id: node(coordinator),
                    coordinator_incarnation: incarnation,
                    committed_handoff: None,
                    current_joint_decision_sha256: None,
                },
            );
            invalid.recovery_nonce = normal.recovery_nonce;
            assert!(fixture
                .provider
                .acquire_recovery_role(
                    &participant(&fixture.topology, "n2"),
                    invalid,
                    &CancellationToken::uncancelable()
                )
                .is_err());
        }
        normal.initiator_node_id = node("n1");
        normal.initiator_incarnation = fixture.topology.nodes[&node("n1")].incarnation;
        assert!(
            fixture
                .provider
                .acquire_recovery_role(
                    &participant(&fixture.topology, "n2"),
                    normal,
                    &CancellationToken::uncancelable()
                )
                .is_err(),
            "normal mutation role remains leader-only"
        );
    }

    #[test]
    fn fresh_role_is_data_voter_scoped_and_drain_is_not_removal() {
        let mut topology = topology_with_count(3);
        topology.nodes.get_mut(&node("n2")).unwrap().lifecycle =
            crate::ClusterNodeLifecycle::Draining;
        let fixture = Fixture::with_topology(topology);
        let request = role_request(
            &fixture,
            "n2",
            "n2",
            MetadataRecoveryRolePurpose::NormalRecovery,
        );
        let response = fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                request.clone(),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(response.request, request);
        assert!(
            fixture.acquire(fixture.request()).is_err(),
            "ordinary acquisition remains strict"
        );
        let request = role_request(
            &fixture,
            "n3",
            "n2",
            MetadataRecoveryRolePurpose::NormalRecovery,
        );
        assert!(
            fixture
                .provider
                .acquire_recovery_role(
                    &participant(&fixture.topology, "n3"),
                    request,
                    &CancellationToken::uncancelable()
                )
                .is_err(),
            "metadata membership alone is not a data voter role"
        );
        let busy = Fixture::new();
        busy.provider.inflight.store(4, Ordering::SeqCst);
        assert!(busy
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                role_request(
                    &fixture,
                    "n2",
                    "n2",
                    MetadataRecoveryRolePurpose::NormalRecovery
                ),
                &CancellationToken::uncancelable()
            )
            .is_err());
    }
    #[test]
    fn handoff_witness_is_exact_plan_purpose_for_metadata_coordinator_and_target_learner() {
        use crate::{PlannedReplicaMove, RebalancePlan, ReplicaMoveReason};
        let root = tempfile::tempdir().unwrap();
        let cfg = DistributionConfig {
            enabled: true,
            cluster_id: crate::ClusterId::new("metadata-provider-test").unwrap(),
            node_id: node("n1"),
            node_address: "127.0.0.1:10001".into(),
            replication_factor: 1,
            initial_ranges: 1,
            ..DistributionConfig::default()
        };
        let mut store = DistributionStore::initialize_at(root.path(), cfg, false, 10).unwrap();
        store
            .install_authoritative_topology(topology_with_count(3))
            .unwrap();
        let range = store.topology().ranges.values().next().unwrap().clone();
        let plan = RebalancePlan {
            id: "exact-role-plan".into(),
            topology_generation: store.topology().generation,
            created_at_ms: 20,
            replica_moves: vec![PlannedReplicaMove {
                range_id: range.id,
                expected_epoch: range.epoch,
                source: Some(node("n2")),
                target: node("n3"),
                estimated_bytes: 0,
                reason: ReplicaMoveReason::LoadBalance,
            }],
            leader_transfers: vec![],
            unplaced_ranges: vec![],
            estimated_bytes_in_flight: 0,
        };
        store.apply_rebalance_plan(&plan, &node("n1"), 20).unwrap();
        let mut topology = store.topology().clone();
        // Current topology invariants promote metadata membership before a
        // data learner is allocated; only the DATA role is Learner here.
        let relocation = topology.relocations.values().next().unwrap();
        let session = relocation.handoff_session.as_ref().unwrap();
        let nonce = session.nonce;
        let purpose = MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: relocation.id,
            plan_id: relocation.plan_id.clone(),
            session_id: session.session_id,
            nonce,
        };
        let fixture = Fixture::with_topology(topology);
        let mut request = role_request(&fixture, "n3", "n1", purpose.clone());
        request.recovery_nonce = nonce;
        let response = fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n3"),
                request.clone(),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(response.request.purpose, purpose);
        let mut admitted_topology = fixture.topology.clone();
        admitted_topology.range_image_bootstrap_id = Some(Uuid::now_v7());
        let relocation_id = *admitted_topology.relocations.keys().next().unwrap();
        let admission = crate::distribution_range_recovery::test_handoff_role_admission(
            &root.path().join("fresh-role"),
            &admitted_topology,
            &node("n1"),
            &node("n1"),
            &node("n3"),
            relocation_id,
            Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(
            admission.target_member_identity().incarnation,
            admitted_topology.nodes[&node("n3")].incarnation
        );
        let session = admitted_topology.relocations[&relocation_id]
            .handoff_session
            .as_ref()
            .unwrap();
        admission
            .require_live_scope(
                relocation_id,
                "exact-role-plan",
                session.session_id,
                session.nonce,
            )
            .unwrap();
        assert!(admission
            .require_live_scope(
                relocation_id,
                "wrong-plan",
                session.session_id,
                session.nonce
            )
            .is_err());
        let mut normal = role_request(
            &fixture,
            "n3",
            "n2",
            MetadataRecoveryRolePurpose::NormalRecovery,
        );
        normal.recovery_nonce = nonce;
        assert!(fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n3"),
                normal,
                &CancellationToken::uncancelable()
            )
            .is_err());
        if let MetadataRecoveryRolePurpose::CommittedHandoff { session_id, .. } =
            &mut request.purpose
        {
            *session_id = Uuid::now_v7();
        }
        assert!(fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n3"),
                request,
                &CancellationToken::uncancelable()
            )
            .is_err());
    }

    #[test]
    fn role_scope_rejects_wrong_peer_epoch_nonce_and_response_challenge() {
        let fixture = Fixture::new();
        let parent = CancellationToken::uncancelable();
        let request = role_request(
            &fixture,
            "n2",
            "n2",
            MetadataRecoveryRolePurpose::NormalRecovery,
        );
        let started = Instant::now();
        let response = fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                request.clone(),
                &parent,
            )
            .unwrap();
        let observation = MetadataRecoveryRoleObservation::from_transport(
            &request,
            &fixture.topology.cluster_id,
            &node("n1"),
            participant(&fixture.topology, "n1"),
            response,
            started + Duration::from_secs(1),
            started,
            &parent,
        )
        .unwrap();
        observation
            .require_initiator(&participant(&fixture.topology, "n2"))
            .unwrap();
        assert!(observation
            .require_initiator(&participant(&fixture.topology, "n1"))
            .is_err());
        assert!(observation
            .require_live_request(request.read_request_id, Uuid::now_v7())
            .is_err());
        let mut invalid = request.clone();
        invalid.range_epoch += 1;
        assert!(fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                invalid,
                &CancellationToken::uncancelable()
            )
            .is_err());
        let mut invalid = request;
        invalid.recovery_nonce = Uuid::nil();
        assert!(fixture
            .provider
            .acquire_recovery_role(
                &participant(&fixture.topology, "n2"),
                invalid,
                &CancellationToken::uncancelable()
            )
            .is_err());
    }

    #[test]
    fn fresh_quorum_returns_without_waiting_for_silent_voter() {
        let fixture = Fixture::new();
        let started = Instant::now();
        let request = fixture.request();
        let response = fixture.acquire(request.clone()).unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(response.metadata_leader_id, node("n1"));
        assert_eq!(request.requester_node_id, node("n2"));
        assert_eq!(
            response.fresh_old_voters,
            BTreeSet::from([node("n1"), node("n2")])
        );
        let observation = crate::MetadataReadObservation::from_transport(
            &request,
            &fixture.topology.cluster_id,
            &node("n1"),
            participant(&fixture.topology, "n1"),
            response,
            started + Duration::from_secs(1),
            started,
        )
        .unwrap();
        assert!(observation
            .committed_topology(request.read_request_id)
            .is_ok());
        assert!(observation.committed_topology(Uuid::now_v7()).is_err());
        let drain = Instant::now() + Duration::from_secs(1);
        while fixture.provider.inflight_contexts() != 0 && Instant::now() < drain {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(fixture.provider.inflight_contexts(), 0);
    }
    #[test]
    fn leadership_or_joint_configuration_change_invalidates_round() {
        for mode in [1, 2] {
            let fixture = Fixture::new();
            fixture.transport.mode.store(mode, Ordering::SeqCst);
            assert!(fixture.acquire(fixture.request()).is_err());
        }
    }
    #[test]
    fn stale_epoch_wrong_caller_and_expired_parent_refuse() {
        let fixture = Fixture::new();
        let mut request = fixture.request();
        request.range_epoch += 1;
        assert!(fixture.acquire(request).is_err());
        assert!(fixture
            .provider
            .acquire(
                &participant(&fixture.topology, "n3"),
                fixture.request(),
                &CancellationToken::uncancelable()
            )
            .is_err());
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() - Duration::from_millis(1));
        assert!(fixture
            .provider
            .acquire(
                &participant(&fixture.topology, "n2"),
                fixture.request(),
                &parent
            )
            .is_err());
        assert_eq!(fixture.provider.inflight_contexts(), 0);
    }
    #[test]
    fn response_nonce_quorum_identity_and_deadline_cannot_be_reused() {
        let fixture = Fixture::new();
        let request = fixture.request();
        let response = fixture.acquire(request.clone()).unwrap();
        let check = |request: &MetadataReadAcquireRequest,
                     response: MetadataReadAcquireResponse,
                     started: Instant| {
            crate::MetadataReadObservation::from_transport(
                request,
                &fixture.topology.cluster_id,
                &node("n1"),
                participant(&fixture.topology, "n1"),
                response,
                Instant::now() + Duration::from_secs(5),
                started,
            )
        };
        let mut changed = request.clone();
        changed.acquisition_id = Uuid::now_v7();
        assert!(check(&changed, response.clone(), Instant::now()).is_err());
        let mut changed = response.clone();
        changed.fresh_new_voters.clear();
        assert!(check(&request, changed, Instant::now()).is_err());
        let mut changed = response.clone();
        changed.metadata_leader_incarnation += 1;
        assert!(check(&request, changed, Instant::now()).is_err());
        assert!(check(&request, response, Instant::now() - Duration::from_secs(2)).is_err());
    }
    #[test]
    fn limits_and_busy_admission_fail_closed() {
        let fixture = Fixture::new();
        let mut limits = MetadataReadProviderLimits::default();
        limits.workers_per_context = 5;
        assert!(MetadataReadProvider::with_limits(
            fixture.transport.leader.clone(),
            fixture.transport.clone(),
            limits
        )
        .is_err());
        fixture.provider.inflight.store(4, Ordering::SeqCst);
        assert!(fixture.acquire(fixture.request()).is_err());
        assert_eq!(fixture.provider.inflight_contexts(), 4);
    }
    #[test]
    fn held_probe_allows_committed_load_only_update_but_rejects_epoch_change() {
        for change_epoch in [false, true] {
            let fixture = Fixture::new();
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            let (released_tx, released_rx) = mpsc::sync_channel(1);
            *fixture.transport.gate.lock() = Some((entered_tx, released_rx));
            thread::scope(|scope| {
                let acquisition = scope.spawn(|| fixture.acquire(fixture.request()));
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                // Exercise the actual distribution load-update operation and
                // commit its topology through the actual metadata stores.
                let mut loads = DistributionStore::initialize_at(
                    fixture._root.path().join("loads"),
                    DistributionConfig {
                        enabled: true,
                        cluster_id: fixture.topology.cluster_id.clone(),
                        node_id: node("n1"),
                        node_address: "127.0.0.1:10001".into(),
                        replication_factor: 1,
                        initial_ranges: 1,
                        ..DistributionConfig::default()
                    },
                    false,
                    10,
                )
                .unwrap();
                loads
                    .install_authoritative_topology(fixture.topology.clone())
                    .unwrap();
                assert!(loads
                    .update_range_load(RangeId::new(1).unwrap(), 999_999, 876, &node("n1"), 40)
                    .unwrap());
                let mut changed = loads.topology().clone();
                if change_epoch {
                    changed.ranges.values_mut().next().unwrap().epoch += 1;
                }
                let mut leader = fixture.transport.leader.lock();
                let entry = leader.propose_topology(changed).unwrap();
                let response = fixture
                    .transport
                    .follower
                    .lock()
                    .handle_append_request(leader.append_request_from(1, 16).unwrap())
                    .unwrap();
                leader
                    .record_append_response(&node("n2"), &response)
                    .unwrap();
                assert_eq!(leader.status().commit_index, entry.index);
                drop(leader);
                released_tx.send(()).unwrap();
                let result = acquisition.join().unwrap();
                if change_epoch {
                    assert!(result.is_err());
                } else {
                    let response = result.unwrap();
                    let range = response
                        .committed_topology
                        .range_by_id(RangeId::new(1).unwrap())
                        .unwrap();
                    assert_eq!(range.approximate_bytes, 999_999);
                    assert_eq!(range.approximate_qps, 876);
                }
            });
        }
    }

    #[test]
    fn host_metadata_actual_store_rechecks_member_after_quorum_but_does_not_grant_range() {
        for remove_requester in [false, true] {
            let mut topology = topology_with_count(3);
            topology.range_image_bootstrap_id = Some(Uuid::now_v7());
            topology.nodes.get_mut(&node("n3")).unwrap().metadata_role =
                crate::distribution::MetadataMemberRole::Learner;
            let fixture = Fixture::with_topology(topology);
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            let (released_tx, released_rx) = mpsc::sync_channel(1);
            *fixture.transport.gate.lock() = Some((entered_tx, released_rx));
            let request = crate::distribution_consensus::HostMetadataReadAcquireRequest {
                protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
                acquisition_id: Uuid::now_v7(),
                read_request_id: Uuid::now_v7(),
                requester_node_id: node("n3"),
                requester_incarnation: 1,
                budget_ms: 1000,
            };
            thread::scope(|scope| {
                let acquisition = scope.spawn(|| {
                    fixture.provider.acquire_host_metadata(
                        &participant(&fixture.topology, "n3"),
                        request.clone(),
                        &CancellationToken::uncancelable(),
                    )
                });
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                let mut leader = fixture.transport.leader.lock();
                let mut changed = leader.committed_topology().clone();
                changed.generation += 1;
                if remove_requester {
                    changed.nodes.get_mut(&node("n3")).unwrap().lifecycle =
                        crate::distribution::ClusterNodeLifecycle::Decommissioned;
                } else {
                    let range = changed.ranges.values_mut().next().unwrap();
                    range.epoch += 1;
                    range.leader = node("n1");
                    range.replicas[0].node_id = node("n1");
                }
                let entry = leader.propose_topology(changed).unwrap();
                let response = fixture
                    .transport
                    .follower
                    .lock()
                    .handle_append_request(leader.append_request_from(1, 16).unwrap())
                    .unwrap();
                leader
                    .record_append_response(&node("n2"), &response)
                    .unwrap();
                assert_eq!(leader.status().commit_index, entry.index);
                drop(leader);
                released_tx.send(()).unwrap();
                let result = acquisition.join().unwrap();
                if remove_requester {
                    assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("requester incarnation/lifecycle changed"));
                } else {
                    let response = result.unwrap();
                    response.validate(&request).unwrap();
                    let range = response
                        .evidence
                        .committed_topology
                        .ranges
                        .values()
                        .next()
                        .unwrap();
                    assert_eq!(range.epoch, 2);
                    assert_eq!(range.leader, node("n1"));
                    assert_eq!(response.request.requester_node_id, node("n3"));
                }
            });
        }
    }

    #[derive(Debug)]
    struct QuorumTransport {
        topology: ClusterTopology,
        peers: BTreeMap<ClusterNodeId, Mutex<MetadataConsensusStore>>,
        silent: BTreeSet<ClusterNodeId>,
        contacted: Mutex<BTreeSet<ClusterNodeId>>,
        active: AtomicUsize,
        peak: AtomicUsize,
        original_deadline: Instant,
    }
    impl MetadataReadProbeTransport for QuorumTransport {
        fn local_participant(&self) -> MetadataReadParticipant {
            participant(&self.topology, "n1")
        }
        fn probe(
            &self,
            destination: &ClusterNodeId,
            request: MetadataReadProbeRequest,
            token: &CancellationToken,
        ) -> Result<MetadataReadAck> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            struct Active<'a>(&'a AtomicUsize);
            impl Drop for Active<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _active = Active(&self.active);
            assert!(token.deadline().unwrap() <= self.original_deadline);
            self.contacted.lock().insert(destination.clone());
            if self.silent.contains(destination) {
                loop {
                    token.check()?;
                    thread::sleep(Duration::from_millis(2));
                }
            }
            let response = self.peers[destination]
                .lock()
                .handle_read_authority_probe(request.clone())?;
            MetadataReadAck::from_transport(
                &request,
                destination,
                participant(&self.topology, destination.as_str()),
                response,
            )
        }
    }
    fn sliced_quorum_case(
        count: u64,
        workers: usize,
        silent: BTreeSet<ClusterNodeId>,
        expected: bool,
    ) {
        let root = tempfile::tempdir().unwrap();
        let topology = topology_with_count(count);
        let mut leader = open(&root.path().join("n1"), "n1", &topology);
        let mut peers = (2..=count)
            .map(|i| {
                let name = format!("n{i}");
                (
                    node(&name),
                    Mutex::new(open(&root.path().join(&name), &name, &topology)),
                )
            })
            .collect::<BTreeMap<_, _>>();
        leader.start_election().unwrap();
        leader
            .become_leader(&topology.nodes.keys().cloned().collect())
            .unwrap();
        for (id, peer) in &mut peers {
            let response = peer
                .get_mut()
                .handle_append_request(leader.append_request_from(1, 16).unwrap())
                .unwrap();
            leader.record_append_response(id, &response).unwrap();
        }
        let deadline = Instant::now() + Duration::from_millis(900);
        let transport = Arc::new(QuorumTransport {
            topology: topology.clone(),
            peers,
            silent,
            contacted: Mutex::new(BTreeSet::new()),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            original_deadline: deadline,
        });
        let limits = MetadataReadProviderLimits {
            workers_per_context: workers,
            ..MetadataReadProviderLimits::default()
        };
        let provider = MetadataReadProvider::with_limits(
            Arc::new(Mutex::new(leader)),
            transport.clone(),
            limits,
        )
        .unwrap();
        let range = topology.ranges.values().next().unwrap();
        let request = MetadataReadAcquireRequest {
            protocol_version: METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
            acquisition_id: Uuid::now_v7(),
            read_request_id: Uuid::now_v7(),
            requester_node_id: node("n2"),
            requester_incarnation: topology.nodes[&node("n2")].incarnation,
            range_id: range.id,
            range_epoch: range.epoch,
            budget_ms: 900,
        };
        let token = CancellationToken::uncancelable().child_with_deadline(deadline);
        let result = provider.acquire(&participant(&topology, "n2"), request, &token);
        assert_eq!(result.is_ok(), expected, "{result:?}");
        assert!(transport.peak.load(Ordering::SeqCst) <= workers);
        let drain = Instant::now() + Duration::from_secs(1);
        while provider.inflight_contexts() != 0 && Instant::now() < drain {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(provider.inflight_contexts(), 0);
        assert_eq!(transport.active.load(Ordering::SeqCst), 0);
        for id in topology
            .nodes
            .keys()
            .filter(|id| **id != node("n1") && !transport.silent.contains(*id))
        {
            assert!(
                transport.contacted.lock().contains(id),
                "healthy voter {id} was starved"
            );
        }
        if let Ok(response) = result {
            assert!(response.fresh_old_voters.len() >= count as usize / 2 + 1);
        }
    }
    #[test]
    fn four_silent_sorted_voters_do_not_starve_healthy_nine_node_quorum() {
        sliced_quorum_case(
            9,
            4,
            (2..=5).map(|i| node(&format!("n{i}"))).collect(),
            true,
        );
        // The configurable single-worker case must also make progress.
        sliced_quorum_case(3, 1, BTreeSet::from([node("n2")]), true);
    }
    #[test]
    fn sliced_probes_never_admit_an_unavailable_quorum() {
        sliced_quorum_case(
            9,
            4,
            (2..=6).map(|i| node(&format!("n{i}"))).collect(),
            false,
        );
    }

    #[test]
    fn finished_local_authority_keeps_original_context_deadline() {
        let fixture = Fixture::new();
        let leader = fixture.transport.leader.lock();
        let mut context = leader
            .begin_read_authority(
                Uuid::now_v7(),
                Duration::from_millis(100),
                &participant(&fixture.topology, "n1"),
            )
            .unwrap();
        let deadline = context.deadline();
        let request = leader
            .read_authority_probe(&context, &node("n2"), 1, 16)
            .unwrap();
        drop(leader);
        let ack = fixture
            .transport
            .probe(&node("n2"), request, &CancellationToken::uncancelable())
            .unwrap();
        let mut leader = fixture.transport.leader.lock();
        leader
            .record_read_authority_ack(&mut context, &ack)
            .unwrap();
        let authority = leader.finish_read_authority(context).unwrap();
        assert_eq!(authority.deadline(), deadline);
        assert!(authority.remaining_budget().unwrap() <= Duration::from_millis(100));
        drop(leader);
        thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
        );
        assert!(!authority.is_current_budget());
        assert!(authority.remaining_budget().is_none());
    }

    #[cfg(feature = "tls-replication")]
    mod tls {
        use super::*;
        use crate::{
            start_cluster_data_server, start_cluster_data_server_with_metadata_read_provider,
            BicDb, ClusterDataNodeService, ClusterDataServerHandle, ClusterNetworkTransportConfig,
            DbConfig, ReplicationTlsConfig, TcpClusterRelocationTransport,
        };
        use parking_lot::RwLock;
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
            KeyUsagePurpose,
        };
        use std::net::TcpListener;
        use std::path::{Path, PathBuf};

        struct Identity {
            cert: PathBuf,
            key: PathBuf,
            ca: PathBuf,
        }
        impl Identity {
            fn tls(&self) -> ReplicationTlsConfig {
                ReplicationTlsConfig {
                    cert_path: self.cert.clone(),
                    key_path: self.key.clone(),
                    ca_path: self.ca.clone(),
                    require_client_cert: true,
                    dev_localhost_plaintext: false,
                }
            }
            fn pin(&self) -> String {
                crate::replication_transport::replication_certificate_sha256(&self.cert).unwrap()
            }
        }
        fn identities(root: &Path) -> BTreeMap<ClusterNodeId, Identity> {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::default();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![
                KeyUsagePurpose::KeyCertSign,
                KeyUsagePurpose::DigitalSignature,
            ];
            let ca = params.self_signed(&key).unwrap();
            let ca_path = root.join("ca.pem");
            std::fs::write(&ca_path, ca.pem()).unwrap();
            ["n1", "n2", "n3"]
                .into_iter()
                .map(|name| {
                    let leaf_key = KeyPair::generate().unwrap();
                    let mut params =
                        CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
                            .unwrap();
                    params.is_ca = IsCa::ExplicitNoCa;
                    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
                    params.extended_key_usages = vec![
                        ExtendedKeyUsagePurpose::ServerAuth,
                        ExtendedKeyUsagePurpose::ClientAuth,
                    ];
                    let cert = params.signed_by(&leaf_key, &ca, &key).unwrap();
                    let identity = Identity {
                        cert: root.join(format!("{name}.pem")),
                        key: root.join(format!("{name}.key")),
                        ca: ca_path.clone(),
                    };
                    std::fs::write(&identity.cert, cert.pem()).unwrap();
                    std::fs::write(&identity.key, leaf_key.serialize_pem()).unwrap();
                    (node(name), identity)
                })
                .collect()
        }
        struct NetworkFixture {
            _root: tempfile::TempDir,
            _silent: TcpListener,
            servers: Vec<ClusterDataServerHandle>,
            services: Vec<Arc<Mutex<ClusterDataNodeService>>>,
            client: TcpClusterRelocationTransport,
            leader_client: Arc<TcpClusterRelocationTransport>,
            range_epoch: u64,
        }
        impl NetworkFixture {
            fn new(wired: bool) -> Self {
                let root = tempfile::tempdir().unwrap();
                let identities = identities(root.path());
                let mut topology = topology_with_count(3);
                let one = TcpListener::bind("127.0.0.1:0").unwrap();
                let two = TcpListener::bind("127.0.0.1:0").unwrap();
                let silent = TcpListener::bind("127.0.0.1:0").unwrap();
                for (name, listener) in [("n1", &one), ("n2", &two), ("n3", &silent)] {
                    let member = topology.nodes.get_mut(&node(name)).unwrap();
                    member.address = listener.local_addr().unwrap().to_string();
                    member.tls_certificate_sha256 = Some(identities[&node(name)].pin());
                }
                topology.validate().unwrap();
                let range_epoch = topology
                    .range_by_id(RangeId::new(1).unwrap())
                    .unwrap()
                    .epoch;
                let mut leader = open(&root.path().join("consensus-n1"), "n1", &topology);
                let mut follower = open(&root.path().join("consensus-n2"), "n2", &topology);
                leader.start_election().unwrap();
                leader
                    .become_leader(&BTreeSet::from([node("n1"), node("n2")]))
                    .unwrap();
                let response = follower
                    .handle_append_request(leader.append_request_from(1, 16).unwrap())
                    .unwrap();
                leader
                    .record_append_response(&node("n2"), &response)
                    .unwrap();
                let consensus = [Arc::new(Mutex::new(leader)), Arc::new(Mutex::new(follower))];
                let shared = Arc::new(RwLock::new(topology.clone()));
                let network = |name: &str| ClusterNetworkTransportConfig {
                    connect_timeout_ms: 100,
                    io_timeout_ms: 2000,
                    max_frame_bytes: 4 * 1024 * 1024,
                    max_inbound_connections: 8,
                    dev_localhost_plaintext: false,
                    tls: Some(identities[&node(name)].tls()),
                };
                let leader_client = Arc::new(
                    TcpClusterRelocationTransport::new(
                        topology.cluster_id.clone(),
                        node("n1"),
                        shared.clone(),
                        network("n1"),
                    )
                    .unwrap(),
                );
                let provider = Arc::new(
                    MetadataReadProvider::new(consensus[0].clone(), leader_client.clone()).unwrap(),
                );
                let client = TcpClusterRelocationTransport::new(
                    topology.cluster_id.clone(),
                    node("n2"),
                    shared.clone(),
                    network("n2"),
                )
                .unwrap();
                let mut services = Vec::new();
                let mut servers = Vec::new();
                drop(one);
                drop(two);
                for (i, name) in ["n1", "n2"].into_iter().enumerate() {
                    let db = Arc::new(RwLock::new(
                        BicDb::open_with_config(
                            root.path().join(format!("db-{name}")),
                            DbConfig::default().with_fsync(false),
                        )
                        .unwrap(),
                    ));
                    let service = Arc::new(Mutex::new(
                        ClusterDataNodeService::open(
                            topology.cluster_id.clone(),
                            node(name),
                            root.path().join(format!("service-{name}")),
                            db,
                            false,
                        )
                        .unwrap(),
                    ));
                    let address = topology.nodes[&node(name)].address.clone();
                    let server = if wired && i == 0 {
                        start_cluster_data_server_with_metadata_read_provider(
                            &address,
                            network(name),
                            shared.clone(),
                            service.clone(),
                            None,
                            Some(consensus[i].clone()),
                            None,
                            Some(provider.clone()),
                        )
                        .unwrap()
                    } else {
                        start_cluster_data_server(
                            &address,
                            network(name),
                            shared.clone(),
                            service.clone(),
                            None,
                            Some(consensus[i].clone()),
                            None,
                        )
                        .unwrap()
                    };
                    services.push(service);
                    servers.push(server);
                }
                Self {
                    _root: root,
                    _silent: silent,
                    servers,
                    services,
                    client,
                    leader_client,
                    range_epoch,
                }
            }
            fn acquire(&self) -> Result<crate::MetadataReadObservation> {
                self.client.acquire_metadata_read_observation(
                    &node("n1"),
                    Uuid::now_v7(),
                    RangeId::new(1).unwrap(),
                    self.range_epoch,
                    Duration::from_millis(1800),
                    &CancellationToken::uncancelable(),
                )
            }
        }
        impl Drop for NetworkFixture {
            fn drop(&mut self) {
                for server in self.servers.drain(..) {
                    server.join().unwrap();
                }
            }
        }
        #[test]
        fn actual_mtls_range_leader_acquires_from_different_metadata_leader_without_silent_voter_or_data_latch(
        ) {
            let fixture = NetworkFixture::new(true);
            let _one = fixture.services[0].lock();
            let _two = fixture.services[1].lock();
            let started = Instant::now();
            let observation = fixture.acquire().unwrap();
            assert!(started.elapsed() < Duration::from_millis(1400));
            assert_eq!(observation.metadata_leader_id(), &node("n1"));
            assert_eq!(observation.request().requester_node_id, node("n2"));
            assert!(observation
                .committed_topology(observation.request().read_request_id)
                .is_ok());
        }
        #[test]
        fn actual_mtls_role_witness_binds_the_real_recipient_and_initiator() {
            let fixture = NetworkFixture::new(true);
            let request = MetadataRecoveryRoleRequest {
                protocol_version: crate::METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
                acquisition_id: Uuid::now_v7(),
                read_request_id: Uuid::now_v7(),
                initiator_node_id: node("n2"),
                initiator_incarnation: fixture
                    .client
                    .local_metadata_read_participant()
                    .member_incarnation(),
                initiator_certificate_sha256: fixture
                    .client
                    .local_metadata_read_participant()
                    .certificate_sha256()
                    .map(str::to_string),
                initiator_plaintext_loopback: false,
                recipient_node_id: node("n2"),
                recipient_incarnation: fixture
                    .client
                    .local_metadata_read_participant()
                    .member_incarnation(),
                range_id: RangeId::new(1).unwrap(),
                range_epoch: fixture.range_epoch,
                recovery_nonce: Uuid::now_v7(),
                recipient_challenge: Uuid::now_v7(),
                purpose: MetadataRecoveryRolePurpose::NormalRecovery,
                budget_ms: 1500,
            };
            let observation = fixture
                .client
                .acquire_metadata_recovery_role_observation(
                    &node("n1"),
                    request.clone(),
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            observation
                .require_initiator(&fixture.client.local_metadata_read_participant())
                .unwrap();
            assert!(observation
                .require_initiator(&fixture.leader_client.local_metadata_read_participant())
                .is_err());
            let mut wrong = request;
            wrong.recipient_node_id = node("n3");
            assert!(fixture
                .client
                .acquire_metadata_recovery_role_observation(
                    &node("n1"),
                    wrong,
                    &CancellationToken::uncancelable()
                )
                .is_err());
        }

        #[test]
        fn unwired_server_and_non_range_leader_refuse_acquisition() {
            let fixture = NetworkFixture::new(false);
            assert!(fixture.acquire().is_err());
            drop(fixture);
            let fixture = NetworkFixture::new(true);
            assert!(fixture
                .leader_client
                .acquire_metadata_read_observation(
                    &node("n1"),
                    Uuid::now_v7(),
                    RangeId::new(1).unwrap(),
                    fixture.range_epoch,
                    Duration::from_millis(500),
                    &CancellationToken::uncancelable()
                )
                .is_err());
            assert!(fixture
                .client
                .acquire_metadata_read_observation(
                    &node("n1"),
                    Uuid::now_v7(),
                    RangeId::new(1).unwrap(),
                    fixture.range_epoch + 1,
                    Duration::from_millis(500),
                    &CancellationToken::uncancelable()
                )
                .is_err());
        }
    }
}
