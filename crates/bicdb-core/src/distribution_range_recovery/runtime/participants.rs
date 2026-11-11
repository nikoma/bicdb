//! Recipient-local quorum collection. Only readonly observations cross the
//! network here; no service/DB latch or native mutation permit crosses an RPC.
use super::*;
use crate::distribution_range_consensus::RangeWriteTransport;
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc;

const WORKERS: usize = 4;
const ATTEMPTS: usize = 2;
struct Job {
    phase: u64,
    node: ClusterNodeId,
    request: RecoveryFencedProbeRequest,
    cancellation: CancellationToken,
}
struct ParticipantPool {
    jobs: mpsc::SyncSender<Job>,
    results: mpsc::Receiver<(u64, ClusterNodeId, Result<RecoveryHistoryObservation>)>,
    cancellation: CancellationToken,
    workers: usize,
    next_phase: u64,
}
impl Drop for ParticipantPool {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
impl ParticipantPool {
    fn new(
        transport: Option<Arc<dyn RangeWriteTransport>>,
        count: usize,
        parent: &CancellationToken,
        operation: &Arc<Operation>,
    ) -> Result<Self> {
        let workers = WORKERS.min(count);
        if workers > 0 && transport.is_none() {
            return Err(invalid("participant observation transport is not wired"));
        }
        let cancellation = parent.child();
        let (jobs, rx) = mpsc::sync_channel::<Job>(workers.max(1));
        let rx = Arc::new(Mutex::new(rx));
        let (tx, results) = mpsc::sync_channel(workers.max(1));
        // Construct the cancellation owner before spawning: a partial spawn
        // failure cancels already-started workers and retains their permits.
        let pool = Self {
            jobs,
            results,
            cancellation,
            workers,
            next_phase: 0,
        };
        for _ in 0..workers {
            let rx = Arc::clone(&rx);
            let tx = tx.clone();
            let transport = Arc::clone(transport.as_ref().unwrap());
            let token = pool.cancellation.clone();
            let permit = Arc::clone(operation);
            std::thread::Builder::new()
                .name("range-recovery-observer".into())
                .spawn(move || {
                    let _permit = permit;
                    while token.check().is_ok() {
                        let job = match rx.lock().recv_timeout(LATCH_POLL) {
                            Ok(job) => job,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        let result = job.cancellation.check().and_then(|_| {
                            transport.recovery_fenced_history_observation(
                                &job.node,
                                &job.request,
                                &job.cancellation,
                            )
                        });
                        if tx.send((job.phase, job.node, result)).is_err() {
                            break;
                        }
                    }
                })?;
        }
        Ok(pool)
    }
    fn collect(
        &mut self,
        runtime: &RangeRecoveryRuntime,
        scope: &RecoveryFenceScope,
        members: &[ClusterNodeId],
        index: Option<u64>,
        parent: &CancellationToken,
        mut record: impl FnMut(RecoveryHistoryObservation) -> Result<bool>,
    ) -> Result<()> {
        parent.check()?;
        self.next_phase += 1;
        let phase = self.next_phase;
        let cancellation = parent.child();
        struct Cancel(CancellationToken);
        impl Drop for Cancel {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let _cancel = Cancel(cancellation.clone());
        // The local branch acquires its own readonly metadata witness but
        // does not recursively reserve this already-owned logical operation.
        if members.contains(&runtime.local_node_id) {
            let request = RecoveryFencedProbeRequest {
                scope: scope.clone(),
                index,
                budget_ms: remaining_ms(&cancellation)?,
            };
            if let Ok(observation) = runtime.observe_history_inner(
                &runtime.provider.local_participant(),
                &request,
                &cancellation,
            ) {
                if record(observation)? {
                    return Ok(());
                }
            }
        }
        let mut queue = members
            .iter()
            .filter(|n| *n != &runtime.local_node_id)
            .cloned()
            .map(|node| (node, 0usize))
            .collect::<VecDeque<_>>();
        let mut outstanding = BTreeMap::new();
        let mut last_error = None;
        loop {
            cancellation.check()?;
            while outstanding.len() < self.workers {
                let Some((node, attempt)) = queue.pop_front() else {
                    break;
                };
                let remaining = cancellation
                    .deadline()
                    .and_then(|d| d.checked_duration_since(Instant::now()))
                    .ok_or_else(|| invalid("participant round deadline expired"))?;
                let waves = (queue.len() + outstanding.len() + 1).div_ceil(self.workers.max(1));
                let slice = remaining / u32::try_from(waves + 2).unwrap_or(u32::MAX);
                if slice.is_zero() {
                    return Err(invalid("participant deadline slice exhausted"));
                }
                let token = cancellation.child_with_deadline(Instant::now() + slice);
                let request = RecoveryFencedProbeRequest {
                    scope: scope.clone(),
                    index,
                    budget_ms: remaining_ms(&token)?,
                };
                match self.jobs.try_send(Job {
                    phase,
                    node: node.clone(),
                    request,
                    cancellation: token,
                }) {
                    Ok(()) => {
                        outstanding.insert(node, attempt + 1);
                    }
                    Err(mpsc::TrySendError::Full(_)) => {
                        // A canceled prior phase may still occupy bounded
                        // queue slots. Do not turn healthy quorum progress
                        // into an admission failure or add more workers.
                        queue.push_front((node, attempt));
                        break;
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        return Err(invalid("participant job workers disconnected"))
                    }
                }
            }
            if outstanding.is_empty() && queue.is_empty() {
                return Err(invalid(&format!(
                    "current participant quorum unavailable: {}",
                    last_error.unwrap_or_else(|| "no qualifying observations".into())
                )));
            }
            match self.results.recv_timeout(LATCH_POLL) {
                Ok((old_phase, _, _)) if old_phase != phase => continue,
                Ok((_, node, result)) => {
                    let attempt = outstanding
                        .remove(&node)
                        .ok_or_else(|| invalid("unexpected participant worker result"))?;
                    match result {
                        Ok(observation) => {
                            if record(observation)? {
                                return Ok(());
                            }
                        }
                        Err(error) => {
                            last_error = Some(format!("{node}: {error}"));
                            if attempt < ATTEMPTS {
                                queue.push_back((node, attempt));
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("participant workers disconnected"))
                }
            }
        }
    }
}
fn remaining_ms(token: &CancellationToken) -> Result<u64> {
    token.check()?;
    let duration = token
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .ok_or_else(|| invalid("historical operation requires a finite original deadline"))?;
    Ok(u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .clamp(1, 5000))
}
impl RangeRecoveryRuntime {
    fn historical_decision(
        &self,
        initiator: &MetadataReadParticipant,
        request: &RecoveryHistoricalCommandRequest,
        token: &CancellationToken,
        operation: &Arc<Operation>,
    ) -> Result<(
        Arc<HistoricalCommandDecision>,
        Arc<FrozenRangeMutationPermit>,
        ParticipantPool,
    )> {
        let scope = &request.scope;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let witness = self.provider.witness_recovery_role(
            initiator,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            scope.purpose.clone(),
            token,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            initiator,
            &self.local_node_id,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            &scope.purpose,
        )?;
        scope.require_role(&role)?;
        if !role.member_credentials().contains_key(&self.local_node_id) {
            return Err(invalid(
                "historical participant must be an actual current/old data voter",
            ));
        }
        let cancellation = token.child_with_deadline(role.deadline());
        {
            let service = loop {
                cancellation.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.preflight_native_recovery(&role, scope, &cancellation)?;
        }
        let frozen = self.frozen(scope, &cancellation, true)?;
        {
            let mut service = loop {
                cancellation.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.fence_native_recovery(&role, scope, &frozen, &cancellation)?;
        }
        let members = role
            .member_credentials()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut pool = ParticipantPool::new(
            self.participant_transport.clone(),
            members.len().saturating_sub(1),
            &cancellation,
            operation,
        )?;
        let mut quorum = RecoveryFenceQuorumRound::begin(role, &cancellation)?;
        let required = quorum.required_quorum();
        let mut count = 0;
        pool.collect(self, scope, &members, None, &cancellation, |observation| {
            quorum.record(observation.fence_observation())?;
            count += 1;
            Ok(count >= required)
        })?;
        let authority = Arc::new(quorum.finish()?);
        let mut decision =
            HistoricalCommandDecisionRound::begin(authority, request.command.clone())?;
        pool.collect(
            self,
            scope,
            &members,
            Some(request.command.index),
            &cancellation,
            |observation| {
                decision.record(observation)?;
                Ok(decision.is_ready())
            },
        )?;
        Ok((Arc::new(decision.finish()?), frozen, pool))
    }
    pub(crate) fn certify_historical(
        &self,
        initiator: &MetadataReadParticipant,
        request: &RecoveryHistoricalCommandRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        self.certify_historical_inner(initiator, request, parent, false)
    }
    pub(crate) fn certify_sparse_historical(
        &self,
        initiator: &MetadataReadParticipant,
        request: &RecoveryHistoricalCommandRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        self.certify_historical_inner(initiator, request, parent, true)
    }
    fn certify_historical_inner(
        &self,
        initiator: &MetadataReadParticipant,
        request: &RecoveryHistoricalCommandRequest,
        parent: &CancellationToken,
        sparse: bool,
    ) -> Result<RecoveryHistoryObservation> {
        let cancellation = request.cancellation(parent)?;
        let operation = self.reserve(request.scope.range_id)?;
        let (decision, frozen, _pool) =
            self.historical_decision(initiator, request, &cancellation, &operation)?;
        let role = decision.authority().role();
        {
            let mut service = loop {
                cancellation.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            if sparse {
                service.certify_sparse_native_historical_command(
                    role,
                    &decision,
                    &frozen,
                    &cancellation,
                )?;
            } else {
                service.certify_native_historical_command(
                    role,
                    &decision,
                    &frozen,
                    &cancellation,
                )?;
            }
        }
        self.observe_history_inner(
            &self.provider.local_participant(),
            &request.probe()?,
            &cancellation,
        )
    }
    pub(crate) fn apply_historical(
        &self,
        initiator: &MetadataReadParticipant,
        request: &RecoveryHistoricalCommandRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        let cancellation = request.cancellation(parent)?;
        let operation = self.reserve(request.scope.range_id)?;
        let (decision, frozen, mut pool) =
            self.historical_decision(initiator, request, &cancellation, &operation)?;
        let members = decision
            .authority()
            .member_credentials()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let role = decision.authority().role();
        let mut certification = CurrentHistoricalCertificationRound::begin(Arc::clone(&decision))?;
        pool.collect(
            self,
            &request.scope,
            &members,
            Some(request.command.index),
            &cancellation,
            |observation| {
                if observation.current_certification().is_none() {
                    return Ok(false);
                }
                certification.record(observation)?;
                Ok(certification.is_ready())
            },
        )?;
        let admission = certification.finish()?;
        {
            let mut service = loop {
                cancellation.check()?;
                if let Some(s) = self.service.try_lock_for(LATCH_POLL) {
                    break s;
                }
            };
            service.apply_native_historical_command(role, &admission, &frozen, &cancellation)?;
        }
        self.observe_history_inner(
            &self.provider.local_participant(),
            &request.probe()?,
            &cancellation,
        )
    }
}
