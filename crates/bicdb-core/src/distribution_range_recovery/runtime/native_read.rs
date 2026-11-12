//! Read-only observation workers share receiver capacity, but never create a
//! recovery session/freeze. All actual IO, including local IO, runs in bounded
//! owned workers. OS IO may outlive cancellation; its context stays retained.
use super::*;
use crate::distribution_consensus::MetadataReadObservation;
use crate::distribution_native_read::{
    require_peer, NativeRangeReadObservation, NativeRangeReadProbeRequest,
    NativeReadQuorumCandidate, NativeReadQuorumRound,
};
use std::collections::VecDeque;
use std::sync::mpsc;

struct CancelRead(Option<CancellationToken>);
impl Drop for CancelRead {
    fn drop(&mut self) {
        if let Some(token) = &self.0 {
            token.cancel();
        }
    }
}
struct ReadJob {
    node: ClusterNodeId,
    token: CancellationToken,
    // STRONG_TRACE_BEGIN
    #[cfg(test)]
    attempt: usize,
    #[cfg(test)]
    queued: Instant,
    // STRONG_TRACE_END
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct NativeReadIoBarrier {
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl NativeReadIoBarrier {
    pub(crate) fn entered(&self) -> bool {
        self.entered.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub(crate) fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl RangeRecoveryRuntime {
    #[cfg(test)]
    pub(crate) fn test_hold_native_read_io(&self) -> Arc<NativeReadIoBarrier> {
        let barrier = Arc::new(NativeReadIoBarrier::default());
        *self.test_read_io_barrier.lock() = Some(Arc::clone(&barrier));
        barrier
    }
    /// Incoming authenticated observation only. An untrusted declared leader
    /// is checked against actual physical configuration by the same service.
    pub(crate) fn observe_current_native_range(
        self: &Arc<Self>,
        caller: &MetadataReadParticipant,
        request: &NativeRangeReadProbeRequest,
        parent: &CancellationToken,
    ) -> Result<NativeRangeReadObservation> {
        let token = request.cancellation(parent)?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let _read_context = crate::distribution_native_read::trace::Context::enter(
            &self.local_node_id,
            request.business_request_id,
            request.challenge,
            request.scope.range.id,
            request.scope.range.epoch,
            token.deadline(),
        );
        #[cfg(test)]
        let _read_receiver =
            crate::distribution_native_read::trace::Span::current("read-receiver-dispatch");
        // STRONG_TRACE_END
        require_peer(&request.scope, caller)?;
        if caller.node_id() != &request.scope.range.leader {
            return Err(invalid(
                "native read probe requires authenticated scoped leader",
            ));
        }
        let operation = self.reserve_observer(request.scope.range.id)?;
        let worker_token = token.child();
        let mut cancel = CancelRead(Some(worker_token.clone()));
        let runtime = Arc::clone(self);
        let request = request.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("native-read-receiver".into())
            .spawn(move || {
                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                let _receiver_context = crate::distribution_native_read::trace::Context::enter(
                    &runtime.local_node_id,
                    request.business_request_id,
                    request.challenge,
                    request.scope.range.id,
                    request.scope.range.epoch,
                    worker_token.deadline(),
                );
                #[cfg(test)]
                let _receiver_worker =
                    crate::distribution_native_read::trace::Span::current("read-receiver-worker");
                // STRONG_TRACE_END
                let _operation = operation;

                let result = runtime.observe_current_native_range_inner(&request, &worker_token);

                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                crate::distribution_native_read::trace::Span::detail(
                    "read-receiver-result",
                    format_args!("ok={} error={:?}", result.is_ok(), result.as_ref().err()),
                );
                // STRONG_TRACE_END
                let _ = tx.send(result);
            })?;
        loop {
            token.check()?;
            match rx.recv_timeout(LATCH_POLL) {
                Ok(result) => {
                    token.check()?;
                    cancel.0.take();
                    return result;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("native read receiver worker disconnected"))
                }
            }
        }
    }
    fn observe_current_native_range_inner(
        &self,
        request: &NativeRangeReadProbeRequest,
        token: &CancellationToken,
    ) -> Result<NativeRangeReadObservation> {
        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let _read_context = crate::distribution_native_read::trace::Context::enter(
            &self.local_node_id,
            request.business_request_id,
            request.challenge,
            request.scope.range.id,
            request.scope.range.epoch,
            token.deadline(),
        );
        #[cfg(test)]
        let read_service_wait =
            crate::distribution_native_read::trace::Span::current("read-service-wait");
        // STRONG_TRACE_END
        token.check()?;
        let service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_service_wait);
        #[cfg(test)]
        let read_service_validation =
            crate::distribution_native_read::trace::Span::current("read-service-validation");
        // STRONG_TRACE_END
        let actual = service.observe_current_native_range_probe(request, token)?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_service_validation);
        // STRONG_TRACE_END
        drop(service);
        #[cfg(test)]
        if let Some(barrier) = self.test_read_io_barrier.lock().take() {
            barrier
                .entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            // Test-only bounded stand-in for an uncancellable IO return after
            // the REAL producer. No reply/facts are manufactured or altered.
            let until = Instant::now() + Duration::from_secs(5);
            while !barrier.released.load(std::sync::atomic::Ordering::SeqCst)
                && Instant::now() < until
            {
                std::thread::sleep(LATCH_POLL);
            }
        }
        token.check()?;
        NativeRangeReadObservation::from_actual_service(actual, self.provider.local_participant())
    }
    /// The result is deliberately UNADMITTED. Actual generic fresh metadata
    /// is consumed, but durable host/birth enrollment and snapshot/final-MQ
    /// acceptance are separate Root-owned gates.
    pub(crate) fn acquire_native_read_candidate(
        self: &Arc<Self>,
        metadata: MetadataReadObservation,
        business: Uuid,
        challenge: Uuid,
        parent: &CancellationToken,
    ) -> Result<NativeReadQuorumCandidate> {
        let operation = self.reserve(metadata.request().range_id)?;
        self.acquire_native_read_candidate_with_operation(
            metadata, business, challenge, parent, operation,
        )
    }
    pub(super) fn acquire_native_read_candidate_with_operation(
        self: &Arc<Self>,
        metadata: MetadataReadObservation,
        business: Uuid,
        challenge: Uuid,
        parent: &CancellationToken,
        operation: Arc<Operation>,
    ) -> Result<NativeReadQuorumCandidate> {
        let mut round = NativeReadQuorumRound::begin(
            metadata,
            business,
            challenge,
            &self.local_node_id,
            parent,
        )?;
        let request = round.request().clone();
        require_peer(&request.scope, &self.provider.local_participant())?;
        // Exclusive COLLECTOR reservation (max2) leaves receiver headroom.
        // Local inner probing reuses it rather than recursively reserving.
        if operation.range != request.scope.range.id || !operation.exclusive {
            return Err(invalid(
                "native read collector operation is for another range/purpose",
            ));
        }
        let token = request.cancellation(parent)?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let _read_context = crate::distribution_native_read::trace::Context::enter(
            &self.local_node_id,
            request.business_request_id,
            request.challenge,
            request.scope.range.id,
            request.scope.range.epoch,
            token.deadline(),
        );
        #[cfg(test)]
        let _read_collector =
            crate::distribution_native_read::trace::Span::current("read-collector");
        // STRONG_TRACE_END
        let worker_token = token.child();
        let _cancel = CancelRead(Some(worker_token.clone()));
        let members = request.scope.members.keys().cloned().collect::<Vec<_>>();
        let workers = members.len().min(4);
        let mut queue = members
            .into_iter()
            .map(|n| (n, 0usize))
            .collect::<VecDeque<_>>();
        // Local first; peer scheduling remains fair after finite remote slices.
        queue
            .make_contiguous()
            .sort_by_key(|(n, _)| n != &self.local_node_id);
        let (tx, rx) = mpsc::sync_channel::<ReadJob>(workers);
        let rx = Arc::new(Mutex::new(rx));
        let (out, results) = mpsc::sync_channel(workers);
        for _ in 0..workers {
            let runtime = Arc::clone(self);
            let rx = Arc::clone(&rx);
            let out = out.clone();
            let worker = worker_token.clone();
            let operation = Arc::clone(&operation);
            let request = request.clone();
            let proof_token = token.clone();
            std::thread::Builder::new()
                .name("native-read-probe".into())
                .spawn(move || {

// STRONG_TRACE_BEGIN
#[cfg(test)]
let _probe_worker_context = crate::distribution_native_read::trace::Context::enter(
    &runtime.local_node_id, request.business_request_id, request.challenge,
    request.scope.range.id, request.scope.range.epoch, proof_token.deadline());
#[cfg(test)]
let _probe_worker = crate::distribution_native_read::trace::Span::current("read-probe-worker-owned-context");
// STRONG_TRACE_END
                    let _operation = operation;
                    while worker.check().is_ok() {
                        let job = match rx.lock().recv_timeout(LATCH_POLL) {
                            Ok(job) => job,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };

// STRONG_TRACE_BEGIN
#[cfg(test)]
let _job_context = crate::distribution_native_read::trace::Context::enter(
    &runtime.local_node_id, request.business_request_id, request.challenge,
    request.scope.range.id, request.scope.range.epoch, job.token.deadline());
#[cfg(test)]
let read_attempt = crate::distribution_native_read::trace::Span::current("read-attempt");
#[cfg(test)]
crate::distribution_native_read::trace::Span::detail("read-attempt", format_args!("peer={} attempt={} queued_us={} request_budget_ms={} parent_deadline_us={:?} local={}", job.node, job.attempt, job.queued.elapsed().as_micros(), request.budget_ms, crate::distribution_native_read::trace::deadline_us(proof_token.deadline()), job.node == runtime.local_node_id));
// STRONG_TRACE_END
                        let result = job.token.check().and_then(|_| {
                            if job.node == runtime.local_node_id {
                                runtime.observe_current_native_range_inner(&request, &proof_token)
                            } else {
                                runtime
                                    .participant_transport
                                    .as_ref()
                                    .ok_or_else(|| {
                                        invalid("authenticated native read transport is not wired")
                                    })?
                                    .observe_current_native_range(&job.node, &request, &job.token)
                            }
                        });

// STRONG_TRACE_BEGIN
#[cfg(test)]
crate::distribution_native_read::trace::Span::detail("read-attempt-result", format_args!("peer={} attempt={} ok={} error={:?}", job.node, job.attempt, result.is_ok(), result.as_ref().err()));
#[cfg(test)]
drop(read_attempt);
// STRONG_TRACE_END

// STRONG_TRACE_BEGIN
#[cfg(test)]
if let Ok(observation) = &result {
    let facts = observation.facts();
    crate::distribution_native_read::trace::Span::detail("read-attempt-observed-point", format_args!("peer={} attempt={} R={} H={} local_B={} requested_metadata_context={}", job.node, job.attempt, facts.range_index, facts.logical_history_sha256, facts.local_visibility_db_sequence, request.requested_metadata_context_id));
}
// STRONG_TRACE_END
                        if out.send((job.node, result)).is_err() {
                            break;
                        }
                    }
                })?;
        }
        drop(out);
        let mut outstanding = BTreeMap::new();
        let mut failures = BTreeMap::new();
        loop {
            token.check()?;
            if round.has_matching_majority()? {
                // Close the factual collection after draining all results
                // already delivered. A known higher/conflicting point vetoes;
                // a stalled minority never holds the majority verdict hostage.
                while let Ok((node, result)) = results.try_recv() {
                    outstanding
                        .remove(&node)
                        .ok_or_else(|| invalid("unexpected native read drain result"))?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    crate::distribution_native_read::trace::Span::detail(
                        "read-result-received",
                        format_args!(
                            "peer={} ok={} error={:?} path=majority-drain",
                            node,
                            result.is_ok(),
                            result.as_ref().err()
                        ),
                    );
                    // STRONG_TRACE_END

                    match result {
                        Ok(observation) => {
                            round.record_attempt(observation)?;
                        }
                        Err(error) => {
                            failures.insert(node, error.to_string());
                        }
                    }
                }
                if round.has_matching_majority()? {
                    break;
                }
            }
            while outstanding.len() < workers {
                let Some((node, attempt)) = queue.pop_front() else {
                    break;
                };
                let remaining = token
                    .deadline()
                    .and_then(|d| d.checked_duration_since(Instant::now()))
                    .ok_or_else(|| invalid("native read round expired"))?;
                let waves = (queue.len() + outstanding.len() + 1).div_ceil(workers);
                let child = if node == self.local_node_id {
                    worker_token.clone()
                } else {
                    worker_token.child_with_deadline(
                        Instant::now() + remaining / u32::try_from(waves + 2).unwrap_or(u32::MAX),
                    )
                };

                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                crate::distribution_native_read::trace::Span::detail(
                    "read-schedule",
                    format_args!(
                        "peer={} attempt={} waves={} outstanding={} child_deadline_us={:?}",
                        node,
                        attempt + 1,
                        waves,
                        outstanding.len(),
                        crate::distribution_native_read::trace::deadline_us(child.deadline())
                    ),
                );
                // STRONG_TRACE_END
                tx.try_send(ReadJob {
                    node: node.clone(),
                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    attempt: attempt + 1,
                    #[cfg(test)]
                    queued: Instant::now(),
                    // STRONG_TRACE_END
                    token: child,
                })
                .map_err(|_| invalid("bounded native read job queue invariant failed"))?;
                outstanding.insert(node, attempt + 1);
            }
            if outstanding.is_empty() {
                break;
            }
            match results.recv_timeout(LATCH_POLL) {
                Ok((node, result)) => {
                    let attempt = outstanding
                        .remove(&node)
                        .ok_or_else(|| invalid("unexpected native read worker result"))?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    crate::distribution_native_read::trace::Span::detail(
                        "read-result-received",
                        format_args!(
                            "peer={} ok={} error={:?}",
                            node,
                            result.is_ok(),
                            result.as_ref().err()
                        ),
                    );
                    // STRONG_TRACE_END
                    match result {
                        Ok(observation) => {
                            if !round.record_attempt(observation)? {
                                failures.insert(
                                    node.clone(),
                                    "expired authenticated attempt; point retained without vote"
                                        .into(),
                                );
                                if attempt < 2 {
                                    queue.push_back((node, attempt));
                                }
                            }
                        }
                        Err(error) => {
                            failures.insert(node.clone(), error.to_string());
                            if attempt < 2 && node != self.local_node_id {
                                queue.push_back((node, attempt));
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("native read probe workers disconnected"))
                }
            }
        }
        drop(tx);
        worker_token.cancel();
        // Local catalogue/history revalidation is also actual IO: run it in an
        // owned worker, retaining this same context through its actual drain.
        let runtime = Arc::clone(self);
        let finish_token = token.clone();
        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let finish_labels = (
            request.business_request_id,
            request.challenge,
            request.scope.range.id,
            request.scope.range.epoch,
        );
        #[cfg(test)]
        let finish_queued = Instant::now();
        // STRONG_TRACE_END

        let (out, result) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("native-read-candidate".into())
            .spawn(move || {
                let _operation = operation;

                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                let _finish_context = crate::distribution_native_read::trace::Context::enter(
                    &runtime.local_node_id,
                    finish_labels.0,
                    finish_labels.1,
                    finish_labels.2,
                    finish_labels.3,
                    finish_token.deadline(),
                );
                #[cfg(test)]
                let read_finish =
                    crate::distribution_native_read::trace::Span::current("read-finish-worker");
                #[cfg(test)]
                crate::distribution_native_read::trace::Span::detail(
                    "read-finish-worker",
                    format_args!("queued_us={}", finish_queued.elapsed().as_micros()),
                );
                // STRONG_TRACE_END
                let verdict = (|| {
                    finish_token.check()?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    let finish_service_wait = crate::distribution_native_read::trace::Span::current(
                        "read-finish-service-wait",
                    );
                    // STRONG_TRACE_END
                    let service = loop {
                        finish_token.check()?;
                        if let Some(service) = runtime.service.try_lock_for(LATCH_POLL) {
                            break service;
                        }
                    };

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    drop(finish_service_wait);
                    #[cfg(test)]
                    let _finish_validation = crate::distribution_native_read::trace::Span::current(
                        "read-finish-validation",
                    );
                    // STRONG_TRACE_END
                    round.finish(&service)
                })();

                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                crate::distribution_native_read::trace::Span::detail(
                    "read-finish-result",
                    format_args!("ok={} error={:?}", verdict.is_ok(), verdict.as_ref().err()),
                );
                #[cfg(test)]
                drop(read_finish);
                // STRONG_TRACE_END
                let _ = out.send(verdict);
            })?;
        loop {
            token.check()?;
            match result.recv_timeout(LATCH_POLL) {
                Ok(verdict) => {
                    token.check()?;
                    return verdict
                        .map_err(|e| invalid(&format!("{e}; failed probes: {failures:?}")));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("native read candidate worker disconnected"))
                }
            }
        }
    }
}
