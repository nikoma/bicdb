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
                let _operation = operation;
                let result = runtime.observe_current_native_range_inner(&request, &worker_token);
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
        token.check()?;
        let service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        let actual = service.observe_current_native_range_probe(request, token)?;
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
        let operation = self.reserve(request.scope.range.id)?;
        let token = request.cancellation(parent)?;
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
                    let _operation = operation;
                    while worker.check().is_ok() {
                        let job = match rx.lock().recv_timeout(LATCH_POLL) {
                            Ok(job) => job,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
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
                tx.try_send(ReadJob {
                    node: node.clone(),
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
        let (out, result) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("native-read-candidate".into())
            .spawn(move || {
                let _operation = operation;
                let verdict = (|| {
                    finish_token.check()?;
                    let service = loop {
                        finish_token.check()?;
                        if let Some(service) = runtime.service.try_lock_for(LATCH_POLL) {
                            break service;
                        }
                    };
                    round.finish(&service)
                })();
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
