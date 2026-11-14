//! Recipient-owned closed readiness collection. This collects only actual
//! frozen status proofs; it never sends a claimed certificate/voter list.
use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc;

struct ReadyJob {
    node: ClusterNodeId,
    request: NativeReadyRequest,
    token: CancellationToken,
    #[cfg(test)]
    queued_at: Instant,
    #[cfg(test)]
    attempt: usize,
    #[cfg(test)]
    parent_deadline: Option<Instant>,
}
impl RangeRecoveryRuntime {
    pub(crate) fn acquire_closed_native_ready(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<(
        Arc<crate::distribution_range_consensus::PreparedNativeRecoveryReady>,
        ClosedNativeReadyQuorum,
    )> {
        let operation = self.reserve(request.scope.range_id)?;
        self.acquire_closed_native_ready_with_operation(initiator, request, parent, operation)
    }
    pub(super) fn acquire_closed_native_ready_with_operation(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
        operation: Arc<Operation>,
    ) -> Result<(
        Arc<crate::distribution_range_consensus::PreparedNativeRecoveryReady>,
        ClosedNativeReadyQuorum,
    )> {
        let token = request.cancellation(parent)?;
        #[cfg(test)]
        let mut collector_trace = NativeProbeTraceSpan::new(
            "ready",
            "collector-total",
            &self.local_node_id,
            &request.scope,
            request.challenge,
            token.deadline(),
        );
        #[cfg(test)]
        NativeProbeTraceSpan::event(
            "ready",
            "collector-original-parent",
            &self.local_node_id,
            &request.scope,
            request.challenge,
            parent.deadline(),
            format!(
                "request_budget_ms={} effective_collector_deadline_us={:?}",
                request.budget_ms,
                NativeProbeTraceSpan::deadline_us(token.deadline())
            ),
        );
        let scope = &request.scope;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        #[cfg(test)]
        let mut role_trace = NativeProbeTraceSpan::new(
            "ready",
            "collector-role",
            &self.local_node_id,
            scope,
            request.challenge,
            token.deadline(),
        );
        let witness = self.provider.witness_recovery_role(
            initiator,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            scope.purpose.clone(),
            &token,
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
        let token = token.child_with_deadline(role.deadline());
        #[cfg(test)]
        {
            role_trace.detail(format!(
                "role_context={} role_remaining_ms={} witness_challenge={}",
                role.context_id(),
                role.deadline()
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
                challenge
            ));
            role_trace.complete();
            drop(role_trace);
        }
        let prepared = self
            .retained_native_prepared(scope)?
            .ok_or_else(|| invalid("local replica has no actual owned native preparation"))?;
        let members = role
            .member_credentials()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut round = ClosedNativeReadyQuorumRound::begin(role, &token)?;
        // Local status uses the same independently challenged readonly role as
        // remote status. It does not recursively reserve this operation.
        let local = NativeReadyRequest {
            scope: scope.clone(),
            challenge: Uuid::now_v7(),
            budget_ms: ready_remaining_ms(&token)?,
        };
        #[cfg(test)]
        let mut local_trace = NativeProbeTraceSpan::new(
            "ready",
            "collector-local-status",
            &self.local_node_id,
            scope,
            local.challenge,
            token.deadline(),
        );
        let local_result =
            self.observe_native_ready_inner(&self.provider.local_participant(), &local, &token);
        #[cfg(test)]
        {
            local_trace.detail(format!(
                "result_ok={} error={:?}",
                local_result.is_ok(),
                local_result.as_ref().err().map(ToString::to_string)
            ));
            if local_result.is_ok() {
                local_trace.complete();
            }
            drop(local_trace);
        }
        if let Ok(observation) = local_result {
            #[cfg(test)]
            let mut record_trace = NativeProbeTraceSpan::new(
                "ready",
                "collector-local-record",
                &self.local_node_id,
                scope,
                local.challenge,
                token.deadline(),
            );
            let recorded = round.record(observation);
            #[cfg(test)]
            {
                record_trace.detail(format!(
                    "result_ok={} error={:?}",
                    recorded.is_ok(),
                    recorded.as_ref().err().map(ToString::to_string)
                ));
                if recorded.is_ok() {
                    record_trace.complete();
                }
            }
            recorded?;
            if round.is_ready() {
                let closed = round.finish()?;
                #[cfg(test)]
                collector_trace.complete();
                return Ok((prepared, closed));
            }
        }
        let mut queue = members
            .into_iter()
            .filter(|n| n != &self.local_node_id)
            .map(|n| (n, 0usize))
            .collect::<VecDeque<_>>();
        let workers = 4.min(queue.len());
        let transport = self
            .participant_transport
            .as_ref()
            .ok_or_else(|| invalid("native-ready participant transport is not wired"))?;
        let worker_token = token.child();
        // Declared before the channels: they disconnect first, then teardown
        // waits, bounded, for the status workers to release this operation.
        let mut status_workers = CollectionWorkers::new(worker_token.clone());
        let (jobs, rx) = mpsc::sync_channel::<ReadyJob>(workers.max(1));
        let rx = Arc::new(Mutex::new(rx));
        let (tx, results) = mpsc::sync_channel(workers.max(1));
        for _ in 0..workers {
            let rx = Arc::clone(&rx);
            let tx = tx.clone();
            let transport = Arc::clone(transport);
            let token = worker_token.clone();
            let permit = Arc::clone(&operation);
            let handle = std::thread::Builder::new()
                .name("native-ready-status".into())
                .spawn(move || {
                    let _permit = permit;
                    while token.check().is_ok() {
                        let job = match rx.lock().recv_timeout(LATCH_POLL) {
                            Ok(j) => j,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        #[cfg(test)]
                        let mut peer_trace = NativeProbeTraceSpan::new("ready", "peer-worker-send-return", &job.node, &job.request.scope, job.request.challenge, job.token.deadline());
                        #[cfg(test)]
                        peer_trace.detail(format!("attempt={} request_budget_ms={} queue_elapsed_us={} parent_remaining_ms={:?} parent_deadline_us={:?}", job.attempt, job.request.budget_ms, job.queued_at.elapsed().as_micros(), job.parent_deadline.map(|d|d.saturating_duration_since(Instant::now()).as_millis()), NativeProbeTraceSpan::deadline_us(job.parent_deadline)));
                        let result = job.token.check().and_then(|_| {
                            #[cfg(test)]
                            NativeProbeTraceSpan::event("ready", "peer-transport-call", &job.node, &job.request.scope, job.request.challenge, job.token.deadline(), format!("attempt={} request_budget_ms={}", job.attempt, job.request.budget_ms));
                            transport.observe_native_ready_observation(
                                &job.node,
                                &job.request,
                                &job.token,
                            )
                        });
                        #[cfg(test)]
                        {
                            peer_trace.detail(format!("attempt={} result_ok={} returned_native_r={:?} error={:?}", job.attempt, result.is_ok(), result.as_ref().ok().map(|o|o.reply().range_index), result.as_ref().err().map(ToString::to_string)));
                            if result.is_ok() { peer_trace.complete(); }
                            drop(peer_trace);
                        }
                        if tx.send((job.node, result)).is_err() {
                            break;
                        }
                    }
                })?;
            status_workers.push(handle);
        }
        drop(tx);
        let mut outstanding = BTreeMap::new();
        #[cfg(test)]
        let mut trace_requests = BTreeMap::new();
        let mut last_error = None;
        loop {
            token.check()?;
            if round.is_ready() {
                let closed = round.finish()?;
                #[cfg(test)]
                collector_trace.complete();
                return Ok((prepared, closed));
            }
            while outstanding.len() < workers {
                let Some((node, attempt)) = queue.pop_front() else {
                    break;
                };
                let remaining = token
                    .deadline()
                    .and_then(|d| d.checked_duration_since(Instant::now()))
                    .ok_or_else(|| invalid("closed native-ready round expired"))?;
                let waves = (queue.len() + outstanding.len() + 1).div_ceil(workers.max(1));
                let slice = remaining / u32::try_from(waves + 2).unwrap_or(u32::MAX);
                if slice.is_zero() {
                    return Err(invalid("native-ready deadline slice exhausted"));
                }
                let child = worker_token.child_with_deadline(Instant::now() + slice);
                let request = NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: ready_remaining_ms(&child)?,
                };
                #[cfg(test)]
                let queued_at = Instant::now();
                #[cfg(test)]
                {
                    NativeProbeTraceSpan::event("ready", "peer-queue", &node, scope, request.challenge, child.deadline(), format!("attempt={} request_budget_ms={} waves={} parent_remaining_ms={:?} allocated_slice_us={}", attempt+1, request.budget_ms, waves, token.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()), slice.as_micros()));
                    trace_requests.insert(
                        node.clone(),
                        (request.challenge, child.deadline(), queued_at, attempt + 1),
                    );
                }
                jobs.try_send(ReadyJob {
                    node: node.clone(),
                    request,
                    token: child,
                    #[cfg(test)]
                    queued_at,
                    #[cfg(test)]
                    attempt: attempt + 1,
                    #[cfg(test)]
                    parent_deadline: token.deadline(),
                })
                .map_err(|_| invalid("bounded native-ready job queue invariant failed"))?;
                outstanding.insert(node, attempt + 1);
            }
            if outstanding.is_empty() {
                return Err(invalid(&format!(
                    "closed current native-ready majority unavailable: {}",
                    last_error.unwrap_or_else(|| "no matching physical majority".into())
                )));
            }
            match results.recv_timeout(LATCH_POLL) {
                Ok((node, result)) => {
                    #[cfg(test)]
                    let trace_request = trace_requests.remove(&node);
                    #[cfg(test)]
                    if let Some((challenge, deadline, queued_at, attempt)) = trace_request {
                        NativeProbeTraceSpan::event("ready", "peer-collector-receipt", &node, scope, challenge, deadline, format!("attempt={} queue_to_receipt_us={} parent_remaining_ms={:?} result_ok={} returned_native_r={:?} error={:?}", attempt, queued_at.elapsed().as_micros(), token.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()), result.is_ok(), result.as_ref().ok().map(|o|o.reply().range_index), result.as_ref().err().map(ToString::to_string)));
                    }
                    let attempt = outstanding
                        .remove(&node)
                        .ok_or_else(|| invalid("unexpected native-ready worker response"))?;
                    match result {
                        Ok(observation) => {
                            let recorded = round.record(observation);
                            #[cfg(test)]
                            if let Some((challenge, deadline, _, _)) = trace_request {
                                NativeProbeTraceSpan::event(
                                    "ready",
                                    "peer-collector-record",
                                    &node,
                                    scope,
                                    challenge,
                                    deadline,
                                    format!(
                                        "parent_remaining_ms={:?} result_ok={} error={:?}",
                                        token.deadline().map(|d| d
                                            .saturating_duration_since(Instant::now())
                                            .as_millis()),
                                        recorded.is_ok(),
                                        recorded.as_ref().err().map(ToString::to_string)
                                    ),
                                );
                            }
                            recorded?;
                        }
                        Err(error) => {
                            last_error = Some(format!("{node}: {error}"));
                            if attempt < 2 {
                                queue.push_back((node, attempt));
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("native-ready status workers disconnected"))
                }
            }
        }
    }
}
fn ready_remaining_ms(token: &CancellationToken) -> Result<u64> {
    token.check()?;
    let remaining = token
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .ok_or_else(|| invalid("native readiness requires a finite original deadline"))?;
    Ok(u64::try_from(remaining.as_millis())
        .unwrap_or(u64::MAX)
        .clamp(1, 5000))
}
