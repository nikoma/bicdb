//! Actual durable completion-decision collection, including activated anchors.
//! Original R/H is not a current native-head or read/source barrier.
use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc;

enum CompletionCollectionMode {
    RequireQuorum,
    CompleteOrDecision,
}
struct CompletionJob {
    node: ClusterNodeId,
    request: NativeCompletionRequest,
    token: CancellationToken,
}
struct CompletionRoundCancel(CancellationToken);
impl Drop for CompletionRoundCancel {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
impl RangeRecoveryRuntime {
    pub(crate) fn acquire_native_completion_quorum(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<NativeCompletionQuorum> {
        let operation = self.reserve(request.scope.range_id)?;
        self.acquire_native_completion_quorum_with_operation(initiator, request, parent, operation)
    }
    pub(super) fn acquire_native_completion_quorum_with_operation(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
        operation: Arc<Operation>,
    ) -> Result<NativeCompletionQuorum> {
        match self.collect_native_completion_protocol(
            initiator,
            request,
            parent,
            parent,
            operation,
            CompletionCollectionMode::RequireQuorum,
        )? {
            NativeCompletionSelection::Quorum(q) => Ok(q),
            NativeCompletionSelection::Constraint(_) | NativeCompletionSelection::Decision(_) => {
                Err(invalid("distinct durable completion quorum unavailable"))
            }
        }
    }
    pub(super) fn select_native_completion_protocol(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
        original_parent: &CancellationToken,
        operation: Arc<Operation>,
    ) -> Result<NativeCompletionSelection> {
        self.collect_native_completion_protocol(
            initiator,
            request,
            parent,
            original_parent,
            operation,
            CompletionCollectionMode::CompleteOrDecision,
        )
    }
    fn collect_native_completion_protocol(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
        original_parent: &CancellationToken,
        operation: Arc<Operation>,
        mode: CompletionCollectionMode,
    ) -> Result<NativeCompletionSelection> {
        let token = request.cancellation(parent)?;
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
        let members = role
            .member_credentials()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut round = NativeCompletionQuorumRound::begin(role, &token)?;
        // Local status uses the same independently challenged readonly role as
        // remote status. It does not recursively reserve this operation.
        let local = NativeCompletionRequest {
            scope: scope.clone(),
            challenge: Uuid::now_v7(),
            budget_ms: completion_remaining_ms(&token)?,
        };
        if let Ok(observation) =
            self.observe_native_completion_inner(&self.provider.local_participant(), &local, &token)
        {
            round.record(observation)?;
            if matches!(mode, CompletionCollectionMode::CompleteOrDecision)
                && round.has_activated_decision()
            {
                return round.finish_selection(original_parent);
            }
            if round.is_ready() {
                return Ok(NativeCompletionSelection::Quorum(round.finish()?));
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
            .ok_or_else(|| invalid("native-completion participant transport is not wired"))?;
        let worker_token = token.child();
        let _cancel = CompletionRoundCancel(worker_token.clone());
        let (jobs, rx) = mpsc::sync_channel::<CompletionJob>(workers.max(1));
        let rx = Arc::new(Mutex::new(rx));
        let (tx, results) = mpsc::sync_channel(workers.max(1));
        for _ in 0..workers {
            let rx = Arc::clone(&rx);
            let tx = tx.clone();
            let transport = Arc::clone(transport);
            let token = worker_token.clone();
            let permit = Arc::clone(&operation);
            std::thread::Builder::new()
                .name("native-completion-status".into())
                .spawn(move || {
                    let _permit = permit;
                    while token.check().is_ok() {
                        let job = match rx.lock().recv_timeout(LATCH_POLL) {
                            Ok(j) => j,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        let result = job.token.check().and_then(|_| {
                            transport.observe_native_completion_observation(
                                &job.node,
                                &job.request,
                                &job.token,
                            )
                        });
                        if tx.send((job.node, result)).is_err() {
                            break;
                        }
                    }
                })?;
        }
        drop(tx);
        let mut outstanding = BTreeMap::new();
        let mut last_error = None;
        loop {
            if token.check().is_err() {
                return round.finish_selection(original_parent);
            }
            if matches!(mode, CompletionCollectionMode::CompleteOrDecision)
                && round.has_activated_decision()
            {
                return round.finish_selection(original_parent);
            }
            if round.is_ready() {
                return Ok(NativeCompletionSelection::Quorum(round.finish()?));
            }
            while outstanding.len() < workers {
                let Some((node, attempt)) = queue.pop_front() else {
                    break;
                };
                let remaining = token
                    .deadline()
                    .and_then(|d| d.checked_duration_since(Instant::now()))
                    .ok_or_else(|| invalid("closed native-completion round expired"))?;
                let waves = (queue.len() + outstanding.len() + 1).div_ceil(workers.max(1));
                let slice = remaining / u32::try_from(waves + 2).unwrap_or(u32::MAX);
                if slice.is_zero() {
                    return Err(invalid("native-completion deadline slice exhausted"));
                }
                let child = worker_token.child_with_deadline(Instant::now() + slice);
                let request = NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: completion_remaining_ms(&child)?,
                };
                jobs.try_send(CompletionJob {
                    node: node.clone(),
                    request,
                    token: child,
                })
                .map_err(|_| invalid("bounded native-completion job queue invariant failed"))?;
                outstanding.insert(node, attempt + 1);
            }
            if outstanding.is_empty() {
                return round.finish_selection(original_parent);
            }
            match results.recv_timeout(LATCH_POLL) {
                Ok((node, result)) => {
                    let attempt = outstanding
                        .remove(&node)
                        .ok_or_else(|| invalid("unexpected native-completion worker response"))?;
                    match result {
                        Ok(observation) => round.record(observation)?,
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
                    return Err(invalid("native-completion status workers disconnected"))
                }
            }
        }
    }
}
fn completion_remaining_ms(token: &CancellationToken) -> Result<u64> {
    token.check()?;
    let remaining = token
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .ok_or_else(|| invalid("native completion requires a finite original deadline"))?;
    Ok(u64::try_from(remaining.as_millis())
        .unwrap_or(u64::MAX)
        .clamp(1, 5000))
}
