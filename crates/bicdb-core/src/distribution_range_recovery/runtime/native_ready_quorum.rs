//! Recipient-owned closed readiness collection. This collects only actual
//! frozen status proofs; it never sends a claimed certificate/voter list.
use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc;

struct ReadyJob {
    node: ClusterNodeId,
    request: NativeReadyRequest,
    token: CancellationToken,
}
struct ReadyRoundCancel(CancellationToken);
impl Drop for ReadyRoundCancel {
    fn drop(&mut self) {
        self.0.cancel();
    }
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
        if let Ok(observation) =
            self.observe_native_ready_inner(&self.provider.local_participant(), &local, &token)
        {
            round.record(observation)?;
            if round.is_ready() {
                return Ok((prepared, round.finish()?));
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
        let _cancel = ReadyRoundCancel(worker_token.clone());
        let (jobs, rx) = mpsc::sync_channel::<ReadyJob>(workers.max(1));
        let rx = Arc::new(Mutex::new(rx));
        let (tx, results) = mpsc::sync_channel(workers.max(1));
        for _ in 0..workers {
            let rx = Arc::clone(&rx);
            let tx = tx.clone();
            let transport = Arc::clone(transport);
            let token = worker_token.clone();
            let permit = Arc::clone(&operation);
            std::thread::Builder::new()
                .name("native-ready-status".into())
                .spawn(move || {
                    let _permit = permit;
                    while token.check().is_ok() {
                        let job = match rx.lock().recv_timeout(LATCH_POLL) {
                            Ok(j) => j,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        let result = job.token.check().and_then(|_| {
                            transport.observe_native_ready_observation(
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
            token.check()?;
            if round.is_ready() {
                return Ok((prepared, round.finish()?));
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
                jobs.try_send(ReadyJob {
                    node: node.clone(),
                    request,
                    token: child,
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
                    let attempt = outstanding
                        .remove(&node)
                        .ok_or_else(|| invalid("unexpected native-ready worker response"))?;
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
