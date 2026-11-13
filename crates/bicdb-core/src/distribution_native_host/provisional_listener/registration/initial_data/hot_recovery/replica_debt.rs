//! Retained replica obligations after leader availability. No leader range
//! reservation is held while an absent/lagging peer is contacted or replayed.
use super::*;

pub(super) struct ReplicaDebt {
    scope: RecoveryFenceScope,
    command: RangeWriteCommand,
    range: crate::RangeDescriptor,
    members: Vec<ClusterNodeId>,
    next: BTreeMap<ClusterNodeId, Phase>,
    binding: NativeHotReplicaBinding,
    cursor: usize,
    pending: BTreeSet<ClusterNodeId>,
    progress: BTreeMap<ClusterNodeId, serde_json::Value>,
    semantic_veto: BTreeMap<ClusterNodeId, Phase>,
    retry_after: BTreeMap<ClusterNodeId, Instant>,
    peer_opens: BTreeMap<ClusterNodeId, HotRecoveryPeerOpen>,
}
impl ReplicaDebt {
    pub(super) fn diagnostic(&self, boundary: &str) -> serde_json::Value {
        serde_json::json!({"phase":"ReplicaDebt","range_id":self.range.id,
            "recovery_nonce":self.scope.recovery_nonce,"anchor_index":self.command.index,
            "public_ready_published":true,"replica_debt_pending":self.pending,
            "actual_progress":self.progress,"boundary":boundary,"tracked_after_catchup":true,
            "recipient_next":self.next.iter().map(|(n,p)|(n.to_string(),format!("{p:?}"))).collect::<BTreeMap<_,_>>(),
            "diagnostic_not_authority":true})
    }
    pub(super) fn from_published(job: Job) -> Self {
        let pending = job
            .members
            .iter()
            .filter(|n| **n != job.range.leader)
            .cloned()
            .collect();
        Self {
            scope: job.scope,
            command: job.command,
            range: job.range,
            members: job.members,
            next: job.recipient_next,
            binding: job.replica_binding.expect("captured before actual Publish"),
            cursor: 0,
            pending,
            progress: BTreeMap::new(),
            semantic_veto: job.semantic_veto,
            // New real continuation probes once with fresh authority. Prior
            // transport cooldowns are not permissions or durable phase facts.
            retry_after: BTreeMap::new(),
            peer_opens: job.peer_opens,
        }
    }
}
impl NativeHotRecoveryMaintenance {
    pub(super) fn run_replica_debt(
        &self,
        debt: &mut ReplicaDebt,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.live(parent)?;
        #[cfg(test)]
        let _test_schedule = self
            .debt_schedule_test
            .try_lock()
            .ok_or_else(|| invalid("actual replica debt scheduling held by test owner"))?;
        let members = debt
            .members
            .iter()
            .filter(|n| **n != self.initial.seed.spec.node_id)
            .cloned()
            .collect::<Vec<_>>();
        if members.is_empty() {
            return Ok(());
        }
        let node = members[debt.cursor % members.len()].clone();
        debt.cursor = (debt.cursor + 1) % members.len();
        if !debt.semantic_veto.contains_key(&node)
            && debt
                .retry_after
                .get(&node)
                .is_some_and(|at| Instant::now() < *at)
        {
            self.record(
                serde_json::json!({"phase":"ReplicaDebt","range_id":debt.range.id,
                "target":node,"public_ready_published":true,"replica_debt_pending":debt.pending,
                "deferred_transport_probe":true,"diagnostic_not_authority":true}),
            );
            return Ok(());
        }
        let result = (|| -> Result<()> {
            // One selected physical recipient per bounded fair attempt; no
            // leader active_ranges reservation or engine latch crosses IO.
            for _ in 0..8 {
                self.live(parent)?;
                let role = self.fresh_role(&debt.range, debt.scope.recovery_nonce, parent)?;
                debt.scope.require_role(&role)?;
                let phase_parent = parent.child_with_deadline(role.deadline());
                self.coordinator.require_hot_replica_binding(
                    &self.runtime,
                    &self.host,
                    &debt.binding,
                    &role,
                    &phase_parent,
                )?;
                let phase = debt.next.get(&node).copied();
                match phase {
                    Some(Phase::Complete) | Some(Phase::Activate) => {
                        let phase = phase.unwrap();
                        let budget_ms = Self::remaining(&phase_parent)?.saturating_sub(250).max(1);
                        let request = HotRecoveryPhaseRequest::Complete {
                            request: NativeCompletionRequest {
                                scope: debt.scope.clone(),
                                challenge: Uuid::now_v7(),
                                budget_ms,
                            },
                            activate: phase == Phase::Activate,
                        };
                        let actual = self.initial.transport.hot_recovery_phase_observation(
                            &node,
                            request,
                            debt.command.index,
                            Duration::from_millis(budget_ms),
                            &phase_parent,
                        )?;
                        let original = debt.peer_opens.get(&node).ok_or_else(|| {
                            invalid("replica continuation has no original observed physical open")
                        })?;
                        original.require_same(actual.observed_open())?;
                        self.live(&phase_parent)?;
                        role.require_live_request()?;
                        self.coordinator.require_hot_replica_binding(
                            &self.runtime,
                            &self.host,
                            &debt.binding,
                            &role,
                            &phase_parent,
                        )?;
                        if phase == Phase::Complete {
                            debt.next.insert(node.clone(), Phase::Activate);
                        } else {
                            debt.next.remove(&node);
                        }
                        if debt.semantic_veto.get(&node) == Some(&phase) {
                            debt.semantic_veto.remove(&node);
                        }
                        debt.retry_after.remove(&node);
                        #[cfg(test)]
                        eprintln!("NATIVE_HOT_REPLICA_CONTINUATION target={} phase={:?} range={} anchor={} no_leader_reservation=true", node,phase,debt.range.id,debt.command.index);
                    }
                    None => {
                        let budget_ms = Self::remaining(&phase_parent)?.saturating_sub(250).max(1);
                        let open = self.initial.transport.hot_recovery_phase_observation(
                            &node,
                            HotRecoveryPhaseRequest::Complete {
                                request: NativeCompletionRequest {
                                    scope: debt.scope.clone(),
                                    challenge: Uuid::now_v7(),
                                    budget_ms,
                                },
                                activate: true,
                            },
                            debt.command.index,
                            Duration::from_millis(budget_ms),
                            &phase_parent,
                        )?;
                        let original = debt.peer_opens.get(&node).ok_or_else(|| {
                            invalid("tracked replica has no original observed physical open")
                        })?;
                        original.require_same(open.observed_open())?;
                        self.live(&phase_parent)?;
                        let progress = self.coordinator.repair_hot_native_replica_step(
                            &self.runtime,
                            &self.host,
                            &debt.binding,
                            &role,
                            &node,
                            original,
                            &phase_parent,
                        )?;
                        self.live(&phase_parent)?;
                        let fresh =
                            self.fresh_role(&debt.range, debt.scope.recovery_nonce, parent)?;
                        debt.scope.require_role(&fresh)?;
                        self.coordinator.require_hot_replica_binding(
                            &self.runtime,
                            &self.host,
                            &debt.binding,
                            &fresh,
                            &phase_parent,
                        )?;
                        // Recheck actual target open AFTER recertification /
                        // materialization too; a captured birth/UUID is not a
                        // current slot or a cold-open serving grant.
                        let budget_ms = Self::remaining(&phase_parent)?.saturating_sub(250).max(1);
                        let current = self.initial.transport.hot_recovery_phase_observation(
                            &node,
                            HotRecoveryPhaseRequest::Complete {
                                request: NativeCompletionRequest {
                                    scope: debt.scope.clone(),
                                    challenge: Uuid::now_v7(),
                                    budget_ms,
                                },
                                activate: true,
                            },
                            debt.command.index,
                            Duration::from_millis(budget_ms),
                            &phase_parent,
                        )?;
                        original.require_same(current.observed_open())?;
                        self.live(&phase_parent)?;
                        fresh.require_live_request()?;
                        self.coordinator.require_hot_replica_binding(
                            &self.runtime,
                            &self.host,
                            &debt.binding,
                            &fresh,
                            &phase_parent,
                        )?;
                        let at_cut = progress.at_observed_source_cut();
                        debt.progress.insert(node.clone(), progress.diagnostic());
                        if debt.semantic_veto.get(&node) == Some(&Phase::Replica) {
                            debt.semantic_veto.remove(&node);
                        }
                        debt.retry_after.remove(&node);
                        if at_cut {
                            debt.pending.remove(&node);
                            return Ok(());
                        }
                        debt.pending.insert(node.clone());
                    }
                    Some(_) => {
                        // Early unavailable targets may lack native material.
                        // Activated anchors cannot be cast to ActiveFenceQQ.
                        // Keep the actual obligation; never manufacture a
                        // prepared point/genesis or orphan it as "ready".
                        return Err(invalid("retained early-phase/native-gap replica debt needs a genuine completed-decision materialization continuation"));
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = &result {
            debt.pending.insert(node.clone());
            if HotRecoveryPeerFailure::from_error(error).is_some()
                && !debt.semantic_veto.contains_key(&node)
            {
                debt.retry_after
                    .insert(node.clone(), Instant::now() + Duration::from_secs(10));
            } else if !matches!(
                error,
                crate::BicDbError::QueryCanceled | crate::BicDbError::QueryTimedOut
            ) && !debt.next.get(&node).is_some_and(|p| {
                matches!(
                    p,
                    Phase::Fence | Phase::Certify | Phase::Apply | Phase::Prepare
                )
            }) {
                debt.semantic_veto
                    .entry(node.clone())
                    .or_insert(debt.next.get(&node).copied().unwrap_or(Phase::Replica));
            }
        }
        self.record(serde_json::json!({"phase":"ReplicaDebt","range_id":debt.range.id,
            "recovery_nonce":debt.scope.recovery_nonce,"anchor_index":debt.command.index,
            "target":node,"public_ready_published":true,"replica_debt_pending":debt.pending,
            "recipient_next":debt.next.iter().map(|(n,p)|(n.to_string(),format!("{p:?}"))).collect::<BTreeMap<_,_>>(),
            "semantic_veto":debt.semantic_veto.iter().map(|(n,p)|(n.to_string(),format!("{p:?}"))).collect::<BTreeMap<_,_>>(),
            "actual_progress":debt.progress,"completed_this_attempt":result.is_ok(),
            "error":result.as_ref().err().map(ToString::to_string),
            "tracked_after_catchup":true,"no_leader_reservation_during_peer_io":true,
            "diagnostic_not_authority":true}));
        // Keep tracking after current convergence: later ordinary ACKs may
        // leave a lagging follower without another user request to repair it.
        result
    }
}

#[cfg(test)]
pub(crate) struct NativeHotDebtScheduleHold<'a> {
    _actual: parking_lot::MutexGuard<'a, ()>,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}
#[cfg(test)]
impl NativeHotRecoveryMaintenance {
    pub(crate) fn hold_debt_schedule_for_test(
        &self,
        parent: &CancellationToken,
    ) -> Result<NativeHotDebtScheduleHold<'_>> {
        if parent.deadline().is_none() {
            return Err(invalid("test debt hold requires finite acquisition"));
        }
        loop {
            parent.check()?;
            if let Some(actual) = self.debt_schedule_test.try_lock() {
                parent.check()?;
                return Ok(NativeHotDebtScheduleHold {
                    _actual: actual,
                    _not_send: std::marker::PhantomData,
                });
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}
