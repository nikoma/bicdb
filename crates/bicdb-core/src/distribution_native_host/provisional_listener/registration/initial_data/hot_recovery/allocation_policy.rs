//! Allocation continuation uses the SAME process/State/installed writer.
//! Each phase has a fresh actual Role and the original finite attempt window.
//! A native audit only admits the exact descriptor; closed native readiness,
//! completion, activation and final readQQ/MQ are separate real producers.
use super::*;

// Diagnostic classification only; never format the original error under State
// or operation guards. The actual Result is propagated unchanged.
fn error_kind(error: &BicDbError) -> &'static str {
    match error {
        BicDbError::QueryCanceled => "query_canceled",
        BicDbError::QueryTimedOut => "query_timed_out",
        BicDbError::CommitOutcomeUnknown { .. } => "commit_outcome_unknown",
        BicDbError::WriteFailed { .. } => "write_failed",
        BicDbError::PagedStorage(_) => "paged_storage",
        BicDbError::Io(_) => "io",
        BicDbError::Cluster(_) => "cluster",
        BicDbError::FormatCompatibility(_) => "format_compatibility",
        _ => "other",
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AllocationPhase {
    Fence,
    Prepare,
    Complete,
    Activate,
    Publish,
}
impl AllocationPhase {
    fn name(self) -> &'static str {
        match self {
            Self::Fence => "Fence",
            Self::Prepare => "Prepare",
            Self::Complete => "Complete",
            Self::Activate => "Activate",
            Self::Publish => "Publish",
        }
    }
}
pub(super) struct AllocationJob {
    range: crate::RangeDescriptor,
    scope: RecoveryFenceScope,
    minimum_index: u64,
    members: Vec<ClusterNodeId>,
    phase: AllocationPhase,
    position: usize,
    peer_opens: BTreeMap<ClusterNodeId, HotRecoveryPeerOpen>,
    first_error_kind: Option<&'static str>,
    last_actual: Option<serde_json::Value>,
}
impl NativeHotRecoveryMaintenance {
    /// Current owned C is only a skip/scheduling hint. A changed policy is
    /// published only by the independent actual OWN/MStore/native producer.
    pub(super) fn current_allocation_ranges(
        &self,
        state: &State,
        parent: &CancellationToken,
    ) -> Result<Vec<crate::RangeDescriptor>> {
        self.live(parent)?;
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let current = self
            .initial
            .topology
            .try_read()
            .ok_or_else(|| invalid("allocation scheduling topology busy"))?
            .clone();
        if !self
            .host
            .allocation_policy_is_current(&current)
            .unwrap_or(false)
        {
            self.quiet.store(false, Ordering::Release);
            let business = Uuid::now_v7();
            let leader = self
                .initial
                .provider
                .current_metadata_leader_hint(&token)?
                .ok_or_else(|| invalid("allocation current metadata leader unavailable"))?;
            let own = self.initial.transport.acquire_host_metadata_observation(
                &leader,
                business,
                Duration::from_millis(Self::remaining(&token)?),
                &token,
            )?;
            let committed = own.committed_topology(business)?.clone();
            let affected = self.host.allocation_changed_ranges(&committed)?;
            if affected
                .iter()
                .any(|id| state.jobs.contains_key(id) || state.replica_debts.contains_key(id))
            {
                return Err(invalid("allocation retains an original hot job/replica debt; distinct historical continuation required"));
            }
            let point = ClusterDataNodeService::capture_actual_joint_metadata_point_waiting(
                &self.initial.service,
                &own,
                business,
                &token,
            )?
            .ok_or_else(|| invalid("allocation needs actual attached current MStore"))?;
            point.require_external_current()?;
            self.live(&token)?;
            let db = self
                .initial
                .seed
                .database
                .try_read()
                .ok_or_else(|| invalid("allocation original DB busy"))?;
            let prepared = ClusterDataNodeService::prepare_native_writer_allocation_refresh(
                &self.initial.service,
                &db,
                &self.coordinator,
                &self.host,
                own,
                business,
                point,
            )?;
            let actual =
                self.host
                    .refresh_allocation_policy(&db, &self.coordinator, prepared, &token);
            drop(db);
            // Original return is emitted before any later worker request. All
            // service/DB/journal/version guards have unwound; DATA only.
            #[cfg(test)]
            self.record(serde_json::json!({"phase":"allocation_original_audit_return",
                "actual_returned_ok":actual.is_ok(),"actual_error_kind":actual.as_ref().err().map(error_kind),
                "actual_owner_attempt":self.host.test_allocation_evidence()}));
            actual?;
        }
        self.live(&token)?;
        let current = self
            .initial
            .topology
            .try_read()
            .ok_or_else(|| invalid("allocation current routing busy"))?;
        // No new foreign range is enrolled from routing. Only the original
        // physical host's range inventory is considered by this finite lane.
        Ok(self
            .initial
            .seed
            .spec
            .initial_ranges
            .iter()
            .filter_map(|original| {
                current
                    .range_by_id(original.id)
                    .filter(|r| r.leader == self.initial.seed.spec.node_id)
                    .cloned()
            })
            .collect())
    }
    pub(in crate::distribution_native_host::provisional_listener::registration::initial_data) fn allocation_open_observed(
        &self,
        range: &crate::RangeDescriptor,
    ) -> bool {
        if !self
            .coordinator
            .initial_data_leader_admission_observed(range)
        {
            return false;
        }
        let Some(db) = self.initial.seed.database.try_read() else {
            return false;
        };
        db.current_native_range_source_lineage(range.id)
            .is_ok_and(|point| {
                point.native_write_range().is_ok_and(|actual| {
                    crate::distribution_native_read::canonical_range(actual)
                        == crate::distribution_native_read::canonical_range(range)
                })
            })
    }
    /// Returns true only when this attempt selected actual allocation work.
    /// Existing origin jobs/debts remain retained and keep their own path.
    pub(super) fn continue_allocation(
        &self,
        state: &mut State,
        ranges: &[crate::RangeDescriptor],
        parent: &CancellationToken,
    ) -> Result<bool> {
        let selected = ranges
            .iter()
            .find(|r| state.allocation_jobs.contains_key(&r.id))
            .cloned()
            .or_else(|| {
                ranges
                    .iter()
                    .find(|r| {
                        !self.allocation_open_observed(r)
                            && self
                                .initial
                                .seed
                                .spec
                                .initial_ranges
                                .iter()
                                .any(|before| before.id == r.id && before.epoch < r.epoch)
                    })
                    .cloned()
            });
        let Some(range) = selected else {
            return Ok(false);
        };
        self.quiet.store(false, Ordering::Release);
        if !state.allocation_jobs.contains_key(&range.id) {
            if state.jobs.contains_key(&range.id) || state.replica_debts.contains_key(&range.id) {
                return Err(invalid(
                    "allocation cannot replace retained original hot obligations",
                ));
            }
            if state.jobs.len() + state.replica_debts.len() + state.allocation_jobs.len()
                >= MAX_JOBS
            {
                return Err(invalid(
                    "shared native maintenance retained job bound exhausted",
                ));
            }
            self.live(parent)?;
            let minimum_index = {
                let db = self
                    .initial
                    .seed
                    .database
                    .try_read()
                    .ok_or_else(|| invalid("allocation source DB busy"))?;
                let writer = self.admission.writer();
                writer.require_allocation_native_range(&db, &range)?;
                // Strict native producer: pending/Unknown cannot become a clean
                // transition, and no synthetic zero command/certificate exists.
                db.current_native_range_source_lineage(range.id)?
                    .range_index()
            };
            let role = self.fresh_role(&range, Uuid::now_v7(), parent)?;
            let scope = RecoveryFenceScope::from_role(&role)?;
            let mut members = role
                .member_credentials()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            // Origin closes last and opens last. ALL current voters are kept
            // for this narrow clean allocation lane; no unavailable peer is
            // converted to a successful observation or silently discarded.
            members.sort_by_key(|node| *node == self.initial.seed.spec.node_id);
            state.allocation_jobs.insert(
                range.id,
                AllocationJob {
                    range: range.clone(),
                    scope,
                    minimum_index,
                    members,
                    phase: AllocationPhase::Fence,
                    position: 0,
                    peer_opens: BTreeMap::new(),
                    first_error_kind: None,
                    last_actual: None,
                },
            );
        }
        #[cfg(test)]
        if self.host.test_allocation_recovery_is_paused(parent)? {
            return Err(invalid(
                "actual same-worker allocation held before closed recovery, no Ready grant",
            ));
        }
        let job = state.allocation_jobs.get_mut(&range.id).unwrap();
        if crate::distribution_native_read::canonical_range(&range)
            != crate::distribution_native_read::canonical_range(&job.range)
        {
            return Err(invalid(
                "allocation original job changed committed descriptor; retained without reset",
            ));
        }
        let result = self.run_allocation(job, parent);
        if let Err(error) = &result {
            if job.first_error_kind.is_none() {
                job.first_error_kind = Some(error_kind(error));
            }
        }
        self.record(serde_json::json!({"phase":"same_owner_allocation","range":job.range,
            "nonce":job.scope.recovery_nonce,"actual_phase":job.phase.name(),"position":job.position,
            "last_actual":job.last_actual,"first_error_kind":job.first_error_kind,
            "actual_error_kind":result.as_ref().err().map(error_kind),"scheduling_DATA_not_permission":true}));
        result?;
        state.allocation_jobs.remove(&range.id);
        Ok(true)
    }
    fn run_allocation(&self, job: &mut AllocationJob, parent: &CancellationToken) -> Result<()> {
        self.live(parent)?;
        let held = self.coordinator.reserve_hot_native_recovery(
            &self.runtime,
            &self.host,
            &job.range,
            parent,
        )?;
        loop {
            // The original caller bounds this complete turn. Each distinct
            // recipient operation obtains its own fresh authority inside that
            // unchanged bound; a previous operation's short Role is not the
            // lifetime of the retained multi-phase job.
            self.live(parent)?;
            let role = self.fresh_role(&job.range, job.scope.recovery_nonce, parent)?;
            job.scope.require_role(&role)?;
            let token = parent.child_with_deadline(role.deadline());
            self.live(&token)?;
            role.require_live_request()?;
            held.require_actual_store_slot()?;
            if job.phase == AllocationPhase::Publish {
                self.coordinator.publish_allocation_native_recovery(
                    &self.runtime,
                    &self.host,
                    &held,
                    &role,
                    &job.scope,
                    &token,
                )?;
                return Ok(());
            }
            if job.position == job.members.len() {
                held.require_actual_store_slot()?;
                job.phase = match job.phase {
                    AllocationPhase::Fence => AllocationPhase::Prepare,
                    AllocationPhase::Prepare => AllocationPhase::Complete,
                    AllocationPhase::Complete => AllocationPhase::Activate,
                    AllocationPhase::Activate => AllocationPhase::Publish,
                    AllocationPhase::Publish => unreachable!(),
                };
                job.position = 0;
                continue;
            }
            let node = job.members[job.position].clone();
            let budget_ms = Self::remaining(&token)?;
            if budget_ms < MIN_PHASE_START_MS {
                return Err(crate::BicDbError::QueryTimedOut);
            }
            let request = match job.phase {
                AllocationPhase::Fence => {
                    HotRecoveryPhaseRequest::Fence(RecoveryEpochFenceRequest {
                        scope: job.scope.clone(),
                        budget_ms,
                    })
                }
                AllocationPhase::Prepare => HotRecoveryPhaseRequest::Prepare(NativeReadyRequest {
                    scope: job.scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms,
                }),
                AllocationPhase::Complete | AllocationPhase::Activate => {
                    HotRecoveryPhaseRequest::Complete {
                        request: NativeCompletionRequest {
                            scope: job.scope.clone(),
                            challenge: Uuid::now_v7(),
                            budget_ms,
                        },
                        activate: job.phase == AllocationPhase::Activate,
                    }
                }
                AllocationPhase::Publish => unreachable!(),
            };
            let actual = self.initial.transport.hot_recovery_phase_observation(
                &node,
                request,
                job.minimum_index,
                Duration::from_millis(budget_ms),
                &token,
            );
            // Retain actual API outcome BEFORE any outer currentness failure.
            job.last_actual = Some(serde_json::json!({"node":node,"phase":job.phase.name(),
                "returned_ok":actual.is_ok(),"actual_error_kind":actual.as_ref().err().map(error_kind)}));
            let actual_open = actual?.observed_open().clone();
            if let Some(original) = job.peer_opens.get(&node) {
                original.require_same(&actual_open)?;
            } else {
                job.peer_opens.insert(node, actual_open);
            }
            // The actual validated successful recipient result belongs to
            // this job even if the outer caller expires immediately after it.
            // Retain that progress before late checks, so the next turn does
            // not repeat an already successful Fence/Prepare/Complete/Activate.
            job.position += 1;
            self.live(&token)?;
            role.require_live_request()?;
            held.require_actual_store_slot()?;
        }
    }
}

impl NativeHotRecoveryMaintenance {
    /// Read-only bounded observation of the real process worker. Never runs a
    /// mutator, replaces enrollment or retries an SDK operation. Return DATA is
    /// not a Ready grant; each subsequent APP/Strong performs its real proofs.
    pub(in crate::distribution_native_host::provisional_listener::registration::initial_data) fn observe_allocation_epoch(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<()> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        if self.process.get().is_none() {
            return Err(invalid(
                "allocation observation requires the original process worker",
            ));
        }
        loop {
            self.live(&token)?;
            self.process.get().unwrap().check()?;
            let current = self
                .initial
                .topology
                .try_read()
                .and_then(|c| c.range_by_id(range).cloned());
            if let Some(current) = current {
                if let Some(db) = self.initial.seed.database.try_read() {
                    let writer = self.admission.writer();
                    if writer
                        .require_allocation_native_range(&db, &current)
                        .is_ok()
                        && (current.leader != self.initial.seed.spec.node_id
                            || self
                                .coordinator
                                .initial_data_leader_admission_observed(&current))
                    {
                        if !current.replicas.iter().any(|r| {
                            r.node_id == self.initial.seed.spec.node_id
                                && r.role == crate::RangeReplicaRole::Voter
                        }) {
                            // Cluster descriptor audit only on a nonhosting
                            // member. This returns no local range/Ready grant.
                            self.host
                                .require_for_coordinator(&db, &current, &self.coordinator)?;
                            self.live(&token)?;
                            return Ok(());
                        }
                        let point = db.current_native_range_source_lineage(range)?;
                        if crate::distribution_native_read::canonical_range(
                            point.native_write_range()?,
                        ) == crate::distribution_native_read::canonical_range(&current)
                        {
                            self.host
                                .require_for_coordinator(&db, &current, &self.coordinator)?;
                            self.live(&token)?;
                            return Ok(());
                        }
                    }
                }
            }
            token.check()?;
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
