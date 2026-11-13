//! Same-configuration native catch-up. Retained raw Applied commands are not
//! physical proof: each replay is checked against actual native history and
//! receives a new, actual distinct certificate quorum before materialization.
use super::*;
use crate::distribution_native_apply::{NativeOriginStatusRequest, NATIVE_ORDINARY_BUDGET_MS};

impl<Transport: RangeWriteTransport> RangeWriteCoordinator<Transport> {
    pub(super) fn repair_native_voter(
        &self,
        database: &crate::BicDb,
        range: &RangeDescriptor,
        voter: &ClusterNodeId,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        let budget = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_millis(NATIVE_ORDINARY_BUDGET_MS));
        let lineage = database.current_native_range_source_lineage(range.id)?;
        let request = NativeOriginStatusRequest {
            cluster_id: self.cluster_id.clone(),
            bootstrap_id: lineage.bootstrap_id(),
            range_id: range.id,
            range_epoch: range.epoch,
            challenge: Uuid::now_v7(),
            budget_ms: NATIVE_ORDINARY_BUDGET_MS,
        };
        let mut remote = self
            .transport
            .observe_native_origin(voter, &request, &budget)?;
        database.require_native_replica_prefix_compatible(&lineage, remote.reply())?;
        let local = self.local_service.lock().range_write_progress(range.id);
        if remote.reply().progress.last_index > local.last_index
            || remote.reply().progress.resolved_through > local.resolved_through
        {
            return Err(range_write_error(
                "native target raw catalog is ahead; fresh recovery is required",
            ));
        }
        let target = lineage.range_index();
        let voters = range
            .replicas
            .iter()
            .filter(|r| r.role == RangeReplicaRole::Voter)
            .map(|r| r.node_id.clone())
            .collect::<Vec<_>>();
        let quorum = voters.len() / 2 + 1;
        let limit = self
            .repair_limits
            .max_commands_per_batch
            .saturating_mul(self.repair_limits.max_batches_per_admission);
        for _ in 0..limit {
            budget.check()?;
            let after = remote
                .reply()
                .native_index
                .min(remote.reply().progress.resolved_through);
            if after == target {
                return Ok(());
            }
            if after > target {
                return Err(range_write_error("native target is ahead of actual source"));
            }
            let probe = self.local_service.lock().probe_range_write_for_peer(
                range,
                &self.local_node_id,
                after + 1,
            )?;
            let entry = probe.entry.as_ref().ok_or_else(|| range_write_error(
                "native catch-up needs a retained command payload; compacted history requires current-voter image repair"))?;
            let command = &entry.command;
            if entry.state != RangeWriteState::Applied || command.range_epoch != range.epoch {
                return Err(range_write_error("native catch-up requires same-epoch actual Applied source; abort/historical recovery is separate"));
            }
            database.require_native_command_materialized(&lineage, command)?;
            let plan = NativeRangeCommandIntent::plan_source(
                command,
                range,
                &self.topology.read_recursive(),
                &lineage,
            )?;
            plan.validate_native_configuration(&lineage)?;
            self.transport
                .validate_native_origin_pre_admission(&voters, command, &plan)?;
            // Local certification is re-read from the actual service, not a
            // cached certificate or a supplied list. Target is staged first.
            let local_ack = self
                .local_service
                .lock()
                .certify_range_write_with_mutation(
                    range,
                    &self.local_node_id,
                    command,
                    authority,
                )?;
            self.validate_ack(&self.local_node_id, command, &local_ack)?;
            if !matches!(
                local_ack.state,
                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
            ) {
                return Err(range_write_error(
                    "native source did not durably certify replay",
                ));
            }
            let target_ack = self
                .transport
                .recertify_native_origin(voter, command, &budget)?;
            self.validate_ack(voter, command, &target_ack)?;
            if !matches!(
                target_ack.state,
                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
            ) {
                return Err(range_write_error(
                    "native target did not durably certify replay",
                ));
            }
            let mut certified = BTreeSet::from([self.local_node_id.clone(), voter.clone()]);
            let candidates = voters
                .iter()
                .filter(|id| **id != self.local_node_id && *id != voter)
                .collect::<Vec<_>>();
            for (position, node) in candidates.iter().enumerate() {
                if certified.len() >= quorum {
                    break;
                }
                budget.check()?;
                // Leave a fair fraction of the original budget to each other
                // candidate; no slow endpoint renews the parent deadline.
                let remaining = budget
                    .deadline()
                    .unwrap()
                    .saturating_duration_since(Instant::now());
                let slice = remaining / (candidates.len() - position) as u32;
                let attempt = budget.child_with_deadline(Instant::now() + slice);
                if let Ok(ack) = self
                    .transport
                    .recertify_native_origin(node, command, &attempt)
                {
                    if self.validate_ack(node, command, &ack).is_ok()
                        && matches!(
                            ack.state,
                            RangeWriteState::QuorumCommitted | RangeWriteState::Applied
                        )
                    {
                        certified.insert((*node).clone());
                    }
                }
            }
            let certificate = plan.certify(&certified)?;
            let next = self
                .transport
                .apply_native_origin(voter, command, &certificate, &budget)?;
            database.require_native_replica_prefix_compatible(&lineage, next.reply())?;
            if next.reply().command_index != Some(command.index)
                || next.reply().command_checksum_sha256.as_deref()
                    != Some(command.checksum_sha256.as_str())
                || next.reply().progress.resolved_through < command.index
                || next.reply().native_index < command.index
            {
                return Err(range_write_error(
                    "native catch-up did not prove actual physical command materialization",
                ));
            }
            remote = next;
        }
        Err(range_write_error(
            "bounded native catch-up exhausted; fresh recovery/image required",
        ))
    }
}
