//! Same-original-open replica continuation. No range reservation, DB/service
//! guard or mutation permit crosses a peer RPC. No recovered grant is minted.
use super::*;
use crate::db::SourceRangeImageLineage;
use crate::distribution_native_apply::{NativeOriginStatusRequest, NativeRecertifyRequest};

pub(crate) struct NativeHotReplicaBinding {
    coordinator: std::sync::Weak<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    range: RangeDescriptor,
    database_open: String,
    store_slot: crate::distribution_data::RangeWriteServiceSlotGuard,
    store_open: Uuid,
}
pub(crate) struct NativeHotReplicaProgress {
    source_cut: u64,
    remote_cut: u64,
    remote_resolved: u64,
    logical_history_sha256: String,
}
impl NativeHotReplicaProgress {
    pub(crate) fn at_observed_source_cut(&self) -> bool {
        self.remote_cut == self.source_cut && self.remote_resolved >= self.source_cut
    }
    pub(crate) fn diagnostic(&self) -> serde_json::Value {
        serde_json::json!({"actual_source_native_cut":self.source_cut,
            "actual_remote_native_cut":self.remote_cut,"actual_remote_resolved":self.remote_resolved,
            "actual_remote_history_sha256":self.logical_history_sha256,
            "at_observed_source_cut":self.at_observed_source_cut(),
            "scope":"actual physical progress DATA; not linearizable Ready or new admission"})
    }
}
impl RangeWriteCoordinator<TcpClusterRelocationTransport> {
    pub(crate) fn capture_hot_replica_binding(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        held: &NativeHotRangeReservation<'_>,
        role: &RecoveryRoleAdmission,
        parent: &CancellationToken,
    ) -> Result<NativeHotReplicaBinding> {
        parent.check()?;
        self.require_hot_reservation(held, role)?;
        runtime.require_point_read_service(&self.local_service, &self.local_node_id)?;
        let service = self
            .local_service
            .try_lock()
            .ok_or_else(|| range_write_error("hot debt original service busy"))?;
        let database = service.database();
        let db = database
            .try_read()
            .ok_or_else(|| range_write_error("hot debt original DB busy"))?;
        host.require_for_coordinator(&db, role.current_range(), self)?;
        service.require_native_writer_host_for_range(&db, role.current_range())?;
        if service.native_initial_listener_store_open_id() != held.store_open {
            return Err(range_write_error(
                "hot debt captured Store differs from reservation",
            ));
        }
        self.require_hot_reservation(held, role)?;
        parent.check()?;
        Ok(NativeHotReplicaBinding {
            coordinator: Arc::downgrade(self),
            range: held.range.clone(),
            database_open: db.incarnation_id().to_owned(),
            store_slot: held.store_slot.clone(),
            store_open: held.store_open,
        })
    }
    pub(crate) fn require_hot_replica_binding(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        binding: &NativeHotReplicaBinding,
        role: &RecoveryRoleAdmission,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.with_hot_replica_actual(runtime, host, binding, role, parent, |_, _| Ok(()))
    }
    fn with_hot_replica_source<R>(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        binding: &NativeHotReplicaBinding,
        role: &RecoveryRoleAdmission,
        parent: &CancellationToken,
        use_actual: impl FnOnce(
            &mut ClusterDataNodeService,
            &crate::BicDb,
            &SourceRangeImageLineage,
        ) -> Result<R>,
    ) -> Result<R> {
        self.with_hot_replica_actual(runtime, host, binding, role, parent, |service, db| {
            let source = db.current_native_range_source_lineage(binding.range.id)?;
            use_actual(service, db, &source)
        })
    }
    fn with_hot_replica_actual<R>(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        binding: &NativeHotReplicaBinding,
        role: &RecoveryRoleAdmission,
        parent: &CancellationToken,
        use_actual: impl FnOnce(&mut ClusterDataNodeService, &crate::BicDb) -> Result<R>,
    ) -> Result<R> {
        parent.check()?;
        role.require_live_request()?;
        binding.store_slot.require_live()?;
        if !binding
            .coordinator
            .upgrade()
            .is_some_and(|actual| Arc::ptr_eq(&actual, self))
            || !self.native_image_mode
            || !matches!(
                role.purpose(),
                crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.local_node_id != role.current_range().leader
            || role.local_node_id() != &self.local_node_id
            || crate::distribution_native_read::canonical_range(role.current_range())
                != binding.range
            || binding.store_open != binding.store_slot.id()
        {
            return Err(range_write_error(
                "hot replica continuation lost original writer/range/Store binding",
            ));
        }
        runtime.require_point_read_service(&self.local_service, &self.local_node_id)?;
        let mut service = self
            .local_service
            .try_lock()
            .ok_or_else(|| range_write_error("hot replica source service busy"))?;
        let database = service.database();
        let db = database
            .try_read()
            .ok_or_else(|| range_write_error("hot replica source DB busy"))?;
        host.require_for_coordinator(&db, role.current_range(), self)?;
        service.require_native_writer_host_for_range(&db, role.current_range())?;
        if db.incarnation_id() != binding.database_open
            || service.native_initial_listener_store_open_id() != binding.store_open
        {
            return Err(range_write_error(
                "hot replica continuation actual DB/Store open changed",
            ));
        }
        let topology = self
            .topology
            .try_read()
            .ok_or_else(|| range_write_error("hot replica source topology busy"))?;
        if topology
            .range_by_id(binding.range.id)
            .is_none_or(|r| crate::distribution_native_read::canonical_range(r) != binding.range)
        {
            return Err(range_write_error(
                "hot replica current data configuration changed",
            ));
        }
        drop(topology);
        let result = use_actual(&mut service, &db)?;
        // Every closure here is local bounded validation/persistence only.
        binding.store_slot.require_live()?;
        host.require_for_coordinator(&db, role.current_range(), self)?;
        role.require_live_request()?;
        parent.check()?;
        Ok(result)
    }
    pub(crate) fn repair_hot_native_replica_step(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        binding: &NativeHotReplicaBinding,
        role: &RecoveryRoleAdmission,
        voter: &ClusterNodeId,
        original_peer: &crate::distribution_transport::HotRecoveryPeerOpen,
        parent: &CancellationToken,
    ) -> Result<NativeHotReplicaProgress> {
        let token = parent.child_with_deadline(role.deadline());
        let range = role.current_range();
        if voter == &self.local_node_id || !role.member_credentials().contains_key(voter) {
            return Err(range_write_error(
                "hot suffix target is not another actual current voter",
            ));
        }
        self.require_hot_replica_binding(runtime, host, binding, role, &token)?;
        let request = NativeOriginStatusRequest {
            cluster_id: self.cluster_id.clone(),
            bootstrap_id: role.bootstrap_id(),
            range_id: range.id,
            range_epoch: range.epoch,
            challenge: Uuid::now_v7(),
            budget_ms: crate::distribution_native_apply::remaining_budget(&token)?,
        };
        let remote = self
            .transport
            .observe_hot_native_replica(voter, &request, &token)?;
        self.require_hot_replica_binding(runtime, host, binding, role, &token)?;
        let selected = self.with_hot_replica_source(runtime, host, binding, role, &token, |service, db, source| {
            db.require_native_replica_prefix_compatible(source, remote.reply())?;
            if remote.reply().progress.current_epoch != range.epoch {
                return Err(range_write_error("hot replica raw epoch differs from actual current domain"));
            }
            let after = remote.reply().native_index.min(remote.reply().progress.resolved_through);
            if after == source.range_index() { return Ok(None); }
            if after > source.range_index() { return Err(range_write_error("hot target is ahead of actual source")); }
            let entry = service.probe_range_write_for_peer(range, &self.local_node_id, after+1)?.entry
                .ok_or_else(|| range_write_error("hot suffix needs retained command; compacted/native-gap image recovery remains required"))?;
            if entry.state != RangeWriteState::Applied || entry.command.range_epoch != range.epoch {
                return Err(range_write_error("hot suffix requires actual same-epoch Applied source"));
            }
            db.require_native_command_materialized(source, &entry.command)?;
            let topology = self.topology.try_read().ok_or_else(|| range_write_error("hot suffix topology busy"))?;
            let plan = NativeRangeCommandIntent::plan_source(&entry.command, range, &topology, source)?;
            plan.validate_native_configuration(source)?;
            drop(topology);
            let gate = service.range_mutation_gate();
            let permit = gate.try_enter(range.id)?;
            let local = service.certify_range_write_with_mutation(range, &self.local_node_id, &entry.command,
                &RangeMutationAuthority::Mutation(&permit))?;
            self.validate_ack(&self.local_node_id, &entry.command, &local)?;
            if !matches!(local.state, RangeWriteState::Applied | RangeWriteState::QuorumCommitted) {
                return Err(range_write_error("actual source did not certify retained materialized command"));
            }
            Ok(Some((entry.command, plan)))
        })?;
        if let Some((command, plan)) = selected {
            let voters = role
                .member_credentials()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            self.transport
                .validate_native_origin_pre_admission(&voters, &command, &plan)?;
            let mut certified = BTreeSet::from([self.local_node_id.clone()]);
            // Real target ACK is mandatory; no expected counter or raw catalog
            // state can stand in for its bounded actual receiver persistence.
            let request = NativeRecertifyRequest::from_source_plan(&command, &plan, &token)?;
            let actual = self
                .transport
                .recertify_native_replica_bounded(voter, &request, &token)?;
            actual.require_live()?;
            actual.require_original_open(original_peer)?;
            self.validate_ack(voter, &command, actual.ack())?;
            certified.insert(voter.clone());
            for other in voters
                .iter()
                .filter(|n| **n != self.local_node_id && *n != voter)
            {
                if certified.len() >= voters.len() / 2 + 1 {
                    break;
                }
                let request = NativeRecertifyRequest::from_source_plan(&command, &plan, &token)?;
                let actual = self
                    .transport
                    .recertify_native_replica_bounded(other, &request, &token)?;
                actual.require_live()?;
                self.validate_ack(other, &command, actual.ack())?;
                certified.insert(other.clone());
            }
            self.with_hot_replica_source(
                runtime,
                host,
                binding,
                role,
                &token,
                |_, db, current| {
                    db.require_native_command_materialized(current, &command)?;
                    plan.validate_native_configuration(current)
                },
            )?;
            let certificate = plan.certify(&certified)?;
            #[cfg(test)]
            eprintln!("NATIVE_HOT_REPLICA_REAL_CERTIFICATE target={} command={} command_sha256={} index={} voters={:?} quorum={} role_context={} remaining_ms={:?}",
                voter,command.command_id,command.checksum_sha256,command.index,certificate.certified_voters(),certificate.required_quorum(),role.context_id(),token.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()));
            let applied =
                self.transport
                    .apply_hot_native_replica(voter, &command, &certificate, &token)?;
            let progress = self.with_hot_replica_source(
                runtime,
                host,
                binding,
                role,
                &token,
                |_, db, current| {
                    db.require_native_replica_prefix_compatible(current, applied.reply())?;
                    if applied.reply().command_index != Some(command.index)
                        || applied.reply().command_checksum_sha256.as_deref()
                            != Some(command.checksum_sha256.as_str())
                        || applied.reply().progress.resolved_through < command.index
                        || applied.reply().native_index < command.index
                    {
                        return Err(range_write_error(
                            "hot suffix reply lacks actual physical command coverage",
                        ));
                    }
                    Ok(NativeHotReplicaProgress {
                        source_cut: current.range_index(),
                        remote_cut: applied.reply().native_index,
                        remote_resolved: applied.reply().progress.resolved_through,
                        logical_history_sha256: applied.reply().logical_history_sha256.clone(),
                    })
                },
            )?;
            #[cfg(test)]
            eprintln!("NATIVE_HOT_REPLICA_REAL_NATIVE_APPLY target={} command={} command_sha256={} index={} progress={} remaining_ms={:?}",
                voter,command.command_id,command.checksum_sha256,command.index,progress.diagnostic(),token.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()));
            token.check()?;
            role.require_live_request()?;
            return Ok(progress);
        }
        self.with_hot_replica_source(runtime, host, binding, role, &token, |_, db, current| {
            db.require_native_replica_prefix_compatible(current, remote.reply())?;
            Ok(NativeHotReplicaProgress {
                source_cut: current.range_index(),
                remote_cut: remote.reply().native_index,
                remote_resolved: remote.reply().progress.resolved_through,
                logical_history_sha256: remote.reply().logical_history_sha256.clone(),
            })
        })
    }
}
