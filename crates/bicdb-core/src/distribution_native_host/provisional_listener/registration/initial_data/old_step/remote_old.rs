//! Actual node-local OLD receiver. Its table owns the runtime before mutation,
//! constraints and actual closure before any final metadata or reply check.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_range_handoff::{
    NativeJointHandoffRuntime, NativeRemoteOldOperationV1, NativeRemoteOldReplyV1,
    NativeRemoteOldRequestV1,
};

pub(crate) struct ActualNativeRemoteOldFrame {
    reply: NativeRemoteOldReplyV1,
}
impl ActualNativeRemoteOldFrame {
    pub(crate) fn into_reply(self) -> NativeRemoteOldReplyV1 {
        self.reply
    }
}

impl NativeInitialDataAdmission {
    pub(crate) fn observe_remote_old(
        &self,
        peer: &MetadataReadParticipant,
        request: &NativeRemoteOldRequestV1,
        parent: &CancellationToken,
    ) -> Result<ActualNativeRemoteOldFrame> {
        let token = request.cancellation(parent)?;
        self.require_live()?;
        let process = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("OLD source process owner ended"))?;
        process.require_original_process()?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("OLD source service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("OLD source runtime ended"))?;
        self.require_listener(&service, &runtime)?;
        let metadata = self
            .old_metadata
            .upgrade()
            .ok_or_else(|| invalid("OLD source MStore ended"))?;
        let provider = self
            .old_provider
            .upgrade()
            .ok_or_else(|| invalid("OLD source metadata provider ended"))?;
        let transport = self
            .old_transport
            .upgrade()
            .ok_or_else(|| invalid("OLD source transport ended"))?;
        let table = self
            .old_closures
            .upgrade()
            .ok_or_else(|| invalid("OLD retained owner table ended"))?;

        // ALL authority acquisition is outside table/session/engine latches.
        // Close uses the actual initiating TLS peer. Observe is readonly and
        // does not borrow the controller's leader/initiator purpose.
        let receiver_business = Uuid::now_v7();
        let receiver_challenge = Uuid::now_v7();
        let role = if request.operation != NativeRemoteOldOperationV1::Observe {
            let purpose = request.purpose();
            let observed = provider.witness_recovery_role(
                peer,
                receiver_business,
                request.range_id,
                request.range_epoch,
                request.nonce,
                receiver_challenge,
                purpose.clone(),
                &token,
            )?;
            Some(Arc::new(RecoveryRoleAdmission::from_witness(
                observed,
                peer,
                transport.local_metadata_read_participant().node_id(),
                receiver_business,
                request.range_id,
                request.range_epoch,
                request.nonce,
                receiver_challenge,
                &purpose,
            )?))
        } else {
            None
        };
        let leader = provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("OLD source current metadata leader unavailable"))?;
        let remaining = token
            .deadline()
            .unwrap()
            .saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BicDbError::QueryTimedOut);
        }
        let own = transport.acquire_host_metadata_observation(
            &leader,
            receiver_business,
            remaining.min(Duration::from_secs(5)),
            &token,
        )?;
        let own_current = own.committed_topology(receiver_business)?;
        let local = own.requester_node_id();

        if let Some(role) = role {
            if request.operation == NativeRemoteOldOperationV1::Constrain {
                return self.constrain_remote_old(
                    peer,
                    request,
                    role,
                    own,
                    receiver_business,
                    metadata,
                    service,
                    table,
                    &token,
                );
            }
            let step = self.observe_hosted_initial_old_step(
                metadata.clone(),
                role,
                own,
                receiver_business,
                &token,
            )?;
            request.require_plan(step.plan())?;
            let retained = {
                let mut entries = table
                    .try_lock()
                    .ok_or_else(|| invalid("OLD source owner table busy"))?;
                if let Some(old) = entries.get(&request.range_id) {
                    old.clone()
                } else {
                    if entries.len() >= 8 {
                        return Err(invalid("OLD source owner capacity; no refusal eviction"));
                    }
                    let actual_runtime = Arc::new(NativeJointHandoffRuntime::new(service.clone())?);
                    let held = Arc::new(Mutex::new(RetainedInitialOld {
                        writer: step.writer.clone(),
                        plan_sha256: request.scope_sha256.clone(),
                        plan: step.plan().clone(),
                        runtime: actual_runtime,
                        actual: None,
                        constraints: None,
                        constraint_owner: None,
                        outcome: Some(Default::default()),
                    }));
                    entries.insert(request.range_id, held.clone());
                    held
                }
            };
            let mut held = retained
                .try_lock()
                .ok_or_else(|| invalid("OLD source operation already active"))?;
            if held.plan_sha256 != request.scope_sha256 || !Arc::ptr_eq(&held.writer, &step.writer)
            {
                return Err(invalid("OLD source retains another exact session"));
            }
            step.require_joint_runtime(&held.runtime)?;
            if held.constraints.is_none() {
                held.constraints = Some(
                    held.runtime
                        .observe_constraints_for_initial_step(&step, &token)?,
                );
            }
            if held.actual.is_none() {
                // The actual native/promise/Frozen/pin owner is retained here
                // BEFORE metadata/reply checks may fail. No phase or DTO can
                // stand in for a possibly completed mutation.
                let actual_runtime = held.runtime.clone();
                let outcome = held.outcome.as_mut().ok_or_else(|| {
                    invalid("remote OLD missing original retained producer outcome owner")
                })?;
                let observation =
                    actual_runtime.close_old_retained_for_initial_step(&step, outcome, &token)?;
                held.actual = Some(Arc::new(observation));
                // The observation now owns the SAME actual receipts/pin. Drop
                // only duplicate references after the original owner is safe.
                held.outcome.take();
            }
            drop(held);
            step.require_metadata_current()?;
            step.require_live()?;
            // A separate fresh readonly operation below observes the retained
            // fact using the SAME original request token. No role renewal.
            let frame = self.finish_remote_old_observation(
                peer,
                request,
                &step.own,
                receiver_business,
                &metadata,
                &service,
                &table,
                &token,
            )?;
            step.require_metadata_current()?;
            step.require_live()?;
            return Ok(frame);
        }

        let entry = own_current
            .nodes
            .get(peer.node_id())
            .ok_or_else(|| invalid("OLD observer absent from current membership"))?;
        if entry.incarnation != peer.member_incarnation()
            || peer.plaintext_loopback()
            || peer
                .certificate_sha256()
                .is_none_or(|p| !entry.accepts_tls_certificate_sha256(p))
        {
            return Err(invalid(
                "OLD readonly observer actual TLS incarnation/pin differs",
            ));
        }
        let retained = table
            .try_lock()
            .ok_or_else(|| invalid("OLD source table busy"))?
            .get(&request.range_id)
            .cloned()
            .ok_or_else(|| invalid("OLD source has no retained actual operation"))?;
        {
            let held = retained
                .try_lock()
                .ok_or_else(|| invalid("OLD source operation still active"))?;
            if held.actual.is_none() && held.constraint_owner.is_none() {
                return Err(invalid("OLD source native closure is not known complete"));
            }
            request.require_plan(&held.plan)?;
            if !held.plan.old_members().contains_key(peer.node_id())
                || !held.plan.old_members().contains_key(local)
            {
                return Err(invalid(
                    "OLD readonly fetch requires actual old donor and recipient",
                ));
            }
        }
        self.finish_remote_old_observation(
            peer,
            request,
            &own,
            receiver_business,
            &metadata,
            &service,
            &table,
            &token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn constrain_remote_old(
        &self,
        peer: &MetadataReadParticipant,
        request: &NativeRemoteOldRequestV1,
        role: Arc<RecoveryRoleAdmission>,
        own: HostMetadataReadObservation,
        business: Uuid,
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        service: Arc<Mutex<ClusterDataNodeService>>,
        table: Arc<RetainedInitialOldTable>,
        token: &CancellationToken,
    ) -> Result<ActualNativeRemoteOldFrame> {
        let step =
            self.observe_initial_old_constraint_step(metadata.clone(), role, own, business, token)?;
        request.require_plan(step.plan())?;
        let retained = {
            let mut entries = table
                .try_lock()
                .ok_or_else(|| invalid("OLD constraint owner table busy"))?;
            if let Some(owner) = entries.get(&request.range_id) {
                owner.clone()
            } else {
                if entries.len() >= 8 {
                    return Err(invalid(
                        "OLD constraint owner capacity; no refusal eviction",
                    ));
                }
                let runtime = Arc::new(NativeJointHandoffRuntime::new(service.clone())?);
                let owner = Arc::new(Mutex::new(RetainedInitialOld {
                    writer: Arc::clone(step.writer_snapshot()),
                    plan_sha256: request.scope_sha256.clone(),
                    plan: step.plan().clone(),
                    runtime,
                    actual: None,
                    constraints: None,
                    constraint_owner: None,
                    outcome: None,
                }));
                entries.insert(request.range_id, owner.clone());
                owner
            }
        };
        {
            let mut held = retained
                .try_lock()
                .ok_or_else(|| invalid("OLD constraint operation already active"))?;
            if held.plan_sha256 != request.scope_sha256
                || !Arc::ptr_eq(&held.writer, step.writer_snapshot())
                || held.actual.is_some()
                || held.outcome.is_some()
            {
                return Err(invalid(
                    "OLD constraint capture cannot retag another original producer",
                ));
            }
            let runtime = held.runtime.clone();
            step.require_joint_runtime(&runtime)?;
            let facts = runtime.observe_constraints_for_constraint_step(
                &step,
                &mut held.constraint_owner,
                token,
            )?;
            held.constraints = Some(facts);
        }
        step.require_metadata_current()?;
        step.require_live()?;
        let frame = self.finish_remote_old_observation(
            peer,
            request,
            step.own(),
            business,
            &metadata,
            &service,
            &table,
            token,
        )?;
        if frame.reply.closed.is_some() {
            return Err(invalid(
                "constraint-only producer cannot emit a closed OLD vote",
            ));
        }
        step.require_metadata_current()?;
        step.require_live()?;
        Ok(frame)
    }
    fn finish_remote_old_observation(
        &self,
        _peer: &MetadataReadParticipant,
        request: &NativeRemoteOldRequestV1,
        own: &HostMetadataReadObservation,
        own_business: Uuid,
        metadata: &Arc<Mutex<MetadataConsensusStore>>,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        table: &Arc<RetainedInitialOldTable>,
        token: &CancellationToken,
    ) -> Result<ActualNativeRemoteOldFrame> {
        token.check()?;
        self.require_live()?;
        let (plan, actual, constraint_owner, retained_constraints, writer) = {
            let entries = table
                .try_lock()
                .ok_or_else(|| invalid("OLD source retained table busy at observation"))?;
            let held = entries
                .get(&request.range_id)
                .ok_or_else(|| invalid("OLD source retained operation disappeared"))?;
            let held = held
                .try_lock()
                .ok_or_else(|| invalid("OLD source retained operation busy at observation"))?;
            if held.plan_sha256 != request.scope_sha256 {
                return Err(invalid("OLD observation changed retained scope"));
            }
            (
                held.plan.clone(),
                held.actual.clone(),
                held.constraint_owner.clone(),
                held.constraints.clone().ok_or_else(|| {
                    invalid("OLD actual irreversible constraints were not retained")
                })?,
                held.writer.clone(),
            )
        };
        request.require_plan(&plan)?;
        if !Arc::ptr_eq(&writer, &self.writer()) {
            return Err(invalid(
                "readonly OLD response cannot retag retained original writer",
            ));
        }
        let local = own.requester_node_id();
        plan.require_source_host_metadata(own, own_business, local)?;
        let constraints = if actual.is_some() {
            retained_constraints
        } else {
            let owner = constraint_owner.as_ref().ok_or_else(|| {
                invalid("OLD native outcome remains uncertain without actual constraint owner")
            })?;
            let s = service.try_lock().ok_or_else(|| {
                invalid("OLD actual constraint service busy at readonly observation")
            })?;
            let current = owner.actual_facts(&s)?;
            if current != retained_constraints {
                return Err(invalid("OLD actual retained constraint facts changed"));
            }
            current
        };
        let member = {
            let store = loop {
                token.check()?;
                own.require_live_request(own_business)?;
                if let Some(s) = metadata.try_lock_for(METADATA_LOCK_POLL) {
                    break s;
                }
            };
            store.require_listener_database(&self.database)?;
            store.observe_owned_native_metadata_member(own, own_business, token)?
        };
        let facts = actual
            .as_ref()
            .map(|actual| actual.actual_materialized_facts())
            .transpose()?;
        let (birth, db_open) = {
            let s = service
                .try_lock()
                .ok_or_else(|| invalid("OLD source actual service busy at observation"))?;
            let db = s.database();
            let db = db
                .try_read()
                .ok_or_else(|| invalid("OLD source actual DB busy at observation"))?;
            self.database.require_database(&db)?;
            member.require_database(&db)?;
            self.store_slot.require_live()?;
            if actual.is_some() {
                let lineage = db.current_native_range_source_lineage(request.range_id)?;
                plan.require_old_native_configuration(&lineage)?;
                s.require_remote_old_original_writer(
                    &db,
                    &writer,
                    &self.store_slot,
                    lineage.native_write_range()?,
                )?;
            } else {
                writer.require_old_constraint_predecessor(&db, &plan)?;
                s.require_remote_old_original_writer(
                    &db,
                    &writer,
                    &self.store_slot,
                    &plan.old_native_predecessor_data()?,
                )?;
            }
            let topology = own.committed_topology(own_business)?;
            let bootstrap = topology
                .range_image_bootstrap_id
                .ok_or_else(|| invalid("OLD actual native bootstrap disappeared"))?;
            let native = db.native_recovery_admission(
                &topology.cluster_id,
                bootstrap,
                local,
                own.requester_incarnation(),
            )?;
            plan.require_actual_native(&native)?;
            (
                crate::distribution::NativeStoreBirthV1::from_actual_native(&native)?,
                db.incarnation_id().to_owned(),
            )
        };
        if constraints.scope_sha256 != request.scope_sha256
            || constraints.node_id != *local
            || constraints.birth_id != birth.birth_id()
            || constraints.birth_sha256 != birth.birth_sha256()
            || facts.as_ref().is_some_and(|f| {
                f.scope_sha256 != request.scope_sha256
                    || f.node_id != *local
                    || f.birth_id != birth.birth_id()
                    || f.birth_sha256 != birth.birth_sha256()
            })
        {
            return Err(invalid("OLD actual native fact/constraints/birth differ"));
        }
        let mut reply = NativeRemoteOldReplyV1 {
            request: request.clone(),
            node_id: local.clone(),
            birth,
            database_open: db_open,
            store_open: self.store_slot.id(),
            metadata_store_id: member.store_id(),
            metadata_term: own.term(),
            metadata_read_index: own.read_index(),
            metadata_context_id: own.context_id(),
            closed: facts,
            constraints,
            checksum_sha256: String::new(),
        };
        reply.checksum_sha256 = reply.checksum()?;
        {
            let store = loop {
                token.check()?;
                own.require_live_request(own_business)?;
                if let Some(s) = metadata.try_lock_for(METADATA_LOCK_POLL) {
                    break s;
                }
            };
            member.require_current(&store)?;
        }
        if let Some(actual) = actual {
            actual.require_current()?;
        } else {
            let s = service.try_lock().ok_or_else(|| {
                invalid("OLD actual constraint service busy before readonly reply")
            })?;
            constraint_owner.as_ref().unwrap().actual_facts(&s)?;
        }
        self.require_live()?;
        token.check()?;
        own.require_live_request(own_business)?;
        self.image_sources
            .upgrade()
            .ok_or_else(|| invalid("OLD hosted process ended before reply"))?
            .require_original_process()?;
        Ok(ActualNativeRemoteOldFrame { reply })
    }
}
