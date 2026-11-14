//! Original target image obligations retained across a READONLY export-NEW
//! observation. OWN is never converted to Role, LocalNEW or Serving authority.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_range_image::{
    NativeObserveRegisteredNewRequestV1, NativeRegisteredNewExportReplyV1,
    NativeSourceExportBindingV1,
};
use crate::distribution_range_mutation::SourceCaptureReadPin;
use std::time::{Duration, Instant};

pub(super) struct RegisteredExportNewPin {
    binding: NativeSourceExportBindingV1,
    frozen: Arc<crate::distribution_range_image_install::NativeImageInstallFreeze>,
    pin: Option<Arc<SourceCaptureReadPin>>,
    requests: std::collections::BTreeMap<Uuid, Vec<u8>>,
}
impl NativeRegisteredReplicaAdmission {
    /// Dedicated immutable Registered ingress. A retained original owner is a
    /// prerequisite, not leader authority; only fresh local OWN authorizes this
    /// bounded observation of its protected native/materialized image.
    pub(crate) fn observe_registered_export_new(
        &self,
        peer: &MetadataReadParticipant,
        request: &NativeObserveRegisteredNewRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeRegisteredNewExportReplyV1> {
        let token = request.cancellation(parent)?;
        self.require_live()?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("Registered NEW original backend ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("Registered NEW original runtime ended"))?;
        self.require_listener(&backend.service, &runtime)?;
        if peer.node_id() != &request.binding.source_id
            || peer.plaintext_loopback()
            || peer.member_incarnation() != request.binding.source_birth.node_incarnation()
            || backend.node_id != request.binding.target_id
        {
            return Err(invalid(
                "Registered NEW callback is not actual selected donor to original target",
            ));
        }
        // Clone the ORIGINAL retained owner and end the table latch before MQ,
        // service/DB/native work. A caller never inserts or replaces this owner.
        let table = self
            .joint_targets
            .upgrade()
            .ok_or_else(|| invalid("Registered NEW original joint table ended"))?;
        let owner = {
            let table = table
                .try_lock()
                .ok_or_else(|| invalid("Registered NEW original owner table busy"))?;
            table
                .get(&request.binding.range_id)
                .cloned()
                .ok_or_else(|| {
                    invalid(
                        "Registered NEW requires complete original native base/catalogue/A owner",
                    )
                })?
        };
        if !Arc::ptr_eq(owner.service(), &backend.service) {
            return Err(invalid("Registered NEW original service changed"));
        }
        let target = owner.target();
        request.binding.require_manifest(target.manifest())?;
        target.readonly_plan.require_manifest(target.manifest())?;
        let leader = backend
            .provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("Registered NEW metadata leader absent"))?;
        let business = Uuid::now_v7();
        let remaining = token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let own = backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining.min(Duration::from_secs(5)),
            &token,
        )?;
        target
            .readonly_plan
            .require_fresh_target_own(&own, business)?;
        let topology = own.committed_topology(business)?;
        let donor = topology
            .nodes
            .get(&request.binding.source_id)
            .ok_or_else(|| invalid("Registered NEW donor absent"))?;
        if donor.native_store_birth_v1.as_ref() != Some(&request.binding.source_birth)
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !donor.accepts_tls_certificate_sha256(pin))
        {
            return Err(invalid("Registered NEW donor TLS/native identity differs"));
        }
        let member = {
            let store = backend.lock_metadata(&token)?;
            store.require_listener_database(&self.database)?;
            let member = store.observe_owned_native_metadata_member(&own, business, &token)?;
            if member.store_id() != self.metadata_store_id {
                return Err(invalid("Registered NEW original MStore UUID changed"));
            }
            member
        };
        let point =
            crate::distribution_data::ClusterDataNodeService::capture_actual_joint_metadata_point(
                owner.service(),
                &own,
                business,
                &token,
            )?
            .ok_or_else(|| invalid("Registered NEW requires actual current native MStore point"))?;
        point.require_external_current()?;
        let frozen = owner.retained_freeze()?;
        let mut reply = {
            let service = owner
                .service()
                .try_lock()
                .ok_or_else(|| invalid("Registered NEW actual service busy"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Registered NEW actual DB busy"))?;
            self.database.require_database(&db)?;
            member.require_database(&db)?;
            service.with_writer_data_domain_store(&db, |store| {
                point.require_native_store_point(&db, store)?;
                // Same existing full native base/final/profile/catalogue/A validator
                // used by certificate-backed LOCAL NEW; its constructor is untouched.
                target.require_current(&db, store, frozen.try_as_frozen()?, &token)?;
                self.original_hook.require_current(&db)?;
                let native = target.native();
                if crate::distribution::NativeStoreBirthV1::from_actual_native(native)?
                    != request.binding.target_birth
                {
                    return Err(invalid(
                        "Registered NEW original actual target native birth differs",
                    ));
                }
                let exact_bytes =
                    crate::distribution_range_image::bounded_json(request, 64 * 1024)?;
                if owner.remote_new.try_lock()
                    .ok_or_else(|| invalid("Registered export NEW handoff custody busy"))?
                    .as_ref().is_some_and(|phase| phase.handoff_started())
                {
                    return Err(invalid("Registered export NEW cannot reintroduce a pin after original decision handoff"));
                }
                let mut retained = owner
                    .export_new
                    .try_lock()
                    .ok_or_else(|| invalid("Registered NEW original pin observation busy"))?;
                if let Some(actual) = retained.as_ref() {
                    if actual.binding != request.binding || !Arc::ptr_eq(&actual.frozen, &frozen) {
                        return Err(invalid(
                            "Registered NEW cannot replace original export image/freeze/pin",
                        ));
                    }
                } else {
                    let pin = db.range_mutation_gate().try_source_capture_read_pin(
                        request.binding.range_id,
                        frozen.try_as_frozen()?,
                    )?;
                    // Retain the actual pin BEFORE any following check or reply.
                    *retained = Some(RegisteredExportNewPin {
                        binding: request.binding.clone(),
                        frozen: frozen.clone(),
                        pin: Some(Arc::new(pin)),
                        requests: Default::default(),
                    });
                }
                let retained = retained.as_mut().unwrap();
                if let Some(old) = retained.requests.get(&request.challenge) {
                    if old != &exact_bytes {
                        return Err(invalid(
                            "Registered NEW challenge reused with changed exact request",
                        ));
                    }
                } else {
                    if retained.requests.len() >= 64 {
                        return Err(invalid(
                            "Registered NEW request custody capacity; no eviction",
                        ));
                    }
                    retained.requests.insert(request.challenge, exact_bytes);
                }
                db.range_mutation_gate().validate_source_capture_read_pin(
                    request.binding.range_id,
                    retained.pin.as_ref().ok_or_else(|| invalid("Registered export NEW original pin handed off"))?,
                )?;
                let base = target.base();
                let image = &target.image;
                let seal = target.source_seal();
                let facts = crate::distribution_range_handoff::NativeJointMaterializedFacts {
                    scope_sha256: target.readonly_plan.checksum_sha256()?,
                    node_id: backend.node_id.clone(),
                    birth_id: base.birth_id(),
                    birth_sha256: base.birth_sha256().into(),
                    native_receipt_sha256: base.native_receipt_sha256().into(),
                    range_index: base.range_index(),
                    logical_genesis_sha256: base.logical_genesis_sha256().into(),
                    logical_history_sha256: base.logical_history_sha256().into(),
                    actual_local_visibility_sequence: base.target_snapshot_bound().get(),
                    source_snapshot_db_sequence: Some(seal.reserved_db_upper_bound),
                    image_root_sha256: Some(seal.image_root_sha256.clone()),
                    metadata_term: own.term(),
                    metadata_read_index: own.read_index(),
                    metadata_context_id: own.context_id(),
                };
                Ok(NativeRegisteredNewExportReplyV1 {
                    request: request.clone(),
                    materialized: facts,
                    target_database_open: db.incarnation_id().to_string(),
                    target_store_open: target.store_open_id(),
                    target_metadata_store: self.metadata_store_id,
                    final_image: image.pointer(),
                    target_catalogue: image.target_catalogue_id(),
                    target_namespaces: image.target_namespace_ids().to_vec(),
                    image_generations: image.image_generations(),
                    record_count: image.record_count(),
                    serialized_record_bytes: image.serialized_record_bytes(),
                    checksum_sha256: String::new(),
                })
            })?
        };
        // No MStore lock spans any target engine/native/pin lock.
        {
            let store = backend.lock_metadata(&token)?;
            store.require_listener_database(&self.database)?;
            member.require_current(&store)?;
        }
        point.require_external_current()?;
        target
            .readonly_plan
            .require_fresh_target_own(&own, business)?;
        self.require_listener(&backend.service, &runtime)?;
        token.check()?;
        reply.checksum_sha256 = reply.checksum()?;
        reply.require_manifest(target.manifest())?;
        Ok(reply)
    }
}

impl NativeRegisteredRemoteJointOwner {
    /// Share the SAME exclusive native pin between its two genuine owned
    /// obligations. The gate never permits a second independently minted pin.
    pub(crate) fn retain_remote_new_materialization_pin(
        &self,
        issuance: &crate::distribution_range_handoff::NativeRemoteNewTargetPinIssuance<'_>,
    ) -> Result<Arc<SourceCaptureReadPin>> {
        let (database, binding, frozen) = issuance.require_owner(self)?;
        self.require_first_policy_attempt_lifetime()?;
        binding.require_manifest(self.target.manifest())?;
        database
            .range_mutation_gate()
            .validate_frozen(binding.range_id, frozen.try_as_frozen()?)?;
        let mut retained = self
            .export_new
            .try_lock()
            .ok_or_else(|| invalid("Registered actual shared materialization pin custody busy"))?;
        if let Some(entry) = retained.as_ref() {
            if entry.binding != *binding || !Arc::ptr_eq(&entry.frozen, frozen) {
                return Err(invalid(
                    "Registered NEW cannot replace original shared pin/freeze binding",
                ));
            }
        } else {
            let pin = database
                .range_mutation_gate()
                .try_source_capture_read_pin(binding.range_id, frozen.try_as_frozen()?)?;
            *retained = Some(RegisteredExportNewPin {
                binding: binding.clone(),
                frozen: frozen.clone(),
                pin: Some(Arc::new(pin)),
                requests: Default::default(),
            });
        }
        let actual = retained.as_ref().unwrap().pin.as_ref().ok_or_else(|| {
            invalid("Registered original shared pin was already handed off; no recreation")
        })?;
        database
            .range_mutation_gate()
            .validate_source_capture_read_pin(binding.range_id, actual)?;
        Ok(actual.clone())
    }
    pub(crate) fn handoff_remote_new_export_pin(
        &self,
        permit: &crate::distribution_range_handoff::NativeRemoteNewPinHandoffPermit<'_>,
    ) -> Result<()> {
        permit.require_registered(self)?;
        self.require_first_policy_attempt_lifetime()?;
        let mut retained = self
            .export_new
            .try_lock()
            .ok_or_else(|| invalid("Registered original export pin handoff busy"))?;
        let dropped = {
            let _version = permit.publication_guard()?;
            permit.require_memory_current()?;
            let dropped = retained.as_mut().and_then(|entry| entry.pin.take());
            // Keep the exact original binding, request evidence and freeze in
            // its original owner. This flag precedes every postcheck/reply.
            permit.record_target_export_detached();
            dropped
        };
        drop(retained);
        drop(dropped); // actual gate Drop never under publication guard
        Ok(())
    }
}
