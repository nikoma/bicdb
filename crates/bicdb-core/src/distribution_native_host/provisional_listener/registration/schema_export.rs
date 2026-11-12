//! Actual schema DATA export, not learner admission or source/read authority.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;

impl NativeMetadataRegistrationController {
    pub(crate) fn export_learner_schema(
        &self,
        caller: &MetadataReadParticipant,
        range_id: crate::RangeId,
        epoch: u64,
        parent: &CancellationToken,
    ) -> Result<crate::ClusterSchemaBundle> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let leader = self
            .bootstrap(&token)?
            .metadata_leader_id
            .ok_or_else(|| invalid("native schema source has no metadata leader"))?;
        let first_business = Uuid::now_v7();
        let first = self.launch.transport.acquire_host_metadata_observation(
            &leader,
            first_business,
            Duration::from_secs(5),
            &token,
        )?;
        let topology = first.committed_topology(first_business)?;
        let requester = topology
            .nodes
            .get(caller.node_id())
            .ok_or_else(|| invalid("native schema caller absent"))?;
        let source = topology
            .nodes
            .get(&self.launch.seed.spec.node_id)
            .ok_or_else(|| invalid("native schema source absent"))?;
        let range = topology
            .range_by_id(range_id)
            .filter(|r| r.epoch == epoch)
            .ok_or_else(|| invalid("native schema range/epoch changed"))?;
        if caller.plaintext_loopback()
            || requester.lifecycle != ClusterNodeLifecycle::Active
            || requester.metadata_role != MetadataMemberRole::Learner
            || requester.incarnation != caller.member_incarnation()
            || !caller
                .certificate_sha256()
                .is_some_and(|pin| requester.accepts_tls_certificate_sha256(pin))
            || topology
                .ranges
                .values()
                .any(|r| r.replicas.iter().any(|p| &p.node_id == caller.node_id()))
            || source.lifecycle != ClusterNodeLifecycle::Active
            || source.metadata_role != MetadataMemberRole::Voter
            || !range
                .replicas
                .iter()
                .any(|r| r.node_id == source.id && r.role == crate::RangeReplicaRole::Voter)
        {
            return Err(invalid("native schema export requires actual unassigned learner/current registered source voter"));
        }
        let expected = range
            .required_schema_sha256(&topology.nodes)
            .ok_or_else(|| invalid("native source has no committed schema digest"))?
            .to_owned();
        let birth = source
            .native_store_birth_v1
            .as_ref()
            .ok_or_else(|| invalid("native schema source birth unbound"))?;
        let (database, open) = {
            let service = self
                .launch
                .service
                .try_lock()
                .ok_or_else(|| invalid("native schema service busy"))?;
            self.require_service(
                &self.launch.service,
                service.native_initial_listener_store_open_id(),
            )?;
            (service.database(), service.listener_open_guard())
        };
        {
            let store = self.launch.lock_initial_metadata(&token)?;
            store.require_listener_database(&open)?;
            if store.committed_topology() != topology {
                return Err(invalid(
                    "native schema source has not installed actual current CONFIG",
                ));
            }
        }
        let (bundle, native, schema, generation, ids) = {
            let db = database
                .try_read()
                .ok_or_else(|| invalid("native schema source DB busy"))?;
            open.require_database(&db)?;
            db.check_recovery_state()?;
            let native = db.native_recovery_admission(
                &topology.cluster_id,
                topology
                    .range_image_bootstrap_id
                    .ok_or_else(|| invalid("native schema bootstrap absent"))?,
                &source.id,
                source.incarnation,
            )?;
            if native.birth_id() != birth.birth_id()
                || native.birth_sha256() != birth.birth_sha256()
                || native.writer_enforcement_version() != 1
                || native.restore_activation() != first.restore_activation(first_business)?
            {
                return Err(invalid("native schema actual source birth/restore differs"));
            }
            let schema = db.schema_compatibility_authority();
            let fingerprint = db.schema_compatibility_fingerprint()?;
            let generation = schema.generation();
            if fingerprint.sha256 != expected
                || schema.verified_sha256().as_deref() != Some(expected.as_str())
            {
                return Err(invalid("native schema actual verified digest differs"));
            }
            let mut ids = db
                .collections()
                .into_iter()
                .map(|m| (m.name, m.local_namespace_id))
                .collect::<Vec<_>>();
            ids.sort();
            if ids.iter().any(|(_, id)| id.is_none_or(|id| id.is_nil())) {
                return Err(invalid("native schema namespace identity missing"));
            }
            let bundle = db.cluster_schema_bundle()?;
            bundle.verify()?;
            crate::distribution_range_consensus::validate_native_json_size(
                &bundle,
                MAX_PLAN_BYTES,
            )?;
            if bundle.fingerprint.sha256 != expected {
                return Err(invalid("native exported bundle digest differs"));
            }
            (bundle, native, schema, generation, ids)
        };
        first.require_live_request(first_business)?;
        token.check()?;
        // Distinct genuinely fresh OWN MQ, after ALL service/DB latches drop.
        let post_business = Uuid::now_v7();
        let post = self.launch.transport.acquire_host_metadata_observation(
            &leader,
            post_business,
            Duration::from_secs(5),
            &token,
        )?;
        if post.context_id() == first.context_id()
            || post.term() < first.term()
            || post.read_index() < first.read_index()
            || post.committed_topology(post_business)? != topology
            || post.restore_activation(post_business)?
                != first.restore_activation(first_business)?
        {
            return Err(invalid(
                "native schema CONFIG/domain changed across actual export",
            ));
        }
        {
            let store = self.launch.lock_initial_metadata(&token)?;
            store.require_listener_database(&open)?;
            if store.committed_topology() != topology {
                return Err(invalid("native schema current owned CONFIG changed"));
            }
        }
        {
            let db = database
                .try_read()
                .ok_or_else(|| invalid("native schema post-export DB busy"))?;
            open.require_database(&db)?;
            native.require_current(&db)?;
            db.check_recovery_state()?;
            let mut current = db
                .collections()
                .into_iter()
                .map(|m| (m.name, m.local_namespace_id))
                .collect::<Vec<_>>();
            current.sort();
            if current != ids
                || schema.generation() != generation
                || schema.verified_sha256().as_deref() != Some(expected.as_str())
            {
                return Err(invalid(
                    "native schema/namespace generation changed during export",
                ));
            }
        }
        post.require_live_request(post_business)?;
        token.check()?;
        Ok(bundle)
    }
}
