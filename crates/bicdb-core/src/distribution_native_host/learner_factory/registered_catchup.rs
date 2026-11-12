//! Existing protected Registered roots only. Blank/create/Prepared rules in
//! the parent stay Learner-only; this context never grants voting readiness.
use super::*;
impl NativeUnassignedLearnerFactory {
    fn registered_context(
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<(ClusterTopology, ClusterNodeId, String)> {
        observed.require_live_request(business)?;
        network.validate()?;
        let topology = observed.committed_topology(business)?;
        let local = observed.requester_node_id();
        let member = topology
            .nodes
            .get(local)
            .ok_or_else(|| invalid("Registered current member absent"))?;
        let tls = network
            .tls
            .as_ref()
            .ok_or_else(|| invalid("Registered catch-up TLS absent"))?;
        let pin = crate::replication_transport::replication_certificate_sha256(&tls.cert_path)?;
        if network.dev_localhost_plaintext
            || !tls.require_client_cert
            || tls.dev_localhost_plaintext
            || member.lifecycle != ClusterNodeLifecycle::Active
            || member.incarnation != observed.requester_incarnation()
            || member.native_store_birth_v1.is_none()
            || topology.range_image_bootstrap_id.is_none()
            || !member.accepts_tls_certificate_sha256(&pin)
            || topology
                .ranges
                .values()
                .any(|r| r.replicas.iter().any(|p| &p.node_id == local))
        {
            return Err(invalid("Registered catch-up requires exact current bound own identity, TLS and no data assignment"));
        }
        let mut schema = None;
        for range in topology.ranges.values() {
            let required = range
                .required_schema_sha256(&topology.nodes)
                .ok_or_else(|| invalid("Registered catch-up schema domain absent"))?;
            if schema.as_ref().is_some_and(|old: &String| old != required) {
                return Err(invalid("Registered catch-up schema transition unsupported"));
            }
            schema = Some(required.to_owned());
        }
        if topology
            .nodes
            .values()
            .filter(|m| {
                m.lifecycle == ClusterNodeLifecycle::Active
                    && m.metadata_role == MetadataMemberRole::Voter
            })
            .any(|m| {
                !m.native_store_birth_v1
                    .as_ref()
                    .is_some_and(|b| b.writer_enforcement_version() == 1)
            })
        {
            return Err(invalid(
                "Registered catch-up needs actual enrolled native metadata cohort",
            ));
        }
        Ok((
            topology.clone(),
            local.clone(),
            schema.ok_or_else(|| invalid("Registered catch-up has no schema"))?,
        ))
    }
    fn require_registered_database(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<ClusterTopology> {
        let (topology, local, schema) = Self::registered_context(observed, business, network)?;
        if local != self.spec.node_id
            || !self.spec.initial_ranges.is_empty()
            || schema != self.manifest.schema_sha256
            || observed.restore_activation(business)? != self.spec.restore_activation.as_ref()
            || topology.nodes[&local].native_store_birth_v1.as_ref() != Some(&self.manifest.birth)
        {
            return Err(invalid(
                "Registered actual factory birth/schema/restore/no-data scope differs",
            ));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("Registered actual DB busy"))?;
        let native = db.native_recovery_admission(
            &self.spec.cluster_id,
            self.spec.bootstrap_id,
            &local,
            self.spec.member_incarnation,
        )?;
        if db.current_commit_seq() != 0
            || db.schema_compatibility_fingerprint()?.sha256 != schema
            || NativeStoreBirthV1::from_actual_native(&native)? != self.manifest.birth
            || native.restore_activation() != self.spec.restore_activation.as_ref()
        {
            return Err(invalid(
                "Registered actual DB schema/birth/restore/business history differs",
            ));
        }
        let record = db.read_native_metadata_store(&topology, &local)?;
        if !record.registered() || record.initial_namespace_ids().is_none() {
            return Err(invalid(
                "Registered continuation refuses blank/Prepared/old-unbound metadata record",
            ));
        }
        record.require_namespaces_current(&db)?;
        observed.require_live_request(business)?;
        Ok(topology)
    }
    pub(in crate::distribution_native_host) fn registered_metadata_context(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<ClusterTopology> {
        self.require_registered_database(observed, business, network)
    }
    pub(in crate::distribution_native_host) fn require_registered_metadata_catchup(
        &self,
        proof: &crate::distribution_consensus::NativeRegisteredMetadataCatchupObservation<'_>,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<()> {
        let topology = self.require_registered_database(observed, business, network)?;
        if proof.topology()? != &topology {
            return Err(invalid("Registered catch-up current MQ changed"));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("Registered catch-up DB busy"))?;
        proof.require_database(&db)?;
        observed.require_live_request(business)
    }
    pub(crate) fn reopen_registered_metadata(
        root: &Path,
        config: DbConfig,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        let (_, local, schema) = Self::registered_context(observed, business, network)?;
        let mut bytes = Vec::new();
        File::open(root.join(MANIFEST))?
            .take((MAX_PLAN_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_PLAN_BYTES {
            return Err(invalid("Registered manifest bound exceeded"));
        }
        let manifest: LearnerManifest = serde_json::from_slice(&bytes)?;
        if manifest.version != 1
            || manifest.spec.node_id != local
            || !manifest.spec.initial_ranges.is_empty()
            || manifest.schema_sha256 != schema
            || manifest.spec.restore_activation.as_ref() != observed.restore_activation(business)?
        {
            return Err(invalid("Registered persisted factory scope differs"));
        }
        let (db, factory) = BicDb::open_range_image_database(root, config, &manifest.spec)?;
        let actual = Self {
            spec: manifest.spec.clone(),
            database: Arc::new(RwLock::new(db)),
            factory,
            manifest,
        };
        actual.require_registered_database(observed, business, network)?;
        observed.require_live_request(business)?;
        token.check()?;
        Ok(actual)
    }
}
