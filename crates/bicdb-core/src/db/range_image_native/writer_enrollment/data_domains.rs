//! SAME original writer selected-range audits. Native records are constraints,
//! not deserializable authority. CLOSED is never ordinary range permission.
use super::*;
use crate::distribution_consensus::HostMetadataReadAcquireResponse;
use crate::distribution_range_handoff::NativeJointDecisionData;

const MAX_AUDITS: u32 = 65_536;
const MAX_AUDIT_BYTES: usize = 256 * 1024;
#[derive(Default, Debug)]
pub(super) struct WriterDataDomains {
    serving: BTreeMap<RangeId, Arc<VerifiedServingDomain>>,
    pending: BTreeSet<RangeId>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct OriginalWriterBinding {
    policy_sha256: String,
    registration: Uuid,
    coordinator: Uuid,
    database_incarnation: String,
    store_open_id: Uuid,
}
impl OriginalWriterBinding {
    fn from_actual(original: &NativeWriterPolicyRecord) -> Self {
        Self {
            policy_sha256: original.checksum_sha256.clone(),
            registration: original.generation,
            coordinator: original.coordinator_id,
            database_incarnation: original.database_incarnation.clone(),
            store_open_id: original.store_open_id,
        }
    }
    fn require_original(&self, original: &NativeWriterPolicyRecord) -> Result<()> {
        if self != &Self::from_actual(original) {
            return Err(invalid(
                "DATA policy audit changed original writer/registration/coordinator/open",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ClosedDomainRecord {
    version: u32,
    sequence: u32,
    original: OriginalWriterBinding,
    previous_serving_sha256: Option<String>,
    decision: NativeJointDecisionData,
    local: NativeClosedWriterPoint,
    metadata_audit: serde_json::Value,
    metadata_context: Uuid,
    metadata_store_id: Uuid,
    metadata_term: u64,
    metadata_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl ClosedDomainRecord {
    fn checksum(&self) -> Result<String> {
        let mut body = self.clone();
        body.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("native.closed-writer-data-domain.v1", body),
            MAX_AUDIT_BYTES,
        )?)))
    }
    fn validate(&self, original: &NativeWriterPolicyRecord) -> Result<()> {
        self.original.require_original(original)?;
        self.decision.validate()?;
        if self.version != 1
            || self.sequence == 0
            || self.sequence > MAX_AUDITS
            || self.native_page_xid == 0
            || self.checksum()? != self.checksum_sha256
            || self.metadata_context.is_nil()
            || self.metadata_store_id.is_nil()
            || self.metadata_term < original.metadata_term
            || self.metadata_index < original.metadata_read_index
            || self.local.decision_sha256 != self.decision.checksum_sha256()
            || self.local.birth_id != original.facts.local_birth_id
            || self.local.birth_sha256 != original.facts.local_birth_sha256
            || self.local.schema_sha256 != original.facts.schema_sha256
            || self.local.range_index != self.decision.range_index()
            || self.local.logical_genesis_sha256 != self.decision.logical_genesis_sha256()
            || self.local.logical_history_sha256 != self.decision.logical_history_sha256()
            || self
                .previous_serving_sha256
                .as_ref()
                .is_some_and(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(invalid("damaged CLOSED writer DATA-domain audit"));
        }
        let audit: HostMetadataReadAcquireResponse =
            serde_json::from_value(self.metadata_audit.clone())?;
        audit.validate(&audit.request)?;
        self.decision.require_metadata_domain(
            &audit.evidence.committed_topology,
            audit.evidence.restore_activation.as_ref(),
        )?;
        if audit.request.requester_node_id != original.facts.local_node_id
            || audit.request.requester_incarnation != original.facts.local_incarnation
            || audit.evidence.context_id != self.metadata_context
            || audit.evidence.term != self.metadata_term
            || audit.evidence.read_index != self.metadata_index
        {
            return Err(invalid("CLOSED writer DATA historical OWN audit differs"));
        }
        Ok(())
    }
}
fn require_original_metadata_store(db: &BicDb, record: &ClosedDomainRecord) -> Result<()> {
    let audit: HostMetadataReadAcquireResponse =
        serde_json::from_value(record.metadata_audit.clone())?;
    let actual = db.read_native_metadata_store_point(
        &audit.evidence.committed_topology,
        &audit.request.requester_node_id,
    )?;
    if !actual.registered() || actual.store_id() != record.metadata_store_id {
        return Err(invalid(
            "writer DATA original protected MStore UUID changed",
        ));
    }
    actual.require_namespaces_current(db)
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ServingDomainRecord {
    version: u32,
    sequence: u32,
    original: OriginalWriterBinding,
    closed_sha256: String,
    decision_sha256: String,
    range: RangeDescriptor,
    local: NativeClosedWriterPoint,
    native_bridge_sha256: String,
    catalogue_activation_sha256: String,
    released_generation: Uuid,
    open_quorum_audit: serde_json::Value,
    post_metadata_audit: serde_json::Value,
    native_page_xid: u64,
    checksum_sha256: String,
}
/// Constructed only AFTER real all-released Open quorum publication/readback.
/// There is intentionally no Deserialize/cold-loader constructor for this cache.
#[derive(Debug)]
struct VerifiedServingDomain {
    record: ServingDomainRecord,
    continuity: crate::db::NativeJointOpenContinuity,
}
fn closed_current(generation: Uuid, range: RangeId) -> String {
    format!("writer-data-domain/{generation}/{range}/closed/current")
}
fn serving_current(generation: Uuid, range: RangeId) -> String {
    format!("writer-data-domain/{generation}/{range}/serving/current")
}
fn archive(generation: Uuid, sequence: u32) -> String {
    format!("writer-data-domain/{generation}/history/{sequence}")
}

mod closed;
pub(crate) use closed::{NativeClosedWriterDomain, NativeWriterDataDomainTransition};
mod policy;
pub(crate) use policy::NativeWriterDataDomainPolicy;
mod serving;
pub(crate) use serving::NativeWriterServingDomainPublication;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AuditCounter {
    version: u32,
    original: OriginalWriterBinding,
    sequence: u32,
    checksum_sha256: String,
}
impl AuditCounter {
    fn checksum(&self) -> Result<String> {
        let mut body = self.clone();
        body.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &body,
            16 * 1024,
        )?)))
    }
    fn validate(&self, original: &NativeWriterPolicyRecord) -> Result<()> {
        self.original.require_original(original)?;
        if self.version != 1
            || self.sequence == 0
            || self.sequence > MAX_AUDITS
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("writer DATA audit accounting is damaged"));
        }
        Ok(())
    }
}
fn counter_key(generation: Uuid) -> String {
    format!("writer-data-domain/{generation}/accounting")
}
fn next_sequence(db: &BicDb, original: &NativeWriterPolicyRecord) -> Result<u32> {
    let current: Option<AuditCounter> =
        db.read_native_range_image_record(&counter_key(original.generation))?;
    let sequence = match current {
        Some(c) => {
            c.validate(original)?;
            c.sequence
                .checked_add(1)
                .ok_or_else(|| invalid("writer DATA audit sequence overflow"))?
        }
        None => 1,
    };
    if sequence > MAX_AUDITS {
        return Err(invalid(
            "writer DATA audit bound exhausted; checkpoint/compaction remains unsupported",
        ));
    }
    Ok(sequence)
}
fn load_serving(
    db: &BicDb,
    writer: &NativeWriterHostState,
    range: RangeId,
) -> Result<Option<ServingDomainRecord>> {
    let actual: Option<ServingDomainRecord> =
        db.read_native_range_image_record(&serving_current(writer.record.generation, range))?;
    let cached = writer.data_domains.read().serving.get(&range).cloned();
    match (&actual,cached) {
        (None,None)=>{},
        (Some(record),Some(cached))=>{
            record.validate(&writer.record)?;
            if *record!=cached.record {return Err(invalid("writer DATA native Serving predecessor differs from actual retained cache"));}
            require_serving_record_current(db,writer,record)?;
        }
        _=>return Err(invalid("writer DATA Serving predecessor needs exact retained publication reconciliation; JSON is not authority")),
    }
    Ok(actual)
}
impl ServingDomainRecord {
    fn checksum(&self) -> Result<String> {
        let mut body = self.clone();
        body.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("native.serving-writer-data-domain.v1", body),
            MAX_AUDIT_BYTES,
        )?)))
    }
    fn validate(&self, original: &NativeWriterPolicyRecord) -> Result<()> {
        self.original.require_original(original)?;
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.version != 1
            || self.sequence == 0
            || self.sequence > MAX_AUDITS
            || self.native_page_xid == 0
            || self.released_generation.is_nil()
            || self.range.id.get() == 0
            || !digest(&self.closed_sha256)
            || !digest(&self.decision_sha256)
            || !digest(&self.native_bridge_sha256)
            || !digest(&self.catalogue_activation_sha256)
            || self.local.decision_sha256 != self.decision_sha256
            || self.local.birth_id != original.facts.local_birth_id
            || self.local.birth_sha256 != original.facts.local_birth_sha256
            || self.local.schema_sha256 != original.facts.schema_sha256
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged Serving writer DATA-domain audit"));
        }
        Ok(())
    }
}
fn require_closed_record_current(
    db: &BicDb,
    writer: &NativeWriterHostState,
    record: &ClosedDomainRecord,
) -> Result<()> {
    record.validate(&writer.record)?;
    require_original_metadata_store(db, record)?;
    if !crate::format::native_writer_data_domain_active(db.data_path())? {
        return Err(invalid("writer DATA audit lost sticky reader fence"));
    }
    let counter: AuditCounter = db
        .read_native_range_image_record(&counter_key(writer.record.generation))?
        .ok_or_else(|| invalid("writer DATA audit accounting missing"))?;
    counter.validate(&writer.record)?;
    if counter.sequence < record.sequence {
        return Err(invalid("writer DATA audit counter regressed"));
    }
    for key in [
        closed_current(writer.record.generation, record.decision.old_range().id),
        archive(writer.record.generation, record.sequence),
    ] {
        let actual: ClosedDomainRecord = db
            .read_native_range_image_record(&key)?
            .ok_or_else(|| invalid("writer CLOSED audit CURRENT/archive missing"))?;
        actual.validate(&writer.record)?;
        if actual != *record {
            return Err(invalid("writer CLOSED audit CURRENT/archive changed"));
        }
    }
    let facts = &writer.record.facts;
    let native = db.native_recovery_admission(
        &facts.cluster_id,
        facts.bootstrap_id,
        &facts.local_node_id,
        facts.local_incarnation,
    )?;
    let decision = db.read_native_joint_decision(
        &native,
        record.decision.old_range().id,
        record.decision.checksum_sha256(),
    )?;
    if decision.data() != &record.decision
        || decision.checksum_sha256() != record.local.local_decision_sha256
    {
        return Err(invalid(
            "writer CLOSED audit changed protected joint decision",
        ));
    }
    let actual = db.current_native_range_source_lineage(record.decision.old_range().id)?;
    let point = &record.local;
    if actual.base.checksum_sha256 != point.physical_base_sha256
        || actual.base.local_namespace_ids != point.namespace_ids
        || actual.history_generation() != point.history_generation
        || actual.schema_sha256() != point.schema_sha256
        || actual.logical_genesis_sha256() != point.logical_genesis_sha256
        || actual.range_index() < point.range_index
        || actual.actual_local_visibility_sequence() < point.local_visibility_sequence
    {
        return Err(invalid(
            "writer CLOSED audit actual native base/UID/history/floor changed",
        ));
    }
    let history = db.read_native_history_entry(
        record.decision.old_range().id,
        point.history_generation,
        point.range_index,
    )?;
    if history.commitment_sha256 != point.logical_history_sha256 {
        return Err(invalid("writer CLOSED audit immutable cut prefix changed"));
    }
    if actual.range_index() == point.range_index {
        if actual.native_receipt_sha256() != point.native_prefix_sha256
            || actual.actual_local_visibility_sequence() != point.local_visibility_sequence
        {
            return Err(invalid(
                "writer CLOSED audit actual local cut mapping changed",
            ));
        }
    } else {
        // Higher history is allowed only AFTER this exact CLOSED audit has a
        // real verified Serving publication on the SAME writer; counters alone
        // never permit a frozen/closed domain to advance.
        let cache = writer
            .data_domains
            .read()
            .serving
            .get(&record.decision.old_range().id)
            .cloned()
            .ok_or_else(|| {
                invalid("CLOSED writer native cut advanced without actual Serving publication")
            })?;
        if cache.record.closed_sha256 != record.checksum_sha256 {
            return Err(invalid(
                "writer DATA higher native history belongs to another transition",
            ));
        }
        require_serving_record_current(db, writer, &cache.record)?;
    }
    if let Some(expected) = &point.old_close_sha256 {
        db.require_closed_writer_old_archive(
            &native,
            &record.decision,
            &actual,
            point.local_promise.as_ref(),
            expected,
        )?;
    }
    Ok(())
}
fn require_serving_record_current(
    db: &BicDb,
    writer: &NativeWriterHostState,
    record: &ServingDomainRecord,
) -> Result<()> {
    record.validate(&writer.record)?;
    if !crate::format::native_writer_data_domain_active(db.data_path())? {
        return Err(invalid("writer Serving audit lost sticky reader fence"));
    }
    let counter: AuditCounter = db
        .read_native_range_image_record(&counter_key(writer.record.generation))?
        .ok_or_else(|| invalid("writer Serving audit accounting missing"))?;
    counter.validate(&writer.record)?;
    if counter.sequence < record.sequence {
        return Err(invalid("writer Serving audit accounting regressed"));
    }
    let closed: ClosedDomainRecord = db
        .read_native_range_image_record(&closed_current(writer.record.generation, record.range.id))?
        .ok_or_else(|| invalid("writer Serving audit original CLOSED record missing"))?;
    closed.validate(&writer.record)?;
    if closed.checksum_sha256 != record.closed_sha256
        || closed.decision.checksum_sha256() != record.decision_sha256
        || closed.decision.activation_range() != &record.range
        || closed.local != record.local
    {
        return Err(invalid(
            "writer Serving audit changed actual CLOSED predecessor",
        ));
    }
    require_original_metadata_store(db, &closed)?;
    require_serving_quorum_audit(record, &closed.decision, &writer.record)?;
    let actual = db.current_native_range_source_lineage(record.range.id)?;
    if actual.base.checksum_sha256 != record.local.physical_base_sha256
        || actual.base.local_namespace_ids != record.local.namespace_ids
        || actual.history_generation() != record.local.history_generation
        || actual.schema_sha256() != record.local.schema_sha256
        || actual.logical_genesis_sha256() != record.local.logical_genesis_sha256
        || actual.range_index() < record.local.range_index
        || actual.actual_local_visibility_sequence() < record.local.local_visibility_sequence
        || db
            .read_native_history_entry(
                record.range.id,
                record.local.history_generation,
                record.local.range_index,
            )?
            .commitment_sha256
            != record.local.logical_history_sha256
    {
        return Err(invalid(
            "writer Serving audit lost original actual native base/UID/history/cut",
        ));
    }
    for key in [
        serving_current(writer.record.generation, record.range.id),
        archive(writer.record.generation, record.sequence),
    ] {
        let actual: ServingDomainRecord = db
            .read_native_range_image_record(&key)?
            .ok_or_else(|| invalid("writer Serving audit CURRENT/archive missing"))?;
        actual.validate(&writer.record)?;
        if actual != *record {
            return Err(invalid("writer Serving audit CURRENT/archive changed"));
        }
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServingQuorumAudit {
    version: u32,
    decision_sha256: String,
    members: BTreeMap<crate::ClusterNodeId, ServingMemberAudit>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServingMemberAudit {
    metadata: HostMetadataReadAcquireResponse,
    native_bridge_sha256: String,
    catalogue_activation_sha256: String,
    released_generation: Uuid,
    local_node_id: crate::ClusterNodeId,
}
fn require_serving_quorum_audit(
    record: &ServingDomainRecord,
    decision: &NativeJointDecisionData,
    original: &NativeWriterPolicyRecord,
) -> Result<()> {
    let audit: ServingQuorumAudit = serde_json::from_value(record.open_quorum_audit.clone())?;
    let post: HostMetadataReadAcquireResponse =
        serde_json::from_value(record.post_metadata_audit.clone())?;
    post.validate(&post.request)?;
    decision.require_metadata_domain(
        &post.evidence.committed_topology,
        post.evidence.restore_activation.as_ref(),
    )?;
    if audit.version != 1
        || audit.decision_sha256 != decision.checksum_sha256()
        || audit.members.keys().ne(decision.new_members().keys())
        || post.request.requester_node_id != original.facts.local_node_id
        || post.request.requester_incarnation != original.facts.local_incarnation
    {
        return Err(invalid(
            "writer Serving historical audit omits or changes actual NEW members",
        ));
    }
    let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
    for (node, member) in &audit.members {
        member.metadata.validate(&member.metadata.request)?;
        decision.require_metadata_domain(
            &member.metadata.evidence.committed_topology,
            member.metadata.evidence.restore_activation.as_ref(),
        )?;
        if &member.local_node_id != node
            || &member.metadata.request.requester_node_id != node
            || member.metadata.request.requester_incarnation
                != decision.new_members()[node].incarnation
            || member.released_generation.is_nil()
            || !digest(&member.native_bridge_sha256)
            || !digest(&member.catalogue_activation_sha256)
            || post.evidence.term < member.metadata.evidence.term
            || post.evidence.read_index < member.metadata.evidence.read_index
            || post.evidence.context_id == member.metadata.evidence.context_id
        {
            return Err(invalid(
                "writer Serving historical all-released/post-MQ audit changed",
            ));
        }
    }
    let local = &audit.members[&original.facts.local_node_id];
    if local.released_generation != record.released_generation
        || local.native_bridge_sha256 != record.native_bridge_sha256
        || local.catalogue_activation_sha256 != record.catalogue_activation_sha256
    {
        return Err(invalid(
            "writer Serving audit substituted original local activation/open",
        ));
    }
    Ok(())
}
impl NativeWriterHostState {
    pub(super) fn effective_serving_data_range(
        &self,
        range: RangeId,
    ) -> Result<Option<RangeDescriptor>> {
        self.require_lifetime()?;
        let domains = self.data_domains.read();
        if domains.pending.contains(&range) {
            return Err(invalid(
                "selected writer DATA policy publication is CLOSED or unresolved",
            ));
        }
        let Some(verified) = domains.serving.get(&range) else {
            return Ok(None);
        };
        verified.record.validate(&self.record)?;
        if verified.continuity.released_generation()? != verified.record.released_generation {
            return Err(invalid(
                "writer Serving DATA policy actual open generation changed",
            ));
        }
        Ok(Some(verified.record.range.clone()))
    }
    pub(super) fn require_serving_data_database(&self, db: &BicDb, range: RangeId) -> Result<()> {
        self.effective_serving_data_range(range)?;
        let cached = self.data_domains.read().serving.get(&range).cloned();
        if let Some(verified) = cached {
            require_serving_record_current(db, self, &verified.record)?;
            verified.continuity.released_generation()?;
        }
        Ok(())
    }
    pub(super) fn require_borrowed_original_hook(
        &self,
        db: &BicDb,
        hook: &crate::db::commit_admission_hook::CommitAdmissionSlot,
    ) -> Result<()> {
        let registration = self.require_lifetime()?;
        if db.incarnation_id() != self.record.database_incarnation
            || db.data_path().canonicalize()? != self.root
            || !Arc::ptr_eq(hook.registration(), &registration)
            || hook
                .native_writer()
                .is_none_or(|state| !std::ptr::eq(state.as_ref(), self))
        {
            return Err(invalid(
                "writer DATA publication changed actual original hook/open",
            ));
        }
        Ok(())
    }
}
impl InstalledNativeWriterHostGuard {
    pub(crate) fn require_data_domain_origin<T: RangeWriteTransport + 'static>(
        &self,
        db: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        writer: &Arc<NativeWriterHostState>,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.state, writer) {
            return Err(invalid("writer DATA transition replaced original State"));
        }
        let expected: Arc<dyn CommitAdmission> = coordinator.clone();
        if self
            .state
            .coordinator
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, &expected))
            || coordinator.native_writer_coordinator_id() != self.state.record.coordinator_id
        {
            return Err(invalid(
                "writer DATA transition replaced original coordinator",
            ));
        }
        self.state.require_original_identity_database(db)?;
        self.state.require_verified_registry_database(db)
    }
}
