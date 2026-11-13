//! SAME-State necessary Serving DATA policy, after real all-released proofs.
//! This publishes no coordinator Ready, APP ingress or Strong-read result.
use super::*;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

pub(crate) struct NativeWriterServingDomainPublication {
    closed: Arc<NativeClosedWriterDomain>,
    publication: Publication,
}
enum Publication {
    Unwritten,
    Prepared(ServingDomainRecord),
    Uncertain(ServingDomainRecord),
    NativeVerified(Arc<VerifiedServingDomain>),
    Published(Arc<VerifiedServingDomain>),
}
impl NativeClosedWriterDomain {
    pub(crate) fn begin_serving_publication(
        self: &Arc<Self>,
    ) -> Result<NativeWriterServingDomainPublication> {
        self.require_lifetime()?;
        Ok(NativeWriterServingDomainPublication {
            closed: self.clone(),
            publication: Publication::Unwritten,
        })
    }
}
impl NativeWriterServingDomainPublication {
    pub(crate) fn publication_unresolved(&self) -> bool {
        matches!(&self.publication, Publication::Uncertain(_))
    }
    pub(crate) fn verified(&self) -> bool {
        matches!(&self.publication, Publication::Published(_))
    }
    pub(crate) fn advance(
        &mut self,
        admission: &NativeJointServingPolicyAdmission,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        self.closed.require_lifetime()?;
        policy.require_owned_metadata_current()?;
        // No engine or metadata-store guard spans this complete external sweep.
        admission.require_external_current()?;
        let actual = self
            .closed
            .service
            .upgrade()
            .ok_or_else(|| invalid("Serving writer actual service ended"))?;
        let result = {
            let service = actual
                .try_lock()
                .ok_or_else(|| invalid("Serving writer service busy"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Serving writer DB busy"))?;
            self.advance_local(&db, &actual, &service, admission, policy)
        };
        let post = policy.require_owned_metadata_current();
        result?;
        post?;
        self.closed.require_lifetime()?;
        policy.require_live()?;
        admission.require_live()?;
        let actual = match &self.publication {
            Publication::NativeVerified(actual) | Publication::Published(actual) => actual.clone(),
            _ => {
                return Err(invalid(
                    "Serving writer publication has no actual verified native receipt",
                ))
            }
        };
        if actual.continuity.released_generation()? != actual.record.released_generation {
            return Err(invalid(
                "Serving writer open generation changed after owned MStore postcheck",
            ));
        }
        self.closed.writer.require_lifetime()?;
        // Native readback and protected owned-MStore postcheck are complete.
        // Only this original State's selected range becomes effective; other
        // ranges and all native evidence remain unchanged.
        let mut domains = self.closed.writer.data_domains.write();
        domains
            .serving
            .insert(self.closed.range().id, actual.clone());
        domains.pending.remove(&self.closed.range().id);
        self.publication = Publication::Published(actual);
        Ok(())
    }
    fn advance_local(
        &mut self,
        db: &BicDb,
        actual_service: &Arc<parking_lot::Mutex<crate::ClusterDataNodeService>>,
        service: &crate::ClusterDataNodeService,
        admission: &NativeJointServingPolicyAdmission,
        policy: &NativeWriterDataDomainPolicy,
    ) -> Result<()> {
        service.with_writer_data_domain_store(db, |store| {
            self.advance_with_store(db, actual_service, service, admission, policy, store)
        })
    }
    fn advance_with_store(
        &mut self,
        db: &BicDb,
        actual_service: &Arc<parking_lot::Mutex<crate::ClusterDataNodeService>>,
        service: &crate::ClusterDataNodeService,
        admission: &NativeJointServingPolicyAdmission,
        policy: &NativeWriterDataDomainPolicy,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        let writer = self.closed.writer.clone();
        self.closed
            .require_current_for_store(db, store, self.closed.range())?;
        policy.require_local(db, actual_service, self.closed.slot.id())?;
        if !Weak::ptr_eq(&policy.metadata_owner(), &self.closed.metadata)
            || policy.metadata_store_id() != self.closed.record.metadata_store_id
            || admission.data() != self.closed.data()
            || admission.local_node_id() != &writer.record.facts.local_node_id
        {
            return Err(invalid(
                "Serving writer publication changed original MStore/decision/local member",
            ));
        }
        policy.facts().require_range(self.closed.range())?;
        admission.require_local_current(actual_service, service, db, &writer)?;
        let continuity = admission.observe_local_continuity(store)?;
        let generation = continuity.released_generation()?;
        let quorum = admission.open_quorum_audit()?;
        let post_metadata = policy.joint_domain_audit(self.closed.data())?;
        let counterpoint = admission.post_metadata_audit()?;
        let old: HostMetadataReadAcquireResponse = serde_json::from_value(counterpoint)?;
        if policy.metadata_term() < old.evidence.term
            || policy.metadata_read_index() < old.evidence.read_index
            || policy.metadata_context_id() == old.evidence.context_id
        {
            return Err(invalid(
                "Serving writer OWN/MStore acquisition must follow actual all-released post-MQ",
            ));
        }
        if let Publication::NativeVerified(actual) | Publication::Published(actual) =
            &self.publication
        {
            require_serving_record_current(db, &writer, &actual.record)?;
            actual.continuity.released_generation()?;
            if matches!(&self.publication, Publication::Published(_)) {
                let cached = writer
                    .data_domains
                    .read()
                    .serving
                    .get(&self.closed.range().id)
                    .cloned()
                    .ok_or_else(|| invalid("Serving writer verified cache disappeared"))?;
                if !Arc::ptr_eq(&cached, actual) {
                    return Err(invalid("Serving writer verified cache changed"));
                }
            }
            return Ok(());
        }
        let _admission = db
            .serializable_commit_admission
            .try_write()
            .ok_or_else(|| invalid("Serving writer admission busy"))?;
        self.closed
            .require_current_for_store(db, store, self.closed.range())?;
        admission.require_local_current(actual_service, service, db, &writer)?;
        let _journal = db.try_native_image_journal()?;
        if matches!(&self.publication, Publication::Unwritten) {
            let existing: Option<ServingDomainRecord> = db.read_native_range_image_record(
                &serving_current(writer.record.generation, self.closed.range().id),
            )?;
            if existing.as_ref().is_some_and(|record| {
                Some(&record.checksum_sha256) != self.closed.record.previous_serving_sha256.as_ref()
            }) {
                return Err(invalid(
                    "Serving writer cannot adopt a foreign or unretained native publication",
                ));
            }
            self.publication = Publication::Prepared(ServingDomainRecord {
                version: 1,
                sequence: next_sequence(db, &writer.record)?,
                original: OriginalWriterBinding::from_actual(&writer.record),
                closed_sha256: self.closed.record.checksum_sha256.clone(),
                decision_sha256: self.closed.data().checksum_sha256().into(),
                range: self.closed.range().clone(),
                local: self.closed.record.local.clone(),
                native_bridge_sha256: admission.native().checksum_sha256().into(),
                catalogue_activation_sha256: admission.activation().activation_sha256()?,
                released_generation: generation,
                open_quorum_audit: quorum,
                post_metadata_audit: post_metadata,
                native_page_xid: 0,
                checksum_sha256: String::new(),
            });
        }
        let record = match &self.publication {
            Publication::Prepared(r) | Publication::Uncertain(r) => r.clone(),
            _ => return Err(invalid("Serving writer lost retained publication")),
        };
        if record.released_generation != generation
            || record.native_bridge_sha256 != admission.native().checksum_sha256()
            || record.catalogue_activation_sha256 != admission.activation().activation_sha256()?
        {
            return Err(invalid(
                "Serving writer fresh reconciliation replaced original open/native activation",
            ));
        }
        writer
            .data_domains
            .write()
            .pending
            .insert(self.closed.range().id);
        if matches!(&self.publication, Publication::Prepared(_)) {
            let mut record = record;
            record.sequence = next_sequence(db, &writer.record)?;
            let hook = db
                .commit_admission
                .try_read()
                .ok_or_else(|| invalid("Serving writer hook busy"))?;
            writer.require_borrowed_original_hook(db, &hook)?;
            db.in_paged_transaction(|paged, xid| {
                policy.require_local(db, actual_service, self.closed.slot.id())?;
                self.closed.require_lifetime()?;
                admission.require_live()?;
                writer.require_borrowed_original_hook(db, &hook)?;
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.validate(&writer.record)?;
                self.publication = Publication::Uncertain(record.clone());
                let mut counter = AuditCounter {
                    version: 1,
                    original: record.original.clone(),
                    sequence: record.sequence,
                    checksum_sha256: String::new(),
                };
                counter.checksum_sha256 = counter.checksum()?;
                for (key, bytes) in [
                    (
                        counter_key(writer.record.generation),
                        bounded_json(&counter, 16 * 1024)?,
                    ),
                    (
                        archive(writer.record.generation, record.sequence),
                        bounded_json(&record, MAX_AUDIT_BYTES)?,
                    ),
                    (
                        serving_current(writer.record.generation, self.closed.range().id),
                        bounded_json(&record, MAX_AUDIT_BYTES)?,
                    ),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                policy.require_local(db, actual_service, self.closed.slot.id())?;
                self.closed.require_lifetime()?;
                admission.require_live()?;
                Ok(())
            })?
            .ok_or_else(|| invalid("Serving writer lost actual native page store"))?;
            drop(hook);
        }
        let record = match &self.publication {
            Publication::Uncertain(r) => r.clone(),
            _ => return Err(invalid("Serving writer attempted audit unavailable")),
        };
        require_serving_record_current(db, &writer, &record)?;
        // Drop the local native journal before constructing another full
        // actual historical-cut scope. Native audit and original exclusion
        // remain owned throughout; no ordinary selected-range policy exists.
        drop(_journal);
        admission.require_local_current(actual_service, service, db, &writer)?;
        policy.require_local(db, actual_service, self.closed.slot.id())?;
        let continuity = admission.observe_local_continuity(store)?;
        if continuity.released_generation()? != record.released_generation {
            return Err(invalid(
                "Serving writer released generation changed before cache publication",
            ));
        }
        let verified = Arc::new(VerifiedServingDomain { record, continuity });
        // Keep the selected-range refusal until the OUTSIDE-engine protected
        // MStore postcheck succeeds. A native receipt alone is not the cache.
        self.publication = Publication::NativeVerified(verified);
        Ok(())
    }
}
