//! Actual Registered image -> irreversible native base. The catalogue remains
//! CLOSED/Installing. Neither a decoded final pointer nor this base is Serving.
use super::*;
use crate::db::{
    HistoricalRegisteredFinalImage, NativeRegisteredRemoteFinalImageReceipt,
    NativeRegisteredRemoteImageContext, NativeRegisteredRemoteImageProgress,
    NativeRegisteredRemoteMutationScope,
};

fn archive(attempt: Uuid, xid: u64) -> String {
    format!("registered-remote-base/{attempt}/archive/{xid}")
}
const MAX_RETIRED_BASE_ATTEMPTS: usize = 8;
const MAX_RETIRED_BASE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RETIRED_ERROR_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PublicationRequest {
    role_context: Uuid,
    own_context: Uuid,
    business: Uuid,
    metadata_store: Uuid,
}
impl PublicationRequest {
    fn actual(context: &NativeRegisteredRemoteImageContext<'_>) -> Result<Self> {
        let (role_context, own_context, business, metadata_store) =
            context.actual_publication_request_ids();
        if [role_context, own_context, business, metadata_store]
            .iter()
            .any(Uuid::is_nil)
        {
            return Err(invalid("Registered native base request identity missing"));
        }
        Ok(Self {
            role_context,
            own_context,
            business,
            metadata_store,
        })
    }
    fn require_fresh_after(&self, old: &Self) -> Result<()> {
        if self.metadata_store != old.metadata_store
            || self.role_context == old.role_context
            || self.own_context == old.own_context
        {
            return Err(invalid(
                "Registered native base replacement requires genuinely fresh Role and OWN request",
            ));
        }
        Ok(())
    }
}

/// Exact original owner's attempted native publication. Fields are private,
/// with no serde/DTO constructor. Retained BEFORE either native put.
pub(crate) struct RegisteredRemoteBaseBoundary {
    receipt: NativeInstalledRangeImageReceipt,
    bytes: Vec<u8>,
    database_open: String,
    request: PublicationRequest,
}
impl RegisteredRemoteBaseBoundary {
    pub(crate) fn require_boundary(
        &self,
        db: &BicDb,
        scope: &NativeRegisteredRemoteMutationScope<'_>,
        original: &NativeRangeInstallSafety,
    ) -> Result<()> {
        scope.require_database(db)?;
        if self.database_open != db.incarnation_id()
            || self.receipt.attempt_id != scope.attempt_id()
            || self.receipt.capture_range.id != scope.range_id()
            || original.range_id() != scope.range_id()
            || original.birth_id() != scope.native().birth_id()
            || original.base_index().is_some()
            || original.applied_index().is_some()
            || original.pending_index().is_some()
            || original.materialized_pending_index().is_some()
        {
            return Err(invalid("Registered base original NoDomain owner changed"));
        }
        let actual: Option<NativeInstalledRangeImageReceipt> =
            db.read_native_range_image_record(&base_key(scope.range_id()))?;
        match actual {
            None => scope.require_irreversible_install_safety(original),
            Some(actual) => {
                actual.validate()?;
                if actual != self.receipt
                    || bounded_json(&actual, MAX_NATIVE_RECEIPT_BYTES)? != self.bytes
                {
                    return Err(invalid(
                        "Registered base differs from exact owned native attempt",
                    ));
                }
                // Do not replace the original no-domain preimage with a
                // general permission to ignore new prefixes/pending commands.
                for key in [
                    format!("prefix:{}", scope.range_id().get()),
                    format!("pending:{}", scope.range_id().get()),
                    format!("genesis:{}", scope.range_id().get()),
                ] {
                    if db
                        .read_native_range_image_record::<serde_json::Value>(&key)?
                        .is_some()
                    {
                        return Err(invalid(
                            "Registered base acquired an unowned native prefix/pending/genesis",
                        ));
                    }
                }
                db.validate_registered_installed_history(&actual)
            }
        }
    }
}

pub(crate) struct RegisteredRemoteBasePublication {
    final_image: Arc<NativeRegisteredRemoteFinalImageReceipt>,
    receipt: NativeInstalledRangeImageReceipt,
    attempted: Option<Arc<RegisteredRemoteBaseBoundary>>,
    commit_returned: Option<u64>,
    verified: Option<NativeInstalledRangeImageBase>,
    issued_request: Option<PublicationRequest>,
    issued_xid: Option<u64>,
    attempts_issued: u64,
    puts_started: u8,
    puts_completed: u8,
    attempt_error: Option<String>,
    retired: Vec<RetiredBaseAttempt>,
    retired_bytes: usize,
}
/// Same-open outcome custody only. Exact original Arc/bytes survive retirement;
/// this is not a cold abort certificate or permission for a logical row write.
struct RetiredBaseAttempt {
    boundary: Arc<RegisteredRemoteBaseBoundary>,
    original_error: String,
    retiring_request: PublicationRequest,
    native_store_open: Uuid,
    birth_id: Uuid,
    source_manifest_sha256: String,
    puts_started: u8,
    puts_completed: u8,
}
impl RegisteredRemoteBasePublication {
    pub(crate) fn verified_base(&self) -> Option<&NativeInstalledRangeImageBase> {
        self.verified.as_ref()
    }
    pub(crate) fn retain(
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
        final_image: Arc<NativeRegisteredRemoteFinalImageReceipt>,
    ) -> Result<Self> {
        context.require_current(db)?;
        context
            .native_scope()
            .require_irreversible_install_safety(context.actual_preflight().safety())?;
        final_image.require_current(db, context)?;
        let fact = db.read_registered_final_image_integrity(
            context.native_scope().native(),
            &final_image.pointer(),
        )?;
        let receipt = receipt_from_final(&fact, 0)?;
        if db
            .read_native_range_image_record::<serde_json::Value>(&base_key(
                receipt.capture_range.id,
            ))?
            .is_some()
            || db.native_range_image_installing(receipt.capture_range.id)?
        {
            return Err(invalid(
                "Registered base requires original native base/marker absence",
            ));
        }
        context.require_current(db)?;
        Ok(Self {
            final_image,
            receipt,
            attempted: None,
            commit_returned: None,
            verified: None,
            issued_request: None,
            issued_xid: None,
            attempts_issued: 0,
            puts_started: 0,
            puts_completed: 0,
            attempt_error: None,
            retired: Vec::new(),
            retired_bytes: 0,
        })
    }
    pub(crate) fn boundary(&self) -> Option<Arc<RegisteredRemoteBaseBoundary>> {
        self.attempted.clone()
    }
    fn require_healthy(db: &BicDb) -> Result<()> {
        db.check_recovery_state()?;
        let paged = db
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Registered native base page store absent"))?;
        paged
            .store()
            .durability_health()
            .check()
            .map_err(crate::error::durability_error)?;
        paged
            .store()
            .wal()
            .health()
            .check()
            .map_err(crate::error::durability_error)?;
        db.check_recovery_state()
    }
    fn retire_proven_abort(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<()> {
        let Some(boundary) = self.attempted.as_ref().cloned() else {
            return Ok(());
        };
        if self.commit_returned.is_some() || self.verified.is_some() {
            return Ok(());
        }
        Self::require_healthy(db)?;
        let paged = db
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Registered native base page store absent"))?;
        // Same order as in_paged_transaction: actual native exclusion is held
        // by context; this serializes terminal outcome/current-key inspection.
        let _writer = paged.write_guard();
        Self::require_healthy(db)?;
        if paged
            .store()
            .transactions()
            .status(boundary.receipt.native_page_xid)
            != bicdb_page::TxStatus::Aborted
        {
            return Ok(()); // Committed/unknown MUST keep the original owner.
        }
        context.require_current(db)?;
        self.final_image.require_current(db, context)?;
        let fresh = PublicationRequest::actual(context)?;
        fresh.require_fresh_after(&boundary.request)?;
        if boundary.database_open != db.incarnation_id()
            || boundary.receipt.native_page_xid == 0
            || self.issued_xid != Some(boundary.receipt.native_page_xid)
            || self.issued_request.as_ref() != Some(&boundary.request)
            || self.receipt != boundary.receipt
            || bounded_json(&boundary.receipt, MAX_NATIVE_RECEIPT_BYTES)? != boundary.bytes
            || boundary.receipt.attempt_id != context.native_scope().attempt_id()
            || boundary.receipt.birth_id != context.native_scope().native().birth_id()
            || self.attempt_error.is_none()
        {
            return Err(invalid(
                "Registered native abort lacks original same-open issued outcome custody",
            ));
        }
        if db
            .read_native_range_image_record::<serde_json::Value>(&base_key(
                boundary.receipt.capture_range.id,
            ))?
            .is_some()
            || db
                .read_native_range_image_record::<serde_json::Value>(&archive(
                    boundary.receipt.attempt_id,
                    boundary.receipt.native_page_xid,
                ))?
                .is_some()
        {
            return Err(invalid("Registered aborted metadata attempt has a visible base/archive; original owner retained"));
        }
        context
            .native_scope()
            .require_irreversible_install_safety(context.actual_preflight().safety())?;
        let next_bytes = self
            .retired_bytes
            .checked_add(boundary.bytes.len())
            .ok_or_else(|| invalid("Registered native base abort history byte overflow"))?;
        if self
            .attempt_error
            .as_ref()
            .is_some_and(|e| e.len() > MAX_RETIRED_ERROR_BYTES)
        {
            return Err(invalid("Registered native base abort history error byte bound exceeded; original attempt retained"));
        }
        if self.retired.len() >= MAX_RETIRED_BASE_ATTEMPTS || next_bytes > MAX_RETIRED_BASE_BYTES {
            return Err(invalid("Registered native base proven-abort retry budget exhausted; original attempt retained"));
        }
        context.require_current(db)?;
        self.final_image.require_current(db, context)?;
        Self::require_healthy(db)?;
        if paged
            .store()
            .transactions()
            .status(boundary.receipt.native_page_xid)
            != bicdb_page::TxStatus::Aborted
        {
            return Err(invalid(
                "Registered native abort status changed; original attempt retained",
            ));
        }
        // Retain the EXACT old Arc and provenance BEFORE clearing a slot.
        self.retired.push(RetiredBaseAttempt {
            boundary,
            original_error: self.attempt_error.clone().expect("checked original error"),
            retiring_request: fresh,
            native_store_open: context.actual_preflight().catalogue().store_open_id(),
            birth_id: context.native_scope().native().birth_id(),
            source_manifest_sha256: context
                .actual_preflight()
                .source()
                .manifest()
                .checksum_sha256
                .clone(),
            puts_started: self.puts_started,
            puts_completed: self.puts_completed,
        });
        self.retired_bytes = next_bytes;
        self.attempted = None;
        self.issued_xid = None;
        self.issued_request = None;
        self.puts_started = 0;
        self.puts_completed = 0;
        self.attempt_error = None;
        Ok(())
    }
    pub(crate) fn publish(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<NativeRegisteredRemoteImageProgress> {
        context.require_current(db)?;
        self.final_image.require_current(db, context)?;
        self.retire_proven_abort(db, context)?;
        if self.attempted.is_none() {
            if self.issued_request.is_some() || self.issued_xid.is_some() {
                return Err(invalid("Registered native allocation has no exact retained boundary; outcome remains fenced"));
            }
            context
                .native_scope()
                .require_irreversible_install_safety(context.actual_preflight().safety())?;
            let controlled = db
                .range_mutation_gate()
                .try_controlled_mutation(context.native_scope().frozen())?;
            let request = PublicationRequest::actual(context)?;
            // Check size with the longest possible xid before begin. The
            // actual issued xid/request is retained immediately on entry.
            let mut largest = self.receipt.clone();
            largest.native_page_xid = u64::MAX;
            largest.checksum_sha256 = largest.checksum()?;
            bounded_json(&largest, MAX_NATIVE_RECEIPT_BYTES)?;
            self.issued_request = Some(request.clone());
            let result = db.in_paged_transaction(|paged, xid| {
                self.issued_xid = Some(xid);
                self.attempts_issued = self
                    .attempts_issued
                    .checked_add(1)
                    .ok_or_else(|| invalid("Registered native allocation count overflow"))?;
                let mut receipt = self.receipt.clone();
                receipt.native_page_xid = xid;
                receipt.checksum_sha256 = receipt.checksum()?;
                receipt.validate()?;
                let bytes = bounded_json(&receipt, MAX_NATIVE_RECEIPT_BYTES)?;
                let boundary = Arc::new(RegisteredRemoteBaseBoundary {
                    receipt: receipt.clone(),
                    bytes: bytes.clone(),
                    database_open: db.incarnation_id().into(),
                    request: request.clone(),
                });
                // Original host owns exact bytes+xid BEFORE native mutation.
                self.receipt = receipt;
                self.attempted = Some(boundary.clone());
                #[cfg(test)]
                before_put_cut::fire();
                let owned = context.with_owned_base(boundary);
                owned.require_current(db)?;
                self.final_image.require_current(db, &owned)?;
                let fact = db.read_registered_final_image_integrity(
                    context.native_scope().native(),
                    &self.final_image.pointer(),
                )?;
                if receipt_from_final(&fact, xid)? != self.receipt {
                    return Err(invalid(
                        "Registered replacement native template differs from actual final image",
                    ));
                }
                for key in [
                    base_key(self.receipt.capture_range.id),
                    archive(self.receipt.attempt_id, xid),
                ] {
                    self.puts_started += 1;
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                    self.puts_completed += 1;
                    #[cfg(test)]
                    if self.puts_completed == 1 {
                        after_first_put_cut::fire();
                    }
                    owned.require_current(db)?;
                }
                owned.require_current(db)?;
                #[cfg(test)]
                before_commit_cut::fire();
                Ok(())
            });
            if let Err(error) = &result {
                self.attempt_error = Some(error.to_string());
            }
            if let Ok(Some(xid)) = &result {
                self.commit_returned = Some(*xid);
                if *xid != self.receipt.native_page_xid {
                    return Err(invalid(
                        "Registered base actual native commit returned another xid",
                    ));
                }
            }
            drop(controlled);
            result?.ok_or_else(|| invalid("Registered base page store absent"))?;
            #[cfg(test)]
            cuts::fire();
        }
        let boundary = self
            .attempted
            .as_ref()
            .ok_or_else(|| invalid("Registered base native attempt absent"))?
            .clone();
        let owned = context.with_owned_base(boundary);
        owned.require_current(db)?;
        self.final_image.require_current(db, &owned)?;
        db.validate_registered_installed_history(&self.receipt)?;
        let base = db
            .native_installed_range_image_base(
                owned.native_scope().native(),
                self.receipt.capture_range.id,
            )?
            .ok_or_else(|| {
                invalid("Registered base exact committed native outcome not established")
            })?;
        if base.receipt != self.receipt {
            return Err(invalid("Registered base actual current readback differs"));
        }
        owned.require_current(db)?;
        let progress = NativeRegisteredRemoteImageProgress::NativeBase {
            range_index: base.range_index(),
            native_page_xid: base.native_page_xid(),
            checksum_sha256: base.native_receipt_sha256().into(),
        };
        self.verified = Some(base);
        Ok(progress)
    }
    #[cfg(test)]
    pub(crate) fn test_state(&self) -> serde_json::Value {
        let retired = self
            .retired
            .iter()
            .map(|old| {
                serde_json::json!({
            "receipt":old.boundary.receipt,"canonical_bytes":old.boundary.bytes,
            "database_open":old.boundary.database_open,"original_request":old.boundary.request,
            "original_error":old.original_error,"retiring_request":old.retiring_request,
            "native_store_open":old.native_store_open,"birth_id":old.birth_id,
            "source_manifest_sha256":old.source_manifest_sha256,
            "puts_started":old.puts_started,"puts_completed":old.puts_completed,
            "same_open_terminal_status":"Aborted","core_page_wal_healthy_at_retirement":true,
            "cold_abort_certificate":false})
            })
            .collect::<Vec<_>>();
        serde_json::json!({"receipt":self.receipt,"attempted":self.attempted.is_some(),
            "attempted_canonical_bytes":self.attempted.as_ref().map(|b|&b.bytes),
            "attempted_database_open":self.attempted.as_ref().map(|b|&b.database_open),
            "actual_commit_returned":self.commit_returned,"verified":self.verified.is_some(),
            "issued_request":self.issued_request,"issued_native_xid":self.issued_xid,
            "native_attempts_issued":self.attempts_issued,"puts_started":self.puts_started,
            "puts_completed":self.puts_completed,"attempt_error":self.attempt_error,
            "retired":retired,"retired_canonical_bytes":self.retired_bytes,
            "max_retired_attempts":MAX_RETIRED_BASE_ATTEMPTS,"max_retired_canonical_bytes":MAX_RETIRED_BASE_BYTES,
            "max_retired_error_bytes":MAX_RETIRED_ERROR_BYTES,
            "catalogue_floor_publication_owned_here":false,"serving":false})
    }
}

impl NativeInstalledRangeImageBase {
    /// Profile DATA only. This borrowed pointer is not an image/adoption grant.
    pub(crate) fn registered_final_image_pointer(
        &self,
    ) -> Option<&crate::db::NativeRegisteredRemoteFinalImagePointer> {
        self.receipt.registered_final_image.as_ref()
    }
}

fn receipt_from_final(
    fact: &HistoricalRegisteredFinalImage,
    xid: u64,
) -> Result<NativeInstalledRangeImageReceipt> {
    let mf = fact.manifest();
    let birth = fact.target_birth();
    let (global_image_generation, range_image_generation) = fact.image_generations();
    let mut receipt = NativeInstalledRangeImageReceipt {
        format_version: REGISTERED_INSTALL_FORMAT,
        registered_final_image: Some(fact.pointer()),
        joined_final_image: None,
        cluster_id: mf.cluster_id.clone(),
        bootstrap_id: mf.bootstrap_id,
        target_node_id: mf.target_id.clone(),
        target_member_incarnation: birth.node_incarnation(),
        birth_id: birth.birth_id(),
        birth_sha256: birth.birth_sha256().into(),
        purpose: ImageInstallPurpose::RelocationLearner,
        capture_range: mf.capture_range.clone(),
        intended_activation_range: mf.activation_range.clone(),
        base_members: mf.old_members.clone(),
        intended_activation_members: mf.new_members.clone(),
        restore_activation: mf.restore.clone(),
        range_index: mf.range_index,
        history_generation: fact.history_generation(),
        logical_genesis_sha256: mf.h0.clone(),
        logical_history_sha256: mf.h.clone(),
        history_verified_through: mf.range_index,
        attempt_id: mf.target_attempt,
        session_id: mf.handoff_session,
        nonce: mf.nonce,
        source_purpose:
            crate::distribution_range_image::RangeImageSourcePurposeV1::CommittedHandoff {
                relocation_id: mf.relocation_id.clone(),
                plan_id: mf.plan_id.clone(),
            },
        intent_sha256: fact.intent_sha256().into(),
        source_native_receipt_sha256: mf.source_native_receipt.clone(),
        old_config_sha256: mf.old_config_sha256.clone(),
        intended_config_sha256: mf.new_config_sha256.clone(),
        schema_sha256: mf.schema_sha256.clone(),
        reader_features: mf.readers.clone(),
        portable_catalog_sha256: mf.portable_catalogue_sha256.clone(),
        image_root_sha256: fact.image_root_sha256().into(),
        local_namespace_ids: fact.target_namespace_ids().to_vec(),
        global_image_generation,
        range_image_generation,
        reserved_db_upper_bound: fact.target_reserved_b(),
        min_snapshot_db_sequence: fact.target_min_scan_b(),
        record_count: fact.record_count(),
        serialized_record_bytes: fact.serialized_record_bytes(),
        native_page_xid: xid,
        checksum_sha256: String::new(),
    };
    receipt.checksum_sha256 = receipt.checksum()?;
    Ok(receipt)
}
impl BicDb {
    /// Test-only original-owner cut, after actual native xid/bytes retention
    /// and before owned.current or the first native put. No admission minted.
    #[cfg(test)]
    pub(crate) fn test_registered_remote_base_before_first_put(
        callback: Box<dyn FnOnce()>,
    ) -> impl Drop {
        before_put_cut::arm(callback)
    }
    #[cfg(test)]
    pub(crate) fn test_registered_remote_base_after_first_put(
        callback: Box<dyn FnOnce()>,
    ) -> impl Drop {
        after_first_put_cut::arm(callback)
    }
    #[cfg(test)]
    pub(crate) fn test_registered_remote_base_before_commit(
        callback: Box<dyn FnOnce()>,
    ) -> impl Drop {
        before_commit_cut::arm(callback)
    }
    /// Forensic HEALTH/status only, including a fenced handle. No native
    /// visibility read, health reset, engine open or permission is supplied.
    #[cfg(test)]
    pub(crate) fn test_registered_native_health_status(
        &self,
        xid: Option<u64>,
    ) -> serde_json::Value {
        let paged = self.paged_records.as_ref();
        serde_json::json!({"database_open":self.incarnation_id(),
            "core_health":format!("{:?}",self.durability_state()),
            "core_recovery_error":self.check_recovery_state().err().map(|e|e.to_string()),
            "page_health":paged.map(|p|format!("{:?}",p.store().durability_health().state())),
            "page_failure":paged.and_then(|p|p.store().durability_health().failure()),
            "wal_health":paged.map(|p|format!("{:?}",p.store().wal().health().state())),
            "wal_failure":paged.and_then(|p|p.store().wal().health().failure()),
            "requested_own_xid":xid,
            "native_status_without_health_admission":xid.and_then(|xid|paged.map(|p|format!("{:?}",p.store().transactions().status(xid)))),
            "authority_granted":false,"engine_reopened":false})
    }
    #[cfg(test)]
    pub(crate) fn test_invalidate_registered_installed_base(
        &self,
        range: RangeId,
    ) -> Result<serde_json::Value> {
        let _journal = self.try_native_image_journal()?;
        let receipt: NativeInstalledRangeImageReceipt = self
            .read_native_range_image_record(&base_key(range))?
            .ok_or_else(|| {
                invalid("actual Registered installed base absent before invalidation control")
            })?;
        self.validate_registered_installed_history(&receipt)?;
        let before = self.native_range_image_generation_pair(range)?;
        self.invalidate_native_range_images_before_mutation(
            Some(range),
            "actual Registered installed-base invalidation control",
        )?;
        let after = self.native_range_image_generation_pair(range)?;
        if before == after {
            return Err(invalid(
                "actual Registered installed-base invalidation did not change generations",
            ));
        }
        Ok(serde_json::json!({"before":before,"after":after}))
    }

    #[cfg(test)]
    pub(crate) fn test_registered_native_base_json(
        &self,
        range: RangeId,
    ) -> Result<serde_json::Value> {
        let receipt: Option<NativeInstalledRangeImageReceipt> =
            self.read_native_range_image_record(&base_key(range))?;
        let archived = receipt
            .as_ref()
            .map(|r| {
                self.read_native_range_image_record::<NativeInstalledRangeImageReceipt>(&archive(
                    r.attempt_id,
                    r.native_page_xid,
                ))
            })
            .transpose()?
            .flatten();
        Ok(serde_json::json!({"base":receipt,"archive":archived}))
    }

    /// Bounded read-only forensic observations from THIS still-open database.
    /// No recovery, checkpoint, archive engine open or admission is performed.
    #[cfg(test)]
    pub(crate) fn test_registered_native_base_forensic_status(
        &self,
        range: RangeId,
        attempt: Uuid,
        attempted_xid: Option<u64>,
    ) -> Result<serde_json::Value> {
        let _journal = self.try_native_image_journal()?;
        self.check_recovery_state()?;
        let base: Option<NativeInstalledRangeImageReceipt> =
            self.read_native_range_image_record(&base_key(range))?;
        let xid = attempted_xid.or_else(|| base.as_ref().map(|b| b.native_page_xid));
        let archived: Option<NativeInstalledRangeImageReceipt> = xid
            .map(|xid| self.read_native_range_image_record(&archive(attempt, xid)))
            .transpose()?
            .flatten();
        for receipt in base.iter().chain(archived.iter()) {
            bounded_json(receipt, MAX_NATIVE_RECEIPT_BYTES)?;
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("forensic native page store absent"))?;
        let mut statuses = BTreeMap::new();
        for xid in attempted_xid
            .into_iter()
            .chain(base.as_ref().map(|b| b.native_page_xid))
            .chain(archived.as_ref().map(|b| b.native_page_xid))
        {
            statuses.insert(
                xid,
                format!("{:?}", paged.store().transactions().status(xid)),
            );
        }
        Ok(serde_json::json!({"base":base,"attempt_archive":archived,
            "requested_attempt":attempt,"requested_attempted_xid":attempted_xid,
            "native_tx_status_api_observations":statuses,
            "same_live_database_open":self.incarnation_id(),
            "read_only_fixed_keys":true,"cold_or_powerloss_commit_proof":false,"authority_granted":false}))
    }

    pub(super) fn validate_registered_installed_history(
        &self,
        receipt: &NativeInstalledRangeImageReceipt,
    ) -> Result<()> {
        receipt.validate()?;
        let pointer = receipt
            .registered_final_image
            .as_ref()
            .ok_or_else(|| invalid("Registered base has no distinct final-image profile"))?;
        let native = self.native_recovery_admission(
            &receipt.cluster_id,
            receipt.bootstrap_id,
            &receipt.target_node_id,
            receipt.target_member_incarnation,
        )?;
        let archived: NativeInstalledRangeImageReceipt = self
            .read_native_range_image_record(&archive(receipt.attempt_id, receipt.native_page_xid))?
            .ok_or_else(|| invalid("Registered immutable native-base publication missing"))?;
        archived.validate()?;
        if &archived != receipt
            || self
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Registered base native page store absent"))?
                .store()
                .transactions()
                .status(receipt.native_page_xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "Registered native base/archive exact committed transaction unproven",
            ));
        }
        let fact = self.read_registered_final_image_integrity(&native, pointer)?;
        if receipt_from_final(&fact, receipt.native_page_xid)? != *receipt {
            return Err(invalid(
                "Registered installed history differs from immutable verified final image",
            ));
        }
        native.require_current(self)
    }
}

#[cfg(test)]
mod before_put_cut {
    use std::cell::RefCell;
    thread_local! {static CUT:RefCell<Option<Box<dyn FnOnce()>>>=RefCell::new(None);}
    pub(super) fn fire() {
        let callback = CUT.with(|cut| cut.borrow_mut().take());
        if let Some(callback) = callback {
            callback();
        }
    }
    pub(crate) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|cut| {
                cut.borrow_mut().take();
            });
        }
    }
    pub(super) fn arm(callback: Box<dyn FnOnce()>) -> Guard {
        CUT.with(|cut| assert!(cut.borrow_mut().replace(callback).is_none()));
        Guard
    }
}
#[cfg(test)]
mod after_first_put_cut {
    use std::cell::RefCell;
    thread_local! {static CUT:RefCell<Option<Box<dyn FnOnce()>>>=RefCell::new(None);}
    pub(super) fn fire() {
        let f = CUT.with(|c| c.borrow_mut().take());
        if let Some(f) = f {
            f();
        }
    }
    pub(crate) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|c| {
                c.borrow_mut().take();
            });
        }
    }
    pub(super) fn arm(f: Box<dyn FnOnce()>) -> Guard {
        CUT.with(|c| assert!(c.borrow_mut().replace(f).is_none()));
        Guard
    }
}
#[cfg(test)]
mod before_commit_cut {
    use std::cell::RefCell;
    thread_local! {static CUT:RefCell<Option<Box<dyn FnOnce()>>>=RefCell::new(None);}
    pub(super) fn fire() {
        let f = CUT.with(|c| c.borrow_mut().take());
        if let Some(f) = f {
            f();
        }
    }
    pub(crate) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|c| {
                c.borrow_mut().take();
            });
        }
    }
    pub(super) fn arm(f: Box<dyn FnOnce()>) -> Guard {
        CUT.with(|c| assert!(c.borrow_mut().replace(f).is_none()));
        Guard
    }
}

#[cfg(test)]
mod cuts {
    use std::cell::RefCell;
    thread_local! { static CUT: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None); }
    pub(super) fn fire() {
        CUT.with(|cut| {
            if let Some(f) = cut.borrow_mut().take() {
                f();
            }
        });
    }
    pub(crate) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|cut| {
                cut.borrow_mut().take();
            });
        }
    }
    pub(crate) fn arm(f: Box<dyn FnOnce()>) -> Guard {
        CUT.with(|cut| {
            assert!(cut.borrow().is_none());
            *cut.borrow_mut() = Some(f);
        });
        Guard
    }
}
#[cfg(test)]
pub(crate) fn test_registered_remote_base_after_wal(f: Box<dyn FnOnce()>) -> impl Drop {
    cuts::arm(f)
}
