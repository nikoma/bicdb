//! Actual original Joined final image -> SAME selected native base. The
//! original writer/other ranges remain installed; this grants no new domain.
use super::*;
use crate::db::range_image_native::{
    HistoricalJoinedFinalImage, NativeJoinedRemoteFinalImageReceipt,
    NativeJoinedRemoteImageContext, NativeJoinedRemoteMutationScope,
};
fn archive(attempt: Uuid, xid: u64) -> String {
    format!("joined-remote-base/{attempt}/archive/{xid}")
}

/// Exact original candidate retained before IO. No serde/DTO constructor.
pub(crate) struct JoinedRemoteBaseBoundary {
    receipt: NativeInstalledRangeImageReceipt,
    bytes: Vec<u8>,
    database_open: String,
}
impl JoinedRemoteBaseBoundary {
    pub(crate) fn require_boundary(
        &self,
        db: &BicDb,
        scope: &NativeJoinedRemoteMutationScope<'_>,
    ) -> Result<()> {
        scope.require_database(db)?;
        if self.database_open != db.incarnation_id()
            || self.receipt.attempt_id != scope.attempt_id()
            || self.receipt.capture_range.id != scope.range_id()
            || self.receipt.birth_id != scope.native().birth_id()
        {
            return Err(invalid(
                "Joined base boundary changed original A/open/birth",
            ));
        }
        let actual: Option<NativeInstalledRangeImageReceipt> =
            db.read_native_range_image_record(&base_key(scope.range_id()))?;
        match actual {
            None => scope.require_original_install_safety(db),
            Some(actual) => {
                actual.validate()?;
                if actual != self.receipt
                    || bounded_json(&actual, MAX_NATIVE_RECEIPT_BYTES)? != self.bytes
                {
                    return Err(invalid(
                        "Joined base changed exact original attempted bytes",
                    ));
                }
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
                            "Joined base acquired unowned selected prefix/pending/genesis",
                        ));
                    }
                }
                db.validate_joined_installed_history(&actual)
            }
        }
    }
}
pub(crate) struct JoinedRemoteBasePublication {
    final_image: Arc<NativeJoinedRemoteFinalImageReceipt>,
    template: NativeInstalledRangeImageReceipt,
    issued_xid: Option<u64>,
    boundary: Option<Arc<JoinedRemoteBaseBoundary>>,
    commit_returned: Option<u64>,
    verified: Option<NativeInstalledRangeImageBase>,
    last_error: Option<String>,
}
impl JoinedRemoteBasePublication {
    pub(crate) fn retain(
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        final_image: Arc<NativeJoinedRemoteFinalImageReceipt>,
    ) -> Result<Self> {
        context.require_current(db)?;
        context.native_scope().require_original_install_safety(db)?;
        final_image.require_current(db, context)?;
        let fact = db.read_joined_final_image_integrity(
            context.native_scope().native(),
            &final_image.pointer(),
        )?;
        let template = receipt_from_final(&fact, 0)?;
        if db
            .read_native_range_image_record::<serde_json::Value>(&base_key(
                template.capture_range.id,
            ))?
            .is_some()
            || db.native_range_image_installing(template.capture_range.id)?
        {
            return Err(invalid(
                "Joined base requires original selected native base/marker absence",
            ));
        }
        let mut largest = template.clone();
        largest.native_page_xid = u64::MAX;
        largest.checksum_sha256 = largest.checksum()?;
        largest.validate()?;
        bounded_json(&largest, MAX_NATIVE_RECEIPT_BYTES)?;
        Ok(Self {
            final_image,
            template,
            issued_xid: None,
            boundary: None,
            commit_returned: None,
            verified: None,
            last_error: None,
        })
    }
    pub(crate) fn boundary(&self) -> Option<&Arc<JoinedRemoteBaseBoundary>> {
        self.boundary.as_ref()
    }
    pub(crate) fn verified_base(&self) -> Option<&NativeInstalledRangeImageBase> {
        self.verified.as_ref()
    }
    pub(crate) fn advance(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        context.require_current(db)?;
        self.final_image.require_current(db, context)?;
        db.require_joined_publication_storage_health()?;
        crate::format::enable_native_joined_remote_image(db.data_path())?;
        if self.boundary.is_none() {
            if self.issued_xid.is_some() {
                return Err(invalid(
                    "Joined base issued original xid before framing; no replacement",
                ));
            }
            context.native_scope().require_original_install_safety(db)?;
            let controlled = db
                .range_mutation_gate()
                .try_controlled_mutation(context.native_scope().frozen()?)?;
            let result = db.in_paged_transaction(|paged, xid| {
                self.issued_xid = Some(xid); // FIRST native closure operation.
                context.require_current(db)?;
                let mut receipt = self.template.clone();
                receipt.native_page_xid = xid;
                receipt.checksum_sha256 = receipt.checksum()?;
                receipt.validate()?;
                let bytes = bounded_json(&receipt, MAX_NATIVE_RECEIPT_BYTES)?;
                self.boundary = Some(Arc::new(JoinedRemoteBaseBoundary {
                    receipt,
                    bytes,
                    database_open: db.incarnation_id().into(),
                }));
                let boundary = self.boundary.as_ref().unwrap();
                boundary.require_boundary(db, context.native_scope())?;
                self.final_image.require_current(db, context)?;
                let fact = db.read_joined_final_image_integrity(
                    context.native_scope().native(),
                    &self.final_image.pointer(),
                )?;
                if receipt_from_final(&fact, xid)? != boundary.receipt {
                    return Err(invalid(
                        "Joined base candidate differs from actual original final image",
                    ));
                }
                for key in [
                    base_key(boundary.receipt.capture_range.id),
                    archive(boundary.receipt.attempt_id, xid),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &boundary.bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                    context.require_current(db)?;
                }
                Ok(())
            });
            if let Ok(Some(xid)) = &result {
                self.commit_returned = Some(*xid);
            }
            if let Err(error) = &result {
                self.last_error = Some(format!("{error:?}").chars().take(4096).collect());
            }
            drop(controlled);
            result?.ok_or_else(|| invalid("Joined base native page store absent"))?;
        }
        let boundary = self.boundary.as_ref().unwrap();
        boundary.require_boundary(db, context.native_scope())?;
        self.final_image.require_current(db, context)?;
        db.validate_joined_installed_history(&boundary.receipt)?;
        let base = db
            .native_installed_range_image_base(
                context.native_scope().native(),
                boundary.receipt.capture_range.id,
            )?
            .ok_or_else(|| invalid("Joined exact committed base outcome unproven"))?;
        if base.receipt != boundary.receipt || self.issued_xid != Some(base.receipt.native_page_xid)
        {
            return Err(invalid(
                "Joined base actual current differs from original issued native candidate",
            ));
        }
        context.require_current(db)?;
        let identity = context.native_scope().frozen()?.observe_identity()?;
        let slot = &mut self.verified;
        let gate = context.native_scope().frozen()?.try_current_publication()?;
        let version = context
            .actual_request()
            .member()
            .publication_version()
            .try_publication_guard()?;
        context.actual_request().require_request_memory()?;
        context.actual_frame().require_request_live()?;
        identity.require_in_current_publication(&gate)?;
        db.require_joined_publication_storage_health()?;
        let old = slot.replace(base);
        drop(version);
        drop(gate);
        drop(old);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_state(&self) -> serde_json::Value {
        serde_json::json!({"issued_xid":self.issued_xid,"candidate":self.boundary.as_ref().map(|x|&x.receipt),"candidate_bytes_sha256":self.boundary.as_ref().map(|x|hex::encode(sha2::Sha256::digest(&x.bytes))),"actual_commit_returned":self.commit_returned,"verified":self.verified.is_some(),"last_error":self.last_error,"max_native_attempts":1,"no_catalogue_or_Serving":true})
    }
}
impl NativeInstalledRangeImageBase {
    pub(crate) fn joined_final_image_pointer(
        &self,
    ) -> Option<&crate::db::NativeJoinedRemoteFinalImagePointer> {
        self.receipt.joined_final_image.as_ref()
    }
}

fn receipt_from_final(
    fact: &HistoricalJoinedFinalImage,
    xid: u64,
) -> Result<NativeInstalledRangeImageReceipt> {
    let mf = fact.manifest();
    let birth = fact.target_birth();
    let (global_image_generation, range_image_generation) = fact.image_generations();
    let mut receipt = NativeInstalledRangeImageReceipt {
        format_version: JOINED_INSTALL_FORMAT,
        registered_final_image: None,
        joined_final_image: Some(fact.pointer()),
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
    pub(super) fn validate_joined_installed_history(
        &self,
        receipt: &NativeInstalledRangeImageReceipt,
    ) -> Result<()> {
        receipt.validate()?;
        let pointer = receipt
            .joined_final_image
            .as_ref()
            .ok_or_else(|| invalid("Joined installed base distinct profile absent"))?;
        let native = self.native_recovery_admission(
            &receipt.cluster_id,
            receipt.bootstrap_id,
            &receipt.target_node_id,
            receipt.target_member_incarnation,
        )?;
        let archived: NativeInstalledRangeImageReceipt = self
            .read_native_range_image_record(&archive(receipt.attempt_id, receipt.native_page_xid))?
            .ok_or_else(|| invalid("Joined immutable native base publication absent"))?;
        archived.validate()?;
        if &archived != receipt
            || self
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Joined installed base native page store absent"))?
                .store()
                .transactions()
                .status(receipt.native_page_xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "Joined base/archive same committed native xid not proven",
            ));
        }
        let fact = self.read_joined_final_image_integrity(&native, pointer)?;
        if receipt_from_final(&fact, receipt.native_page_xid)? != *receipt {
            return Err(invalid(
                "Joined native base differs from actual immutable final image/history",
            ));
        }
        native.require_current(self)
    }
}
