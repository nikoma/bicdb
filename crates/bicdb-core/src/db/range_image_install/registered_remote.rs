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

/// Exact original owner's attempted native publication. Fields are private,
/// with no serde/DTO constructor. Retained BEFORE either native put.
pub(crate) struct RegisteredRemoteBaseBoundary {
    receipt: NativeInstalledRangeImageReceipt,
    bytes: Vec<u8>,
    database_open: String,
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
        })
    }
    pub(crate) fn boundary(&self) -> Option<Arc<RegisteredRemoteBaseBoundary>> {
        self.attempted.clone()
    }
    pub(crate) fn publish(
        &mut self,
        db: &BicDb,
        context: &NativeRegisteredRemoteImageContext<'_>,
    ) -> Result<NativeRegisteredRemoteImageProgress> {
        context.require_current(db)?;
        self.final_image.require_current(db, context)?;
        if self.attempted.is_none() {
            context
                .native_scope()
                .require_irreversible_install_safety(context.actual_preflight().safety())?;
            let controlled = db
                .range_mutation_gate()
                .try_controlled_mutation(context.native_scope().frozen())?;
            let result = db.in_paged_transaction(|paged, xid| {
                context.require_current(db)?;
                self.final_image.require_current(db, context)?;
                let fact = db.read_registered_final_image_integrity(
                    context.native_scope().native(),
                    &self.final_image.pointer(),
                )?;
                let receipt = receipt_from_final(&fact, xid)?;
                receipt.validate()?;
                let bytes = bounded_json(&receipt, MAX_NATIVE_RECEIPT_BYTES)?;
                let boundary = Arc::new(RegisteredRemoteBaseBoundary {
                    receipt: receipt.clone(),
                    bytes: bytes.clone(),
                    database_open: db.incarnation_id().into(),
                });
                // Original host owns exact bytes+xid BEFORE native mutation.
                self.receipt = receipt;
                self.attempted = Some(boundary.clone());
                let owned = context.with_owned_base(boundary);
                owned.require_current(db)?;
                for key in [
                    base_key(self.receipt.capture_range.id),
                    archive(self.receipt.attempt_id, xid),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                owned.require_current(db)
            });
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
        serde_json::json!({"receipt":self.receipt,"attempted":self.attempted.is_some(),
            "actual_commit_returned":self.commit_returned,"verified":self.verified.is_some(),
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
