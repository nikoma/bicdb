//! Retained selected read point. Physical primary scanning holds no service,
//! native journal, admission or hook guard. DATA progress is published only
//! after a separate genuine fresh-context native/source/version postcheck.
use super::*;
use crate::distribution_native_host::{
    NativeJoinedRangeReceiveRequest, NativeJoinedRemoteInstallPreflight,
};
use crate::distribution_range_image::NativeRemoteImageStep;
use crate::distribution_range_mutation::{FrozenRangeMutationIdentity, FrozenRangeMutationPermit};

const MAX_SCAN_ATTEMPTS: usize = 8;
#[derive(Clone, Debug, Serialize)]
struct ScanAttempt {
    role_context: Uuid,
    own_context: Uuid,
    candidate_records_before: u64,
    candidate_records_after: u64,
    physical_scan_called: bool,
    outcome: String,
    read_point_postcheck_error: Option<String>,
}
pub(in crate::db::range_image_native::joined_remote_image) struct JoinedTargetScanReadPoint {
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    // Shared observational identity only: NOT a permit Arc or authority.
    frozen: Arc<FrozenRangeMutationIdentity>,
    start_revision: String,
    candidate: JoinedTargetVerifier,
    scanned: bool,
    status: JoinedTargetVerificationStatus,
    attempts: Vec<ScanAttempt>,
}
impl JoinedTargetVerifier {
    fn revision(&self) -> Result<String> {
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &(
                (
                    self.generation,
                    &self.database_open,
                    self.target_reserved_b,
                    self.image_generations,
                    &self.index_definitions,
                    &self.seal,
                    self.rows.native_xid,
                    &self.rows.header_sha256,
                    &self.rows.content_sha256,
                    self.rows.last_actual_target_b,
                ),
                (
                    &self.history_header_sha256,
                    self.history_generation,
                    self.collection,
                    &self.after,
                    self.records,
                    self.bytes,
                    self.min_snapshot_b,
                    self.complete,
                    self.invalid,
                    hex::encode(self.content.clone().finalize()),
                ),
            ),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
}
impl JoinedTargetScanReadPoint {
    #[cfg(test)]
    pub(in crate::db::range_image_native::joined_remote_image) fn test_state(
        &self,
    ) -> serde_json::Value {
        serde_json::json!({"start_revision":self.start_revision,"candidate":self.candidate.test_state(),
            "scanned_DATA_not_published":self.scanned,"status":self.status,"attempts":self.attempts,
            "no_read_or_writer_grant":true})
    }
    fn require_owned_context(
        &self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
        actual: &JoinedTargetVerifier,
        history: &HistoryHeader,
    ) -> Result<()> {
        context.require_current(db)?;
        if !Arc::ptr_eq(&self.preflight, context.point.preflight_arc())
            || self.start_revision != actual.revision()?
        {
            return Err(invalid(
                "Joined scan point changed original retained verifier revision",
            ));
        }
        self.frozen.require_same_current(context.scope.frozen()?)?;
        let rows = &self.candidate.rows;
        self.candidate
            .require_current(db, context, &finished_seal(context)?, rows, history)
    }
    fn require_read_point(
        &self,
        db: &BicDb,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        actual_a: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.preflight.require_request(request, frame)?;
        frame.require_request_live()?;
        self.frozen.require_same_current(actual_a)?;
        self.preflight.native().require_current(db)?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        request.member().require_database_point(db)?;
        request
            .member()
            .publication_version()
            .require_current_memory()?;
        if self.candidate.database_open != db.incarnation_id()
            || self.candidate.invalid
            || self.candidate.image_generations
                != db.native_range_image_generation_pair(self.preflight.scope().range_id)?
            || bounded_json(&self.candidate.index_definitions, MAX_NATIVE_RECEIPT_BYTES)?
                != bounded_json(&actual_indexes(db)?, MAX_NATIVE_RECEIPT_BYTES)?
        {
            return Err(invalid(
                "Joined unlocked scan changed actual DB/image/schema/index identity",
            ));
        }
        let NativeRemoteImageSourcePayloadV1::Finished(seal) = frame.payload()? else {
            return Err(invalid(
                "Joined unlocked target scan needs real authenticated Finished frame",
            ));
        };
        if seal != &self.candidate.seal {
            return Err(invalid(
                "Joined unlocked scan changed original Finished seal",
            ));
        }
        request.require_live()?;
        frame.require_request_live()?;
        parent.check()
    }
    fn scan(
        &mut self,
        db: &BicDb,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        actual_a: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
        max_scanned_records: usize,
    ) -> Result<()> {
        self.require_read_point(db, request, frame, actual_a, parent)?;
        if self.scanned {
            return Ok(());
        }
        if self.attempts.len() >= MAX_SCAN_ATTEMPTS {
            return Err(invalid(
                "Joined read point retry evidence capacity exhausted; no eviction",
            ));
        }
        let source = frame.retain_original_request_cancellation()?;
        let cancellation = request
            .cancellation()
            .child_linked_to(&source)
            .child_linked_to(parent);
        self.attempts.push(ScanAttempt {
            role_context: request.role().role().context_id(),
            own_context: request.own().context_id(),
            candidate_records_before: self.candidate.records,
            candidate_records_after: self.candidate.records,
            physical_scan_called: false,
            outcome: "invoked_no_result".into(),
            read_point_postcheck_error: None,
        });
        let index = self.attempts.len() - 1;
        let result = (|| -> Result<()> {
            let state = &mut self.candidate;
            if db.replication_shippable_seq() < state.target_reserved_b {
                self.status = JoinedTargetVerificationStatus::WaitingForDurability;
            } else if db.current_visibility_watermark() < state.target_reserved_b {
                self.status = JoinedTargetVerificationStatus::WaitingForVisibility;
            } else if state.complete {
                self.status = JoinedTargetVerificationStatus::Complete;
            } else {
                let meta = &self.preflight.bindings().collections()[state.collection];
                let mut content = state.content.clone();
                let mut records = state.records;
                let mut bytes = state.bytes;
                self.attempts[index].physical_scan_called = true;
                let exported = db.for_each_range_snapshot_batch_with_cancellation(
                    &meta.name,
                    &self.preflight.source().manifest().capture_range,
                    state.after.as_deref(),
                    &crate::distribution::RangeSnapshotOptions {
                        max_records_per_batch: 1024,
                        max_bytes_per_batch: 8 * 1024 * 1024,
                        max_record_bytes: 8 * 1024 * 1024,
                    },
                    max_scanned_records,
                    &cancellation,
                    |batch| {
                        for record in batch.records {
                            cancellation.check()?;
                            let encoded = bounded_json(&record, 8 * 1024 * 1024)?;
                            for piece in [meta.name.as_bytes(), encoded.as_slice()] {
                                content.update((piece.len() as u64).to_be_bytes());
                                content.update(piece);
                            }
                            records = records
                                .checked_add(1)
                                .ok_or_else(|| invalid("Joined scan count overflow"))?;
                            bytes = bytes
                                .checked_add(encoded.len() as u64)
                                .ok_or_else(|| invalid("Joined scan bytes overflow"))?;
                        }
                        Ok(false)
                    },
                )?;
                if exported.snapshot_commit_sequence < state.target_reserved_b {
                    self.status = JoinedTargetVerificationStatus::WaitingForVisibility;
                } else {
                    let (collection, after) = if exported.completed {
                        (state.collection + 1, None)
                    } else {
                        if exported.resume_after_key <= state.after {
                            return Err(invalid("Joined unlocked raw cursor failed to advance"));
                        }
                        (state.collection, exported.resume_after_key)
                    };
                    let complete = collection == self.preflight.bindings().collections().len();
                    if complete
                        && (records != state.seal.record_count
                            || bytes != state.seal.serialized_record_bytes
                            || hex::encode(content.clone().finalize())
                                != state.seal.image_root_sha256)
                    {
                        state.invalid = true;
                        return Err(invalid("Joined unlocked target scan differs from Finished full payload hash/count/bytes"));
                    }
                    state.content = content;
                    state.records = records;
                    state.bytes = bytes;
                    state.collection = collection;
                    state.after = after;
                    state.complete = complete;
                    state.min_snapshot_b = Some(
                        state
                            .min_snapshot_b
                            .map_or(exported.snapshot_commit_sequence, |b| {
                                b.min(exported.snapshot_commit_sequence)
                            }),
                    );
                    self.status = if complete {
                        JoinedTargetVerificationStatus::Complete
                    } else {
                        JoinedTargetVerificationStatus::Scanning
                    };
                }
            }
            // Tentative read DATA survives an expired postcheck. A later
            // genuine fresh context must independently revalidate it before
            // cursor publication; no old request is renewed or granted.
            self.scanned = true;
            Ok(())
        })();
        self.attempts[index].candidate_records_after = self.candidate.records;
        self.attempts[index].outcome = match &result {
            Ok(()) => "physical_result_retained_publication_pending".into(),
            Err(error) => format!("{error:?}").chars().take(4096).collect(),
        };
        result?;
        let post = self.require_read_point(db, request, frame, actual_a, parent);
        if let Err(error) = &post {
            self.attempts[index].read_point_postcheck_error =
                Some(format!("{error:?}").chars().take(4096).collect());
        }
        post
    }
}
impl NativeJoinedRemoteImageInstallation {
    pub(crate) fn prepare_target_scan(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<()> {
        self.begin_verification(db, context)?;
        let actual = self.verification.as_ref().unwrap();
        if let Some(point) = &self.scan_pending {
            return point.require_owned_context(db, context, actual, &self.header);
        }
        self.scan_pending = Some(JoinedTargetScanReadPoint {
            preflight: self.preflight.clone(),
            frozen: Arc::new(context.scope.frozen()?.observe_identity()?),
            start_revision: actual.revision()?,
            candidate: actual.clone(),
            scanned: false,
            status: JoinedTargetVerificationStatus::Scanning,
            attempts: Vec::new(),
        });
        Ok(())
    }
    /// Caller owns ONLY the actual DB borrow here, never a native/service/
    /// admission/hook scope. Original opaque A and request are still mandatory.
    pub(crate) fn scan_target_unlocked(
        &mut self,
        db: &BicDb,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        lease: &crate::distribution_range_recovery::JoinedRemoteImageInstallFreeze,
        parent: &CancellationToken,
        max_scanned_records: usize,
    ) -> Result<()> {
        if !(1..=16384).contains(&max_scanned_records) {
            return Err(invalid("Joined raw scan budget invalid"));
        }
        if !Arc::ptr_eq(&self.preflight, lease.preflight()) {
            return Err(invalid("Joined unlocked scan changed actual A owner"));
        }
        self.scan_pending
            .as_mut()
            .ok_or_else(|| invalid("Joined actual retained read point absent"))?
            .scan(
                db,
                request,
                frame,
                lease.try_as_frozen()?,
                parent,
                max_scanned_records,
            )
    }
    pub(crate) fn publish_target_scan(
        &mut self,
        db: &BicDb,
        context: &NativeJoinedRemoteImageContext<'_>,
    ) -> Result<NativeJoinedRemoteVerificationProgress> {
        let point = self
            .scan_pending
            .as_ref()
            .ok_or_else(|| invalid("Joined scan result owner absent"))?;
        let actual = self
            .verification
            .as_ref()
            .ok_or_else(|| invalid("Joined verifier owner absent"))?;
        point.require_owned_context(db, context, actual, &self.header)?;
        if !point.scanned {
            return Err(invalid(
                "Joined read point has no completed physical observation",
            ));
        }
        let candidate = point.candidate.clone();
        let progress = candidate.progress(point.status.clone());
        // Clone only the sealed observational identity, never the A permit.
        // This lets both actual destination slots be borrowed before guards.
        let identity = point.frozen.clone();
        // All hashing, native/catalogue/source/schema IO and allocation are
        // complete. Borrow the real destination slot before the short guards.
        let slot = self.verification.as_mut().unwrap();
        let pending = &mut self.scan_pending;
        let frozen = context.scope.frozen()?;
        let gate = frozen.try_current_publication()?;
        let version = context
            .point
            .request()
            .member()
            .publication_version()
            .try_publication_guard()?;
        context.point.request().require_request_memory()?;
        context.point.frame().require_request_live()?;
        identity.require_in_current_publication(&gate)?;
        db.require_joined_publication_storage_health()?;
        let old = std::mem::replace(slot, candidate);
        let old_point = pending.take();
        drop(version);
        drop(gate);
        // Destructors, including private owner weak references, stay outside
        // the final guard. No mutation/read permission is published here.
        drop(old);
        drop(old_point);
        Ok(progress)
    }
}
