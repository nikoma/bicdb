//! Actual held original-writer hook identity. This is no selected-range,
//! metadata, image, mutation, CLOSED, Serving, APP or Strong authority.
use super::*;

/// Only actual DB hook acquisition constructs this lexical non-serde point.
/// It keeps the real read guard, not a memoized bool, raw hook or detached ID.
pub(crate) struct NativeOriginalWriterIdentityBorrow<'a> {
    database: &'a BicDb,
    hook: parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    writer: Arc<NativeWriterHostState>,
}
impl NativeOriginalWriterIdentityBorrow<'_> {
    pub(crate) fn writer(&self) -> &Arc<NativeWriterHostState> {
        &self.writer
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        if !std::ptr::eq(self.database, db) {
            return Err(invalid("borrowed original writer changed actual DB object"));
        }
        self.writer
            .require_original_identity_database_at_hook(db, &self.hook)?;
        self.writer.require_verified_registry_database(db)
    }
    /// Actual guard+coordinator identity under THIS actual borrowed hook. No
    /// recursive hook read, source/metadata proof or range admission is added.
    pub(crate) fn require_guard_origin<T: RangeWriteTransport + 'static>(
        &self,
        db: &BicDb,
        guard: &InstalledNativeWriterHostGuard,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        writer: &Arc<NativeWriterHostState>,
    ) -> Result<()> {
        self.require_database(db)?;
        if !Arc::ptr_eq(&guard.state, writer) || !Arc::ptr_eq(writer, &self.writer) {
            return Err(invalid("borrowed original writer replaced actual State"));
        }
        let expected: Arc<dyn CommitAdmission> = coordinator.clone();
        if self
            .writer
            .coordinator
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, &expected))
            || coordinator.native_writer_coordinator_id() != self.writer.record.coordinator_id
        {
            return Err(invalid(
                "borrowed original writer replaced actual coordinator",
            ));
        }
        self.require_database(db)
    }
}
impl BicDb {
    /// Read-only original identity point from a real installed hook. Callers
    /// holding another hook guard must reuse their existing point, not call
    /// this constructor again. It does not take native mutation admission.
    pub(crate) fn borrow_original_native_writer_identity<'a>(
        &'a self,
        writer: Arc<NativeWriterHostState>,
    ) -> Result<NativeOriginalWriterIdentityBorrow<'a>> {
        let hook = self
            .commit_admission
            .try_read()
            .ok_or_else(|| invalid("original native writer hook identity busy"))?;
        let point = NativeOriginalWriterIdentityBorrow {
            database: self,
            hook,
            writer,
        };
        point.require_database(self)?;
        Ok(point)
    }
}
impl NativeWriterHostState {
    /// Same strict original identity as the ordinary helper, using the
    /// ACTUAL caller-held hook. Ordinary blocking identity is left unchanged.
    fn require_original_identity_database_at_hook(
        &self,
        database: &BicDb,
        hook: &crate::db::commit_admission_hook::CommitAdmissionSlot,
    ) -> Result<()> {
        self.require_lifetime()?;
        database.check_recovery_state()?;
        if database.incarnation_id() != self.record.database_incarnation
            || database.data_path().canonicalize()? != self.root
        {
            return Err(invalid("native writer database/open changed"));
        }
        let facts = &self.record.facts;
        let native = database.native_recovery_admission(
            &facts.cluster_id,
            facts.bootstrap_id,
            &facts.local_node_id,
            facts.local_incarnation,
        )?;
        if native.writer_enforcement_version() != 1
            || native.birth_id() != facts.local_birth_id
            || native.birth_sha256() != facts.local_birth_sha256
        {
            return Err(invalid("native writer birth/enforcement changed"));
        }
        let registration = self
            .registration
            .upgrade()
            .ok_or_else(|| invalid("native writer registration ended"))?;
        if !Arc::ptr_eq(hook.registration(), &registration)
            || hook
                .native_writer()
                .is_none_or(|state| !std::ptr::eq(state.as_ref(), self))
        {
            return Err(invalid(
                "native writer policy is not the actual installed hook",
            ));
        }
        let stored: NativeWriterPolicyRecord = database
            .read_native_range_image_record(POLICY_POINTER)?
            .ok_or_else(|| invalid("native host writer receipt missing"))?;
        stored.validate()?;
        if stored != self.record {
            return Err(invalid("native host writer receipt changed"));
        }
        Ok(())
    }
}

#[cfg(test)]
impl BicDb {
    /// Denial/identity regression on THIS actual installed native hook. Only
    /// diagnostics escape; no State, read point, grant or mutable hook escapes.
    pub(crate) fn test_original_writer_identity_with_queued_hook_writer(
        &self,
        queued_marker: &std::path::Path,
    ) -> Result<serde_json::Value> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::{Duration, Instant};
        let writer = self
            .commit_admission
            .try_read()
            .ok_or_else(|| invalid("queued-hook actual original hook initially busy"))?
            .native_writer()
            .cloned()
            .ok_or_else(|| invalid("queued-hook test requires real installed native writer"))?;
        let point = self.borrow_original_native_writer_identity(writer.clone())?;
        let before = writer.record.checksum_sha256.clone();
        let entered = AtomicBool::new(false);
        let acquired = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                entered.store(true, Ordering::Release);
                // Actual exclusive acquisition, no synthetic queue flag and
                // no hook replacement. The original read point is still live.
                let _actual_writer = self.commit_admission.write();
                acquired.store(true, Ordering::Release);
            });
            let until = Instant::now() + Duration::from_secs(1);
            let mut read_refused = false;
            while Instant::now() < until {
                if entered.load(Ordering::Acquire) && self.commit_admission.try_read().is_none() {
                    read_refused = true;
                    break;
                }
                std::thread::yield_now();
            }
            let before_acquired = acquired.load(Ordering::Acquire);
            let observed = serde_json::json!({
                "actual_writer_call_entered":entered.load(Ordering::Acquire),
                "actual_try_read_refused_while_original_read_live":read_refused,
                "own_writer_acquired_before_point_drop":before_acquired,
                "actual_original_policy_sha256":before,
                "actual_native_birth":writer.record.facts.local_birth_id,
                "actual_original_database_open":self.incarnation_id(),
                "actual_hook_registration_generation":point.hook.registration().generation(),
                "method":"owned exclusive write call entered; try_read refused while real original shared point held",
                "not_a_synthetic_queued_state_or_Serving_grant":true,
            });
            let mut prerequisite = read_refused && !before_acquired;
            // Keep all validation/results as DATA before assertions, and drop
            // the point BEFORE joining even on prerequisite/evidence errors.
            let marker_result = if prerequisite {
                (|| -> Result<()> {
                    let bytes = serde_json::to_vec_pretty(&observed)?;
                    let mut file = std::fs::File::create(queued_marker)?;
                    std::io::Write::write_all(&mut file, &bytes)?;
                    file.sync_all()?;
                    std::fs::File::open(queued_marker.parent().unwrap())?.sync_all()?;
                    Ok(())
                })()
            } else {
                Err(invalid(
                    "actual queued hook writer prerequisite not reached",
                ))
            };
            let started = Instant::now();
            let validation = if marker_result.is_ok() {
                point.require_database(self)
            } else {
                prerequisite = false;
                Err(invalid(
                    "actual queued hook marker/prerequisite unavailable",
                ))
            };
            let elapsed_us = started.elapsed().as_micros();
            // Separate validation return from point destruction, own writer
            // join and later fixture Drop. A child timeout is not by itself
            // evidence that borrowed validation deadlocked.
            let validation_marker = queued_marker
                .parent()
                .unwrap()
                .join("actual-hook-validation-returned.json");
            let validation_marker_error = (|| -> Result<()> {
                let bytes = serde_json::to_vec_pretty(&serde_json::json!({
                    "borrowed_validation_returned":true,
                    "borrowed_validation_error":validation.as_ref().err().map(ToString::to_string),
                    "borrowed_validation_elapsed_us":elapsed_us,
                    "original_point_still_held_before_drop":true,
                    "own_writer_join_not_started":true,
                }))?;
                let mut file = std::fs::File::create(validation_marker)?;
                std::io::Write::write_all(&mut file, &bytes)?;
                file.sync_all()?;
                Ok(())
            })()
            .err()
            .map(|error| error.to_string());
            drop(point);
            let joined = worker.join().is_ok();
            let ordinary = writer.require_original_identity_database(self);
            Ok(serde_json::json!({
                "queued_observation":observed,"prerequisite_reached":prerequisite,
                "marker_error":marker_result.as_ref().err().map(ToString::to_string),
                "borrowed_validation_error":validation.as_ref().err().map(ToString::to_string),
                "borrowed_validation_elapsed_us":elapsed_us,
                "validation_marker_error":validation_marker_error,
                "own_writer_joined":joined,
                "own_writer_acquired_after_point_drop":acquired.load(Ordering::Acquire),
                "ordinary_identity_after_cleanup_error":ordinary.as_ref().err().map(ToString::to_string),
                "original_policy_unchanged":before==writer.record.checksum_sha256,
                "scope":"actual installed-writer held-hook identity component only; no Joined A/R2/APP/Strong/physical qualification",
            }))
        })
    }
}
