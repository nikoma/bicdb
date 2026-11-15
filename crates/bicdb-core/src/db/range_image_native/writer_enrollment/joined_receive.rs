//! Borrowed actual installed writer for a selected Joined image receiver.
//! Identity and storage constraints only: no NoDomain/R0, remote source fact,
//! row transaction, native install, decision, Open or ordinary range grant.
use super::*;
use crate::distribution_native_host::NativeJoinedRangeReceiveRequest;
use crate::distribution_range_consensus::RangeWriteStore;
mod rotation;
pub(crate) use rotation::NativeJoinedRemoteRotationPoint;

enum JoinedReceiveGuard<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}
impl<T> std::ops::Deref for JoinedReceiveGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(value) => value,
            Self::Borrowed(value) => value,
        }
    }
}

pub(crate) struct NativeJoinedReceiveStorageScope<'a> {
    db: &'a BicDb,
    request: &'a NativeJoinedRangeReceiveRequest,
    native: &'a NativeRecoveryAdmission,
    frozen: &'a FrozenRangeMutationPermit,
    _admission: JoinedReceiveGuard<'a, parking_lot::RwLockReadGuard<'a, ()>>,
    hook: JoinedReceiveGuard<
        'a,
        parking_lot::RwLockReadGuard<'a, crate::db::commit_admission_hook::CommitAdmissionSlot>,
    >,
    journal: JoinedReceiveGuard<'a, NativeImageJournalGuard<'a>>,
}
impl NativeJoinedReceiveStorageScope<'_> {
    pub(crate) fn role(&self) -> &crate::distribution_range_recovery::RecoveryRoleAdmission {
        self.request.role().role()
    }
    pub(crate) fn require_request(&self, request: &NativeJoinedRangeReceiveRequest) -> Result<()> {
        self.require_database(self.db)?;
        if !std::ptr::eq(self.request, request) {
            return Err(invalid(
                "Joined storage point is not the actual caller's borrowed request",
            ));
        }
        request.require_live()
    }
    pub(crate) fn frozen(&self) -> &FrozenRangeMutationPermit {
        self.frozen
    }
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        self.native
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.request.origin().range_id()
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.request.require_live()?;
        if !std::ptr::eq(db, self.db) {
            return Err(invalid("Joined receive scope changed actual borrowed DB"));
        }
        self.journal.require_database(db)?;
        db.require_joined_publication_storage_health()?;
        self.native.require_current(db)?;
        // Original Registered birth/open remains, but B0 and an empty whole
        // native namespace would incorrectly forbid the existing served range.
        db.require_original_registered_receiver_birth_open_with_journal(
            self.native,
            &self.journal,
        )?;
        self.request.member().require_database_point(db)?;
        let guard = self.request.origin().guard();
        guard.state.require_borrowed_original_hook(db, &self.hook)?;
        let stored: NativeWriterPolicyRecord =
            db.read_native_range_image_record(POLICY_POINTER)?
                .ok_or_else(|| invalid("Joined receive original writer policy disappeared"))?;
        stored.validate()?;
        if stored != guard.state.record {
            return Err(invalid(
                "Joined receive replaced original installed native policy",
            ));
        }
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.range_id(), self.frozen)?;
        self.request
            .member()
            .publication_version()
            .require_current_memory()?;
        self.request.require_live()
    }
    pub(crate) fn require_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_database(db)?;
        let writer = store.native_writer_host_state()?;
        if !Arc::ptr_eq(&writer, &self.request.origin().guard().state) {
            return Err(invalid(
                "Joined receive Store carries another installed State",
            ));
        }
        writer.require_store(db.data_path(), store.native_writer_store_open_id())?;
        db.require_native_writer_catalogue_custody(store)?;
        self.require_database(db)
    }
    /// Actual lower bounds, including pending/invalidated state, are DATA.
    /// A caller must not interpret all-None as a certified zero history.
    pub(crate) fn require_completed_row_scan(
        &self,
        scan: &NativeJoinedRowAbsenceScan,
    ) -> Result<()> {
        self.require_database(self.db)?;
        scan.require_complete_at_storage_point(self.db, self.request, self.frozen)?;
        self.require_database(self.db)
    }
    pub(crate) fn require_install_safety(&self, safety: &NativeRangeInstallSafety) -> Result<()> {
        self.require_database(self.db)?;
        safety.require_current_under_journal(self.db, self.native, self.frozen, &self.journal)
    }
    pub(crate) fn observe_selected_install_safety(&self) -> Result<NativeRangeInstallSafety> {
        self.require_database(self.db)?;
        self.db.native_range_install_safety_under_journal(
            self.native,
            self.range_id(),
            self.frozen,
            &self.journal,
        )
    }
    /// Never a zero floor: require a genuinely unused selected native lane.
    /// Existing stages, history, decisions, invalidations and pending row-WAL
    /// debt are refusals; unrelated ranges' records are neither cleared nor
    /// compared against a frozen whole-database business watermark.
    pub(crate) fn require_unmaterialized_selected(&self) -> Result<NativeRangeInstallSafety> {
        self.require_database(self.db)?;
        self.request
            .origin()
            .guard()
            .require_new_joined_data_range_point(self.range_id())?;
        let safety = self.observe_selected_install_safety()?;
        if safety.base_index().is_some()
            || safety.applied_index().is_some()
            || safety.pending_index().is_some()
            || safety.materialized_pending_index().is_some()
            || safety.history_point().is_some()
            || safety.source_invalidated()
        {
            return Err(invalid(
                "Joined selected range retains an actual native lower bound/obligation",
            ));
        }
        let r = self.range_id().get();
        let exact = [
            format!("genesis:{r}"),
            format!("install:active:{r}"),
            format!("install:base:{r}"),
            format!("prefix:{r}"),
            format!("pending:{r}"),
            format!("range-invalidation:{r}"),
            INVALIDATION_KEY.into(),
            format!("history-stage:active:{r}"),
            format!("serving-configuration/{r}"),
            format!("joint-handoff/old-close/current/{r}"),
            format!("joint-handoff/decision/current/{r}"),
            format!("joint-handoff/serving-prepared/current/{r}"),
            format!("joint-handoff/serving-bridge/current/{r}"),
        ];
        for key in exact {
            if self
                .db
                .read_native_range_image_record::<serde_json::Value>(&key)?
                .is_some()
            {
                return Err(invalid(
                    "Joined selected native record/history is not an unused receive lane",
                ));
            }
        }
        let prefixes = [
            format!("history:{r}:"),
            format!("history-stage:published:{r}:"),
            format!("abort-no-effect:{r}:"),
            format!("serving-configuration-history/{r}/"),
            format!("joint-handoff/old-close/history/{r}/"),
            format!("joint-handoff/decision/history/{r}/"),
            format!("joint-handoff/serving-prepared/history/{r}/"),
            format!("joint-handoff/serving-bridge/history/{r}/"),
        ];
        let paged = self
            .db
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Joined selected native page store absent"))?;
        let snapshot = paged.latest_snapshot();
        for prefix in prefixes {
            self.request.cancellation().check()?;
            let key = native_key(&prefix);
            let mut records = paged
                .store()
                .scan_from(&snapshot, &key)
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            if let Some(record) = records.next() {
                let (actual, _) = record.map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                if actual.starts_with(&key) {
                    return Err(invalid(
                        "Joined selected historical native obligation remains retained",
                    ));
                }
            }
        }
        // The sticky index parser is restrictive: malformed/missing-format or
        // unknown debt is never treated as empty. Other ranges may have debt.
        let pending =
            crate::db::registered_row_retention::read_pending_data(self.db.data_path(), paged)?;
        if pending.iter().any(|p| p.intent().range == self.range_id()) {
            return Err(invalid(
                "Joined selected range has outstanding original row WAL intent",
            ));
        }
        self.require_database(self.db)?;
        Ok(safety)
    }
}
impl InstalledNativeWriterHostGuard {
    pub(crate) fn require_new_joined_data_range(&self, db: &BicDb, range: RangeId) -> Result<()> {
        self.require_joined_receive_identity(db)?;
        self.require_new_joined_data_range_point(range)
    }
    // Caller already holds/validates the actual hook/native-policy scope.
    pub(super) fn require_new_joined_data_range_point(&self, range: RangeId) -> Result<()> {
        let original = self.state.record.facts.ranges.get(&range).ok_or_else(|| {
            invalid("Joined receive range absent from original configured virtual ranges")
        })?;
        if original.replicas.iter().any(|r| {
            r.node_id == self.state.record.facts.local_node_id
                && r.role == crate::RangeReplicaRole::Voter
        }) {
            return Err(invalid(
                "Joined receive selected range was already an original writer domain",
            ));
        }
        let domains = self
            .state
            .data_domains
            .try_read()
            .ok_or_else(|| invalid("Joined selected DATA-domain publication busy"))?;
        domains.require_unstarted_range(range)?;
        Ok(())
    }
    /// Full same-State hook/native-policy check for each row-scan chunk. This
    /// is outside any final version publication guard and holds no hook or
    /// journal guard across the subsequent actual paged scan.
    pub(crate) fn require_joined_receive_identity(&self, db: &BicDb) -> Result<()> {
        db.require_joined_publication_storage_health()?;
        self.state.require_original_identity_database(db)?;
        self.state.require_verified_registry_database(db)
    }
}
impl BicDb {
    /// This request was captured outside engine locks. The caller must perform
    /// its current-MStore pre/post checks there as well; the local version
    /// point below rejects known stale state, never replaces a quorum lease.
    pub(crate) fn with_joined_receive_storage_scope<R>(
        &self,
        request: &NativeJoinedRangeReceiveRequest,
        native: &NativeRecoveryAdmission,
        frozen: &FrozenRangeMutationPermit,
        operation: impl for<'scope> FnOnce(&'scope NativeJoinedReceiveStorageScope<'_>) -> Result<R>,
    ) -> Result<R> {
        request.require_live()?;
        let role = request.role().role();
        if native.local_node_id() != role.local_node_id()
            || native.local_member_incarnation() != role.local_member_incarnation()
            || native.cluster_id() != role.cluster_id()
            || native.bootstrap_id() != role.bootstrap_id()
            || request.origin().range_id() != role.current_range().id
            || frozen.owner()
                != crate::distribution_range_consensus::RecoveryFenceScope::from_role(role)?
                    .freeze_owner()?
        {
            return Err(invalid(
                "Joined receive storage request changed native/selected freeze identity",
            ));
        }
        let scope = NativeJoinedReceiveStorageScope {
            db: self,
            request,
            native,
            frozen,
            _admission: JoinedReceiveGuard::Owned(
                self.serializable_commit_admission
                    .try_read()
                    .ok_or_else(|| invalid("Joined receive actual writer admission busy"))?,
            ),
            hook: JoinedReceiveGuard::Owned(
                self.commit_admission
                    .try_read()
                    .ok_or_else(|| invalid("Joined receive original installed hook busy"))?,
            ),
            journal: JoinedReceiveGuard::Owned(self.try_native_image_journal()?),
        };
        scope.require_database(self)?;
        let result = operation(&scope);
        let post = scope.require_database(self);
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(value) => {
                post?;
                Ok(value)
            }
        }
    }
}
