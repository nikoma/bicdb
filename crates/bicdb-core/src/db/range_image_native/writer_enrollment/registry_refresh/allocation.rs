//! SAME live State allocation audit. Native ReturnedOk is retained before
//! outer checks. Entered-Unknown is permanently refused by this producer;
//! disk bytes never turn an old failed fsync into a successful helper return.
use super::*;
use crate::distribution_data::NativeWriterAllocationRefresh;
#[cfg(test)]
mod publication_health_cut;

#[derive(Debug, Clone, Copy)]
enum Outcome {
    NotEntered,
    Entered,
    Known(u64),
    Unknown,
}
#[derive(Debug)]
pub(in crate::db::range_image_native::writer_enrollment) struct RetainedAllocationAttempt {
    record: RegistryExtensionRecord,
    previous_facts: HostWriterPolicyFacts,
    outcome: Outcome,
    // Bounded diagnostic DATA only. The original error is returned unchanged;
    // never invoke its Display under caller/attempt/native publication guards.
    first_error_kind: Option<&'static str>,
}

fn error_kind(error: &BicDbError) -> &'static str {
    match error {
        BicDbError::QueryCanceled => "query_canceled",
        BicDbError::QueryTimedOut => "query_timed_out",
        BicDbError::CommitOutcomeUnknown { .. } => "commit_outcome_unknown",
        BicDbError::WriteFailed { .. } => "write_failed",
        BicDbError::PagedStorage(_) => "paged_storage",
        BicDbError::Io(_) => "io",
        BicDbError::Cluster(_) => "cluster",
        BicDbError::FormatCompatibility(_) => "format_compatibility",
        _ => "other",
    }
}
impl NativeWriterHostState {
    fn allocation_previous(&self, db: &BicDb) -> Result<(HostWriterPolicyFacts, String, u32)> {
        let previous = self.load_registry_chain(db)?;
        Ok(previous.map_or_else(
            || {
                (
                    self.record.facts.clone(),
                    self.record.checksum_sha256.clone(),
                    1,
                )
            },
            |r| (r.facts, r.checksum_sha256, r.sequence + 1),
        ))
    }
}
impl InstalledNativeWriterHostGuard {
    /// Identity only. Does not accept the new descriptor for ordinary APP.
    pub(crate) fn require_allocation_refresh_origin<T: RangeWriteTransport + 'static>(
        &self,
        db: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        actual_writer: &Arc<NativeWriterHostState>,
    ) -> Result<()> {
        let original = self
            .state
            .coordinator
            .upgrade()
            .ok_or_else(|| invalid("allocation original coordinator ended"))?;
        let current: Arc<dyn CommitAdmission> = coordinator.clone();
        if !Arc::ptr_eq(actual_writer, &self.state)
            || !Arc::ptr_eq(&current, &original)
            || coordinator.native_writer_coordinator_id() != self.state.record.coordinator_id
        {
            return Err(invalid("allocation replaced original State/coordinator"));
        }
        self.state.require_original_identity_database(db)
    }
    pub(crate) fn allocation_policy_is_current(
        &self,
        topology: &crate::ClusterTopology,
    ) -> Result<bool> {
        self.state.require_lifetime()?;
        if self
            .state
            .registry_publication_pending
            .load(Ordering::Acquire)
        {
            return Ok(false);
        }
        let verified = self.state.registry_extension.read();
        let facts = verified
            .as_ref()
            .map_or(&self.state.record.facts, |v| &v.0.facts);
        facts.require_refresh_topology(topology)?;
        drop(verified);
        Ok(topology
            .ranges
            .values()
            .all(|r| self.state.require_range(r).is_ok()))
    }
    pub(crate) fn allocation_changed_ranges(
        &self,
        topology: &crate::ClusterTopology,
    ) -> Result<Vec<crate::RangeId>> {
        self.state.require_lifetime()?;
        let verified = self.state.registry_extension.read();
        let facts = verified
            .as_ref()
            .map_or(&self.state.record.facts, |v| &v.0.facts);
        Ok(topology
            .ranges
            .values()
            .filter(|r| facts.require_range(r).is_err())
            .map(|r| r.id)
            .collect())
    }
    pub(crate) fn require_allocation_candidate(
        &self,
        facts: &HostWriterPolicyFacts,
        topology: &crate::ClusterTopology,
    ) -> Result<()> {
        self.state.require_lifetime()?;
        let verified = self.state.registry_extension.read();
        let previous = verified
            .as_ref()
            .map_or(&self.state.record.facts, |v| &v.0.facts);
        facts.require_allocation_successor(previous, topology)
    }
    /// Native audit only; no epoch recovery/Ready/Strong marker is published.
    pub(crate) fn refresh_allocation_policy<T: RangeWriteTransport + 'static>(
        &self,
        db: &BicDb,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        prepared: NativeWriterAllocationRefresh,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let _gate = db
            .serializable_commit_admission
            .try_write()
            .ok_or_else(|| invalid("allocation admission gate busy"))?;
        prepared.with_current(db, coordinator, self, || Ok(()))?;
        let _journal = db.try_native_image_journal()?;
        let mut retained = self
            .state
            .allocation_attempt
            .try_lock()
            .ok_or_else(|| invalid("allocation audit attempt busy"))?;
        let result = (|| -> Result<()> {
            if retained.is_none() {
                let (previous, predecessor, sequence) = self.state.allocation_previous(db)?;
                let audit: HostMetadataReadAcquireResponse =
                    serde_json::from_value(prepared.audit().clone())?;
                prepared
                    .policy()
                    .facts()
                    .require_allocation_successor(&previous, &audit.evidence.committed_topology)?;
                if sequence > MAX_EXTENSIONS {
                    return Err(invalid(
                        "allocation native policy history exhausted; no eviction",
                    ));
                }
                // Validate the REAL native physical predecessor before effects.
                for (id, after) in &prepared.policy().facts().ranges {
                    let before = &previous.ranges[id];
                    if before == after {
                        continue;
                    }
                    // Facts cover the cluster; physical inventory covers only
                    // this original member's ranges. Exact unchanged-voter
                    // succession above proves foreign ranges stay foreign.
                    if !before.replicas.iter().any(|r| {
                        r.node_id == self.state.record.facts.local_node_id
                            && r.role == crate::RangeReplicaRole::Voter
                    }) {
                        continue;
                    }
                    let lineage =
                        db.current_native_range_source_lineage_under_journal(*id, &_journal)?;
                    db.require_native_allocation_predecessor(&lineage, before)?;
                    let prefix = db.native_range_prefix_observation(*id)?;
                    if prefix.pending_command_index.is_some() {
                        return Err(invalid("allocation retains an unresolved actual native origin; no fake epoch recovery"));
                    }
                }
                let record = RegistryExtensionRecord {
                    version: 2,
                    purpose: Some(ExtensionPurpose::AllocationEpoch),
                    sequence,
                    original_policy_sha256: self.state.record.checksum_sha256.clone(),
                    registration_generation: self.state.record.generation,
                    coordinator_id: self.state.record.coordinator_id,
                    database_incarnation: self.state.record.database_incarnation.clone(),
                    store_open_id: self.state.record.store_open_id,
                    predecessor_sha256: predecessor,
                    facts: prepared.policy().facts().clone(),
                    metadata_context_id: prepared.policy().metadata_context_id(),
                    metadata_term: prepared.policy().metadata_term(),
                    metadata_read_index: prepared.policy().metadata_read_index(),
                    metadata_audit: prepared.audit().clone(),
                    native_page_xid: 0,
                    checksum_sha256: String::new(),
                };
                bounded_json(&record, MAX_EXTENSION_BYTES)?;
                *retained = Some(RetainedAllocationAttempt {
                    record,
                    previous_facts: previous,
                    outcome: Outcome::NotEntered,
                    first_error_kind: None,
                });
            }
            let attempt = retained.as_mut().unwrap();
            if prepared.policy().metadata_term() < attempt.record.metadata_term
                || prepared.policy().metadata_read_index() < attempt.record.metadata_read_index
            {
                return Err(invalid(
                    "allocation fresh metadata precedes original retained candidate",
                ));
            }
            if &attempt.record.facts != prepared.policy().facts() {
                return Err(invalid(
                    "allocation must retain exact original candidate; current policy differs",
                ));
            }
            let issued: HostMetadataReadAcquireResponse =
                serde_json::from_value(attempt.record.metadata_audit.clone())?;
            let fresh: HostMetadataReadAcquireResponse =
                serde_json::from_value(prepared.audit().clone())?;
            for (id, before) in &attempt.previous_facts.ranges {
                if attempt.record.facts.ranges.get(id) == Some(before) {
                    continue;
                }
                let old = issued
                    .evidence
                    .committed_topology
                    .active_relocation_for_range(*id)
                    .ok_or_else(|| invalid("allocation original candidate plan absent"))?;
                let now = fresh
                    .evidence
                    .committed_topology
                    .active_relocation_for_range(*id)
                    .ok_or_else(|| invalid("allocation fresh original plan absent"))?;
                if old.id != now.id
                    || old.plan_id != now.plan_id
                    || old.handoff_session != now.handoff_session
                    || old.range_id != now.range_id
                    || old.source != now.source
                    || old.target != now.target
                    || old.target_replica_id != now.target_replica_id
                    || old.expected_epoch != now.expected_epoch
                    || old.learner_epoch != now.learner_epoch
                {
                    return Err(invalid(
                        "allocation fresh request substituted original plan/session/candidate",
                    ));
                }
            }
            match attempt.outcome {
                Outcome::Entered | Outcome::Unknown => return Err(invalid("allocation original native IO outcome unknown; no replay or byte-based success")),
                Outcome::Known(_) => {},
                Outcome::NotEntered => {
                    parent.check()?;
                    prepared.with_current(db, coordinator, self, || Ok(()))?;
                    // Sticky old-reader refusal comes BEFORE the first audit.
                    format::enable_native_writer_allocation_policy(db.data_path())?;
                    prepared.with_current(db, coordinator, self, || Ok(()))?;
                    parent.check()?;
                    self.state.registry_publication_pending.store(true, Ordering::Release);
                    #[cfg(test)]
                    let mode=self.state.allocation_fault.load(Ordering::Acquire);
                    #[cfg(test)]
                    // These two cuts belong to later actual boundaries:
                    // known native return (2) and held publication version (4).
                    if !matches!(mode, 2 | 4) {self.state.allocation_fault.store(0,Ordering::Release);}
                    #[cfg(test)]
                    if mode==1 {
                        let cut=AllocationCut{mode,parent_live:parent.check().is_ok(),own_live:prepared.require_memory().is_ok(),
                            parent_deadline:parent.deadline(),fired:0,xid:0,atomic_health_refusal:false,publication_error:None};
                        let fire=cut.parent_live && cut.own_live;
                        *self.state.allocation_cut.lock()=Some(cut);
                        if fire {parent.cancel();}
                        parent.check()?;
                    }
                    #[cfg(test)]
                    let fault=(mode==3).then(||bicdb_page::test_faults::arm_io_failure("page_log.sync",db.data_path(),1,false));
                    #[cfg(test)]
                    if mode==3 {
                        *self.state.allocation_cut.lock()=Some(AllocationCut{mode,parent_live:parent.check().is_ok(),
                            own_live:prepared.require_memory().is_ok(),parent_deadline:parent.deadline(),fired:0,xid:0,atomic_health_refusal:false,publication_error:None});
                    }
                    attempt.outcome = Outcome::Entered;
                    let actual = db.in_paged_transaction(|paged, xid| {
                        parent.check()?;
                        prepared.require_memory()?;
                        attempt.record.native_page_xid=xid;
                        attempt.record.checksum_sha256=attempt.record.checksum()?;
                        attempt.record.validate(&self.state.record)?;
                        let bytes=bounded_json(&attempt.record,MAX_EXTENSION_BYTES)?;
                        paged.store().put(xid,&native_key(&history_key(self.state.record.generation,attempt.record.sequence)),&bytes)
                            .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                        paged.store().put(xid,&native_key(&current_key(self.state.record.generation)),&bytes)
                            .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                        Ok(())
                    });
                    #[cfg(test)]
                    {
                        if let Some(fault)=&fault {
                            if let Some(cut)=self.state.allocation_cut.lock().as_mut(){cut.fired=fault.fired();}
                        }
                        drop(fault);
                    }
                    #[cfg(test)]
                    // Only THIS selected native-fault attempt owns this stamp.
                    // A later mode-0 retry must preserve a mode-1 pre-IO XID 0.
                    if mode == 3 {
                        if let Some(cut) = self.state.allocation_cut.lock().as_mut() {
                            if cut.mode == 3 {
                                cut.xid = attempt.record.native_page_xid;
                            }
                        }
                    }
                    match actual {
                        Ok(Some(xid)) => attempt.outcome=Outcome::Known(xid),
                        Ok(None) => {attempt.outcome=Outcome::Unknown; return Err(invalid("allocation actual native page transaction absent"));},
                        Err(error) => {attempt.outcome=Outcome::Unknown; return Err(error);},
                    }
                }
            }
            #[cfg(test)]
            if self.state.allocation_fault.load(Ordering::Acquire) == 2 {
                self.state.allocation_fault.store(0, Ordering::Release);
                let cut = AllocationCut {
                    mode: 2,
                    parent_live: parent.check().is_ok(),
                    own_live: prepared.require_memory().is_ok(),
                    parent_deadline: parent.deadline(),
                    fired: 0,
                    xid: 0,
                    atomic_health_refusal: false,
                    publication_error: None,
                };
                let fire = cut.parent_live && cut.own_live;
                *self.state.allocation_cut.lock() = Some(cut);
                if let Some(cut) = self.state.allocation_cut.lock().as_mut() {
                    cut.xid = attempt.record.native_page_xid;
                }
                if fire {
                    parent.cancel();
                }
                parent.check()?;
            }
            // A new request may inspect ONLY this real successful native return.
            let Outcome::Known(xid) = attempt.outcome else {
                unreachable!()
            };
            let stored: RegistryExtensionRecord = db
                .read_native_range_image_record(&current_key(self.state.record.generation))?
                .ok_or_else(|| invalid("allocation CURRENT absent after known native return"))?;
            if xid != stored.native_page_xid
                || stored != attempt.record
                || self.state.load_registry_chain(db)?.as_ref() != Some(&stored)
                || db
                    .paged_records
                    .as_ref()
                    .ok_or_else(|| invalid("allocation actual page store absent"))?
                    .store()
                    .transactions()
                    .status(xid)
                    != bicdb_page::TxStatus::Committed
            {
                return Err(invalid(
                    "allocation known native CURRENT/archive/ancestry/XID differs",
                ));
            }
            prepared.with_current(db, coordinator, self, || Ok(()))?;
            parent.check()?;
            let verified = Arc::new(VerifiedRegistryExtension(stored));
            #[cfg(test)]
            let mut health_cut = if self
                .state
                .allocation_fault
                .compare_exchange(4, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                Some(publication_health_cut::PublicationHealthCut::start(
                    db.native_metadata_owner_health()?,
                )?)
            } else {
                None
            };
            let mut old = None;
            prepared.with_current(db, coordinator, self, || {
                let hook = db
                    .commit_admission
                    .try_read()
                    .ok_or_else(|| invalid("allocation final hook busy"))?;
                let registration = self.state.require_lifetime()?;
                if !Arc::ptr_eq(hook.registration(), &registration)
                    || hook
                        .native_writer()
                        .is_none_or(|s| !Arc::ptr_eq(s, &self.state))
                {
                    return Err(invalid(
                        "allocation original hook replaced before publication",
                    ));
                }
                let mut cache = self
                    .state
                    .registry_extension
                    .try_write()
                    .ok_or_else(|| invalid("allocation verified cache busy"))?;
                let version = prepared.point().try_allocation_publication_guard()?;
                #[cfg(test)]
                if let Some(health_cut) = health_cut.as_mut() {
                    let cut = AllocationCut {
                        mode: 4,
                        parent_live: parent.check().is_ok(),
                        own_live: prepared.require_publication_memory().is_ok(),
                        parent_deadline: parent.deadline(),
                        fired: 0,
                        xid,
                        atomic_health_refusal: false,
                        publication_error: None,
                    };
                    let fire = cut.parent_live && cut.own_live;
                    *self.state.allocation_cut.lock() = Some(cut);
                    let actual_fence = if fire {
                        // The SAME actual version is held. Only the separate
                        // worker records/formats the injected health failure.
                        health_cut.fire(parent)
                    } else {
                        Ok(())
                    };
                    if let Some(cut) = self.state.allocation_cut.lock().as_mut() {
                        cut.fired = health_cut.fired();
                    }
                    actual_fence?;
                }
                parent.check()?;
                prepared.require_publication_memory()?;
                version.require_memory()?;
                old = cache.replace(verified);
                self.state
                    .registry_publication_pending
                    .store(false, Ordering::Release);
                Ok(())
            })?;
            drop(old);
            Ok(())
        })();
        if let Err(error) = &result {
            #[cfg(test)]
            if let Some(cut) = self.state.allocation_cut.lock().as_mut() {
                if cut.mode == 4 {
                    // Original actual Result, after the final version/service
                    // guards have unwound. No error payload is cloned.
                    cut.atomic_health_refusal = matches!(error,
                        BicDbError::PagedStorage(message)
                        if message == "actual allocation database durability is fenced; details deferred");
                    cut.publication_error = Some(if cut.atomic_health_refusal {
                        "PagedStorage: actual allocation atomic health refusal"
                    } else {
                        match error {
                            BicDbError::QueryCanceled => "QueryCanceled",
                            BicDbError::QueryTimedOut => "QueryTimedOut",
                            _ => "Other original error; see actual worker return",
                        }
                    });
                }
            }
            if let Some(attempt) = retained.as_mut() {
                if attempt.first_error_kind.is_none() {
                    attempt.first_error_kind = Some(error_kind(error));
                }
            }
        } else {
            *retained = None;
        }
        result
    }
}

impl NativeWriterHostState {
    /// Exact effective descriptor from the SAME successfully published native
    /// audit. This performs no IO write/reconciliation and grants no Ready.
    pub(crate) fn require_allocation_native_range(
        &self,
        db: &BicDb,
        range: &crate::RangeDescriptor,
    ) -> Result<()> {
        self.require_database(db, range)?;
        if !format::native_writer_allocation_policy_active(db.data_path())? {
            return Err(invalid("allocation native range lacks reader barrier"));
        }
        let cache = self.registry_extension.read();
        let verified = cache
            .as_ref()
            .ok_or_else(|| invalid("allocation effective native policy absent"))?;
        if verified.0.version != 2
            || verified.0.purpose != Some(ExtensionPurpose::AllocationEpoch)
            || self
                .record
                .facts
                .ranges
                .get(&range.id)
                .is_none_or(|r| r.epoch >= range.epoch)
        {
            return Err(invalid(
                "allocation native range is not a verified original-State epoch successor",
            ));
        }
        verified.0.facts.require_range(range)
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(in crate::db::range_image_native::writer_enrollment) struct AllocationCut {
    mode: u8,
    parent_live: bool,
    own_live: bool,
    parent_deadline: Option<std::time::Instant>,
    fired: u64,
    xid: u64,
    atomic_health_refusal: bool,
    publication_error: Option<&'static str>,
}
#[cfg(test)]
impl InstalledNativeWriterHostGuard {
    pub(crate) fn test_pause_allocation_recovery(&self, hold: bool) -> Result<()> {
        if hold {
            self.state.require_lifetime()?;
        }
        self.state
            .allocation_recovery_hold
            .store(hold, Ordering::Release);
        Ok(())
    }
    pub(crate) fn test_allocation_recovery_is_paused(
        &self,
        parent: &CancellationToken,
    ) -> Result<bool> {
        parent.check()?;
        self.state.require_lifetime()?;
        if self.state.allocation_recovery_hold.load(Ordering::Acquire) {
            self.state
                .allocation_recovery_cut
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    Some(n.saturating_add(1))
                })
                .ok();
            return Ok(true);
        }
        Ok(false)
    }
    pub(crate) fn test_arm_allocation_fault(&self, mode: u8) -> Result<()> {
        self.state.require_lifetime()?;
        if !(1..=4).contains(&mode)
            || self
                .state
                .allocation_fault
                .compare_exchange(0, mode, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(invalid(
                "allocation actual fault already armed or unsupported",
            ));
        }
        Ok(())
    }
    pub(crate) fn test_allocation_evidence(&self) -> serde_json::Value {
        let attempt = self.state.allocation_attempt.lock();
        let cut = self.state.allocation_cut.lock();
        let cache = self.state.registry_extension.read();
        serde_json::json!({"registration":self.state.record.generation,"coordinator":self.state.record.coordinator_id,
            "database_open":self.state.record.database_incarnation,"store_open":self.state.record.store_open_id,
            "pending":self.state.registry_publication_pending.load(Ordering::Acquire),
            "recovery_hold":self.state.allocation_recovery_hold.load(Ordering::Acquire),
            "actual_live_recovery_hold_cuts":self.state.allocation_recovery_cut.load(Ordering::Acquire),
            "cut":cut.as_ref().map(|c|serde_json::json!({"mode":c.mode,"parent_live_before":c.parent_live,"own_live_before":c.own_live,
                "original_deadline_recorded":c.parent_deadline.is_some(),"fault_fired":c.fired,"original_xid":c.xid,
                "actual_typed_atomic_health_refusal":c.atomic_health_refusal,
                "actual_original_publication_error":c.publication_error})),
            "retained":attempt.as_ref().map(|a|serde_json::json!({"record":a.record,"actual_outcome":format!("{:?}",a.outcome),"first_error_kind":a.first_error_kind})),
            "published":cache.as_ref().map(|v|serde_json::json!({"record":v.0})),"DATA_only_no_authority":true})
    }
}

#[cfg(test)]
impl InstalledNativeWriterHostGuard {
    pub(crate) fn test_allocation_physical_inventory(
        &self,
        db: &BicDb,
    ) -> Result<serde_json::Value> {
        self.state.require_original_identity_database(db)?;
        let birth = db
            .read_native_birth()?
            .ok_or_else(|| invalid("allocation inventory actual native birth absent"))?;
        let native = db.native_recovery_admission(
            &birth.spec.cluster_id,
            birth.spec.bootstrap_id,
            &birth.spec.node_id,
            birth.spec.member_incarnation,
        )?;
        let bindings = db.capture_range_image_bindings()?;
        Ok(
            serde_json::json!({"actual_birth_id":native.birth_id(),"actual_birth_sha256":native.birth_sha256(),
            "actual_initial_range_inventory":birth.spec.initial_ranges,
            "actual_namespace_inventory":bindings.namespace_ids(),"same_actual_open":db.incarnation_id(),
            "DATA_only_no_local_range_grant":true}),
        )
    }
}
