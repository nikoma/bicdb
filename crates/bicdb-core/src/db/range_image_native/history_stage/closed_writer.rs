//! A closed learner image may retain the actual C1 writer. This permission is
//! neither an ordinary writer policy nor a Registered-receiver substitute.
use super::*;
use crate::distribution_range_image::ClosedNativeImageSource;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

fn require_empty_target_slots(db: &BicDb, range: RangeId) -> Result<()> {
    if db
        .native_abort_slot_inventory()?
        .iter()
        .any(|slot| slot.command.range_id == range)
    {
        return Err(invalid(
            "closed C1 image does not support nonempty protected slot obligations",
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct ClosedImageOriginalWriter {
    writer: Arc<NativeWriterHostState>,
    source: ClosedNativeImageSource,
    original_role: Arc<RecoveryRoleAdmission>,
    original_caller: CancellationToken,
    database_incarnation: String,
    store_open: Uuid,
    native_store: Uuid,
    predecessor: crate::RangeDescriptor,
    pending: crate::RangeDescriptor,
    namespaces: Vec<(String, Uuid)>,
    slot: crate::distribution_data::RangeWriteServiceSlotGuard,
}
impl ClosedImageOriginalWriter {
    #[cfg(test)]
    pub(crate) fn test_require_original_request_live(&self) -> Result<()> {
        self.original_caller.check()?;
        self.original_role.require_live_request()
    }
    pub(super) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.original_caller.check()?;
        self.slot.require_live()?;
        self.original_role.require_live_request()?;
        let predecessor = self.source.require_current(&self.original_role)?;
        if db.incarnation_id() != self.database_incarnation
            || predecessor != self.predecessor
            || db.capture_range_image_bindings()?.namespace_ids() != self.namespaces
            || db.native_abort_catalogue_store_id()? != Some(self.native_store)
        {
            return Err(invalid(
                "closed C1 image changed actual DB/Store/predecessor/namespaces",
            ));
        }
        self.writer.require_store(db.data_path(), self.store_open)?;
        self.writer.require_database(db, &self.predecessor)?;
        // Nonempty protected domains need their own exact source-inclusion
        // proof. This first lane never ignores, erases, or reinterprets them.
        require_empty_target_slots(db, self.pending.id)?;
        self.original_role.require_live_request()?;
        self.original_caller.check()
    }
    pub(super) fn require_context(
        &self,
        db: &BicDb,
        context: &NativeHistoryTargetContext<'_>,
    ) -> Result<()> {
        self.require_current(db)?;
        let NativeImageInstallRole::CommittedHandoff(_) = context.role else {
            return Err(invalid(
                "closed C1 image cannot authorize a normal recovery purpose",
            ));
        };
        self.source.require_role(context.role.role())?;
        if context.fence.range() != &self.pending
            || context.fence.purpose() != ImageInstallPurpose::RelocationLearner
            || context.store.native_writer_store_open_id() != self.store_open
            || !Arc::ptr_eq(&context.store.native_writer_host_state()?, &self.writer)
        {
            return Err(invalid(
                "closed C1 image changed actual Store/writer/target/purpose",
            ));
        }
        db.require_native_writer_catalogue_custody(context.store)?;
        self.original_caller.check()
    }
}
impl BicDb {
    /// Eligibility only: the actual staged proof is still mandatory before
    /// ANY row scope/Controlled mutation. No JSON or absent hook creates this.
    pub(crate) fn prepare_closed_image_original_writer(
        &self,
        store: &RangeWriteStore,
        fence: &TargetImageInstallFence,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        caller: &CancellationToken,
        slot: Option<&crate::distribution_data::RangeWriteServiceSlotGuard>,
    ) -> Result<Option<Arc<ClosedImageOriginalWriter>>> {
        caller.check()?;
        let NativeImageInstallRole::CommittedHandoff(handoff) = role else {
            return Ok(None);
        };
        if fence.purpose() != ImageInstallPurpose::RelocationLearner {
            return Ok(None);
        }
        if self.actual_native_writer_enforcement_version()? != 1 {
            return Ok(None);
        }
        let writer = store.native_writer_host_state()?;
        // Shape chooses a lane, not authority. The ordinary path still makes
        // its unchanged full installed-policy checks at row construction/Tx.
        if writer.require_range(fence.range()).is_ok() {
            return Ok(None);
        }
        if source.cut().target_id() != role.role().local_node_id()
            || source.cut().target_incarnation() != role.role().local_member_incarnation()
        {
            return Err(invalid(
                "closed C1 image supports only the exact planned learner",
            ));
        }
        let source = source.closed_native_image_source(role.role())?;
        let predecessor = source.require_current(role.role())?;
        let native_store = self.native_abort_catalogue_store_id()?.ok_or_else(|| {
            invalid("closed C1 image needs actual protected native catalogue owner")
        })?;
        let result = Arc::new(ClosedImageOriginalWriter {
            writer,
            source,
            original_role: handoff.retain_original(),
            original_caller: caller.clone(),
            database_incarnation: self.incarnation_id().into(),
            store_open: store.native_writer_store_open_id(),
            native_store,
            predecessor,
            pending: fence.range().clone(),
            namespaces: self.capture_range_image_bindings()?.namespace_ids(),
            slot: slot
                .cloned()
                .ok_or_else(|| invalid("closed C1 image requires actual service Store slot"))?,
        });
        if result.slot.id() != result.store_open {
            return Err(invalid("closed C1 image service Store slot differs"));
        }
        self.require_native_writer_catalogue_custody(store)?;
        result.require_current(self)?;
        caller.check()?;
        Ok(Some(result))
    }
}

#[cfg(test)]
mod transaction_hook {
    use std::cell::RefCell;
    type Hook = Box<dyn FnOnce()>;
    thread_local! { static HOOK: RefCell<Option<Hook>> = RefCell::new(None); }
    pub(crate) struct Guard(Option<Hook>, std::marker::PhantomData<std::rc::Rc<()>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            HOOK.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }
    pub(crate) fn install(hook: Hook) -> Guard {
        Guard(
            HOOK.with(|slot| slot.replace(Some(hook))),
            std::marker::PhantomData,
        )
    }
    pub(super) fn fire() {
        let hook = HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }
}
#[cfg(test)]
pub(crate) use transaction_hook::install as test_before_closed_image_transaction;
#[cfg(test)]
pub(super) fn before_transaction() {
    transaction_hook::fire();
}
#[cfg(test)]
impl BicDb {
    pub(crate) fn test_closed_image_requires_empty_target_slots(
        &self,
        range: RangeId,
    ) -> Result<()> {
        require_empty_target_slots(self, range)
    }
}
