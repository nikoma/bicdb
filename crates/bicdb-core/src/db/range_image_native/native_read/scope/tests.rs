//! Genuine local scope faults under its real journal; no proof constructors.
use super::*;
use std::cell::RefCell;
use std::rc::Rc;
type Cut = Rc<dyn for<'scope> Fn(&NativeServingObservationScope<'scope>) -> Result<()>>;
thread_local! { static CUT: RefCell<Option<Cut>> = const { RefCell::new(None) }; }
struct Restore(Option<Cut>);
impl Drop for Restore {
    fn drop(&mut self) {
        CUT.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}
pub(super) fn after_entry(scope: &NativeServingObservationScope<'_>) -> Result<()> {
    let cut = CUT.with(|slot| slot.borrow().clone());
    if let Some(cut) = cut {
        cut(scope)?;
    }
    Ok(())
}
impl BicDb {
    pub(crate) fn test_with_native_serving_read_scope_cut<R>(
        cut: impl for<'scope> Fn(&NativeServingObservationScope<'scope>) -> Result<()> + 'static,
        action: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        let previous = CUT.with(|slot| slot.replace(Some(Rc::new(cut))));
        let _restore = Restore(previous);
        action()
    }
    pub(crate) fn test_invalidate_serving_read_schema_generation(
        &self,
        scope: &NativeServingObservationScope<'_>,
    ) -> Result<()> {
        scope.require_database_store(self, scope.store)?;
        let before = self.schema_compatibility.generation();
        // The real production invalidation operation, not an assigned expected
        // generation or a fake schema fingerprint. This is no DDL durability test.
        self.schema_compatibility.invalidate();
        if self.schema_compatibility.generation() <= before {
            return Err(invalid("actual schema generation did not advance"));
        }
        Ok(())
    }
    pub(crate) fn test_remove_serving_read_middle_history(
        &self,
        scope: &NativeServingObservationScope<'_>,
    ) -> Result<()> {
        scope.require_database_store(self, scope.store)?;
        let actual = &scope.lineage;
        if actual.range_index() < 2 {
            return Err(invalid(
                "actual middle-history fault needs nonempty R2 or higher",
            ));
        }
        let range = actual.base_range().id;
        let generation = actual.history_generation();
        let middle = self.read_native_history_entry(range, generation, 1)?;
        let head = self.read_native_history_entry(range, generation, actual.range_index())?;
        let key = crate::db::range_image_native::history::history_key(range, generation, 1);
        self.in_paged_transaction(|paged, xid| {
            paged
                .store()
                .delete(xid, &native_key(&key))
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("actual middle-history fault has no native page store"))?;
        if self
            .read_native_range_image_record::<NativeLogicalHistoryEntryV1>(&key)?
            .is_some()
            || self.read_native_history_entry(range, generation, actual.range_index())? != head
            || middle.index != 1
        {
            return Err(invalid(
                "actual middle-history fault did not preserve real head",
            ));
        }
        Ok(())
    }
    /// Actual protected DATA only, usable after intentionally invalidating
    /// the schema verification cache. Never constructs SourceLineage/Ready.
    pub(crate) fn test_serving_read_protected_data_inventory(
        &self,
        range: RangeId,
    ) -> Result<serde_json::Value> {
        let verified_before = self.schema_compatibility.verified_sha256();
        let journal = self.try_native_image_journal()?;
        journal.require_database(self)?;
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("data inventory native birth missing"))?;
        let base = self
            .actual_native_range_integrity_binding(&birth, range)?
            .ok_or_else(|| invalid("data inventory actual native base unknown"))?;
        self.validate_native_range_base_history(&birth, &base)?;
        let prefix = self.read_native_certified_range_prefix(&birth, &base)?;
        let (r, h, b) = match &prefix {
            Some(prefix) => (
                prefix.range_index(),
                prefix.logical_history_sha256(),
                prefix.actual_local_visibility_sequence(),
            ),
            None => (
                base.range_index,
                base.logical_history_sha256.as_str(),
                base.actual_local_visibility_sequence,
            ),
        };
        if r > crate::distribution_range_recovery::MAX_FENCED_IRREVERSIBLE_COMMANDS as u64 {
            return Err(invalid("test protected-data inventory bound exceeded"));
        }
        let mut history = BTreeMap::new();
        for index in 0..=r {
            let entry = self.read_native_history_entry(range, base.history_generation, index)?;
            history.insert(index, entry);
        }
        if history[&r].commitment_sha256 != h {
            return Err(invalid("data inventory actual prefix/history head differs"));
        }
        let namespace_uid = self.local_namespace_identity("items")?;
        let verified_after = self.schema_compatibility.verified_sha256();
        if verified_after != verified_before {
            return Err(invalid(
                "test data inventory changed schema verification state",
            ));
        }
        Ok(serde_json::json!({
            "point": {"R":r,"H":h,"B":b,"UID":self.incarnation_id(),"birth":birth.birth_id,
                "namespace_uid":namespace_uid,"history_generation":base.history_generation},
            "base":base,"prefix":prefix,"history":history,
            "schema_verified":verified_after,
            "authority":"diagnostic DATA only; no source/currentness/role/grant"
        }))
    }
}
