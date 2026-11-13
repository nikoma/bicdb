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
}
