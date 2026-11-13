//! Test-only native-journal custody. No observation, mutation or grant API.
use super::*;

pub(crate) struct NativeTestCustodyJournalGuard<'a> {
    _actual: range_image_native::NativeImageJournalGuard<'a>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl BicDb {
    /// Hold only this actual DB's existing nonwaiting journal lock. The opaque
    /// !Send guard exposes no underlying journal, predicate or native accessor.
    pub(crate) fn test_native_custody_journal(&self) -> Result<NativeTestCustodyJournalGuard<'_>> {
        Ok(NativeTestCustodyJournalGuard {
            _actual: self.try_native_image_journal()?,
            _local: std::marker::PhantomData,
        })
    }
}
