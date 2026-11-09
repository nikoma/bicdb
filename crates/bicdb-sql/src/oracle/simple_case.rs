//! Statement-scoped Oracle case semantics. Native ASCII key tags stay frozen.
use super::*;
use std::{cell::Cell, marker::PhantomData, rc::Rc};

pub(crate) const READY_ID: &str = "oracle_simple_case_v1";
thread_local! {
    static MODE: Cell<Option<(u64,bool)>> = const { Cell::new(None) };
}

/// A stable per-open instance ID prevents same-address or cross-database reuse.
/// Synchronous guards cannot cross threads.
pub(crate) struct Scope {
    previous: Option<(u64, bool)>,
    _thread: PhantomData<Rc<()>>,
}
impl Scope {
    pub(crate) fn for_db(db: &BicDb) -> Result<Self> {
        let id = db.instance_id();
        let previous = MODE.with(Cell::get);
        let oracle = match previous {
            Some((current, oracle)) if current == id => oracle,
            _ if !is_oracle_database(db)? => false,
            _ => {
                let marker = db.get(PROFILE_COLLECTION, READY_ID)?;
                if marker
                    .as_ref()
                    .is_none_or(|record| record.metadata != ready_metadata())
                {
                    return Err(unsupported("Oracle case semantics require completion of ensure_database_profile before SQL execution"));
                }
                true
            }
        };
        MODE.with(|value| value.set(Some((id, oracle))));
        Ok(Self {
            previous,
            _thread: PhantomData,
        })
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        MODE.with(|value| value.set(self.previous));
    }
}
pub(crate) fn enabled() -> bool {
    MODE.with(|value| value.get().is_some_and(|(_, enabled)| enabled))
}
pub(crate) fn ready_metadata() -> JsonValue {
    serde_json::json!({"format":1,"case_table":"oracle_simple_case_v1"})
}
pub(crate) fn upper(text: &str) -> String {
    if enabled() {
        bicdb_core::oracle_simple_upper_v1(text)
    } else {
        bicdb_core::index_key_upper(text)
    }
}
pub(crate) fn lower(text: &str) -> String {
    if enabled() {
        bicdb_core::oracle_simple_lower_v1(text)
    } else {
        bicdb_core::index_key_lower(text)
    }
}
pub(crate) fn upper_field(field: IndexField) -> IndexField {
    if enabled() {
        IndexField::OracleUpperV1(Box::new(field))
    } else {
        IndexField::Upper(Box::new(field))
    }
}
pub(crate) fn lower_field(field: IndexField) -> IndexField {
    if enabled() {
        IndexField::OracleLowerV1(Box::new(field))
    } else {
        IndexField::Lower(Box::new(field))
    }
}
