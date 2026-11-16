//! Actual callback control only. No supplied command/token is constructed,
//! changed, exported or replayed. The private real pending owner is borrowed
//! only at its existing successful validation cut.
use super::*;
use parking_lot::{Mutex, RwLock};
use std::sync::{Arc, OnceLock, Weak};
struct Entry {
    foreign: Weak<RwLock<BicDb>>,
    data: Arc<Mutex<serde_json::Value>>,
}
fn registry() -> &'static Mutex<BTreeMap<String, Entry>> {
    static VALUE: OnceLock<Mutex<BTreeMap<String, Entry>>> = OnceLock::new();
    VALUE.get_or_init(Default::default)
}
struct Guard {
    key: String,
    data: Arc<Mutex<serde_json::Value>>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut entries = registry().lock();
        if entries
            .get(&self.key)
            .is_some_and(|e| Arc::ptr_eq(&e.data, &self.data))
        {
            entries.remove(&self.key);
        }
    }
}
impl BicDb {
    pub(crate) fn test_arm_native_applying_guard_probe(
        &self,
        foreign: &Arc<RwLock<BicDb>>,
    ) -> Result<(
        impl Drop + Send + 'static,
        impl Fn() -> serde_json::Value + Send + Sync + 'static,
    )> {
        let other = foreign
            .try_read()
            .ok_or_else(|| invalid("applying guard control foreign DB busy"))?;
        if other.incarnation_id() == self.incarnation_id() {
            return Err(invalid(
                "applying guard control requires actual foreign open",
            ));
        }
        drop(other);
        let mut entries = registry().lock();
        if entries.len() >= 16 || entries.contains_key(self.incarnation_id()) {
            return Err(invalid(
                "applying guard control capacity/original registration",
            ));
        }
        let data = Arc::new(Mutex::new(
            serde_json::json!({"actual_callbacks":0,"facts":[],"omitted":0}),
        ));
        entries.insert(
            self.incarnation_id().into(),
            Entry {
                foreign: Arc::downgrade(foreign),
                data: data.clone(),
            },
        );
        let guard = Guard {
            key: self.incarnation_id().into(),
            data: data.clone(),
        };
        Ok((guard, move || data.lock().clone()))
    }
}
pub(super) fn at_actual_apply(
    db: &BicDb,
    own: &NativeRangePendingApply,
    lineage: &SourceRangeImageLineage,
) {
    let binding = registry()
        .lock()
        .get(db.incarnation_id())
        .map(|e| (e.foreign.clone(), e.data.clone()));
    let Some((foreign, data)) = binding else {
        return;
    };
    // The registry guard is dropped before these reads. The caller retains
    // its existing commit guards; this probe acquires no gate/publication
    // permit. The foreign DB is only try-borrowed and refuses at open identity.
    let strict = db.current_native_range_source_lineage(own.range_id());
    let foreign = foreign.upgrade();
    let other = foreign.as_ref().and_then(|f| f.try_read());
    let wrong = other
        .as_ref()
        .map(|other| other.native_range_source_lineage_applying(own));
    let fact = serde_json::json!({"original_actual_open":db.incarnation_id(),
        "source_index":lineage.range_index(),"pending_command_sha256":own.pending.command_checksum_sha256,
        "strict_returned_ok":strict.is_ok(),"strict_error":strict.as_ref().err().map(ToString::to_string),
        "actual_foreign_open":other.as_ref().map(|db|db.incarnation_id()),
        "foreign_returned_ok":wrong.as_ref().map(|r|r.is_ok()),
        "foreign_error":wrong.as_ref().and_then(|r|r.as_ref().err()).map(ToString::to_string),
        "native_effect_or_grant_created":false});
    let mut data = data.lock();
    let count = data["actual_callbacks"].as_u64().unwrap_or(0);
    data["actual_callbacks"] = count.saturating_add(1).into();
    if data["facts"].as_array().unwrap().len() < 8 {
        data["facts"].as_array_mut().unwrap().push(fact);
    } else {
        let n = data["omitted"].as_u64().unwrap_or(0);
        data["omitted"] = n.saturating_add(1).into();
    }
}
