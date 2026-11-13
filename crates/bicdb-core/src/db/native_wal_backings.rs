//! Write-time local allocation data, retained by the same WAL decision as its
//! writes. Historical replay must never resolve a birth through today's name.
//! These facts alone do not authenticate a peer or authorize imported recovery.
use super::*;

const MAX_BACKINGS: usize = 65_536;
const MAX_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn validated(
    backings: BTreeMap<String, CollectionMeta>,
) -> Result<BTreeMap<String, CollectionMeta>> {
    if backings.is_empty() {
        return Ok(backings);
    }
    if backings.len() > MAX_BACKINGS {
        return Err(BicDbError::FormatCompatibility(
            "Native WAL backing inventory exceeds its collection bound".into(),
        ));
    }
    for (name, meta) in &backings {
        if name != &meta.name || meta.native_backing_birth_id.is_none() {
            return Err(BicDbError::FormatCompatibility(
                "Native WAL backing inventory has a mismatched or unborn collection".into(),
            ));
        }
    }
    let metas = backings.values().cloned().collect::<Vec<_>>();
    local_namespace_identity::validate_catalog(&metas, true)?;
    native_collection_birth::validate_catalog(&metas, true, true)?;
    crate::distribution_range_image::bounded_json(&backings, MAX_BYTES)?;
    Ok(backings)
}

impl BicDb {
    /// Called once while encoding the Native commit decision, after writes
    /// have their actual backing and while catalog structure is pinned. WAL
    /// preparation contains only row frames, so it cannot cache stale births.
    pub(super) fn capture_native_wal_backings(
        &self,
        writes: &[TxWrite],
    ) -> Result<BTreeMap<String, CollectionMeta>> {
        if !self.native_collection_birth_active {
            return Ok(BTreeMap::new());
        }
        let names = writes
            .iter()
            .map(|w| &w.collection)
            .collect::<BTreeSet<_>>();
        let mut backings = BTreeMap::new();
        for name in names {
            let state = self.collection_state(name)?;
            if state.meta.native_backing_birth_id.is_some() {
                backings.insert(name.clone(), state.meta.clone());
            }
        }
        validated(backings)
    }

    /// Only actual Native export paths call this after bounding WAL by the
    /// durable/applied prefix and checking its captured allocation inventory.
    pub(super) fn capture_native_wal_export(
        &self,
        frame: CommitFrame,
        backings: BTreeMap<String, CollectionMeta>,
    ) -> Result<CommitFrame> {
        let backings = validated(backings)?;
        if backings.is_empty() || !self.config.fsync {
            return Ok(frame);
        }
        if !self.native_collection_birth_active {
            return Err(BicDbError::FormatCompatibility(
                "Native WAL allocation data has no local format fence".into(),
            ));
        }
        for name in backings.keys() {
            if !frame.writes.iter().any(|write| &write.collection == name) {
                return Err(BicDbError::Corruption {
                    path: self.tx_log_path(),
                    message: "Native commit backing has no corresponding retained write".into(),
                });
            }
        }
        Ok(frame.with_native_wal_capture(&self.incarnation, backings))
    }
}
