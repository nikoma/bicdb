//! Write-time local allocation data, retained by the same WAL decision as its
//! writes. Historical replay must never resolve a birth through today's name.
//! These facts alone do not authenticate a peer or authorize imported recovery.
use super::*;

const MAX_BACKINGS: usize = 65_536;
const MAX_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn identities(
    backings: &BTreeMap<String, CollectionMeta>,
) -> Result<BTreeMap<String, (Uuid, Uuid)>> {
    backings
        .iter()
        .map(
            |(name, meta)| match (meta.native_backing_birth_id, meta.local_namespace_id) {
                (Some(birth), Some(namespace)) => Ok((name.clone(), (birth, namespace))),
                _ => Err(BicDbError::FormatCompatibility(
                    "Native WAL backing lacks its allocation/namespace pair".into(),
                )),
            },
        )
        .collect()
}

/// Local WAL is already this database's Native commit history. Rebind only
/// writes carrying its recorded allocation pair, never a current same-name
/// collection. This creates no cross-node origin map or imported authority.
pub(super) fn rebind_recovery_writes(
    recovery: &mut TxRecovery,
    metas: &[CollectionMeta],
) -> Result<()> {
    if recovery.native_backings_by_tx.is_empty() && recovery.native_backing_history.is_empty() {
        return Ok(());
    }
    let mut births = BTreeMap::new();
    let mut namespaces = BTreeMap::new();
    for meta in metas {
        if let Some(namespace) = meta.local_namespace_id {
            namespaces.insert(namespace, meta.native_backing_birth_id);
            if let Some(birth) = meta.native_backing_birth_id {
                births.insert(birth, (namespace, meta.name.as_str()));
            }
        }
    }
    let resolve = |birth: &Uuid, namespace: &Uuid| -> Result<Option<&str>> {
        match births.get(birth) {
            Some((actual_namespace, name)) if actual_namespace == namespace => Ok(Some(*name)),
            Some(_) => Err(BicDbError::FormatCompatibility(
                "Native WAL birth has a changed local namespace identity".into(),
            )),
            None if namespaces.contains_key(namespace) => Err(BicDbError::FormatCompatibility(
                "Native WAL namespace has a changed local backing birth".into(),
            )),
            None => Ok(None), // Both identities absent: that backing was dropped.
        }
    };
    // Completed paged materialization can discard its row bodies and per-tx
    // bindings, but must not discard the surviving immutable pair's checks.
    for (birth, namespace) in &recovery.native_backing_history {
        resolve(birth, namespace)?;
    }
    for (tx_id, backings) in &recovery.native_backings_by_tx {
        if recovery.states.get(tx_id) != Some(&TxState::Committed) {
            continue;
        }
        let mut destinations = BTreeMap::new();
        for (name, (birth, namespace)) in backings {
            let destination = resolve(birth, namespace)?;
            destinations.insert(name.as_str(), destination);
        }
        if let Some(writes) = recovery.writes.get_mut(tx_id) {
            writes.retain_mut(|write| {
                let Some(destination) = destinations.get(write.collection()) else {
                    return true; // Legacy/unborn writes acquire no new identity.
                };
                let Some(destination) = destination else {
                    return false;
                };
                if write.collection() == *destination {
                    return true;
                }
                match write {
                    RecoveredTxWrite::Full(write) => write.collection = (*destination).to_owned(),
                    RecoveredTxWrite::Patch(patch) => patch.collection = (*destination).to_owned(),
                }
                true
            });
        }
    }
    Ok(())
}

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
    /// Explicitly fence writers that know collection births but do not retain
    /// their historical write-time contracts. This never retrofits older WAL
    /// and grants neither peer authentication nor imported recovery authority.
    pub fn activate_native_wal_backing_capture_v1(&mut self) -> Result<()> {
        self.ensure_writable("activate Native WAL backing capture")?;
        self.check_catalog_recovery_quiescent()?;
        if !self.native_collection_birth_active {
            return Err(BicDbError::InvalidConfig(
                "Native WAL backing capture requires explicit collection birth activation".into(),
            ));
        }
        if let Err(error) = format::enable_native_wal_backing_capture(&self.path) {
            self.fence_durability("native_wal_backing_capture.format_fence", &error);
            return Err(error);
        }
        self.native_wal_backing_capture_active = true;
        Ok(())
    }

    /// Called once while encoding the Native commit decision, after writes
    /// have their actual backing and while catalog structure is pinned. WAL
    /// preparation contains only row frames, so it cannot cache stale births.
    pub(super) fn capture_native_wal_backings(
        &self,
        writes: &[TxWrite],
    ) -> Result<BTreeMap<String, CollectionMeta>> {
        self.observe_native_origin_root()?;
        if !self.native_wal_backing_capture_active {
            return Ok(BTreeMap::new());
        }
        let grouped = tx_writes_by_collection(writes);
        let mut backings = BTreeMap::new();
        for (name, writes) in grouped {
            let state = self.collection_state(name)?;
            if state.meta.native_backing_birth_id.is_some() {
                let mut meta = state.meta.clone();
                // A first vector fixes its dimension in this same commit.
                // Capture the validated result before apply changes the live
                // catalog, without parsing stored row metadata.
                for write in writes {
                    if matches!(write.op, TxWriteOp::Upsert) {
                        validate_write_record(name, write, &mut meta.vector_dim)?;
                    }
                }
                backings.insert(name.to_owned(), meta);
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
        allocations: BTreeMap<String, CollectionMeta>,
    ) -> Result<CommitFrame> {
        let backings = validated(backings)?;
        let allocations = native_catalog_allocations::validated(allocations)?;
        native_origin::validate_catalog(
            &allocations.values().cloned().collect::<Vec<_>>(),
            self.native_origin.as_ref(),
        )?;
        native_origin::validate_catalog(
            &backings.values().cloned().collect::<Vec<_>>(),
            self.native_origin.as_ref(),
        )?;
        if (backings.is_empty() && allocations.is_empty()) || !self.config.fsync {
            return Ok(frame);
        }
        if !self.native_wal_backing_capture_active {
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
        Ok(frame.with_native_wal_capture(
            &self.incarnation,
            backings,
            self.observe_native_origin_root()?,
            allocations,
        ))
    }
}
