//! Actual creation publication. A durable pending marker is installed only at
//! allocation, never inferred for an existing backing from its current name.
use super::*;

pub(super) struct NativeCatalogAllocation {
    source_open: String,
    incarnation: Arc<()>,
    meta: CollectionMeta,
}

impl NativeCatalogAllocation {
    pub(super) fn stage(self, tx: &mut Transaction) -> Result<()> {
        let db = tx.db_ref();
        let state = db.collection_state(&self.meta.name)?;
        if db.incarnation != self.source_open
            || !Arc::ptr_eq(&state.incarnation, &self.incarnation)
            || !state.meta.native_catalog_allocation_pending
            || state.meta.native_backing_birth_id != self.meta.native_backing_birth_id
            || state.meta.local_namespace_id != self.meta.local_namespace_id
            || state.meta.native_backing_origin_root != self.meta.native_backing_origin_root
        {
            return Err(BicDbError::Replication(
                "Native allocation publication lost its actual backing".into(),
            ));
        }
        drop(state);
        tx.native_catalog_allocations
            .insert(self.meta.name.clone(), self.meta);
        Ok(())
    }
}

pub(super) fn validate_catalog(metas: &[CollectionMeta], active: bool) -> Result<()> {
    if metas.iter().any(|m| {
        m.native_catalog_allocation_pending
            && (!active
                || m.native_backing_origin_root.is_none()
                || m.native_backing_birth_id.is_none())
    }) {
        return Err(BicDbError::FormatCompatibility(
            "Native allocation pending marker is unfenced or unowned".into(),
        ));
    }
    Ok(())
}

pub(super) fn validated(
    facts: BTreeMap<String, CollectionMeta>,
) -> Result<BTreeMap<String, CollectionMeta>> {
    let facts = native_wal_backings::validated(facts)?;
    if facts
        .values()
        .any(|m| m.native_catalog_allocation_pending || m.native_backing_origin_root.is_none())
    {
        return Err(BicDbError::FormatCompatibility(
            "Native allocation fact has no completed origin-scoped birth".into(),
        ));
    }
    Ok(facts)
}

impl BicDb {
    pub(super) fn validate_native_catalog_allocation_commit(
        &self,
        writes: &[TxWrite],
        facts: &BTreeMap<String, CollectionMeta>,
    ) -> Result<()> {
        if facts.is_empty() {
            return Ok(());
        }
        if !self.native_catalog_allocation_facts_active || !self.config.fsync {
            return Err(BicDbError::FormatCompatibility(
                "Native allocation facts require fsync and their publisher fence".into(),
            ));
        }
        validated(facts.clone())?;
        native_origin::validate_catalog(
            &facts.values().cloned().collect::<Vec<_>>(),
            self.native_origin.as_ref(),
        )?;
        for (name, fact) in facts {
            let state = self.collection_state(name)?;
            let mut actual = state.meta.clone();
            actual.native_catalog_allocation_pending = false;
            if !state.meta.native_catalog_allocation_pending || actual != *fact {
                return Err(BicDbError::Replication(
                    "Native allocation fact does not own the pending backing".into(),
                ));
            }
            let mut found = false;
            for write in writes {
                if write.collection == collection_rename::CATALOG_LOG_COLLECTION
                    && matches!(write.op, TxWriteOp::Upsert)
                {
                    let record = write.record()?.ok_or_else(|| {
                        BicDbError::Replication("Native creation record is missing".into())
                    })?;
                    if let collection_rename::CatalogOp::Create {
                        name: created,
                        mode,
                    } = serde_json::from_value(record.metadata.clone())?
                    {
                        found |= created == *name && mode == fact.mode;
                    }
                }
            }
            if !found {
                return Err(BicDbError::Replication(
                    "Native allocation fact has no actual catalog-create write".into(),
                ));
            }
        }
        Ok(())
    }

    /// Explicit trusted-host activation for future genuine creations. Earlier
    /// collections/WAL never acquire creation facts from a later activation.
    pub fn activate_native_catalog_allocation_facts_v1(&mut self) -> Result<()> {
        self.ensure_writable("activate Native catalog allocation facts")?;
        self.check_catalog_recovery_quiescent()?;
        if !self.config.fsync
            || !self.native_wal_backing_capture_active
            || self.observe_native_origin_root()?.is_none()
        {
            return Err(BicDbError::InvalidConfig(
                "Native allocation facts require fsync, actual origin and WAL capture".into(),
            ));
        }
        if let Err(error) = format::enable_native_catalog_allocation_facts(&self.path) {
            self.fence_durability("native_catalog_allocation.format_fence", &error);
            return Err(error);
        }
        self.native_catalog_allocation_facts_active = true;
        Ok(())
    }

    pub(super) fn check_native_catalog_allocation_publication(&self) -> Result<()> {
        if self.native_catalog_allocation_logging_active() && !self.config.fsync {
            return Err(BicDbError::InvalidConfig(
                "Native catalog allocation publication requires fsync-enabled storage".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn native_catalog_allocation_logging_active(&self) -> bool {
        self.native_catalog_allocation_facts_active
            && self.config.replication.enabled
            && self.ha_node.role() == NodeRole::Primary
    }

    pub(super) fn pending_native_catalog_allocation(
        &self,
        name: &str,
    ) -> Result<Option<NativeCatalogAllocation>> {
        let state = self.collection_state(name)?;
        if !state.meta.native_catalog_allocation_pending {
            return Ok(None);
        }
        self.check_native_catalog_allocation_publication()?;
        if !self.native_catalog_allocation_logging_active() {
            return Err(BicDbError::Replication(
                "pending Native creation requires its primary publisher".into(),
            ));
        }
        let mut meta = state.meta.clone();
        meta.native_catalog_allocation_pending = false;
        native_origin::validate_catalog(std::slice::from_ref(&meta), self.native_origin.as_ref())?;
        Ok(Some(NativeCatalogAllocation {
            source_open: self.incarnation.clone(),
            incarnation: Arc::clone(&state.incarnation),
            meta,
        }))
    }

    pub(super) fn clear_native_catalog_allocation_pending(&mut self, name: &str) -> Result<()> {
        if !self
            .collection_state(name)?
            .meta
            .native_catalog_allocation_pending
        {
            return Ok(());
        }
        self.collection_state_mut(name)?
            .meta
            .native_catalog_allocation_pending = false;
        self.persist_catalog()?;
        Ok(())
    }

    pub(super) fn finish_native_catalog_allocation(&mut self, name: &str) -> Result<()> {
        let Some(allocation) = self.pending_native_catalog_allocation(name)? else {
            return Ok(());
        };
        let _database_mutation = self.range_mutations.try_enter_database()?;
        self.invalidate_native_range_images_before_mutation(None, "retry collection creation")?;
        let mode = allocation.meta.mode.clone();
        self.log_catalog_op_with_allocation(
            collection_rename::CatalogOp::Create {
                name: name.into(),
                mode,
            },
            Some(allocation),
        )?;
        self.clear_native_catalog_allocation_pending(name)
    }
}

#[cfg(test)]
mod tests;
