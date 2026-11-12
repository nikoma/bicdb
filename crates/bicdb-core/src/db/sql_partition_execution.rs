//! Required-reader capability only. SQL independently proves/publishes its
//! migration certificate; this bit never grants replica/image authority.
use super::*;

/// Process-local cache invalidation only. This cannot be serialized, minted by
/// a caller, or used as a Native authorization or portable origin receipt.
#[derive(Clone, Debug)]
pub struct CollectionCacheStamp {
    instance: u64,
    incarnation: Arc<()>,
    generation: u64,
}

impl PartialEq for CollectionCacheStamp {
    fn eq(&self, other: &Self) -> bool {
        self.same_incarnation(other) && self.generation == other.generation
    }
}
impl Eq for CollectionCacheStamp {}
impl CollectionCacheStamp {
    pub fn same_incarnation(&self, other: &Self) -> bool {
        self.instance == other.instance && Arc::ptr_eq(&self.incarnation, &other.incarnation)
    }
}

impl BicDb {
    /// Cheap metadata stamp. Recreating a collection cannot inherit an old
    /// stamp even when its name and generation counter are identical.
    pub fn collection_cache_stamp(&self, collection: &str) -> Result<Option<CollectionCacheStamp>> {
        self.check_recovery_state()?;
        Ok(self.collections.get(collection).map(|state| {
            let state = state.read();
            CollectionCacheStamp {
                instance: self.instance_id(),
                incarnation: Arc::clone(&state.incarnation),
                generation: state.generation(),
            }
        }))
    }

    /// Exact recent-change coverage for a prior cache stamp. A replacement,
    /// untracked generation bump or expired window requires a complete scan.
    /// Row contents still require the ordinary Native read APIs and authority.
    pub fn collection_cache_changes_since(
        &self,
        collection: &str,
        previous: &CollectionCacheStamp,
    ) -> Result<Option<Vec<ChangedRecord>>> {
        self.check_recovery_state()?;
        let Some(state) = self.collections.get(collection) else {
            return Ok(None);
        };
        let state = state.read();
        let current = state.generation();
        if previous.instance != self.instance_id()
            || !Arc::ptr_eq(&previous.incarnation, &state.incarnation)
            || previous.generation > current
        {
            return Ok(None);
        }
        if previous.generation == current {
            return Ok(Some(Vec::new()));
        }
        let changes = state.recent_changes.lock();
        let mut next = previous.generation.checked_add(1);
        let mut rows = Vec::new();
        for (at, changed) in changes
            .entries
            .iter()
            .filter(|(at, _)| *at > previous.generation)
        {
            if next != Some(*at) {
                return Ok(None);
            }
            rows.extend(changed.iter().cloned());
            next = at.checked_add(1);
        }
        Ok((next == current.checked_add(1)).then_some(rows))
    }

    /// Persist the sticky fence before SQL publishes new partition semantics.
    /// This does not certify a legacy SQL catalog or migrate any records.
    pub fn enable_sql_partition_execution_v1(&mut self) -> Result<()> {
        self.ensure_writable("enable SQL partition execution semantics")?;
        if let Err(error) = format::enable_sql_partition_execution_v1(&self.path, &self.durability)
        {
            self.fence_durability("sql_partition.format_fence", &error);
            return Err(error);
        }
        self.check_recovery_state()?;
        self.sql_partition_execution_v1_active = true;
        Ok(())
    }

    /// Local format capability, not a completed SQL migration certificate.
    pub fn sql_partition_execution_feature_active(&self) -> bool {
        self.sql_partition_execution_v1_active
    }

    /// Legacy migration changes rows and their live indexes together. A host
    /// must supply durable indexed storage and exclusive quiescent access.
    pub fn check_sql_partition_upgrade_quiescent(&self) -> Result<()> {
        self.check_catalog_recovery_quiescent()?;
        if !self.config.fsync || !self.config.load_secondary_indexes {
            return Err(BicDbError::FormatCompatibility(
                "SQL partition upgrade requires fsync and secondary indexes enabled".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(mode: StorageMode) -> DbConfig {
        DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
    }

    #[test]
    fn cache_stamps_pin_runtime_incarnations_and_require_complete_change_coverage() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
            db.create_collection("catalog").unwrap();
            db.insert("catalog", Record::new("one")).unwrap();
            let first = db.collection_cache_stamp("catalog").unwrap().unwrap();
            db.insert("catalog", Record::new("two")).unwrap();
            let changes = db
                .collection_cache_changes_since("catalog", &first)
                .unwrap()
                .unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].record_id, "two");
            let second = db.collection_cache_stamp("catalog").unwrap().unwrap();
            assert!(first.same_incarnation(&second));
            assert_ne!(first, second);
            db.collection_state("catalog").unwrap().bump_generation();
            db.insert("catalog", Record::new("three")).unwrap();
            assert!(db
                .collection_cache_changes_since("catalog", &second)
                .unwrap()
                .is_none());
            db.drop_collection("catalog").unwrap();
            db.create_collection("catalog").unwrap();
            db.insert("catalog", Record::new("replacement")).unwrap();
            let replacement = db.collection_cache_stamp("catalog").unwrap().unwrap();
            assert_eq!(first.generation, replacement.generation);
            assert!(!first.same_incarnation(&replacement));
            assert!(db
                .collection_cache_changes_since("catalog", &first)
                .unwrap()
                .is_none());
            db.close().unwrap();
        }
    }

    #[test]
    fn sql_partition_reader_fence_is_explicit_sticky_and_retains_native_acks() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let cfg = config(mode);
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            db.create_collection("ack").unwrap();
            db.insert(
                "ack",
                Record::new("one").with_metadata(serde_json::json!({"value":"original-ack"})),
            )
            .unwrap();
            assert!(!db.sql_partition_execution_feature_active());
            db.check_sql_partition_upgrade_quiescent().unwrap();
            db.enable_sql_partition_execution_v1().unwrap();
            assert!(db.sql_partition_execution_feature_active());
            let meta = format::read_or_legacy_metadata(root.path()).unwrap();
            assert!(meta
                .feature_flags
                .contains(format::SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG));
            assert!(!meta
                .feature_flags
                .contains(format::LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG));
            db.close().unwrap();
            for _ in 0..2 {
                let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
                assert!(db.sql_partition_execution_feature_active());
                assert_eq!(
                    db.get("ack", "one").unwrap().unwrap().metadata["value"],
                    serde_json::json!("original-ack")
                );
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn sql_partition_fence_io_failures_permanently_fence_only_that_handle() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            for step in ["write", "sync", "rename", "dir_sync"] {
                let root = tempfile::tempdir().unwrap();
                let cfg = config(mode.clone());
                let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
                db.create_collection("ack").unwrap();
                db.insert("ack", Record::new("one")).unwrap();
                let fault = bicdb_page::test_faults::arm_io_failure(
                    &format!("sql_partition.format_fence.{step}"),
                    root.path(),
                    1,
                    false,
                );
                assert!(
                    matches!(
                        db.enable_sql_partition_execution_v1(),
                        Err(BicDbError::WriteFailed { .. })
                    ),
                    "{step}"
                );
                drop(fault);
                assert!(
                    matches!(
                        db.insert("ack", Record::new("unsafe")),
                        Err(BicDbError::WriteFailed { .. })
                    ),
                    "{step}"
                );
                assert!(
                    matches!(
                        db.enable_sql_partition_execution_v1(),
                        Err(BicDbError::WriteFailed { .. })
                    ),
                    "{step}"
                );
                drop(db);
                let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
                assert!(db.get("ack", "one").unwrap().is_some());
                assert!(db.get("ack", "unsafe").unwrap().is_none());
                db.enable_sql_partition_execution_v1().unwrap();
                db.close().unwrap();
                let db = BicDb::open_with_config(root.path(), cfg).unwrap();
                assert!(db.sql_partition_execution_feature_active());
                assert!(db.get("ack", "one").unwrap().is_some());
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn sql_partition_visible_fence_reactivation_really_syncs_again() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let cfg = config(mode);
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            db.create_collection("ack").unwrap();
            db.insert("ack", Record::new("one")).unwrap();
            db.enable_sql_partition_execution_v1().unwrap();
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            assert!(db.sql_partition_execution_feature_active());
            let fault = bicdb_page::test_faults::arm_io_failure(
                "sql_partition.format_fence.sync",
                root.path(),
                1,
                false,
            );
            assert!(matches!(
                db.enable_sql_partition_execution_v1(),
                Err(BicDbError::WriteFailed { .. })
            ));
            drop(fault);
            assert!(matches!(
                db.insert("ack", Record::new("unsafe")),
                Err(BicDbError::WriteFailed { .. })
            ));
            drop(db);
            let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
            assert!(db.get("ack", "one").unwrap().is_some());
            assert!(db.get("ack", "unsafe").unwrap().is_none());
            db.enable_sql_partition_execution_v1().unwrap();
            db.close().unwrap();
        }
    }

    #[test]
    fn sql_partition_legacy_repair_requires_durable_indexes_and_no_pending_owner() {
        for no_fsync in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let mut cfg = config(StorageMode::EmbeddedMemory);
            if no_fsync {
                cfg.fsync = false;
            } else {
                cfg.load_secondary_indexes = false;
            }
            let db = BicDb::open_with_config(root.path(), cfg).unwrap();
            assert!(matches!(
                db.check_sql_partition_upgrade_quiescent(),
                Err(BicDbError::FormatCompatibility(_))
            ));
            assert!(!db.sql_partition_execution_feature_active());
            db.close().unwrap();
        }
        let root = tempfile::tempdir().unwrap();
        let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        let tx = db.begin_transaction().unwrap();
        assert!(db.check_sql_partition_upgrade_quiescent().is_err());
        drop(tx);
        db.check_sql_partition_upgrade_quiescent().unwrap();
        db.close().unwrap();
    }
}
