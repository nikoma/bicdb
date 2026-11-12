//! Required-reader capability only. SQL independently proves/publishes its
//! migration certificate; this bit never grants replica/image authority.
use super::*;

impl BicDb {
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
