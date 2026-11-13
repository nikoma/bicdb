//! Opt-in durable physical namespace lineage. This is not a strong-read permit
//! or proof that a particular range image/certified prefix is installed.
use super::*;

pub(super) fn validate_catalog(metas: &[CollectionMeta], active: bool) -> Result<()> {
    let mut seen = BTreeSet::new();
    for meta in metas {
        validate_collection_meta_namespaces(meta)?;
        let Some(id) = meta.local_namespace_id else {
            // A flag-only interrupted activation remains explicitly untrusted.
            continue;
        };
        if !active || id.is_nil() || !seen.insert(id) {
            return Err(BicDbError::FormatCompatibility(format!(
                "collection `{}` has an unfenced, nil, or duplicate local namespace identity",
                meta.name
            )));
        }
    }
    Ok(())
}

impl BicDb {
    /// Explicit trusted migration for this database's local physical namespace
    /// identities. Legacy stores and ordinary opens/creates do not opt in.
    /// Existing identities remain unchanged; retry after an interrupted
    /// flag-only activation fills only missing identities. This operation does
    /// not enable strong reads or certify any range's physical image.
    pub fn activate_local_namespace_identity_v1(&mut self) -> Result<usize> {
        self.ensure_writable("activate local namespace identity")?;
        self.check_catalog_recovery_quiescent()?;
        self.assign_local_namespace_identities()
    }

    /// The same activation for a host whose own live transaction is about to
    /// change the catalog. Only that pending transaction may exist; any other
    /// pending transaction or foreign row lock still refuses, exactly as live
    /// catalog undo does.
    pub fn activate_local_namespace_identity_for_owner_v1(
        &mut self,
        owner: &Transaction,
    ) -> Result<usize> {
        self.ensure_writable("activate local namespace identity")?;
        self.check_catalog_undo_owner(owner)?;
        self.assign_local_namespace_identities()
    }

    fn assign_local_namespace_identities(&mut self) -> Result<usize> {
        validate_catalog(&self.collections(), self.local_namespace_identity_active)?;
        if let Err(error) = format::enable_local_namespace_identity(&self.path) {
            self.fence_durability("namespace_identity.format_fence", &error);
            return Err(error);
        }
        self.local_namespace_identity_active = true;
        let mut assigned = 0;
        for state in self.collections.values_mut() {
            let meta = &mut state.get_mut().meta;
            if meta.local_namespace_id.is_none() {
                meta.local_namespace_id = Some(Uuid::new_v4());
                assigned += 1;
            }
        }
        // Active catalogs force fsync even if ordinary row writes were opened
        // with fsync=false. A publication failure fences this handle.
        self.persist_catalog()?;
        Ok(assigned)
    }

    /// Local format activation only, not replica/image/readiness authority.
    pub fn local_namespace_identity_feature_active(&self) -> bool {
        self.local_namespace_identity_active
    }

    /// None means legacy or incomplete migration and must remain untrusted for
    /// image receipts. Some binds only this local physical namespace; callers
    /// still need independent image, range, schema and fresh authority proof.
    pub fn local_namespace_identity(&self, collection: &str) -> Result<Option<Uuid>> {
        self.check_recovery_state()?;
        let meta = self.collection_state(collection)?.meta.clone();
        validate_catalog(
            std::slice::from_ref(&meta),
            self.local_namespace_identity_active,
        )?;
        Ok(meta.local_namespace_id)
    }

    pub(super) fn fresh_local_namespace_identity(&self) -> Option<Uuid> {
        self.local_namespace_identity_active.then(Uuid::new_v4)
    }

    /// Only for creation of a missing local backing, never metadata refresh of
    /// an existing target. The source's local identity is never adopted.
    pub(super) fn new_local_collection_metadata(&self, meta: &CollectionMeta) -> CollectionMeta {
        let mut local = meta.portable_contract();
        local.local_namespace_id = self.fresh_local_namespace_identity();
        local.native_backing_birth_id = self.fresh_native_collection_birth();
        local.native_backing_origin_root = self.fresh_native_backing_origin_root();
        local
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DistributionConfig, DistributionStore};
    use serde_json::json;

    fn config(mode: StorageMode) -> DbConfig {
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(mode)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
    }
    fn meta(db: &BicDb, name: &str) -> CollectionMeta {
        db.collections()
            .into_iter()
            .find(|m| m.name == name)
            .unwrap()
    }
    fn whole_range(root: &Path) -> RangeDescriptor {
        let cfg = DistributionConfig {
            enabled: true,
            initial_ranges: 1,
            replication_factor: 1,
            ..Default::default()
        };
        DistributionStore::initialize_at(root, cfg, false, 1)
            .unwrap()
            .topology()
            .ranges
            .values()
            .next()
            .unwrap()
            .clone()
    }

    #[test]
    fn namespace_identity_legacy_default_and_reused_storage_stay_untrusted() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        db.create_collection("items").unwrap();
        let before = meta(&db, "items");
        assert_eq!(db.local_namespace_identity("items").unwrap(), None);
        assert!(!db.local_namespace_identity_feature_active());
        assert!(!serde_json::to_value(&before)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("local_namespace_id"));
        assert!(!format::read_or_legacy_metadata(root.path())
            .unwrap()
            .feature_flags
            .contains(format::LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG));
        db.drop_collection("items").unwrap();
        db.create_collection("items").unwrap();
        assert_eq!(before, meta(&db, "items"));
        db.close().unwrap();
        let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        assert_eq!(db.local_namespace_identity("items").unwrap(), None);
        assert_eq!(before, meta(&db, "items"));
    }

    #[test]
    fn namespace_identity_activation_rename_reopen_and_recreation_have_physical_lineage() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let cfg = config(mode);
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            db.create_collection("items").unwrap();
            assert_eq!(db.activate_local_namespace_identity_v1().unwrap(), 1);
            let original = db.local_namespace_identity("items").unwrap().unwrap();
            assert!(!original.is_nil());
            assert_eq!(db.activate_local_namespace_identity_v1().unwrap(), 0);
            db.rename_collection("items", "renamed").unwrap();
            assert_eq!(
                db.local_namespace_identity("renamed").unwrap(),
                Some(original)
            );
            // A same-backing rename compensation retains physical lineage.
            db.rename_collection("renamed", "items").unwrap();
            let before_drop = meta(&db, "items");
            assert_eq!(before_drop.local_namespace_id, Some(original));
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            assert_eq!(
                db.local_namespace_identity("items").unwrap(),
                Some(original)
            );
            db.drop_collection("items").unwrap();
            db.create_collection("items").unwrap();
            let recreated = db.local_namespace_identity("items").unwrap().unwrap();
            assert_ne!(recreated, original);
            // Logical DDL compensation must not resurrect an identity on the
            // newly allocated physical backing, even with the same storage name.
            db.restore_migrated_collection_contract(&before_drop)
                .unwrap();
            assert_eq!(
                db.local_namespace_identity("items").unwrap(),
                Some(recreated)
            );
            assert_eq!(
                meta(&db, "items").portable_contract(),
                before_drop.portable_contract()
            );
            db.close().unwrap();
            let db = BicDb::open_with_config(root.path(), cfg).unwrap();
            assert_eq!(
                db.local_namespace_identity("items").unwrap(),
                Some(recreated)
            );
        }
    }

    #[test]
    fn namespace_identity_interrupted_flag_only_activation_is_explicit_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(StorageMode::EmbeddedMemory);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.create_collection("legacy").unwrap();
        db.close().unwrap();
        // Actual durable cut: compatibility fence exists, UUID catalog has not
        // been published. Open must not invent lineage from names or timestamps.
        format::enable_local_namespace_identity(root.path()).unwrap();
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        assert!(db.local_namespace_identity_feature_active());
        assert_eq!(db.local_namespace_identity("legacy").unwrap(), None);
        db.create_collection("new").unwrap();
        let new = db.local_namespace_identity("new").unwrap().unwrap();
        assert_eq!(db.activate_local_namespace_identity_v1().unwrap(), 1);
        let legacy = db.local_namespace_identity("legacy").unwrap().unwrap();
        assert_ne!(legacy, new);
        assert_eq!(db.activate_local_namespace_identity_v1().unwrap(), 0);
        db.close().unwrap();
        let db = BicDb::open_with_config(root.path(), cfg).unwrap();
        assert_eq!(db.local_namespace_identity("legacy").unwrap(), Some(legacy));
        assert_eq!(db.local_namespace_identity("new").unwrap(), Some(new));
    }

    #[test]
    fn namespace_identity_local_equality_stays_strict_while_schema_is_portable() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let mut source =
            BicDb::open_with_config(a.path(), config(StorageMode::ServerPaged)).unwrap();
        let mut target =
            BicDb::open_with_config(b.path(), config(StorageMode::ServerPaged)).unwrap();
        source.create_collection("items").unwrap();
        target.create_collection("items").unwrap();
        let before = source.schema_compatibility_fingerprint().unwrap();
        source.activate_local_namespace_identity_v1().unwrap();
        target.activate_local_namespace_identity_v1().unwrap();
        let sm = meta(&source, "items");
        let tm = meta(&target, "items");
        assert_ne!(sm.local_namespace_id, tm.local_namespace_id);
        assert_ne!(sm, tm);
        assert_eq!(sm.portable_contract(), tm.portable_contract());
        assert_eq!(source.schema_compatibility_fingerprint().unwrap(), before);
        assert_eq!(
            source.schema_compatibility_fingerprint().unwrap(),
            target.schema_compatibility_fingerprint().unwrap()
        );
        let sb = source.cluster_schema_bundle().unwrap();
        let tb = target.cluster_schema_bundle().unwrap();
        assert_eq!(sb.checksum_sha256, tb.checksum_sha256);
        assert!(sb
            .collections
            .iter()
            .all(|m| m.local_namespace_id.is_none()));
        assert!(tb
            .collections
            .iter()
            .all(|m| m.local_namespace_id.is_none()));
    }

    #[test]
    fn namespace_identity_snapshot_import_owns_missing_and_preserves_existing_target() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let mut source =
            BicDb::open_with_config(a.path(), config(StorageMode::ServerPaged)).unwrap();
        let mut target =
            BicDb::open_with_config(b.path(), config(StorageMode::ServerPaged)).unwrap();
        source.create_collection("items").unwrap();
        source.activate_local_namespace_identity_v1().unwrap();
        target.activate_local_namespace_identity_v1().unwrap();
        let sm = meta(&source, "items");
        let range_root = tempfile::tempdir().unwrap();
        let range = whole_range(range_root.path());
        let record = Record::new("r").with_metadata(json!({"value":1}));
        let bytes = serde_json::to_vec(&record).unwrap().len();
        let batch = RangeSnapshotBatch {
            collection: "items".into(),
            range_id: range.id,
            range_epoch: range.epoch,
            snapshot_commit_sequence: 1,
            required_reader_features: Vec::new(),
            resume_after_key: "r".into(),
            serialized_record_bytes: bytes,
            records: vec![record],
        };
        assert_eq!(
            target
                .apply_range_snapshot_batch(&range, &sm, &batch)
                .unwrap(),
            1
        );
        let local = target.local_namespace_identity("items").unwrap().unwrap();
        assert_ne!(Some(local), sm.local_namespace_id);
        assert_eq!(
            target
                .apply_range_snapshot_batch(&range, &sm, &batch)
                .unwrap(),
            1
        );
        assert_eq!(
            target.local_namespace_identity("items").unwrap(),
            Some(local)
        );
        assert_eq!(
            target.get("items", "r").unwrap().unwrap().metadata,
            json!({"value":1})
        );
        assert_eq!(
            meta(&target, "items").portable_contract(),
            sm.portable_contract()
        );
    }

    #[test]
    fn namespace_identity_commit_frame_import_never_adopts_source_identity() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let cfg = config(StorageMode::ServerPaged);
        let mut source = BicDb::open_with_config(a.path(), cfg.clone()).unwrap();
        let mut target = BicDb::open_with_config(b.path(), cfg.clone()).unwrap();
        source.create_collection("items").unwrap();
        source.activate_local_namespace_identity_v1().unwrap();
        target.activate_local_namespace_identity_v1().unwrap();
        let sm = meta(&source, "items");
        let range_root = tempfile::tempdir().unwrap();
        let range = whole_range(range_root.path());
        let frame = |sequence: u64| {
            let id = format!("r{sequence}");
            CommitFrame::new(
                "default",
                "local",
                "test-stream",
                sequence,
                sequence,
                0,
                vec![ReplicationWrite {
                    collection: "items".into(),
                    record_id: id.clone(),
                    operation: ReplicationOperationType::Upsert,
                    payload: serde_json::to_vec(
                        &Record::new(id).with_metadata(json!({"sequence":sequence})),
                    )
                    .unwrap(),
                    schema_version: 0,
                    collection_meta: Some(sm.clone()),
                }],
            )
        };
        target
            .apply_range_commit_frame_as_local(&range, &frame(1))
            .unwrap();
        let local = target.local_namespace_identity("items").unwrap().unwrap();
        assert_ne!(Some(local), sm.local_namespace_id);
        target
            .apply_range_commit_frame_as_local(&range, &frame(2))
            .unwrap();
        assert_eq!(
            target.local_namespace_identity("items").unwrap(),
            Some(local)
        );
        assert_eq!(
            target.get("items", "r2").unwrap().unwrap().metadata,
            json!({"sequence":2})
        );
        target.close().unwrap();
        let target = BicDb::open_with_config(b.path(), cfg).unwrap();
        assert_eq!(
            target.local_namespace_identity("items").unwrap(),
            Some(local)
        );
    }

    #[test]
    fn namespace_identity_catalog_rejects_unfenced_nil_and_duplicate_identities() {
        let id = Uuid::new_v4();
        let mut a = CollectionMeta::standard("a");
        a.local_namespace_id = Some(id);
        assert!(validate_catalog(&[a.clone()], false).is_err());
        assert!(validate_catalog(&[a.clone()], true).is_ok());
        let mut b = CollectionMeta::standard("b");
        b.local_namespace_id = Some(id);
        assert!(validate_catalog(&[a, b], true).is_err());
        let mut nil = CollectionMeta::standard("nil");
        nil.local_namespace_id = Some(Uuid::nil());
        assert!(validate_catalog(&[nil], true).is_err());
        assert!(validate_catalog(&[CollectionMeta::standard("legacy")], true).is_ok());
    }

    #[test]
    fn namespace_identity_image_receipt_capability_is_separate_and_default_off() {
        let root = tempfile::tempdir().unwrap();
        // Native image records have a page-store reader. Test the independent
        // sticky capability in its supported storage mode; it still creates
        // no native factory, namespace activation or image/read authority.
        let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        db.close().unwrap();
        assert!(!format::range_image_install_active(root.path()).unwrap());
        format::enable_range_image_install(root.path()).unwrap();
        assert!(format::range_image_install_active(root.path()).unwrap());
        let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        assert!(!db.local_namespace_identity_feature_active());
    }

    #[test]
    #[ignore = "requires the explicitly pinned frozen 1.0.463-beta CLI artifact"]
    fn namespace_identity_frozen_old_reader_refuses_local_flags_without_writes() {
        use sha2::{Digest, Sha256};
        use std::process::Command;
        fn hash(path: &Path) -> String {
            let mut file = File::open(path).unwrap();
            let mut digest = Sha256::new();
            let mut bytes = [0; 64 * 1024];
            loop {
                let read = file.read(&mut bytes).unwrap();
                if read == 0 {
                    break;
                }
                digest.update(&bytes[..read]);
            }
            hex::encode(digest.finalize())
        }
        fn files(root: &Path) -> BTreeMap<String, String> {
            fn walk(root: &Path, path: &Path, out: &mut BTreeMap<String, String>) {
                for entry in fs::read_dir(path).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        walk(root, &path, out);
                    } else {
                        out.insert(
                            path.strip_prefix(root)
                                .unwrap()
                                .to_string_lossy()
                                .into_owned(),
                            hash(&path),
                        );
                    }
                }
            }
            let mut result = BTreeMap::new();
            walk(root, root, &mut result);
            result
        }
        let binary = PathBuf::from(
            std::env::var_os("BICDB_OLD_READER_BINARY").expect("explicit frozen old reader path"),
        );
        assert_eq!(
            hash(&binary),
            "840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d"
        );
        let cfg = config(StorageMode::EmbeddedMemory);
        let legacy = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(legacy.path(), cfg.clone()).unwrap();
        db.create_collection("items").unwrap();
        db.close().unwrap();
        let positive = Command::new(&binary)
            .arg("sql")
            .arg(legacy.path())
            .arg("SELECT 1")
            .output()
            .unwrap();
        assert!(
            positive.status.success(),
            "default legacy compatibility: {}",
            String::from_utf8_lossy(&positive.stderr)
        );
        for flag in [
            format::LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG,
            format::RANGE_IMAGE_INSTALL_FEATURE_FLAG,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            db.create_collection("items").unwrap();
            if flag == format::LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG {
                db.activate_local_namespace_identity_v1().unwrap();
            }
            db.close().unwrap();
            if flag == format::RANGE_IMAGE_INSTALL_FEATURE_FLAG {
                format::enable_range_image_install(root.path()).unwrap();
            }
            let before = files(root.path());
            let output = Command::new(&binary)
                .arg("sql")
                .arg(root.path())
                .arg("SELECT 1")
                .output()
                .unwrap();
            assert!(!output.status.success());
            let error = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                error.contains("unknown format feature flag") && error.contains(flag),
                "{error}"
            );
            assert_eq!(
                files(root.path()),
                before,
                "old reader must refuse before changing database bytes"
            );
            println!("old_reader=1.0.463-beta flag={flag} refusal_before_database_writes=passed");
        }
    }
}
