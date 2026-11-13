//! Per-open/native lineage pins for a frozen range image. These values are
//! deliberately not a range authority and cannot be deserialized into one.
use super::*;

const MAX_IMAGE_NAMESPACES: usize = 65_536;
const MAX_IMAGE_CATALOG_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct NamespacePin {
    name: String,
    local_id: Uuid,
    backing_birth_id: Option<Uuid>,
    origin_root: Option<Uuid>,
    incarnation: Arc<()>,
}

/// Owned live pins keep the old collection identity alive through a drop;
/// pointer comparison detects that a same-name replacement is a new backing.
#[derive(Debug)]
pub(crate) struct RangeImageDbBindings {
    database_incarnation: String,
    schema_generation: u64,
    schema_sha256: String,
    reader_features: Vec<String>,
    namespaces: Vec<NamespacePin>,
    portable_collections: Vec<CollectionMeta>,
    native_origin: Option<NativeOriginObservation>,
}

impl RangeImageDbBindings {
    pub(crate) fn database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        &self.schema_sha256
    }
    pub(crate) fn reader_features(&self) -> &[String] {
        &self.reader_features
    }
    pub(crate) fn collections(&self) -> &[CollectionMeta] {
        &self.portable_collections
    }
    pub(crate) fn native_origin(&self) -> Option<&NativeOriginObservation> {
        self.native_origin.as_ref()
    }
    pub(crate) fn namespace_ids(&self) -> Vec<(String, Uuid)> {
        self.namespaces
            .iter()
            .map(|pin| (pin.name.clone(), pin.local_id))
            .collect()
    }
}

impl BicDb {
    /// Must run under the caller's exclusive range gate, after permanent
    /// distributed fencing. Neither a local UUID nor the format flag grants
    /// authority by itself. Never activate a feature implicitly on this path.
    pub(crate) fn capture_range_image_bindings(&self) -> Result<RangeImageDbBindings> {
        self.check_recovery_state()?;
        if !self.config.fsync || self.paged_records.is_none() {
            return Err(BicDbError::Cluster(
                "trusted range images require fsync-enabled server_paged storage".into(),
            ));
        }
        if !format::range_image_install_active(&self.path)? {
            return Err(BicDbError::Cluster(
                "range image protection has not been explicitly activated".into(),
            ));
        }
        let generation = self.schema_compatibility.generation();
        let schema_sha256 = self.schema_compatibility.verified_sha256().ok_or_else(|| {
            BicDbError::Cluster("range image schema has no current verified digest".into())
        })?;
        let reader_features = format::required_reader_features(&self.path)?;
        let native_origin = self.observe_native_origin_root()?;
        if self.collections.len() > MAX_IMAGE_NAMESPACES {
            return Err(BicDbError::Cluster(
                "range image namespace bound exceeded".into(),
            ));
        }
        let mut names = self.collections.keys().cloned().collect::<Vec<_>>();
        names.sort();
        let mut namespaces = Vec::with_capacity(names.len());
        let mut portable_collections = Vec::with_capacity(names.len());
        let mut catalog_bytes = 0usize;
        for name in names {
            let local_id = self.local_namespace_identity(&name)?.ok_or_else(|| {
                BicDbError::Cluster(format!(
                    "range image namespace `{name}` has no trusted durable local identity"
                ))
            })?;
            let state = self.collection_state(&name)?;
            if !state.paged_lazy {
                return Err(BicDbError::Cluster(format!(
                    "range image namespace `{name}` is not server_paged"
                )));
            }
            // Reject before cloning an oversized policy/catalog subtree. The
            // local UUID overhead is conservatively counted in this bound.
            let encoded = crate::distribution_range_image::bounded_json(
                &state.meta,
                MAX_IMAGE_CATALOG_BYTES.saturating_sub(catalog_bytes),
            )?;
            catalog_bytes = catalog_bytes
                .checked_add(encoded.len())
                .ok_or_else(|| BicDbError::Cluster("range image catalog size overflow".into()))?;
            namespaces.push(NamespacePin {
                name,
                local_id,
                backing_birth_id: state.meta.native_backing_birth_id,
                origin_root: state.meta.native_backing_origin_root,
                incarnation: Arc::clone(&state.incarnation),
            });
            portable_collections.push(state.meta.portable_contract());
        }
        let bindings = RangeImageDbBindings {
            database_incarnation: self.incarnation.clone(),
            schema_generation: generation,
            schema_sha256,
            reader_features,
            namespaces,
            portable_collections,
            native_origin,
        };
        self.validate_range_image_bindings(&bindings)?;
        Ok(bindings)
    }

    /// Revalidate before and after every copied step and before a native seal.
    /// The generation check rejects A -> B -> A schema changes; a digest alone
    /// would not identify that invalid capture interval.
    pub(crate) fn validate_range_image_bindings(
        &self,
        expected: &RangeImageDbBindings,
    ) -> Result<()> {
        self.check_recovery_state()?;
        if !self.config.fsync
            || self.paged_records.is_none()
            || self.incarnation != expected.database_incarnation
            || !format::range_image_install_active(&self.path)?
            || self.schema_compatibility.generation() != expected.schema_generation
            || self.schema_compatibility.verified_sha256().as_deref()
                != Some(expected.schema_sha256.as_str())
            || format::required_reader_features(&self.path)? != expected.reader_features
            || self.collections.len() != expected.namespaces.len()
            || self.observe_native_origin_root()? != expected.native_origin
        {
            return Err(BicDbError::Cluster(
                "range image database/schema/reader identity changed; discard this capture".into(),
            ));
        }
        for pin in &expected.namespaces {
            let state = self.collection_state(&pin.name)?;
            if !Arc::ptr_eq(&state.incarnation, &pin.incarnation)
                || self.local_namespace_identity(&pin.name)? != Some(pin.local_id)
                || state.meta.native_backing_birth_id != pin.backing_birth_id
                || state.meta.native_backing_origin_root != pin.origin_root
            {
                return Err(BicDbError::Cluster(format!(
                    "range image namespace `{}` changed physical lineage; discard this capture",
                    pin.name
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_range_mutation::RangeMutationAuthority;
    use crate::{DistributionConfig, DistributionStore};

    fn config() -> DbConfig {
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
    }
    fn range(root: &Path) -> RangeDescriptor {
        DistributionStore::initialize_at(
            root,
            DistributionConfig {
                enabled: true,
                initial_ranges: 1,
                replication_factor: 1,
                ..Default::default()
            },
            false,
            1,
        )
        .unwrap()
        .topology()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone()
    }
    fn populated(root: &Path) -> BicDb {
        let mut db = BicDb::open_with_config(root, config()).unwrap();
        db.create_collection("items").unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        db.batch_insert("items", (0..23).map(|i| Record::new(format!("row-{i:04}"))))
            .unwrap();
        drop(db);
        let db = BicDb::open_with_config(root, config()).unwrap();
        format::enable_range_image_install(root).unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        db
    }

    #[test]
    fn range_image_scan_budget_advances_over_nonmatching_keys_and_preserves_all_rows() {
        let root = tempfile::tempdir().unwrap();
        let db = populated(root.path());
        let topology_root = tempfile::tempdir().unwrap();
        let mut range = range(topology_root.path());
        range.end_token = Some(1);
        let options = RangeSnapshotOptions {
            max_records_per_batch: 3,
            max_bytes_per_batch: 4096,
            max_record_bytes: 4096,
        };
        let mut cursor = None;
        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(rounds < 12, "raw scan budget did not make bounded progress");
            let result = db
                .for_each_range_snapshot_batch_with_cancellation(
                    "items",
                    &range,
                    cursor.as_deref(),
                    &options,
                    5,
                    &CancellationToken::uncancelable(),
                    |_batch| panic!("test fixture expected no token in [0, 1)"),
                )
                .unwrap();
            if result.completed {
                break;
            }
            assert!(result.resume_after_key > cursor);
            cursor = result.resume_after_key;
        }
        assert_eq!(rounds, 5);
        range.end_token = None;
        let mut cursor = None;
        let mut ids = Vec::new();
        for _ in 0..20 {
            let result = db
                .for_each_range_snapshot_batch_with_cancellation(
                    "items",
                    &range,
                    cursor.as_deref(),
                    &options,
                    5,
                    &CancellationToken::uncancelable(),
                    |batch| {
                        ids.extend(batch.records.into_iter().map(|record| record.id));
                        Ok(false)
                    },
                )
                .unwrap();
            if result.completed {
                break;
            }
            assert!(result.resume_after_key > cursor);
            cursor = result.resume_after_key;
        }
        assert_eq!(ids.len(), 23);
        assert_eq!(ids.iter().collect::<BTreeSet<_>>().len(), 23);
        let cancel = CancellationToken::uncancelable().child();
        cancel.cancel();
        assert!(matches!(
            db.for_each_range_snapshot_batch_with_cancellation(
                "items",
                &range,
                None,
                &options,
                5,
                &cancel,
                |_| Ok(true)
            ),
            Err(BicDbError::QueryCanceled)
        ));
    }

    #[test]
    fn range_image_scan_budget_preserves_semantic_readers_on_every_batch() {
        let root = tempfile::tempdir().unwrap();
        let db = populated(root.path());
        format::enable_decimal_predicate_comparison_v1(root.path()).unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        let topology_root = tempfile::tempdir().unwrap();
        let range = range(topology_root.path());
        let options = RangeSnapshotOptions {
            max_records_per_batch: 3,
            max_bytes_per_batch: 4096,
            max_record_bytes: 1024,
        };
        for scan_budget in [1, 2, 5, 23] {
            let mut cursor = None;
            let mut ids = BTreeSet::new();
            loop {
                let result = db
                    .for_each_range_snapshot_batch_with_cancellation(
                        "items",
                        &range,
                        cursor.as_deref(),
                        &options,
                        scan_budget,
                        &CancellationToken::uncancelable(),
                        |batch| {
                            assert!(batch.required_reader_features.iter().any(|feature| {
                                feature == format::DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG
                            }), "scan budget {scan_budget} lost the Native semantic reader contract");
                            for row in batch.records {
                                assert!(ids.insert(row.id));
                            }
                            Ok(true)
                        },
                    )
                    .unwrap();
                if result.completed {
                    break;
                }
                assert!(result.resume_after_key > cursor);
                cursor = result.resume_after_key;
            }
            assert_eq!(ids.len(), 23);
        }
        assert_eq!(db.get("items", "row-0000").unwrap().unwrap().id, "row-0000");
    }

    #[test]
    fn native_semantic_reader_requirements_invalidate_prior_image_bindings() {
        for flag in [
            format::SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG,
            format::DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG,
            format::APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = populated(root.path());
            let old = db.capture_range_image_bindings().unwrap();
            let acknowledgement = db.get("items", "row-0000").unwrap().unwrap();
            let sequence = db.last_commit_seq();
            let namespace = db.local_namespace_identity("items").unwrap();
            match flag {
                format::SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG => {
                    db.enable_sql_partition_execution_v1().unwrap();
                }
                format::DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG => {
                    format::enable_decimal_predicate_comparison_v1(root.path()).unwrap();
                }
                format::APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG => {
                    db.enable_application_unique_scope().unwrap();
                }
                _ => unreachable!(),
            }
            assert!(db.validate_range_image_bindings(&old).is_err(), "{flag}");
            db.schema_compatibility_fingerprint().unwrap();
            let current = db.capture_range_image_bindings().unwrap();
            assert!(current.reader_features().iter().any(|f| f == flag));
            for local in [
                format::LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG,
                format::RANGE_IMAGE_INSTALL_FEATURE_FLAG,
                format::SERVER_PAGED_FEATURE_FLAG,
            ] {
                assert!(!current.reader_features().iter().any(|f| f == local));
            }
            assert_eq!(db.last_commit_seq(), sequence);
            assert_eq!(db.local_namespace_identity("items").unwrap(), namespace);
            assert_eq!(
                db.get("items", "row-0000")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                acknowledgement.content_hash().unwrap()
            );
            db.close().unwrap();
            for _ in 0..2 {
                let db = BicDb::open_with_config(root.path(), config()).unwrap();
                db.schema_compatibility_fingerprint().unwrap();
                let reopened = db.capture_range_image_bindings().unwrap();
                assert!(reopened.reader_features().iter().any(|f| f == flag));
                assert_eq!(db.local_namespace_identity("items").unwrap(), namespace);
                assert_eq!(
                    db.get("items", "row-0000")
                        .unwrap()
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    acknowledgement.content_hash().unwrap()
                );
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn range_image_bindings_pin_allocation_birth_separately_from_local_namespace() {
        let root = tempfile::tempdir().unwrap();
        let mut db = populated(root.path());
        db.activate_native_collection_birth_v1().unwrap();
        assert_eq!(db.native_collection_birth_id("items").unwrap(), None);
        db.create_collection("born").unwrap();
        db.insert(
            "born",
            Record::new("birth_ACK").with_metadata(serde_json::json!({"value": 41})),
        )
        .unwrap();
        let ack = db
            .get("born", "birth_ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        // Protected image capture currently requires the reopened, purely
        // paged representation, rather than a newly created live collection.
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        let bindings = db.capture_range_image_bindings().unwrap();
        let namespace = db.local_namespace_identity("born").unwrap();
        let birth = db.native_collection_birth_id("born").unwrap();
        assert!(birth.is_some());
        assert!(bindings.collections().iter().all(
            |meta| meta.native_backing_birth_id.is_none() && meta.local_namespace_id.is_none()
        ));
        assert!(!bindings
            .reader_features()
            .iter()
            .any(|f| f == format::NATIVE_COLLECTION_BIRTH_FEATURE_FLAG));
        // White-box corruption control: a digest and local namespace alone
        // must not validate a capture after its allocation identity changes.
        db.collection_state_mut("born")
            .unwrap()
            .meta
            .native_backing_birth_id = Some(Uuid::new_v4());
        assert_eq!(db.local_namespace_identity("born").unwrap(), namespace);
        assert!(db.validate_range_image_bindings(&bindings).is_err());
        db.collection_state_mut("born")
            .unwrap()
            .meta
            .native_backing_birth_id = birth;
        db.validate_range_image_bindings(&bindings).unwrap();
        assert_eq!(
            db.get("born", "birth_ACK")
                .unwrap()
                .unwrap()
                .content_hash()
                .unwrap(),
            ack
        );
    }

    #[test]
    fn range_image_bindings_reject_schema_roundtrip_and_new_backing() {
        let root = tempfile::tempdir().unwrap();
        let mut db = populated(root.path());
        let initial = db.capture_range_image_bindings().unwrap();
        let id = db.local_namespace_identity("items").unwrap();
        db.rename_collection("items", "other").unwrap();
        db.rename_collection("other", "items").unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        assert_eq!(db.local_namespace_identity("items").unwrap(), id);
        assert!(db.validate_range_image_bindings(&initial).is_err());
        let second = db.capture_range_image_bindings().unwrap();
        db.drop_collection("items").unwrap();
        db.create_collection("items").unwrap();
        db.schema_compatibility_fingerprint().unwrap();
        assert_ne!(db.local_namespace_identity("items").unwrap(), id);
        assert!(db.validate_range_image_bindings(&second).is_err());
    }

    #[test]
    fn range_image_gate_rejects_bare_frozen_copy_and_requires_owned_controlled_scope() {
        let root = tempfile::tempdir().unwrap();
        let mut db = populated(root.path());
        let topology_root = tempfile::tempdir().unwrap();
        let range = range(topology_root.path());
        let gate = db.range_mutation_gate();
        let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
        assert!(db.batch_insert("items", [Record::new("unscoped")]).is_err());
        let meta = db
            .collections()
            .into_iter()
            .find(|m| m.name == "items")
            .unwrap();
        let record = Record::new("trusted-copy");
        let batch = RangeSnapshotBatch {
            required_reader_features: Vec::new(),
            collection: "items".into(),
            range_id: range.id,
            range_epoch: range.epoch,
            snapshot_commit_sequence: 100,
            resume_after_key: record.id.clone(),
            serialized_record_bytes: serde_json::to_vec(&record).unwrap().len(),
            records: vec![record],
        };
        assert!(db
            .apply_range_snapshot_batch_preinstalled(&range, &meta, &batch)
            .is_err());
        assert!(
            db.apply_range_snapshot_batch_preinstalled_with_mutation(
                &range,
                &meta,
                &batch,
                &RangeMutationAuthority::Frozen(&frozen),
            )
            .is_err(),
            "a read-only frozen lease is not copy/write admission"
        );
        let controlled = gate.try_controlled_mutation(&frozen).unwrap();
        db.apply_range_snapshot_batch_preinstalled_with_mutation(
            &range,
            &meta,
            &batch,
            &RangeMutationAuthority::Controlled(&controlled),
        )
        .unwrap();
        assert!(db.get("items", "trusted-copy").unwrap().is_some());
        drop(controlled);
        drop(frozen);
        db.batch_insert("items", [Record::new("unscoped")]).unwrap();
    }
}
