//! Reader requirements must survive learner bootstrap as well as WAL replay.
use bicdb_core::{
    BicDb, ClusterNodeId, CollectionMeta, DbConfig, PlacementPolicy, RangeDescriptor, RangeId,
    RangeReplica, RangeReplicaRole, RangeSnapshotBatch, RangeSnapshotOptions, Record, ReplicaId,
    StorageMode,
};

// Frozen v1 decoder: unknown fields are ignored, but rows are mandatory.
#[derive(serde::Serialize, serde::Deserialize)]
struct LegacyBatch {
    collection: String,
    range_id: RangeId,
    range_epoch: u64,
    snapshot_commit_sequence: u64,
    resume_after_key: String,
    serialized_record_bytes: usize,
    records: Vec<Record>,
}

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default().with_fsync(true).with_storage_mode(mode)
}

fn range() -> RangeDescriptor {
    let node = ClusterNodeId::new("source").unwrap();
    RangeDescriptor {
        id: RangeId::new(1).unwrap(),
        start_token: 0,
        end_token: None,
        epoch: 1,
        replicas: vec![RangeReplica {
            id: ReplicaId::new(1).unwrap(),
            node_id: node.clone(),
            role: RangeReplicaRole::Voter,
        }],
        leader: node,
        approximate_bytes: 0,
        approximate_qps: 0,
        placement: PlacementPolicy::default(),
    }
}

fn assert_fenced(path: &std::path::Path) {
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("format.json")).unwrap()).unwrap();
    for expected in ["sql_catalog_intents_v1", "sql_rename_intents_v1"] {
        assert!(
            metadata["feature_flags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|flag| flag == expected),
            "bootstrap published data without reader capability {expected}"
        );
    }
}

#[test]
fn range_snapshot_installs_source_reader_requirements_before_business_rows() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source");
        let mut source =
            BicDb::open_with_config(&source_path, config(StorageMode::ServerPaged)).unwrap();
        source.enable_sql_catalog_intents().unwrap();
        source.enable_sql_rename_intents().unwrap();
        source.create_collection("business").unwrap();
        source
            .insert("business", Record::new("acknowledged"))
            .unwrap();
        source.close().unwrap();
        let source =
            BicDb::open_with_config(&source_path, config(StorageMode::ServerPaged)).unwrap();
        let target_path = root.path().join("target");
        let mut target = BicDb::open_with_config(&target_path, config(mode.clone())).unwrap();
        let range = range();
        let export = source
            .for_each_range_snapshot_batch(
                "business",
                &range,
                None,
                &RangeSnapshotOptions::default(),
                |batch| {
                    let wire = serde_json::to_vec(&batch)?;
                    assert!(serde_json::from_slice::<LegacyBatch>(&wire).is_err());
                    assert_eq!(serde_json::from_slice::<RangeSnapshotBatch>(&wire)?, batch);
                    let before = std::fs::read(target_path.join("format.json"))?;
                    let mut invalid = batch.clone();
                    invalid.required_reader_features = vec!["future_reader_v999".into()];
                    assert!(target
                        .apply_range_snapshot_batch(
                            &range,
                            &CollectionMeta::standard("business"),
                            &invalid
                        )
                        .is_err());
                    assert_eq!(std::fs::read(target_path.join("format.json"))?, before);
                    assert!(!target.collections().iter().any(|m| m.name == "business"));
                    let mut downgraded: serde_json::Value = serde_json::from_slice(&wire)?;
                    downgraded["required_reader_features"] = serde_json::json!([]);
                    assert!(serde_json::from_value::<RangeSnapshotBatch>(downgraded).is_err());
                    target.apply_range_snapshot_batch(
                        &range,
                        &CollectionMeta::standard("business"),
                        &batch,
                    )?;
                    assert_fenced(&target_path);
                    Ok(true)
                },
            )
            .unwrap();
        assert!(export.completed);
        assert_eq!(export.records_exported, 1);
        target.close().unwrap();
        let target = BicDb::open_with_config(&target_path, config(mode)).unwrap();
        assert_fenced(&target_path);
        assert!(target.get("business", "acknowledged").unwrap().is_some());
        target.close().unwrap();
        source.close().unwrap();
    }
}

#[test]
fn schema_bootstrap_installs_source_reader_requirements_even_for_equal_schema() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut source = BicDb::open(root.path().join("source")).unwrap();
        source.create_collection("business").unwrap();
        let target_path = root.path().join("target");
        let mut target = BicDb::open_with_config(&target_path, config(mode.clone())).unwrap();
        // First install exactly the same structural schema without flags.
        let original = source.cluster_schema_bundle().unwrap();
        assert_eq!(original.format_version, 1);
        assert!(!serde_json::to_value(&original)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("required_reader_features"));
        target.install_cluster_schema_bundle(&original).unwrap();
        source.enable_sql_catalog_intents().unwrap();
        source.enable_sql_rename_intents().unwrap();
        let protected = source.cluster_schema_bundle().unwrap();
        assert_eq!(protected.format_version, 2);
        assert_ne!(original.fingerprint.sha256, protected.fingerprint.sha256);
        let before = std::fs::read(target_path.join("format.json")).unwrap();
        let mut invalid = protected.clone();
        invalid.required_reader_features = vec!["future_reader_v999".into()];
        assert!(target.install_cluster_schema_bundle(&invalid).is_err());
        invalid = protected.clone();
        invalid.format_version = 1;
        assert!(target.install_cluster_schema_bundle(&invalid).is_err());
        invalid = protected.clone();
        invalid.required_reader_features.pop();
        assert!(target.install_cluster_schema_bundle(&invalid).is_err());
        assert_eq!(
            std::fs::read(target_path.join("format.json")).unwrap(),
            before
        );
        assert!(target.install_cluster_schema_bundle(&protected).unwrap());
        assert!(!target.install_cluster_schema_bundle(&protected).unwrap());
        assert_fenced(&target_path);
        target.close().unwrap();
        let target = BicDb::open_with_config(&target_path, config(mode)).unwrap();
        assert_fenced(&target_path);
        target.close().unwrap();
        source.close().unwrap();
    }
}

#[test]
fn legacy_unprotected_snapshot_wire_still_round_trips_byte_for_byte() {
    let legacy = LegacyBatch {
        collection: "business".into(),
        range_id: RangeId::new(1).unwrap(),
        range_epoch: 1,
        snapshot_commit_sequence: 3,
        resume_after_key: "acknowledged".into(),
        serialized_record_bytes: 0,
        records: vec![Record::new("acknowledged")],
    };
    let original = serde_json::to_vec(&legacy).unwrap();
    let current: RangeSnapshotBatch = serde_json::from_slice(&original).unwrap();
    assert!(current.required_reader_features.is_empty());
    assert_eq!(serde_json::to_vec(&current).unwrap(), original);
    let mut unfenced_journal = current;
    unfenced_journal.collection = "__bicdb_rename_intents".into();
    assert!(serde_json::to_vec(&unfenced_journal).is_err());
}
