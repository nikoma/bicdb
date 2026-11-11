//! Reader requirements must survive learner bootstrap as well as WAL replay.
use bicdb_core::{
    BicDb, ClusterNodeId, CollectionMeta, DbConfig, PlacementPolicy, RangeDescriptor, RangeId,
    RangeReplica, RangeReplicaRole, RangeSnapshotOptions, Record, ReplicaId, StorageMode,
};

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
        target
            .install_cluster_schema_bundle(&source.cluster_schema_bundle().unwrap())
            .unwrap();
        source.enable_sql_catalog_intents().unwrap();
        source.enable_sql_rename_intents().unwrap();
        target
            .install_cluster_schema_bundle(&source.cluster_schema_bundle().unwrap())
            .unwrap();
        assert_fenced(&target_path);
        target.close().unwrap();
        let target = BicDb::open_with_config(&target_path, config(mode)).unwrap();
        assert_fenced(&target_path);
        target.close().unwrap();
        source.close().unwrap();
    }
}
