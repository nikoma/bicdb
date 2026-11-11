//! Retain real receiver stores for separate, older-process reader-fence checks.
//! No fault is injected and no production qualification is implied.
use bicdb_core::{
    BicDb, ClusterNodeId, DbConfig, PlacementPolicy, RangeDescriptor, RangeId, RangeReplica,
    RangeReplicaRole, RangeSnapshotOptions, Record, ReplicaId, ReplicationConfig, ReplicationMode,
    ReplicationTlsConfig, StorageMode,
};
use serde_json::json;
use std::path::PathBuf;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default().with_fsync(true).with_storage_mode(mode)
}

fn replicated(mode: StorageMode, role: ReplicationMode) -> DbConfig {
    config(mode).with_replication(ReplicationConfig {
        enabled: true,
        mode: role,
        cluster_id: "bootstrap-reader-control".into(),
        listen_addr: Some("127.0.0.1:0".into()),
        tls: Some(ReplicationTlsConfig {
            dev_localhost_plaintext: true,
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn acknowledged() -> Record {
    Record::new("acknowledged").with_metadata(json!({"payload":"retained-source-value"}))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(args.next().ok_or("expected a new fixture directory")?);
    if args.next().is_some() || root.exists() {
        return Err("expected one new, nonexistent fixture directory".into());
    }
    std::fs::create_dir_all(&root)?;
    let source_path = root.join("source");
    let mut source = BicDb::open_with_config(
        &source_path,
        replicated(StorageMode::ServerPaged, ReplicationMode::Primary),
    )?;
    source.sync_quorum().hold("reader-fixture", 0);
    source.persist_standby_slots()?;
    source.create_collection("business")?;
    source.insert("business", acknowledged())?;
    source.enable_sql_catalog_intents()?;
    source.enable_sql_rename_intents()?;
    // Ship a real post-capability commit, retaining the initial native ACK.
    source.insert("business", Record::new("later"))?;
    let frames = source.export_replication_frames_since(0, 1_000)?;
    assert!(!frames.is_empty());
    source.close()?;
    let source = BicDb::open_with_config(
        &source_path,
        replicated(StorageMode::ServerPaged, ReplicationMode::Primary),
    )?;
    let node = ClusterNodeId::new("source")?;
    let range = RangeDescriptor {
        id: RangeId::new(1)?,
        start_token: 0,
        end_token: None,
        epoch: 1,
        replicas: vec![RangeReplica {
            id: ReplicaId::new(1)?,
            node_id: node.clone(),
            role: RangeReplicaRole::Voter,
        }],
        leader: node,
        approximate_bytes: 0,
        approximate_qps: 0,
        placement: PlacementPolicy::default(),
    };
    let meta = source
        .collections()
        .into_iter()
        .find(|m| m.name == "business")
        .unwrap();
    let bundle = source.cluster_schema_bundle()?;
    let mut cases = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let control_path = root.join(format!("{}-legacy-control", mode.as_str()));
        let mut control = BicDb::open_with_config(&control_path, config(mode.clone()))?;
        control.create_collection("business")?;
        control.insert("business", acknowledged())?;
        control.close()?;
        cases.push(json!({"path":control_path,"mode":mode.as_str(),"route":"legacy_control","must_refuse":false}));
        for route in ["wal", "snapshot", "schema_then_snapshot"] {
            let path = root.join(format!("{}-{route}", mode.as_str()));
            let cfg = if route == "wal" {
                replicated(mode.clone(), ReplicationMode::Standby)
            } else {
                config(mode.clone())
            };
            let mut target = BicDb::open_with_config(&path, cfg)?;
            if route == "wal" {
                target.apply_replication_batch(&frames)?;
            } else {
                if route == "schema_then_snapshot" {
                    target.install_cluster_schema_bundle(&bundle)?;
                }
                let export = source.for_each_range_snapshot_batch(
                    "business",
                    &range,
                    None,
                    &RangeSnapshotOptions::default(),
                    |batch| {
                        target.apply_range_snapshot_batch(&range, &meta, &batch)?;
                        Ok(true)
                    },
                )?;
                assert!(export.completed);
                assert_eq!(export.records_exported, 2);
            }
            assert_eq!(
                target.get("business", "acknowledged")?.unwrap(),
                acknowledged()
            );
            target.close()?;
            let format: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path.join("format.json"))?)?;
            assert!(format["feature_flags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|f| f == "sql_rename_intents_v1"));
            cases.push(json!({"path":path,"mode":mode.as_str(),"route":route,"must_refuse":true}));
        }
    }
    source.close()?;
    let manifest =
        json!({"cases":cases,"native_expected":acknowledged(),"production_qualified":false});
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    println!("{}", serde_json::to_string(&manifest)?);
    Ok(())
}
