//! A replica must not expose rename-journal data to a reader lacking recovery.
use bicdb_core::{
    BicDb, DbConfig, Record, ReplicationConfig, ReplicationCursor, ReplicationMode,
    ReplicationTlsConfig, StorageMode,
};

fn config(mode: StorageMode, role: ReplicationMode, journal: bool) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_audit_events(journal)
        .with_storage_mode(mode)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: role,
            cluster_id: "rename-fence".into(),
            listen_addr: Some("127.0.0.1:0".into()),
            record_journal: journal,
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..Default::default()
        })
}

fn flagged(path: &std::path::Path) -> bool {
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("format.json")).unwrap()).unwrap();
    metadata["feature_flags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|flag| flag == "sql_rename_intents_v1")
}

#[test]
fn native_replication_installs_the_rename_reader_fence_before_journal_data() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for journal in [false, true] {
            for live in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let mut primary = BicDb::open_with_config(
                    root.path().join("primary"),
                    config(mode.clone(), ReplicationMode::Primary, journal),
                )
                .unwrap();
                let standby_path = root.path().join("standby");
                let mut standby = BicDb::open_with_config(
                    &standby_path,
                    config(mode.clone(), ReplicationMode::Standby, journal),
                )
                .unwrap();
                primary.create_collection("business").unwrap();
                primary
                    .insert("business", Record::new("acknowledged"))
                    .unwrap();
                standby
                    .apply_replication_batch(
                        &primary.export_replication_frames_since(0, 1000).unwrap(),
                    )
                    .unwrap();
                let before = standby.last_applied_commit_seq();
                assert!(!flagged(&standby_path));
                primary.enable_sql_rename_intents().unwrap();
                primary.create_collection("__bicdb_rename_intents").unwrap();
                primary.insert("__bicdb_rename_intents",Record::new("00000000000000000000000000000001").with_metadata(serde_json::json!({"id":"00000000000000000000000000000001","order":1,"count":0,"remaining":null}))).unwrap();
                let frames = if live {
                    primary
                        .replication_frames_after(&mut ReplicationCursor::new(before), 1000)
                        .unwrap()
                } else {
                    primary
                        .export_replication_frames_since(before, 1000)
                        .unwrap()
                };
                assert!(!frames.is_empty());
                let original_format = std::fs::read(standby_path.join("format.json")).unwrap();
                let mut downgraded = frames[0].clone();
                downgraded.required_reader_features.clear();
                downgraded.protocol_version = if downgraded.journal.is_empty() { 1 } else { 2 };
                downgraded.checksum = downgraded.calculate_checksum();
                assert!(
                    standby.apply_replication_frame(&downgraded).is_err(),
                    "unfenced journal catalog must not be installed"
                );
                let mut future = frames[0].clone();
                future.required_reader_features = vec!["future_reader_v999".into()];
                future.checksum = future.calculate_checksum();
                assert!(standby.apply_replication_frame(&future).is_err());
                assert_eq!(standby.last_applied_commit_seq(), before);
                assert_eq!(
                    std::fs::read(standby_path.join("format.json")).unwrap(),
                    original_format
                );
                assert!(!standby
                    .collections()
                    .iter()
                    .any(|meta| meta.name == "__bicdb_rename_intents"));
                for frame in &frames {
                    standby.apply_replication_frame(frame).unwrap();
                    assert!(
                        flagged(&standby_path),
                        "{mode} journal={journal}: frame{} applied without recovery capability",
                        frame.commit_seq
                    );
                }
                assert!(standby.get("business", "acknowledged").unwrap().is_some());
                assert!(standby
                    .get("__bicdb_rename_intents", "00000000000000000000000000000001")
                    .unwrap()
                    .is_some());
                standby.close().unwrap();
                let standby = BicDb::open_with_config(
                    &standby_path,
                    config(mode.clone(), ReplicationMode::Standby, journal),
                )
                .unwrap();
                assert!(flagged(&standby_path));
                assert!(standby.get("business", "acknowledged").unwrap().is_some());
                standby.close().unwrap();
                primary.close().unwrap();
            }
        }
    }
}
