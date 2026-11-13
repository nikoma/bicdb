use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::Value;

#[test]
fn rename_capability_is_separate_sticky_and_preserves_existing_data() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("business").unwrap();
        db.insert("business", Record::new("acknowledged")).unwrap();
        db.enable_sql_catalog_intents().unwrap();
        let path = root.path().join("format.json");
        let before: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(before["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "sql_catalog_intents_v1"));
        assert!(!before["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "sql_rename_intents_v1"));
        db.enable_sql_rename_intents().unwrap();
        let enabled = std::fs::read(&path).unwrap();
        let after: Value = serde_json::from_slice(&enabled).unwrap();
        let before_flags = before["feature_flags"].as_array().unwrap();
        let after_flags = after["feature_flags"].as_array().unwrap();
        assert_eq!(after_flags.len(), before_flags.len() + 1);
        assert!(before_flags.iter().all(|flag| after_flags.contains(flag)));
        assert!(after_flags.iter().any(|v| v == "sql_rename_intents_v1"));
        db.enable_sql_rename_intents().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), enabled);
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        assert!(db.get("business", "acknowledged").unwrap().is_some());
        assert_eq!(std::fs::read(&path).unwrap(), enabled);
        db.drop_collection("business").unwrap();
        db.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), enabled);
    }
}

#[test]
fn fence_installation_does_not_replace_unknown_capabilities() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let path = root.path().join("format.json");
    let mut metadata: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    metadata["feature_flags"]
        .as_array_mut()
        .unwrap()
        .push(Value::String("future_rename_v999".into()));
    let unsupported = serde_json::to_vec_pretty(&metadata).unwrap();
    std::fs::write(&path, &unsupported).unwrap();
    assert!(db.enable_sql_rename_intents().is_err());
    assert_eq!(std::fs::read(&path).unwrap(), unsupported);
}
