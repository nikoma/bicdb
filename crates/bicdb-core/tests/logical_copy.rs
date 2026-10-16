use bicdb_core::{
    BicDb, CollectionPolicy, DbConfig, EncryptionBinding, EncryptionConfig, Record, StorageMode,
};
use serde_json::json;
use std::io::Read;

#[test]
fn logical_copies_reseal_referenced_blobs_and_keep_database_identity_independent() {
    for (source_mode, target_mode, encrypted_source, encrypted_target) in [
        (
            StorageMode::EmbeddedMemory,
            StorageMode::EmbeddedMemory,
            true,
            true,
        ),
        (
            StorageMode::ServerPaged,
            StorageMode::ServerPaged,
            false,
            false,
        ),
        (
            StorageMode::EmbeddedMemory,
            StorageMode::ServerPaged,
            true,
            false,
        ),
        (
            StorageMode::ServerPaged,
            StorageMode::EmbeddedMemory,
            false,
            true,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source");
        let target_path = root.path().join("target");
        let source_key = EncryptionConfig::with_raw_key([0x41; 32]).with_binding(
            EncryptionBinding::new("018f7b30-4f4d-7b5c-a1f6-a183663e1240", "copy-source", 1)
                .unwrap(),
        );
        let target_key = EncryptionConfig::with_raw_key([0x42; 32]).with_binding(
            EncryptionBinding::new("018f7b30-4f4d-7b5c-a1f6-a183663e1241", "copy-target", 1)
                .unwrap(),
        );
        let source_key = if encrypted_source {
            source_key
        } else {
            EncryptionConfig::disabled()
        };
        let target_key = if encrypted_target {
            target_key
        } else {
            EncryptionConfig::disabled()
        };
        let source_config = DbConfig::default()
            .with_storage_mode(source_mode)
            .with_fsync(false)
            .with_audit_events(true);
        let target_config = DbConfig::default()
            .with_storage_mode(target_mode)
            .with_fsync(false)
            .with_audit_events(true);
        let mut source =
            BicDb::open_with_encryption(&source_path, source_config, source_key).unwrap();
        let mut target =
            BicDb::open_with_encryption(&target_path, target_config.clone(), target_key.clone())
                .unwrap();
        source.create_collection("documents").unwrap();
        source
            .set_collection_mesh_sync_enabled("documents", true)
            .unwrap();
        target.create_collection("documents").unwrap();
        source
            .batch_insert(
                "documents",
                (0..257).map(|n| Record::new(format!("doc-{n:04}")).with_metadata(json!({"n":n}))),
            )
            .unwrap();
        let bytes = b"independent-copy-blob-payload".repeat(100_000);
        source
            .write_attachment_from_reader(
                "documents",
                "doc-0000",
                "body",
                Some("application/octet-stream"),
                std::io::Cursor::new(&bytes),
            )
            .unwrap();
        let identity = source.node_id();
        let key = source.mesh_verifying_key();
        assert_eq!(
            source
                .copy_unprotected_collection_to("documents", &mut target)
                .unwrap(),
            257
        );
        assert_ne!(target.node_id(), identity);
        assert_ne!(target.mesh_verifying_key(), key);
        assert_eq!(
            target
                .export_sync_bundle_delta(&Default::default())
                .unwrap()
                .event_count,
            0
        );
        drop(source);
        std::fs::remove_dir_all(&source_path).unwrap();
        drop(target);
        let target = BicDb::open_with_encryption(&target_path, target_config, target_key).unwrap();
        assert_eq!(target.scan_collection("documents").unwrap().len(), 257);
        let mut stream = target
            .open_attachment_reader("documents", "doc-0000", "body")
            .unwrap()
            .unwrap();
        let mut copied = Vec::new();
        stream.read_to_end(&mut copied).unwrap();
        assert_eq!(copied, bytes);
    }
}

#[test]
fn logical_copy_refuses_protected_sources_and_destinations() {
    let root = tempfile::tempdir().unwrap();
    let mut source = BicDb::open(root.path().join("source")).unwrap();
    let mut target = BicDb::open(root.path().join("target")).unwrap();
    source.create_collection("documents").unwrap();
    target.create_collection("documents").unwrap();
    source
        .insert(
            "documents",
            Record::new("one").with_metadata(json!({"tenant_id":"a"})),
        )
        .unwrap();
    source
        .set_collection_policy("documents", CollectionPolicy::tenant_field("tenant_id"))
        .unwrap();
    assert!(source
        .copy_unprotected_collection_to("documents", &mut target)
        .is_err());
    assert!(target.scan_collection("documents").unwrap().is_empty());
    source.clear_collection_policy("documents").unwrap();
    target
        .set_collection_policy("documents", CollectionPolicy::tenant_field("tenant_id"))
        .unwrap();
    assert!(source
        .copy_unprotected_collection_to("documents", &mut target)
        .is_err());
}
