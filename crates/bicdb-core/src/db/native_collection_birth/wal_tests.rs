use super::*;
use crate::replication::{ReplicationMode, ReplicationTlsConfig};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Primary,
            listen_addr: Some("127.0.0.1:0".into()),
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

fn retained_write_contract_after_name_reuse(live: bool) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.create_collection("items").unwrap();
        db.set_collection_mesh_sync_enabled("items", true).unwrap();
        db.create_collection("independent").unwrap();
        let row = Record::new("old_ACK").with_metadata(json!({"owner":"original"}));
        db.insert("items", row.clone()).unwrap();
        let original_seq = db.last_commit_seq();
        let original_birth = db.native_collection_birth_id("items").unwrap();
        let original_meta = db
            .collections()
            .into_iter()
            .find(|m| m.name == "items")
            .unwrap();
        db.insert(
            "independent",
            Record::new("ACK").with_metadata(json!({"value":81})),
        )
        .unwrap();
        let ack = db
            .get("independent", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        db.rename_collection("items", "kept").unwrap();
        db.create_collection("items").unwrap();
        assert_ne!(
            db.native_collection_birth_id("items").unwrap(),
            original_birth
        );
        assert!(
            !db.collections()
                .into_iter()
                .find(|m| m.name == "items")
                .unwrap()
                .mesh_sync_enabled
        );
        db.insert(
            "items",
            Record::new("new_ACK").with_metadata(json!({"owner":"replacement"})),
        )
        .unwrap();
        for reopen in [false, true, true] {
            if reopen {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            }
            let frames = if live {
                db.replication_frames_after(&mut crate::ReplicationCursor::new(0), usize::MAX)
                    .unwrap()
            } else {
                db.export_replication_frames_since(0, usize::MAX).unwrap()
            };
            let frame = frames
                .iter()
                .find(|f| f.commit_seq == original_seq)
                .unwrap();
            let write = frame
                .writes
                .iter()
                .find(|w| w.collection == "items" && w.record_id == "old_ACK")
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Record>(&write.payload).unwrap(),
                row
            );
            assert_eq!(write.collection_meta.as_ref().unwrap().portable_contract(), original_meta.portable_contract(),
                "retained Native WAL must keep its write-time backing contract instead of resolving the reused name to a newer backing; live={live}, reopen={reopen}");
            assert_eq!(
                db.get("kept", "old_ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                row.content_hash().unwrap()
            );
            assert!(db.get("items", "old_ACK").unwrap().is_none());
            assert_eq!(
                db.get("independent", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                ack
            );
        }
    }
}

#[test]
fn native_birth_retained_batch_export_does_not_rebind_old_writes_to_reused_names() {
    retained_write_contract_after_name_reuse(false);
}

#[test]
fn native_birth_retained_live_export_does_not_rebind_old_writes_to_reused_names() {
    retained_write_contract_after_name_reuse(true);
}
