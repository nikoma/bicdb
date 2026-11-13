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
        // A real local-consumer retention hold survives Embedded rename's
        // checkpoint. It retains WAL without counting as any replica ACK.
        db.sync_quorum().hold("native-birth-export-control", 0);
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        db.create_collection("items").unwrap();
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
        db.create_timeseries_collection("items").unwrap();
        assert_ne!(
            db.native_collection_birth_id("items").unwrap(),
            original_birth
        );
        assert_eq!(
            db.collections()
                .into_iter()
                .find(|m| m.name == "items")
                .unwrap()
                .mode,
            CollectionMode::TimeSeries
        );
        db.insert(
            "items",
            Record::new("new_ACK")
                .with_metadata(json!({"owner":"replacement"}))
                .with_timestamp(1),
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
            let captured = frame.captured_native_wal_backings().unwrap().unwrap();
            assert_eq!(captured.get("items"), Some(&original_meta));
            assert_eq!(captured["items"].native_backing_birth_id, original_birth);
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

#[test]
fn native_birth_wal_capture_never_exports_failed_io_beyond_the_durable_prefix() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for site in ["transaction_log.write", "transaction_log.sync"] {
            let root = tempfile::tempdir().unwrap();
            let cfg = config(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            db.sync_quorum().hold("native-capture-io-control", 0);
            db.activate_local_namespace_identity_v1().unwrap();
            db.activate_native_collection_birth_v1().unwrap();
            db.activate_native_wal_backing_capture_v1().unwrap();
            db.create_collection("items").unwrap();
            db.insert(
                "items",
                Record::new("ACK").with_metadata(json!({"value":17})),
            )
            .unwrap();
            let ack_seq = db.last_commit_seq();
            let ack = db
                .get("items", "ACK")
                .unwrap()
                .unwrap()
                .content_hash()
                .unwrap();
            let birth = db.native_collection_birth_id("items").unwrap();
            let uncertain = Record::new("uncertain").with_metadata(json!({"value":29}));
            let fault = bicdb_page::test_faults::arm_io_failure(site, root.path(), 1, false);
            assert!(
                db.insert("items", uncertain.clone()).is_err(),
                "fault was not injected: {mode:?} {site}"
            );
            drop(fault);
            assert!(db.replication_shippable_seq() <= ack_seq);
            match db.export_replication_frames_since(ack_seq, usize::MAX) {
                Ok(frames) => assert!(frames.iter().all(|frame| frame.commit_seq <= ack_seq
                    && frame
                        .writes
                        .iter()
                        .all(|write| write.record_id != "uncertain"))),
                Err(error) => assert!(matches!(error, BicDbError::WriteFailed { .. }), "{error}"),
            }
            drop(db);
            for _ in 0..2 {
                let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
                assert_eq!(
                    db.get("items", "ACK")
                        .unwrap()
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    ack
                );
                assert_eq!(db.native_collection_birth_id("items").unwrap(), birth);
                if let Some(row) = db.get("items", "uncertain").unwrap() {
                    assert_eq!(
                        row.content_hash().unwrap(),
                        uncertain.content_hash().unwrap()
                    );
                }
                for frame in db
                    .export_replication_frames_since(ack_seq, usize::MAX)
                    .unwrap()
                {
                    if frame
                        .writes
                        .iter()
                        .any(|write| write.record_id == "uncertain")
                    {
                        assert_eq!(
                            frame.captured_native_wal_backings().unwrap().unwrap()["items"]
                                .native_backing_birth_id,
                            birth
                        );
                    }
                }
                db.close().unwrap();
            }
        }
    }
}

#[test]
fn native_birth_wal_activation_is_separate_future_only_and_persists_across_restart() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.sync_quorum().hold("future-only-capture-control", 0);
        assert!(db.activate_native_wal_backing_capture_v1().is_err());
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.create_collection("items").unwrap();
        let birth = db.native_collection_birth_id("items").unwrap();
        assert!(birth.is_some());
        db.insert("items", Record::new("before_capture_ACK"))
            .unwrap();
        let old_seq = db.last_commit_seq();
        assert!(
            db.export_replication_frames_since(old_seq - 1, 1).unwrap()[0]
                .captured_native_wal_backings()
                .unwrap()
                .is_none()
        );
        db.activate_native_wal_backing_capture_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        assert_eq!(db.last_commit_seq(), old_seq);
        assert!(!format::required_reader_features(root.path())
            .unwrap()
            .iter()
            .any(|flag| flag == format::NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG));
        db.insert("items", Record::new("after_capture_ACK"))
            .unwrap();
        let new_seq = db.last_commit_seq();
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            let old = db
                .export_replication_frames_since(old_seq - 1, 1)
                .unwrap()
                .remove(0);
            let new = db
                .export_replication_frames_since(new_seq - 1, 1)
                .unwrap()
                .remove(0);
            assert!(old.captured_native_wal_backings().unwrap().is_none());
            assert_eq!(
                new.captured_native_wal_backings().unwrap().unwrap()["items"]
                    .native_backing_birth_id,
                birth
            );
            assert!(db.get("items", "before_capture_ACK").unwrap().is_some());
            assert!(db.get("items", "after_capture_ACK").unwrap().is_some());
            db.close().unwrap();
        }
    }
}

#[test]
fn native_birth_wal_captures_first_vector_contract_and_complete_native_payload() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.sync_quorum().hold("full-native-wal-control", 0);
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        db.create_collection("items").unwrap();
        let row = Record::new("full_ACK")
            .with_vector(vec![1.25, -2.5, 3.0])
            .with_timestamp(1731000000)
            .with_payload(vec![0, 255, 1, 128])
            .with_metadata(json!({"value": "Native", "unknown": {"nested": [null, true, "雪"]}}));
        db.insert("items", row.clone()).unwrap();
        let seq = db.last_commit_seq();
        let birth = db.native_collection_birth_id("items").unwrap();
        assert_eq!(
            db.collections()
                .into_iter()
                .find(|m| m.name == "items")
                .unwrap()
                .vector_dim,
            Some(3)
        );
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            for frames in [
                db.export_replication_frames_since(seq - 1, 1).unwrap(),
                db.replication_frames_after(&mut crate::ReplicationCursor::new(seq - 1), 1)
                    .unwrap(),
            ] {
                let frame = &frames[0];
                let captured = frame.captured_native_wal_backings().unwrap().unwrap();
                assert_eq!(captured["items"].native_backing_birth_id, birth);
                assert_eq!(captured["items"].vector_dim, Some(3));
                let write = frame
                    .writes
                    .iter()
                    .find(|w| w.record_id == "full_ACK")
                    .unwrap();
                assert_eq!(write.collection_meta.as_ref().unwrap().vector_dim, Some(3));
                assert_eq!(
                    serde_json::from_slice::<Record>(&write.payload)
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    row.content_hash().unwrap()
                );
            }
            assert_eq!(
                db.get("items", "full_ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                row.content_hash().unwrap()
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn native_birth_wal_capture_is_bound_to_actual_frame_and_never_decoded_from_json() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        db.create_collection("items").unwrap();
        db.insert(
            "items",
            Record::new("ACK").with_metadata(json!({"value": 19})),
        )
        .unwrap();
        let seq = db.last_commit_seq();
        let frame = db
            .export_replication_frames_since(seq - 1, 1)
            .unwrap()
            .remove(0);
        let captured = frame.captured_native_wal_backings().unwrap().unwrap();
        assert_eq!(
            captured["items"].native_backing_birth_id,
            db.native_collection_birth_id("items").unwrap()
        );
        assert_eq!(
            captured["items"].local_namespace_id,
            db.local_namespace_identity("items").unwrap()
        );
        let encoded = serde_json::to_value(&frame).unwrap();
        assert!(encoded.get("native_wal_capture").is_none());
        let portable = encoded["writes"][0]["collection_meta"].as_object().unwrap();
        assert!(!portable.contains_key("native_backing_birth_id"));
        assert!(!portable.contains_key("local_namespace_id"));
        let decoded: CommitFrame = serde_json::from_value(encoded).unwrap();
        decoded.verify_checksum().unwrap();
        assert!(decoded.captured_native_wal_backings().unwrap().is_none());
        for change in 0..5 {
            let mut forged = frame.clone();
            match change {
                0 => {
                    forged.writes[0].payload =
                        serde_json::to_vec(&Record::new("ACK").with_metadata(json!({"value": 999})))
                            .unwrap()
                }
                1 => forged.source_node_id.push_str("-other"),
                2 => forged.commit_seq += 1,
                3 => forged.tx_id += 1,
                4 => {
                    forged = forged
                        .with_required_reader_features(vec![
                            format::SQL_CATALOG_INTENTS_FEATURE_FLAG.into(),
                        ])
                        .unwrap()
                }
                _ => unreachable!(),
            }
            forged.checksum = forged.calculate_checksum();
            forged.verify_checksum().unwrap();
            assert!(
                forged.captured_native_wal_backings().is_err(),
                "changed frame reused Native capture: case {change}"
            );
        }
        db.insert("items", Record::new("second_ACK")).unwrap();
        let mut other = db
            .export_replication_frames_since(seq, 1)
            .unwrap()
            .remove(0);
        other.native_wal_capture = frame.native_wal_capture.clone();
        assert!(other.captured_native_wal_backings().is_err());
        assert_eq!(
            db.get("items", "ACK").unwrap().unwrap().metadata,
            json!({"value":19})
        );
    }
}

#[test]
fn native_birth_old_wal_is_never_retrofitted_and_async_sources_mint_no_capture() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
        db.sync_quorum().hold("unborn-wal-control", 0);
        db.create_collection("items").unwrap();
        db.insert("items", Record::new("legacy_ACK")).unwrap();
        let legacy_seq = db.last_commit_seq();
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        db.rename_collection("items", "kept").unwrap();
        db.create_collection("items").unwrap();
        assert!(db.native_collection_birth_id("items").unwrap().is_some());
        for frames in [
            db.export_replication_frames_since(legacy_seq - 1, 1)
                .unwrap(),
            db.replication_frames_after(&mut crate::ReplicationCursor::new(legacy_seq - 1), 1)
                .unwrap(),
        ] {
            let frame = &frames[0];
            assert_eq!(frame.commit_seq, legacy_seq);
            assert!(frame.captured_native_wal_backings().unwrap().is_none());
        }
        let async_root = tempfile::tempdir().unwrap();
        let mut asynchronous =
            BicDb::open_with_config(async_root.path(), config(mode).with_fsync(false)).unwrap();
        asynchronous.activate_local_namespace_identity_v1().unwrap();
        asynchronous.activate_native_collection_birth_v1().unwrap();
        asynchronous
            .activate_native_wal_backing_capture_v1()
            .unwrap();
        asynchronous.create_collection("items").unwrap();
        asynchronous
            .insert("items", Record::new("async_row"))
            .unwrap();
        let seq = asynchronous.last_commit_seq();
        let frame = asynchronous
            .export_replication_frames_since(seq - 1, 1)
            .unwrap()
            .remove(0);
        assert!(asynchronous
            .native_collection_birth_id("items")
            .unwrap()
            .is_some());
        assert!(frame.captured_native_wal_backings().unwrap().is_none());
    }
}

#[test]
fn native_birth_wal_keeps_dropped_identity_and_requires_its_format_fence_after_restart() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.sync_quorum().hold("dropped-birth-wal-control", 0);
        db.create_collection("independent").unwrap();
        db.insert(
            "independent",
            Record::new("ACK").with_metadata(json!({"value":83})),
        )
        .unwrap();
        let ack = db
            .get("independent", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        db.create_collection("born").unwrap();
        db.insert("born", Record::new("old_ACK")).unwrap();
        let seq = db.last_commit_seq();
        let birth = db.native_collection_birth_id("born").unwrap();
        db.drop_collection("born").unwrap();
        assert!(db
            .collections()
            .iter()
            .all(|m| m.native_backing_birth_id.is_none()));
        db.close().unwrap();
        let path = format::metadata_path(root.path());
        let original = fs::read(&path).unwrap();
        let mut damaged: Value = serde_json::from_slice(&original).unwrap();
        damaged["feature_flags"]
            .as_array_mut()
            .unwrap()
            .retain(|flag| flag.as_str() != Some(format::NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG));
        fs::write(&path, serde_json::to_vec_pretty(&damaged).unwrap()).unwrap();
        let error = BicDb::open_with_config(root.path(), cfg.clone()).unwrap_err();
        assert!(
            matches!(error, BicDbError::FormatCompatibility(_)),
            "{error}"
        );
        assert!(
            error.to_string().contains("Native WAL backing identities"),
            "{error}"
        );
        fs::write(&path, original).unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            assert!(db.native_collection_birth_id("born").is_err());
            let frame = db
                .export_replication_frames_since(seq - 1, 1)
                .unwrap()
                .remove(0);
            assert_eq!(
                frame.captured_native_wal_backings().unwrap().unwrap()["born"]
                    .native_backing_birth_id,
                birth
            );
            assert_eq!(
                db.get("independent", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                ack
            );
            db.close().unwrap();
        }
    }
}
