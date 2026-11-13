//! Authorized cold restore of a Native origin root. The raw-copy refusal is
//! covered, unchanged, by the sibling `tests` module.
use super::*;
use crate::{NativeColdRestoreOutcome, ReplicationMode, ReplicationTlsConfig};
use serde_json::json;

fn config(mode: StorageMode, role: ReplicationMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: role,
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
fn meta(db: &BicDb, name: &str) -> CollectionMeta {
    db.collections()
        .into_iter()
        .find(|m| m.name == name)
        .unwrap()
}
fn copy_tree(source: &Path, target: &Path) {
    fs::create_dir(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let dest = target.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &dest);
        } else {
            fs::copy(entry.path(), dest).unwrap();
        }
    }
}
fn refused(path: &Path, cfg: &DbConfig) {
    let error = BicDb::open_with_config(path, cfg.clone()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("copied or replaced physical directory"),
        "{error}"
    );
}

#[test]
fn native_origin_authorized_restore_rebinds_only_a_genuine_stopped_copy() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let source = parent.path().join("source");
        let cfg = config(mode, ReplicationMode::Primary);
        let mut db = BicDb::open_with_config(&source, cfg.clone()).unwrap();
        db.activate_native_origin_root_v1().unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        let root = db.native_origin_root_id().unwrap();
        assert!(root.is_some());
        let identity = db
            .observe_native_origin_root()
            .unwrap()
            .unwrap()
            .record_sha256()
            .to_owned();
        db.create_collection("items").unwrap();
        db.insert(
            "items",
            Record::new("ACK").with_metadata(json!({"value":41})),
        )
        .unwrap();
        let ack = db
            .get("items", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        let birth = meta(&db, "items").native_backing_birth_id;

        // A running source cannot authorize anything.
        let early = parent.path().join("early");
        copy_tree(&source, &early);
        let error = authorize_native_cold_restore(&source, &early).unwrap_err();
        assert!(error.to_string().contains("not stopped"), "{error}");
        db.close().unwrap();

        let copied = parent.path().join("copied");
        copy_tree(&source, &copied);
        refused(&copied, &cfg);

        // A target whose record is not the source's bytes is not its copy.
        let forged = parent.path().join("forged");
        copy_tree(&source, &forged);
        let mut bytes = fs::read(forged.join(ROOT_FILE)).unwrap();
        bytes.push(b'\n');
        fs::write(forged.join(ROOT_FILE), bytes).unwrap();
        let error = authorize_native_cold_restore(&source, &forged).unwrap_err();
        assert!(error.to_string().contains("not a copy"), "{error}");
        refused(&forged, &cfg);

        // An unauthorized copy is never a restore source.
        let second_hand = parent.path().join("second-hand");
        copy_tree(&copied, &second_hand);
        let error = authorize_native_cold_restore(&copied, &second_hand).unwrap_err();
        assert!(error.to_string().contains("unauthorized copy"), "{error}");
        refused(&second_hand, &cfg);

        assert_eq!(
            authorize_native_cold_restore(&source, &copied).unwrap(),
            NativeColdRestoreOutcome::Rebound {
                custody_generation: 1
            }
        );
        assert_eq!(
            authorize_native_cold_restore(&source, &copied).unwrap(),
            NativeColdRestoreOutcome::AlreadyRebound {
                custody_generation: 1
            }
        );
        for later in ["later_one", "later_two"] {
            let mut db = BicDb::open_with_config(&copied, cfg.clone()).unwrap();
            assert_eq!(db.native_origin_root_id().unwrap(), root);
            assert_eq!(
                db.observe_native_origin_root()
                    .unwrap()
                    .unwrap()
                    .record_sha256(),
                identity,
                "the directory-independent identity survives the rebind"
            );
            assert_eq!(meta(&db, "items").native_backing_birth_id, birth);
            assert_eq!(
                db.get("items", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                ack
            );
            // The restored root keeps allocating under the same identity.
            db.create_collection(later).unwrap();
            assert_eq!(meta(&db, later).native_backing_origin_root, root);
            db.close().unwrap();
        }

        // A rebound copy is genuine in turn; its own copy advances custody.
        let third = parent.path().join("third");
        copy_tree(&copied, &third);
        refused(&third, &cfg);
        assert_eq!(
            authorize_native_cold_restore(&copied, &third).unwrap(),
            NativeColdRestoreOutcome::Rebound {
                custody_generation: 2
            }
        );
        let db = BicDb::open_with_config(&third, cfg.clone()).unwrap();
        assert_eq!(db.native_origin_root_id().unwrap(), root);
        assert_eq!(
            db.observe_native_origin_root()
                .unwrap()
                .unwrap()
                .record_sha256(),
            identity
        );
        db.close().unwrap();

        // The original is untouched and still opens.
        let db = BicDb::open_with_config(&source, cfg.clone()).unwrap();
        assert_eq!(db.native_origin_root_id().unwrap(), root);
        db.close().unwrap();
    }
}

#[test]
fn native_origin_restore_is_not_required_without_an_origin_root() {
    let parent = tempfile::tempdir().unwrap();
    let source = parent.path().join("source");
    let cfg = config(StorageMode::EmbeddedMemory, ReplicationMode::Primary);
    let mut db = BicDb::open_with_config(&source, cfg.clone()).unwrap();
    db.create_collection("items").unwrap();
    db.close().unwrap();
    let copied = parent.path().join("copied");
    copy_tree(&source, &copied);
    assert_eq!(
        authorize_native_cold_restore(&source, &copied).unwrap(),
        NativeColdRestoreOutcome::NotRequired
    );
    BicDb::open_with_config(&copied, cfg)
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn native_origin_restored_source_still_matches_its_receivers_bindings() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let source_path = parent.path().join("source");
        let source_cfg = config(mode.clone(), ReplicationMode::Primary);
        let mut source = BicDb::open_with_config(&source_path, source_cfg.clone()).unwrap();
        source.sync_quorum().hold("origin-history", 0);
        source.persist_standby_slots().unwrap();
        assert!(source
            .initialize_fresh_primary_replication_origins()
            .unwrap());
        source.create_collection("items").unwrap();
        source
            .insert(
                "items",
                Record::new("ACK").with_metadata(json!({"value":43})),
            )
            .unwrap();
        let mut target = BicDb::open_with_config(
            parent.path().join("target"),
            config(mode, ReplicationMode::Standby),
        )
        .unwrap();
        let ship = |source: &BicDb, target: &mut BicDb| {
            target
                .apply_replication_batch(
                    &source
                        .export_replication_frames_since(
                            target.last_applied_commit_seq(),
                            usize::MAX,
                        )
                        .unwrap(),
                )
                .unwrap();
        };
        ship(&source, &mut target);
        let first = meta(&target, "items")
            .native_imported_backing_origin
            .unwrap();
        source.close().unwrap();

        let restored_path = parent.path().join("restored");
        copy_tree(&source_path, &restored_path);
        assert_eq!(
            authorize_native_cold_restore(&source_path, &restored_path).unwrap(),
            NativeColdRestoreOutcome::Rebound {
                custody_generation: 1
            }
        );
        let mut restored = BicDb::open_with_config(&restored_path, source_cfg).unwrap();
        let observed = restored.observe_native_origin_root().unwrap().unwrap();
        assert_eq!(observed.root_id(), first.source_root);
        assert_eq!(observed.record_sha256(), first.source_root_sha256);
        assert_eq!(
            Some(first.source_birth),
            meta(&restored, "items").native_backing_birth_id
        );

        // A later allocation from the restored source lands on the same
        // receiver under the same retained source identity.
        restored.create_collection("later").unwrap();
        ship(&restored, &mut target);
        let later = meta(&target, "later")
            .native_imported_backing_origin
            .unwrap();
        assert!(!later.pending);
        assert_eq!(later.source_root, first.source_root);
        assert_eq!(later.source_root_sha256, first.source_root_sha256);
        assert_eq!(later.target_root, first.target_root);
        assert_eq!(
            target.get("items", "ACK").unwrap().unwrap().metadata["value"],
            json!(43)
        );
    }
}

#[test]
fn native_origin_archive_restore_takes_custody_once_at_first_open() {
    use crate::backup::{create_backup, restore_backup, BackupCreateOptions, BackupRestoreOptions};
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let source = parent.path().join("source");
        let cfg = config(mode, ReplicationMode::Primary);
        let mut db = BicDb::open_with_config(&source, cfg.clone()).unwrap();
        db.activate_native_origin_root_v1().unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        let root = db.native_origin_root_id().unwrap();
        let identity = db
            .observe_native_origin_root()
            .unwrap()
            .unwrap()
            .record_sha256()
            .to_owned();
        db.create_collection("items").unwrap();
        db.insert(
            "items",
            Record::new("ACK").with_metadata(json!({"value":47})),
        )
        .unwrap();
        db.close().unwrap();

        // A plain copy cannot cut an archive that would authorize a restore.
        let copied = parent.path().join("copied");
        copy_tree(&source, &copied);
        let options = || BackupCreateOptions {
            passphrase: "restore-custody".into(),
            base_backup: None,
        };
        let error =
            create_backup(&copied, parent.path().join("laundered.bak"), options()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("copied or replaced physical directory"),
            "{error}"
        );

        let archive = parent.path().join("genuine.bak");
        create_backup(&source, &archive, options()).unwrap();
        let restored = parent.path().join("restored");
        restore_backup(
            &archive,
            &restored,
            BackupRestoreOptions {
                passphrase: "restore-custody".into(),
                force: false,
            },
        )
        .unwrap();
        assert!(restored.join(RESTORE_FILE).is_file());

        // The authorization names the restored directory, not its copies.
        let second_hand = parent.path().join("second-hand");
        copy_tree(&restored, &second_hand);
        refused(&second_hand, &cfg);

        for _ in 0..2 {
            let db = BicDb::open_with_config(&restored, cfg.clone()).unwrap();
            assert_eq!(db.native_origin_root_id().unwrap(), root);
            assert_eq!(
                db.observe_native_origin_root()
                    .unwrap()
                    .unwrap()
                    .record_sha256(),
                identity
            );
            assert_eq!(
                db.get("items", "ACK").unwrap().unwrap().metadata["value"],
                json!(47)
            );
            db.close().unwrap();
            assert!(!restored.join(RESTORE_FILE).exists());
            let record = read_record(&restored).unwrap().unwrap();
            assert_eq!((record.version, record.custody_generation), (2, 1));
        }

        // Once consumed, the opened restore is an ordinary root in custody.
        let third = parent.path().join("third");
        copy_tree(&restored, &third);
        refused(&third, &cfg);
    }
}
