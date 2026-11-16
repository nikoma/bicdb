//! Real closed-root allocations and public Core effects; no permission fixture.
use super::*;
use crate::{BicDb, DbConfig, Record, StorageMode};
use std::cell::{Cell, RefCell};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::rc::Rc;

type CreationHook = Box<dyn FnOnce(&Path)>;
thread_local! {
    static AFTER_STAGE_CREATION: RefCell<Option<CreationHook>> = const { RefCell::new(None) };
}

pub(super) fn after_stage_creation(path: &Path) {
    let hook = AFTER_STAGE_CREATION.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook(path);
    }
}

struct HookReset;
impl Drop for HookReset {
    fn drop(&mut self) {
        AFTER_STAGE_CREATION.with(|slot| *slot.borrow_mut() = None);
    }
}

fn config() -> DbConfig {
    DbConfig::default()
        .with_storage_mode(StorageMode::EmbeddedMemory)
        .with_fsync(true)
}

fn bound(epoch: u64, byte: u8) -> EncryptionConfig {
    EncryptionConfig::with_raw_key([byte; 32]).with_binding(
        EncryptionBinding::new("actual-rotation-ownership", "cell-storage-v2", epoch).unwrap(),
    )
}

fn source(parent: &Path) -> (PathBuf, EncryptionConfig, EncryptionConfig, Record) {
    let path = parent.join("actual-source");
    let old = bound(41, 0x41);
    let new = bound(42, 0x42);
    let record = Record::new("original-ack")
        .with_payload(b"full actual source payload before key rotation".to_vec());
    let mut db = BicDb::open_with_encryption(&path, config(), old.clone()).unwrap();
    db.create_collection("records").unwrap();
    db.insert("records", record.clone()).unwrap();
    db.close().unwrap();
    (path, old, new, record)
}

#[test]
fn actual_fresh_rotation_stage_refuses_a_normal_core_owner_that_finished_before_claim() {
    let parent = tempfile::tempdir().unwrap();
    let (path, old, new, original) = source(parent.path());
    let baseline = tree_digest_hex(&path).unwrap();
    let competing = Record::new("different-real-ack")
        .with_payload(b"full unrelated normal Core payload".to_vec());
    let inserted = competing.clone();
    let reached = Rc::new(Cell::new(false));
    let fired = reached.clone();
    let _reset = HookReset;
    AFTER_STAGE_CREATION.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(move |stage| {
            let mut db = BicDb::open_with_config(stage, config()).unwrap();
            db.create_collection("unrelated").unwrap();
            db.insert("unrelated", inserted.clone()).unwrap();
            assert_eq!(
                db.get("unrelated", &inserted.id).unwrap().as_deref(),
                Some(&inserted)
            );
            db.close().unwrap();
            fired.set(true);
        }));
    });
    let result = rotate_bound_database_encryption(
        &path,
        old.clone(),
        new,
        BoundEncryptionRotationOptions::default(),
    );
    eprintln!(
        "ACTUAL_ROTATION_COMPETING_CORE result={result:?} cut={}",
        reached.get()
    );
    assert!(reached.get(), "actual normal Core ACK+close cut required");
    assert!(
        matches!(result, Err(BicDbError::EncryptionKeyInvalid(ref detail))
        if detail == "fresh rotation staging directory was populated by another owner")
    );
    assert_eq!(tree_digest_hex(&path).unwrap(), baseline);
    assert!(!parent
        .path()
        .join(".actual-source.key-rotation.json")
        .exists());
    let stage = parent.path().join(".actual-source.key-epoch-42.staging");
    let db = BicDb::open_with_config(&stage, config()).unwrap();
    assert_eq!(
        db.get("unrelated", &competing.id).unwrap().as_deref(),
        Some(&competing)
    );
    drop(db);
    let db = BicDb::open_with_encryption(&path, config(), old).unwrap();
    assert_eq!(
        db.get("records", &original.id).unwrap().as_deref(),
        Some(&original)
    );
}

#[test]
fn actual_prepared_rotation_stage_keeps_its_new_gate_and_refuses_live_or_missing_owners() {
    let parent = tempfile::tempdir().unwrap();
    let (path, old, new, original) = source(parent.path());
    let prepared = rotate_bound_database_encryption(
        &path,
        old.clone(),
        new.clone(),
        BoundEncryptionRotationOptions {
            fsync: true,
            stop_after: Some(BoundEncryptionRotationCheckpoint::Prepared),
        },
    )
    .unwrap();
    assert_eq!(
        prepared.checkpoint,
        BoundEncryptionRotationCheckpoint::Prepared
    );
    let stage = parent.path().join(".actual-source.key-epoch-42.staging");
    let gate_path = stage.join(storage::BACKUP_WRITE_GATE);
    assert!(gate_path.is_file());
    #[cfg(unix)]
    {
        let source_gate = fs::metadata(path.join(storage::BACKUP_WRITE_GATE)).unwrap();
        let new_gate = fs::metadata(&gate_path).unwrap();
        assert_ne!(
            (source_gate.dev(), source_gate.ino()),
            (new_gate.dev(), new_gate.ino())
        );
        assert_eq!(new_gate.nlink(), 1);
    }
    let baseline = tree_digest_hex(&path).unwrap();
    let stage_baseline = tree_digest_hex(&stage).unwrap();
    let held = bicdb_page::DirectoryLock::acquire(&stage).unwrap();
    let busy = rotate_bound_database_encryption(
        &path,
        old.clone(),
        new.clone(),
        BoundEncryptionRotationOptions::default(),
    );
    eprintln!("ACTUAL_ROTATION_HELD_STAGE {busy:?}");
    assert!(
        matches!(busy, Err(BicDbError::EncryptionKeyInvalid(ref detail))
        if detail.starts_with("rotation staging database must remain closed during key rotation:"))
    );
    assert_eq!(tree_digest_hex(&path).unwrap(), baseline);
    assert_eq!(tree_digest_hex(&stage).unwrap(), stage_baseline);
    drop(held);
    // Deliberate corruption under the actual closed root and SAME EX scope.
    // The gate is digest-excluded, so digest equality may not recreate it.
    {
        let owner = RotationTreeOwner::existing(&stage, "fault staging owner").unwrap();
        let _scope = owner.borrow().unwrap();
        owner.require_at(&stage).unwrap();
        fs::remove_file(&gate_path).unwrap();
    }
    let missing = rotate_bound_database_encryption(
        &path,
        old.clone(),
        new,
        BoundEncryptionRotationOptions::default(),
    );
    eprintln!("ACTUAL_ROTATION_MISSING_STAGE_GATE {missing:?}");
    assert!(matches!(missing, Err(BicDbError::Backup(ref detail))
        if detail == "enrolled physical backup gate is missing; refusing replacement inode"));
    assert!(!gate_path.exists());
    assert_eq!(tree_digest_hex(&path).unwrap(), baseline);
    assert_eq!(tree_digest_hex(&stage).unwrap(), stage_baseline);
    assert!(!prepared.retired_path.exists());
    let db = BicDb::open_with_encryption(&path, config(), old).unwrap();
    assert_eq!(
        db.get("records", &original.id).unwrap().as_deref(),
        Some(&original)
    );
}

#[test]
fn actual_activated_rotation_does_not_report_success_after_its_enrolled_gate_is_removed() {
    let parent = tempfile::tempdir().unwrap();
    let (path, old, new, original) = source(parent.path());
    let activated = rotate_bound_database_encryption(
        &path,
        old.clone(),
        new.clone(),
        BoundEncryptionRotationOptions::default(),
    )
    .unwrap();
    assert!(activated.activated);
    {
        let db = BicDb::open_with_encryption(&path, config(), new.clone()).unwrap();
        assert_eq!(
            db.get("records", &original.id).unwrap().as_deref(),
            Some(&original)
        );
    }
    let baseline = tree_digest_hex(&path).unwrap();
    {
        let owner = RotationTreeOwner::existing(&path, "actual activated fault owner").unwrap();
        let _scope = owner.borrow().unwrap();
        owner.require_at(&path).unwrap();
        fs::remove_file(path.join(storage::BACKUP_WRITE_GATE)).unwrap();
    }
    let refused = rotate_bound_database_encryption(
        &path,
        old,
        new,
        BoundEncryptionRotationOptions::default(),
    );
    eprintln!("ACTUAL_ROTATION_ALREADY_NEW_MISSING_GATE {refused:?}");
    assert!(matches!(refused, Err(BicDbError::Backup(ref detail))
        if detail == "enrolled physical backup gate is missing; refusing replacement inode"));
    assert!(!path.join(storage::BACKUP_WRITE_GATE).exists());
    assert_eq!(tree_digest_hex(&path).unwrap(), baseline);
    assert!(!parent
        .path()
        .join(".actual-source.key-rotation.json")
        .exists());
}

#[test]
fn actual_source_retired_retry_refuses_changed_stage_before_publication() {
    let parent = tempfile::tempdir().unwrap();
    let (path, old, new, original) = source(parent.path());
    let retired = rotate_bound_database_encryption(
        &path,
        old.clone(),
        new.clone(),
        BoundEncryptionRotationOptions {
            fsync: true,
            stop_after: Some(BoundEncryptionRotationCheckpoint::SourceRetired),
        },
    )
    .unwrap();
    assert!(!path.exists());
    let stage = parent.path().join(".actual-source.key-epoch-42.staging");
    {
        let owner = RotationTreeOwner::existing(&stage, "actual stage tamper owner").unwrap();
        let _scope = owner.borrow().unwrap();
        owner.require_at(&stage).unwrap();
        fs::write(
            stage.join("owned-post-prepare-tamper"),
            b"actual intentional fault",
        )
        .unwrap();
    }
    let changed = tree_digest_hex(&stage).unwrap();
    assert_ne!(changed, retired.staged_tree_sha256);
    let refused = rotate_bound_database_encryption(
        &path,
        old.clone(),
        new,
        BoundEncryptionRotationOptions::default(),
    );
    eprintln!("ACTUAL_ROTATION_RETIRED_STAGE_TAMPER {refused:?}");
    assert!(
        matches!(refused, Err(BicDbError::TamperEvidence { ref path, ref message })
        if path == &stage && message == "prepared rotation tree digest changed")
    );
    assert!(!path.exists(), "tampered candidate must not be published");
    assert_eq!(tree_digest_hex(&stage).unwrap(), changed);
    let db = BicDb::open_with_encryption(&retired.retired_path, config(), old).unwrap();
    assert_eq!(
        db.get("records", &original.id).unwrap().as_deref(),
        Some(&original)
    );
}
