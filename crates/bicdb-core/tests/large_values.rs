use std::io::{self, Cursor, Read};

use bicdb_core::{
    create_backup, restore_backup, BackupCreateOptions, BackupRestoreOptions, BicDb, BicDbError,
    CollectionMode, CollectionPolicy, DbConfig, EncryptionBinding, EncryptionConfig, Record,
    SecurityContext, StorageMode, DEFAULT_LARGE_VALUE_CHUNK_BYTES,
    DEFAULT_LARGE_VALUE_THRESHOLD_BYTES,
};
use serde_json::json;

const BACKUP_KEY: &str = "correct horse battery staple";
const DB_KEY: &str = "database encryption key";

fn tenant_policy() -> CollectionPolicy {
    CollectionPolicy::tenant_field("tenant_id")
        .with_read_roles(["reader"])
        .with_write_roles(["writer"])
}

fn tenant_ctx(tenant: &str, roles: &[&str]) -> SecurityContext {
    SecurityContext::new("user-1", tenant).with_roles(roles.iter().copied())
}

#[test]
fn large_attachment_streams_write_and_read_with_bounded_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("documents").unwrap();
    db.insert(
        "documents",
        Record::new("doc-1").with_metadata(json!({"tenant_id": "tenant-a"})),
    )
    .unwrap();

    let payload = deterministic_bytes(DEFAULT_LARGE_VALUE_THRESHOLD_BYTES * 3 + 17);
    let reader = MaxReadTrackingReader::new(payload.clone());
    let descriptor = db
        .write_attachment_from_reader(
            "documents",
            "doc-1",
            "body",
            Some("application/pdf"),
            reader,
        )
        .unwrap();
    assert_eq!(descriptor.size_bytes, payload.len() as u64);
    assert!(descriptor.chunks >= 3);

    let record = db.get("documents", "doc-1").unwrap().unwrap();
    assert!(record.payload.is_none());
    assert_eq!(
        record.metadata["body"]["storage"],
        "content_addressed_blob_store"
    );

    let mut stream = db
        .open_attachment_reader("documents", "doc-1", "body")
        .unwrap()
        .unwrap();
    let mut round_trip = Vec::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = stream.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        round_trip.extend_from_slice(&buffer[..count]);
    }
    assert_eq!(round_trip, payload);

    let report = BicDb::verify_path(temp.path(), DbConfig::default(), None).unwrap();
    assert_eq!(report.large_value_blobs_checked, 1);
    assert!(report.large_value_checksum_failures.is_empty());
}

#[test]
fn cell_bound_large_attachment_is_resealed_to_its_final_object_identity() {
    let temp = tempfile::tempdir().unwrap();
    let encryption = EncryptionConfig::with_raw_key([0x42; 32]).with_binding(
        EncryptionBinding::new("018f7b30-4f4d-7b5c-a1f6-a183663e1240", "cell-bound-test", 3)
            .unwrap(),
    );
    let canary = b"cell-bound-attachment-canary-184f".repeat(50_000);
    {
        let mut db = BicDb::open_with_encryption(
            temp.path(),
            DbConfig::default().with_fsync(false),
            encryption.clone(),
        )
        .unwrap();
        db.create_collection("documents").unwrap();
        db.insert("documents", Record::new("doc-1")).unwrap();
        db.insert("documents", Record::new("doc-2")).unwrap();
        db.write_attachment_from_reader(
            "documents",
            "doc-1",
            "payload",
            Some("application/octet-stream"),
            Cursor::new(canary.clone()),
        )
        .unwrap();
        // Content-addressed deduplication publishes the same final object a
        // second time. The replacement must still be sealed for that final
        // path rather than for either writer's temporary path.
        db.write_attachment_from_reader(
            "documents",
            "doc-2",
            "payload",
            Some("application/octet-stream"),
            Cursor::new(canary.clone()),
        )
        .unwrap();
        db.close().unwrap();
    }

    let mut persisted = Vec::new();
    collect_file_bytes(temp.path(), &mut persisted);
    assert!(!contains(&persisted, b"cell-bound-attachment-canary-184f"));

    let db = BicDb::open_with_encryption(
        temp.path(),
        DbConfig::default().with_fsync(false),
        encryption,
    )
    .unwrap();
    for record_id in ["doc-1", "doc-2"] {
        let mut reader = db
            .open_attachment_reader("documents", record_id, "payload")
            .unwrap()
            .unwrap();
        let mut round_trip = Vec::new();
        reader.read_to_end(&mut round_trip).unwrap();
        assert_eq!(round_trip, canary);
    }
}

#[test]
fn large_attachment_backup_restores_and_backup_archive_is_encrypted() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("large.bicbackup");
    let payload = b"invoice scan secret payload".repeat(100_000);

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("documents").unwrap();
        db.insert("documents", Record::new("doc-1")).unwrap();
        db.write_attachment_from_reader(
            "documents",
            "doc-1",
            "scan",
            Some("application/pdf"),
            Cursor::new(payload.clone()),
        )
        .unwrap();
        db.close().unwrap();
    }

    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: BACKUP_KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    assert!(!contains(
        &std::fs::read(&backup_path).unwrap(),
        b"invoice scan secret payload"
    ));
    restore_backup(
        &backup_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: BACKUP_KEY.to_string(),
            force: true,
        },
    )
    .unwrap();

    let db =
        BicDb::open_with_config(restored.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut stream = db
        .open_attachment_reader("documents", "doc-1", "scan")
        .unwrap()
        .unwrap();
    let mut restored_payload = Vec::new();
    stream.read_to_end(&mut restored_payload).unwrap();
    assert_eq!(restored_payload, payload);
}

#[test]
fn encrypted_large_attachment_sidecar_does_not_store_plaintext() {
    let temp = tempfile::tempdir().unwrap();
    let payload = b"encrypted sidecar secret payload".repeat(100_000);
    let mut db = BicDb::open_with_encryption(
        temp.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::EmbeddedMemory)
            .with_fsync(false),
        EncryptionConfig::with_passphrase(DB_KEY),
    )
    .unwrap();
    db.create_collection("documents").unwrap();
    db.insert("documents", Record::new("doc-1")).unwrap();
    db.write_attachment_from_reader(
        "documents",
        "doc-1",
        "scan",
        Some("application/pdf"),
        Cursor::new(payload.clone()),
    )
    .unwrap();
    db.close().unwrap();

    let all_bytes = all_file_bytes(temp.path());
    assert!(!contains(&all_bytes, b"encrypted sidecar secret payload"));
}

#[test]
fn interrupted_large_attachment_write_does_not_commit_reference() {
    let temp = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("documents").unwrap();
    db.insert("documents", Record::new("doc-1")).unwrap();

    let result = db.write_attachment_from_reader(
        "documents",
        "doc-1",
        "body",
        None,
        FailingReader {
            remaining_ok_bytes: DEFAULT_LARGE_VALUE_CHUNK_BYTES + 64,
        },
    );
    assert!(result.is_err());

    let record = db.get("documents", "doc-1").unwrap().unwrap();
    assert!(record.metadata.get("body").is_none());
    assert!(db
        .open_attachment_reader("documents", "doc-1", "body")
        .unwrap()
        .is_none());
}

#[test]
fn secure_attachment_api_blocks_cross_tenant_reads() {
    let temp = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection_with_policy("documents", CollectionMode::Standard, tenant_policy())
        .unwrap();
    db.secure(&tenant_ctx("tenant-a", &["writer"]))
        .insert(
            "documents",
            Record::new("doc-a").with_metadata(json!({"tenant_id": "tenant-a"})),
        )
        .unwrap();
    db.secure(&tenant_ctx("tenant-a", &["writer"]))
        .write_attachment_from_reader(
            "documents",
            "doc-a",
            "body",
            Some("text/plain"),
            Cursor::new(b"tenant-a-only".to_vec()),
        )
        .unwrap();

    assert!(db
        .secure(&tenant_ctx("tenant-b", &["reader"]))
        .open_attachment_reader("documents", "doc-a", "body")
        .unwrap()
        .is_none());
    let mut allowed = db
        .secure(&tenant_ctx("tenant-a", &["reader"]))
        .open_attachment_reader("documents", "doc-a", "body")
        .unwrap()
        .unwrap();
    let mut text = String::new();
    allowed.read_to_string(&mut text).unwrap();
    assert_eq!(text, "tenant-a-only");

    assert!(matches!(
        db.write_attachment_from_reader(
            "documents",
            "doc-a",
            "body",
            None,
            Cursor::new(b"legacy".to_vec())
        ),
        Err(BicDbError::Authorization(_))
    ));
}

fn deterministic_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|idx| (idx % 251) as u8).collect()
}

struct MaxReadTrackingReader {
    inner: Cursor<Vec<u8>>,
}

impl MaxReadTrackingReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            inner: Cursor::new(bytes),
        }
    }
}

impl Read for MaxReadTrackingReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        assert!(out.len() <= DEFAULT_LARGE_VALUE_CHUNK_BYTES);
        self.inner.read(out)
    }
}

struct FailingReader {
    remaining_ok_bytes: usize,
}

impl Read for FailingReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.remaining_ok_bytes == 0 {
            return Err(io::Error::other("simulated interrupted upload"));
        }
        let count = out.len().min(self.remaining_ok_bytes);
        for byte in &mut out[..count] {
            *byte = 0x42;
        }
        self.remaining_ok_bytes -= count;
        Ok(count)
    }
}

fn all_file_bytes(root: &std::path::Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    collect_file_bytes(root, &mut bytes);
    bytes
}

fn collect_file_bytes(path: &std::path::Path, out: &mut Vec<u8>) {
    let metadata = std::fs::metadata(path).unwrap();
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            collect_file_bytes(&entry.unwrap().path(), out);
        }
    } else if metadata.is_file() {
        out.extend(std::fs::read(path).unwrap());
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
