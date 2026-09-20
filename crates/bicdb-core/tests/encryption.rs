use std::fs;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use bicdb_core::{
    rotate_bound_database_encryption, BicDb, BicDbError, BoundEncryptionRotationCheckpoint,
    BoundEncryptionRotationOptions, CollectionMode, CollectionPolicy, ColumnSecurity, DbConfig,
    EncryptionBinding, EncryptionConfig, EncryptionObjectPurpose, GraphProjection, HnswIndexConfig,
    IndexDefinition, IndexField, KeySource, ProtectedDataKeyRotationOptions, Record,
    RedactionPolicy, SecurityContext, StorageMode, SyncCheckpoint,
};
use serde_json::json;

const PASSPHRASE: &str = "correct horse battery staple";

fn encrypted_config() -> EncryptionConfig {
    EncryptionConfig::with_passphrase(PASSPHRASE)
}

/// Encryption at rest is an `embedded_memory` capability today, so this file
/// pins the mode rather than inheriting the ambient default.
///
/// The paged engine writes record bytes outside the encryption layer and refuses
/// to open an encrypted database for that reason (see
/// `encryption::ensure_compatible_with_storage_mode`). Pinning keeps these tests
/// meaningful under `BICDB_STORAGE_MODE=server_paged`, which exists so the whole
/// suite can be swept against the other engine — a sweep that is only useful if
/// the tests that fail under it are the ones revealing real gaps.
fn db_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
        .with_storage_mode(StorageMode::EmbeddedMemory)
}

/// Unencrypted opens in this file, also pinned to `embedded_memory` for the
/// reason given on [`db_config`]. Several of these assert the *error* returned
/// when a key is missing or wrong; under the paged sweep they would instead see
/// the storage-mode refusal and stop testing encryption at all.
fn plain_config() -> DbConfig {
    DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory)
}

fn protected_data_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn set_protected_data_keys() -> std::sync::MutexGuard<'static, ()> {
    let guard = protected_data_env_lock();
    std::env::set_var("BICDB_PROTECTED_FIELD_ENCRYPTION_KEY", "11".repeat(32));
    std::env::set_var("BICDB_PROTECTED_LOOKUP_HMAC_KEY", "22".repeat(32));
    guard
}

fn clear_protected_data_keys() -> std::sync::MutexGuard<'static, ()> {
    let guard = protected_data_env_lock();
    std::env::remove_var("BICDB_PROTECTED_FIELD_ENCRYPTION_KEY");
    std::env::remove_var("BICDB_PROTECTED_LOOKUP_HMAC_KEY");
    guard
}

fn protected_data_policy() -> CollectionPolicy {
    CollectionPolicy::tenant_field("tenant_id")
        .with_read_roles(["reader"])
        .with_write_roles(["writer"])
        .with_column_security(
            "email",
            ColumnSecurity::encrypted("patient-email")
                .with_key_ref("phi-field:v1")
                .with_blind_index("patient-email:v1:")
                .with_redaction(RedactionPolicy::Fixed("[redacted]".to_string()))
                .with_decrypt_roles(["phi-reader"]),
        )
        .with_column_security(
            "phone",
            ColumnSecurity::encrypted("patient-phone")
                .with_key_ref("phi-field:v1")
                .with_blind_index("patient-phone:v1:")
                .with_decrypt_roles(["phi-reader"]),
        )
}

fn ctx(roles: &[&str]) -> SecurityContext {
    SecurityContext::new("user-1", "tenant-a").with_roles(roles.iter().copied())
}

fn tenant_ctx(tenant: &str, roles: &[&str]) -> SecurityContext {
    SecurityContext::new("user-1", tenant).with_roles(roles.iter().copied())
}

#[test]
fn protected_data_fields_encrypt_redact_decrypt_and_hide_plaintext() {
    let _guard = set_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection_with_policy(
        "patients",
        CollectionMode::Standard,
        protected_data_policy().with_non_sensitive_vectors(true),
    )
    .unwrap();
    db.secure(&ctx(&["writer"]))
        .insert(
            "patients",
            Record::new("p-1").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "Asha@example.test",
                "phone": "+1-555-0100",
                "routing": "clinic-7"
            })),
        )
        .unwrap();

    let bytes = all_file_bytes(temp.path());
    assert!(!contains(&bytes, b"Asha@example.test"));
    assert!(!contains(&bytes, b"+1-555-0100"));

    let unauthorized = db
        .secure(&ctx(&["reader"]))
        .get("patients", "p-1")
        .unwrap()
        .unwrap();
    assert_eq!(unauthorized.metadata["email"], "[redacted]");
    assert_eq!(unauthorized.metadata["phone"], serde_json::Value::Null);

    let authorized = db
        .secure(&ctx(&["reader", "phi-reader"]))
        .get("patients", "p-1")
        .unwrap()
        .unwrap();
    assert_eq!(authorized.metadata["email"], "Asha@example.test");
    assert_eq!(authorized.metadata["phone"], "+1-555-0100");
}

#[test]
fn protected_data_missing_keys_fail_closed() {
    let _guard = clear_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection_with_policy(
        "patients",
        CollectionMode::Standard,
        protected_data_policy().with_non_sensitive_vectors(true),
    )
    .unwrap();

    assert!(matches!(
        db.secure(&ctx(&["writer"])).insert(
            "patients",
            Record::new("p-1").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "missing@example.test"
            })),
        ),
        Err(BicDbError::EncryptionKeyRequired)
    ));

    std::env::set_var("BICDB_PROTECTED_FIELD_ENCRYPTION_KEY", "11".repeat(32));
    assert!(matches!(
        db.secure(&ctx(&["writer"])).insert(
            "patients",
            Record::new("p-2").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "missing-lookup@example.test"
            })),
        ),
        Err(BicDbError::EncryptionKeyRequired)
    ));
}

#[test]
fn protected_data_rejects_passphrases_instead_of_fast_hashing_them() {
    let _guard = clear_protected_data_keys();
    std::env::set_var(
        "BICDB_PROTECTED_FIELD_ENCRYPTION_KEY",
        "correct-horse-battery-staple",
    );
    std::env::set_var(
        "BICDB_PROTECTED_LOOKUP_HMAC_KEY",
        "another-human-passphrase",
    );
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection_with_policy(
        "patients",
        CollectionMode::Standard,
        protected_data_policy().with_non_sensitive_vectors(true),
    )
    .unwrap();
    let error = db
        .secure(&ctx(&["writer"]))
        .insert(
            "patients",
            Record::new("p-1").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "secret@example.test"
            })),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("32 random bytes"),
        "unexpected error: {error}"
    );
}

#[test]
fn protected_data_blind_index_lookup_is_namespaced_and_direct_filter_is_rejected() {
    let _guard = set_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection_with_policy(
        "patients",
        CollectionMode::Standard,
        protected_data_policy().with_non_sensitive_vectors(true),
    )
    .unwrap();
    db.secure(&ctx(&["writer"]))
        .insert(
            "patients",
            Record::new("p-1")
                .with_vector(vec![1.0, 0.0])
                .with_metadata(json!({
                    "tenant_id": "tenant-a",
                    "email": "same@example.test",
                    "phone": "same@example.test"
                })),
        )
        .unwrap();

    let email_filter = db
        .blind_index_filter(
            &ctx(&["reader"]),
            "patients",
            "email",
            json!("same@example.test"),
        )
        .unwrap();
    let phone_filter = db
        .blind_index_filter(
            &ctx(&["reader"]),
            "patients",
            "phone",
            json!("same@example.test"),
        )
        .unwrap();
    assert_ne!(
        email_filter.equals_value("__bicdb_blind_index__email"),
        phone_filter.equals_value("__bicdb_blind_index__phone")
    );
    let other_tenant_filter = db
        .blind_index_filter(
            &tenant_ctx("tenant-b", &["reader"]),
            "patients",
            "email",
            json!("same@example.test"),
        )
        .unwrap();
    assert_ne!(
        email_filter.equals_value("__bicdb_blind_index__email"),
        other_tenant_filter.equals_value("__bicdb_blind_index__email")
    );

    let results = db
        .secure(&ctx(&["reader", "phi-reader"]))
        .search_vector("patients", &[1.0, 0.0], 10, Some(&email_filter))
        .unwrap();
    assert_eq!(results[0].record.id, "p-1");

    let direct_filter = bicdb_core::JsonFilter::new().eq("email", "same@example.test");
    assert!(matches!(
        db.secure(&ctx(&["reader", "phi-reader"])).search_vector(
            "patients",
            &[1.0, 0.0],
            10,
            Some(&direct_filter),
        ),
        Err(BicDbError::Authorization(_))
    ));
}

#[test]
fn protected_data_rejects_direct_indexes_over_encrypted_fields() {
    let _guard = set_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection_with_policy(
        "patients",
        CollectionMode::Standard,
        protected_data_policy(),
    )
    .unwrap();

    assert!(matches!(
        db.secure(&ctx(&["writer"])).insert(
            "patients",
            Record::new("vector-phi")
                .with_vector(vec![1.0, 0.0])
                .with_metadata(json!({
                    "tenant_id": "tenant-a",
                    "email": "vector@example.test"
                })),
        ),
        Err(BicDbError::Authorization(_))
    ));

    assert!(matches!(
        db.create_index(IndexDefinition {
            name: "patients_email_idx".to_string(),
            collection: "patients".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["email".to_string()])],
            unique: false,
            kind: bicdb_core::IndexKind::BTree,
            predicate: None,
            exclusion: None,
        }),
        Err(BicDbError::Index(_))
    ));

    db.create_index(IndexDefinition {
        name: "patients_email_blind_idx".to_string(),
        collection: "patients".to_string(),
        fields: vec![IndexField::MetadataPath(vec![
            "__bicdb_blind_index__email".to_string()
        ])],
        unique: true,
        kind: bicdb_core::IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
}

#[test]
fn protected_data_aad_mismatches_fail_decryption() {
    let _guard = set_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection_with_policy(
        "patients",
        CollectionMode::Standard,
        protected_data_policy(),
    )
    .unwrap();
    db.create_collection_with_policy(
        "other_patients",
        CollectionMode::Standard,
        protected_data_policy(),
    )
    .unwrap();
    db.secure(&ctx(&["writer"]))
        .insert(
            "patients",
            Record::new("p-1").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "aad@example.test"
            })),
        )
        .unwrap();

    let bypass = SecurityContext::new("admin", "tenant-a").with_bypass_reason("diagnostics");
    let stored = db
        .secure(&bypass)
        .get("patients", "p-1")
        .unwrap()
        .unwrap()
        .metadata["email"]
        .clone();

    db.secure(&ctx(&["writer"]))
        .insert(
            "patients",
            Record::new("p-2").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": stored.clone()
            })),
        )
        .unwrap();
    assert!(matches!(
        db.secure(&ctx(&["reader", "phi-reader"]))
            .get("patients", "p-2"),
        Err(BicDbError::DecryptionFailed { .. })
    ));

    let bypass_b = SecurityContext::new("admin", "tenant-b").with_bypass_reason("tenant fixture");
    db.secure(&bypass_b)
        .insert(
            "patients",
            Record::new("p-tenant-b").with_metadata(json!({
                "tenant_id": "tenant-b",
                "email": stored.clone()
            })),
        )
        .unwrap();
    assert!(matches!(
        db.secure(&tenant_ctx("tenant-b", &["reader", "phi-reader"]))
            .get("patients", "p-tenant-b"),
        Err(BicDbError::DecryptionFailed { .. })
    ));

    db.secure(&ctx(&["writer"]))
        .insert(
            "other_patients",
            Record::new("p-1").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": stored.clone()
            })),
        )
        .unwrap();
    assert!(matches!(
        db.secure(&ctx(&["reader", "phi-reader"]))
            .get("other_patients", "p-1"),
        Err(BicDbError::DecryptionFailed { .. })
    ));

    let mut tampered = stored;
    tampered["key_version"] = json!(2);
    db.secure(&ctx(&["writer"]))
        .insert(
            "patients",
            Record::new("p-1-key-version").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": tampered
            })),
        )
        .unwrap();
    assert!(matches!(
        db.secure(&ctx(&["reader", "phi-reader"]))
            .get("patients", "p-1-key-version"),
        Err(BicDbError::DecryptionFailed { .. })
    ));

    let mut versioned = protected_data_policy().with_schema_version(2);
    versioned.read_roles.insert("reader".to_string());
    db.set_collection_policy("patients", versioned).unwrap();
    assert!(matches!(
        db.secure(&ctx(&["reader", "phi-reader"]))
            .get("patients", "p-1"),
        Err(BicDbError::DecryptionFailed { .. })
    ));
}

#[test]
fn protected_data_key_rotation_verifies_backup_reencrypts_and_retires_old_key() {
    let _guard = set_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection_with_policy(
        "patients",
        CollectionMode::Standard,
        protected_data_policy(),
    )
    .unwrap();
    db.secure(&ctx(&["writer"]))
        .insert(
            "patients",
            Record::new("p-1").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "rotate@example.test"
            })),
        )
        .unwrap();

    let dry_run = db
        .rotate_protected_data_field_encryption_key(ProtectedDataKeyRotationOptions {
            old_key_material: "11".repeat(32),
            new_key_material: "33".repeat(32),
            new_key_ref: "phi-field:v2".to_string(),
            dry_run: true,
        })
        .unwrap();
    assert_eq!(dry_run.fields_rotated, 1);
    assert!(dry_run.backups.is_empty());
    assert!(!dry_run.old_key_retired);

    let report = db
        .rotate_protected_data_field_encryption_key(ProtectedDataKeyRotationOptions {
            old_key_material: "11".repeat(32),
            new_key_material: "33".repeat(32),
            new_key_ref: "phi-field:v2".to_string(),
            dry_run: false,
        })
        .unwrap();
    assert_eq!(report.protected_collections, 1);
    assert_eq!(report.records_verified, 1);
    assert_eq!(report.fields_rotated, 1);
    assert_eq!(report.backups.len(), 1);
    assert!(report.backups[0].exists());
    assert!(report.old_key_retired);

    std::env::set_var("BICDB_PROTECTED_FIELD_ENCRYPTION_KEY", "11".repeat(32));
    assert!(matches!(
        db.secure(&ctx(&["reader", "phi-reader"]))
            .get("patients", "p-1"),
        Err(BicDbError::DecryptionFailed { .. })
    ));

    std::env::set_var("BICDB_PROTECTED_FIELD_ENCRYPTION_KEY", "33".repeat(32));
    let record = db
        .secure(&ctx(&["reader", "phi-reader"]))
        .get("patients", "p-1")
        .unwrap()
        .unwrap();
    assert_eq!(record.metadata["email"], "rotate@example.test");
    assert_eq!(
        db.collection_policy("patients").unwrap().unwrap().columns["email"]
            .key_ref
            .as_deref(),
        Some("phi-field:v2")
    );
}

#[test]
fn protected_data_security_release_gate_backfills_and_is_idempotent() {
    let _guard = set_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    fs::write(
        source.path().join("safe.sql"),
        "SELECT id FROM patients WHERE __bicdb_blind_index__email = 'abc'",
    )
    .unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p-a")
            .with_vector(vec![1.0, 0.0])
            .with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "legacy-a@example.test",
                "phone": "+1-555-0101",
                "regulated": "lab",
                "pharmacy": "rx",
                "billing": "claim",
                "telehealth": "visit",
                "hospital": "bed",
                "research": "study",
                "export": "bundle",
                "sync": "pending"
            })),
    )
    .unwrap();
    db.insert(
        "patients",
        Record::new("p-b")
            .with_vector(vec![0.0, 1.0])
            .with_metadata(json!({
                "tenant_id": "tenant-b",
                "email": "legacy-b@example.test",
                "phone": "+1-555-0102",
                "regulated": "lab-b"
            })),
    )
    .unwrap();
    db.set_collection_policy(
        "patients",
        protected_data_policy()
            .with_non_sensitive_vectors(true)
            .with_delete_roles(["deleter"]),
    )
    .unwrap();

    let report = db
        .protected_data_security_release_gate(source.path())
        .unwrap();
    assert!(report.passed, "{report:#?}");
    assert_eq!(report.backfill.scanned_rows, 2);
    assert_eq!(report.backfill.encrypted_rows, 2);
    assert_eq!(report.backfill.blind_index_rows_written, 2);
    assert!(report.ciphertext_sampling.sampled_fields >= 4);
    assert!(report.tenant_isolation.passed);

    let bytes = all_file_bytes(temp.path());
    assert!(!contains(&bytes, b"legacy-a@example.test"));
    assert!(!contains(&bytes, b"+1-555-0101"));
    let tenant_a = tenant_ctx("tenant-a", &["reader", "writer", "deleter", "phi-reader"]);
    let tenant_b = tenant_ctx("tenant-b", &["reader", "writer", "deleter", "phi-reader"]);
    assert_eq!(
        db.secure(&tenant_a)
            .scan_collection("patients")
            .unwrap()
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["p-a"]
    );
    assert_eq!(
        db.secure(&tenant_b)
            .search_vector("patients", &[0.0, 1.0], 10, None)
            .unwrap()
            .iter()
            .map(|result| result.record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["p-b"]
    );

    let rerun = db.backfill_protected_data_security_evidence();
    assert!(rerun.verification_status, "{rerun:#?}");
    assert_eq!(rerun.encrypted_rows, 0);
    assert_eq!(rerun.blind_index_rows_written, 0);
    assert_eq!(rerun.skipped_rows, 2);
}

#[test]
fn protected_data_security_evidence_fails_on_missing_indexes_plaintext_leaks_and_raw_sql() {
    let _guard = set_protected_data_keys();
    let temp = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    fs::write(
        source.path().join("unsafe.sql"),
        "SELECT id FROM patients WHERE email = 'leak@example.test'",
    )
    .unwrap();
    let mut db = BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p-a").with_metadata(json!({
            "tenant_id": "tenant-a",
            "email": "leak@example.test",
            "phone": "+1-555-0103"
        })),
    )
    .unwrap();
    db.insert(
        "patients",
        Record::new("p-b").with_metadata(json!({
            "tenant_id": "tenant-b",
            "email": "other@example.test",
            "phone": "+1-555-0104"
        })),
    )
    .unwrap();
    db.set_collection_policy("patients", protected_data_policy())
        .unwrap();

    let before = db.sample_protected_data_ciphertext().unwrap();
    assert!(!before.passed);
    assert!(before
        .plaintext_findings
        .iter()
        .any(|finding| finding.contains("stores plaintext-like email")));

    let coverage = db.verify_protected_data_blind_index_coverage().unwrap();
    assert!(!coverage.passed);
    assert!(coverage
        .missing_indexes
        .iter()
        .any(|finding| finding.contains("__bicdb_blind_index__email")));

    let raw_sql = db.raw_sql_audit(source.path()).unwrap();
    assert!(!raw_sql.passed);
    assert!(raw_sql
        .violations
        .iter()
        .any(|finding| finding.contains("encrypted field `patients.email`")));

    let gate = db
        .protected_data_security_release_gate(source.path())
        .unwrap();
    assert!(!gate.passed);
    assert!(!gate.raw_sql_audit.passed);
}

#[test]
fn encrypted_database_round_trips_and_hides_record_plaintext() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_encryption(temp.path(), db_config(), encrypted_config()).unwrap();
        db.create_collection("patients").unwrap();
        db.insert(
            "patients",
            Record::new("p-1")
                .with_timestamp(1_710_000_000)
                .with_vector(vec![1.0, 0.0, 0.5])
                .with_metadata(json!({"name": "Asha", "clinic": "rural-7"}))
                .with_payload(b"secret protected note".to_vec()),
        )
        .unwrap();
        db.close().unwrap();
    }

    let bytes = all_file_bytes(temp.path());
    assert!(!contains(&bytes, b"Asha"));
    assert!(!contains(&bytes, b"secret protected note"));

    assert!(matches!(
        BicDb::open_with_config(temp.path(), plain_config()).unwrap_err(),
        BicDbError::EncryptionKeyRequired
    ));
    assert!(matches!(
        BicDb::open_with_encryption(
            temp.path(),
            db_config(),
            EncryptionConfig::with_passphrase("wrong passphrase")
        )
        .unwrap_err(),
        BicDbError::DecryptionFailed { .. }
    ));

    let db = BicDb::open_with_encryption(temp.path(), db_config(), encrypted_config()).unwrap();
    let record = db.get("patients", "p-1").unwrap().unwrap();
    assert_eq!(record.metadata["clinic"], "rural-7");
    assert_eq!(record.timestamp, Some(1_710_000_000));
    assert_eq!(record.vector, Some(vec![1.0, 0.0, 0.5]));

    let report = db.verify_integrity().unwrap();
    assert!(report.encrypted);
    assert!(report.encrypted_frames >= 3);
    assert_eq!(report.unencrypted_frames, 0);
}

fn bound_encryption_config(domain: &str, epoch: u64) -> EncryptionConfig {
    EncryptionConfig::with_raw_key([0x5a; 32])
        .with_binding(EncryptionBinding::new(domain, "cell-storage-v2", epoch).unwrap())
}

fn bound_encryption_config_with_key(domain: &str, epoch: u64, key_byte: u8) -> EncryptionConfig {
    EncryptionConfig::with_raw_key([key_byte; 32])
        .with_binding(EncryptionBinding::new(domain, "cell-storage-v2", epoch).unwrap())
}

#[test]
fn bound_encryption_pins_domain_epoch_and_atomic_rewrite_object_identity() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_encryption(
            temp.path(),
            db_config(),
            bound_encryption_config("cell-a", 7),
        )
        .unwrap();
        db.create_collection("records").unwrap();
        db.insert(
            "records",
            Record::new("r-1").with_payload(b"bound confidential payload".to_vec()),
        )
        .unwrap();
        // Compaction encrypts through a temporary file and renames it. The
        // ciphertext must be bound to the final object name, not the random
        // temporary name used during publication.
        db.compact().unwrap();
        db.close().unwrap();
    }

    let metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(temp.path().join("encryption.json")).unwrap()).unwrap();
    assert_eq!(metadata["version"], 2);
    assert_eq!(metadata["binding"]["security_domain"], "cell-a");
    assert_eq!(metadata["binding"]["profile"], "cell-storage-v2");
    assert_eq!(metadata["binding"]["key_epoch"], 7);
    assert_eq!(metadata["object_key_kdf"], "hkdf-sha256-purpose-object");

    let db = BicDb::open_with_encryption(
        temp.path(),
        db_config(),
        bound_encryption_config("cell-a", 7),
    )
    .unwrap();
    assert_eq!(
        db.get("records", "r-1")
            .unwrap()
            .unwrap()
            .payload
            .as_deref(),
        Some(b"bound confidential payload".as_slice())
    );
    drop(db);

    let before = fs::read(temp.path().join("encryption.json")).unwrap();
    for wrong in [
        bound_encryption_config("cell-b", 7),
        bound_encryption_config("cell-a", 8),
    ] {
        assert!(matches!(
            BicDb::open_with_encryption(temp.path(), db_config(), wrong).unwrap_err(),
            BicDbError::EncryptionKeyInvalid(_)
        ));
        assert_eq!(
            fs::read(temp.path().join("encryption.json")).unwrap(),
            before,
            "a refused binding mismatch mutated encryption metadata"
        );
    }
}

#[test]
fn encryption_metadata_refuses_empty_or_symlink_substitution_without_mutation() {
    let empty = tempfile::tempdir().unwrap();
    let metadata_path = empty.path().join("encryption.json");
    fs::write(&metadata_path, []).unwrap();
    assert!(matches!(
        BicDb::open_with_encryption(
            empty.path(),
            db_config(),
            bound_encryption_config("cell-empty-metadata", 1),
        )
        .unwrap_err(),
        BicDbError::EncryptionKeyInvalid(_)
    ));
    assert_eq!(fs::read(&metadata_path).unwrap(), b"");

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        {
            let db = BicDb::open_with_encryption(
                directory.path(),
                db_config(),
                bound_encryption_config("cell-symlink-metadata", 1),
            )
            .unwrap();
            drop(db);
        }
        let metadata_path = directory.path().join("encryption.json");
        let real_path = directory.path().join("encryption.real.json");
        fs::rename(&metadata_path, &real_path).unwrap();
        let before = fs::read(&real_path).unwrap();
        symlink(&real_path, &metadata_path).unwrap();
        assert!(matches!(
            BicDb::open_with_encryption(
                directory.path(),
                db_config(),
                bound_encryption_config("cell-symlink-metadata", 1),
            )
            .unwrap_err(),
            BicDbError::EncryptionKeyInvalid(_)
        ));
        assert_eq!(fs::read(&real_path).unwrap(), before);
    }
}

#[test]
fn bound_encryption_rejects_ciphertext_replayed_under_another_object_path() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_encryption(
            temp.path(),
            db_config(),
            bound_encryption_config("cell-object-replay", 3),
        )
        .unwrap();
        db.create_collection("source").unwrap();
        db.create_collection("target").unwrap();
        db.insert(
            "source",
            Record::new("source-row").with_payload(b"must not move".to_vec()),
        )
        .unwrap();
        db.insert("target", Record::new("target-row")).unwrap();
        db.compact().unwrap();
        db.close().unwrap();
    }

    let segments = temp.path().join("segments");
    fs::copy(segments.join("source.seg"), segments.join("target.seg")).unwrap();
    assert!(matches!(
        BicDb::open_with_encryption(
            temp.path(),
            db_config(),
            bound_encryption_config("cell-object-replay", 3),
        )
        .unwrap_err(),
        BicDbError::DecryptionFailed { .. }
    ));
}

#[test]
fn bound_encryption_separates_purpose_keys_for_the_same_object() {
    let temp = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_encryption(
        temp.path(),
        db_config(),
        bound_encryption_config("cell-purpose-separation", 5),
    )
    .unwrap();
    let cipher = db.bound_object_cipher().unwrap();
    let path = temp.path().join("tmp").join("purpose-fixture.bin");
    let aad = b"purpose-fixture-v1";
    let ciphertext = cipher
        .seal_for(
            EncryptionObjectPurpose::Temporary,
            &path,
            b"purpose separated secret",
            aad,
        )
        .unwrap();

    assert_eq!(
        cipher
            .open_for(EncryptionObjectPurpose::Temporary, &path, &ciphertext, aad,)
            .unwrap(),
        b"purpose separated secret"
    );
    assert!(matches!(
        cipher.open_for(EncryptionObjectPurpose::Blob, &path, &ciphertext, aad),
        Err(BicDbError::DecryptionFailed { .. })
    ));
}

#[test]
fn bound_encryption_seals_vector_and_graph_sidecars_and_reopens_them() {
    let temp = tempfile::tempdir().unwrap();
    let canary = "confidential-index-canary-703b614c";
    {
        let mut db = BicDb::open_with_encryption(
            temp.path(),
            db_config(),
            bound_encryption_config("cell-index-sidecars", 4),
        )
        .unwrap();
        db.create_collection("generic_objects").unwrap();
        db.insert(
            "generic_objects",
            Record::new(canary)
                .with_payload(canary.as_bytes().to_vec())
                .with_vector(vec![1.0, 0.0]),
        )
        .unwrap();
        db.create_vector_index("generic_objects", HnswIndexConfig::default())
            .unwrap();
        db.build_graph_projection(
            GraphProjection::new("generic_graph").nodes_from("generic_objects", "Object"),
        )
        .unwrap();
        db.close().unwrap();
    }

    for path in [
        temp.path()
            .join("vector_indexes")
            .join("generic_objects.hnsw.json"),
        temp.path().join("graphs").join("generic_graph.graph.json"),
    ] {
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..4], b"BICF", "{} is not framed", path.display());
        assert!(
            !bytes
                .windows(canary.len())
                .any(|window| window == canary.as_bytes()),
            "{} leaked the confidential index canary",
            path.display()
        );
    }

    let db = BicDb::open_with_encryption(
        temp.path(),
        db_config(),
        bound_encryption_config("cell-index-sidecars", 4),
    )
    .unwrap();
    assert_eq!(
        db.search_vector_ann("generic_objects", &[1.0, 0.0], 1, 8)
            .unwrap()[0]
            .record
            .id,
        canary
    );
    assert!(db.graph_projection("generic_graph").unwrap().is_some());
    drop(db);

    let vector_path = temp
        .path()
        .join("vector_indexes")
        .join("generic_objects.hnsw.json");
    let mut tampered = fs::read(&vector_path).unwrap();
    let last = tampered.len() - 1;
    tampered[last] ^= 0x80;
    fs::write(&vector_path, tampered).unwrap();
    assert!(matches!(
        BicDb::open_with_encryption(
            temp.path(),
            db_config(),
            bound_encryption_config("cell-index-sidecars", 4),
        )
        .unwrap_err(),
        BicDbError::Corruption { .. }
            | BicDbError::DecryptionFailed { .. }
            | BicDbError::TamperEvidence { .. }
    ));
}

#[cfg(unix)]
#[test]
fn bound_encryption_refuses_a_dangling_catalog_symlink_instead_of_opening_empty() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let config = bound_encryption_config("cell-catalog-link", 2);
    {
        let mut db = BicDb::open_with_encryption(temp.path(), db_config(), config.clone()).unwrap();
        db.create_collection("objects").unwrap();
        db.close().unwrap();
    }

    let catalog = temp.path().join("collections.json");
    fs::remove_file(&catalog).unwrap();
    symlink("missing-catalog-target", &catalog).unwrap();
    assert!(matches!(
        BicDb::open_with_encryption(temp.path(), db_config(), config).unwrap_err(),
        BicDbError::Corruption { .. }
    ));
}

#[test]
fn bound_key_rotation_is_complete_crash_resumable_and_never_mixes_epochs() {
    let parent = tempfile::tempdir().unwrap();
    let database_path = parent.path().join("cell-database");
    let old = bound_encryption_config_with_key("cell-rotation", 11, 0x31);
    let new = bound_encryption_config_with_key("cell-rotation", 12, 0x92);
    let attachment = vec![0xa5_u8; 1024 * 1024 + 47];
    {
        let mut db =
            BicDb::open_with_encryption(&database_path, db_config().with_fsync(true), old.clone())
                .unwrap();
        db.create_collection("records").unwrap();
        db.insert(
            "records",
            Record::new("r-rotation")
                .with_payload(b"rotation confidential payload".to_vec())
                .with_vector(vec![1.0, 0.0]),
        )
        .unwrap();
        db.create_vector_index("records", HnswIndexConfig::default())
            .unwrap();
        db.build_graph_projection(
            GraphProjection::new("rotation_graph").nodes_from("records", "Object"),
        )
        .unwrap();
        db.write_attachment_from_reader(
            "records",
            "r-rotation",
            "attachment",
            Some("application/octet-stream"),
            Cursor::new(attachment.clone()),
        )
        .unwrap();
        db.compact().unwrap();
        db.close().unwrap();
    }

    let prepared = rotate_bound_database_encryption(
        &database_path,
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
    assert!(!prepared.activated);
    assert!(database_path.is_dir());
    assert!(!prepared.retired_path.exists());

    let retired = rotate_bound_database_encryption(
        &database_path,
        old.clone(),
        new.clone(),
        BoundEncryptionRotationOptions {
            fsync: true,
            stop_after: Some(BoundEncryptionRotationCheckpoint::SourceRetired),
        },
    )
    .unwrap();
    assert_eq!(
        retired.checkpoint,
        BoundEncryptionRotationCheckpoint::SourceRetired
    );
    assert!(!retired.activated);
    assert!(!database_path.exists());
    assert!(retired.retired_path.is_dir());

    let activated = rotate_bound_database_encryption(
        &database_path,
        old.clone(),
        new.clone(),
        BoundEncryptionRotationOptions::default(),
    )
    .unwrap();
    assert!(activated.activated);
    assert_eq!(
        activated.checkpoint,
        BoundEncryptionRotationCheckpoint::Activated
    );
    assert!(activated.frames_reencrypted > 0);
    assert!(activated.attachment_chunks_reencrypted >= 2);

    {
        let db = BicDb::open_with_encryption(&database_path, db_config(), new.clone()).unwrap();
        assert_eq!(
            db.get("records", "r-rotation")
                .unwrap()
                .unwrap()
                .payload
                .as_deref(),
            Some(b"rotation confidential payload".as_slice())
        );
        let mut reader = db
            .open_attachment_reader("records", "r-rotation", "attachment")
            .unwrap()
            .unwrap();
        let mut recovered = Vec::new();
        reader.read_to_end(&mut recovered).unwrap();
        assert_eq!(recovered, attachment);
        assert_eq!(
            db.search_vector_ann("records", &[1.0, 0.0], 1, 8).unwrap()[0]
                .record
                .id,
            "r-rotation"
        );
        assert!(db.graph_projection("rotation_graph").unwrap().is_some());
    }
    assert!(matches!(
        BicDb::open_with_encryption(&database_path, db_config(), old.clone()).unwrap_err(),
        BicDbError::EncryptionKeyInvalid(_)
    ));

    let retired_db =
        BicDb::open_with_encryption(&activated.retired_path, db_config(), old).unwrap();
    assert!(retired_db.get("records", "r-rotation").unwrap().is_some());
    assert_eq!(
        retired_db
            .search_vector_ann("records", &[1.0, 0.0], 1, 8)
            .unwrap()[0]
            .record
            .id,
        "r-rotation"
    );
    drop(retired_db);
    let all_active_bytes = all_file_bytes(&database_path);
    assert!(!contains(
        &all_active_bytes,
        b"rotation confidential payload"
    ));
}

#[test]
fn database_encryption_wraps_mesh_signing_secret_and_preserves_identity() {
    let temp = tempfile::tempdir().unwrap();
    let first_key = {
        let db = BicDb::open_with_encryption(temp.path(), db_config(), encrypted_config()).unwrap();
        db.mesh_verifying_key().unwrap()
    };

    let key_path = temp.path().join("mesh_signing_key.json");
    let document: serde_json::Value =
        serde_json::from_slice(&fs::read(&key_path).unwrap()).unwrap();
    assert_eq!(document["format"], 2);
    assert_eq!(document["protection"], "database_encryption");
    assert!(document.get("secret_hex").is_none());
    assert!(document["ciphertext_hex"].as_str().unwrap().len() > 64);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&key_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    let reopened =
        BicDb::open_with_encryption(temp.path(), db_config(), encrypted_config()).unwrap();
    assert_eq!(reopened.mesh_verifying_key().unwrap(), first_key);
}

#[test]
fn raw_key_encryption_opens_with_same_32_byte_key() {
    let temp = tempfile::tempdir().unwrap();
    let key = vec![7_u8; 32];
    {
        let mut db = BicDb::open_with_encryption(
            temp.path(),
            plain_config().with_fsync(false),
            EncryptionConfig::with_raw_key(key.clone()),
        )
        .unwrap();
        db.create_collection("docs").unwrap();
        db.insert("docs", Record::new("doc-1")).unwrap();
        db.close().unwrap();
    }

    let db = BicDb::open_with_encryption(
        temp.path(),
        plain_config().with_fsync(false),
        EncryptionConfig::with_raw_key(key),
    )
    .unwrap();
    assert!(db.get("docs", "doc-1").unwrap().is_some());
    // Release the handle before probing the wrong key: a database directory
    // has one writer, so a second concurrent open is refused for holding the
    // directory, which would mask the key check this asserts.
    drop(db);
    assert!(matches!(
        BicDb::open_with_encryption(
            temp.path(),
            plain_config(),
            EncryptionConfig {
                mode: bicdb_core::EncryptionMode::Enabled,
                key_source: KeySource::RawKey(vec![1, 2, 3]),
                binding: None,
            },
        )
        .unwrap_err(),
        BicDbError::EncryptionKeyInvalid(_)
    ));
}

#[test]
fn existing_unencrypted_database_can_be_opened_and_then_migrated_forward() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(temp.path(), plain_config().with_fsync(false)).unwrap();
        db.create_collection("patients").unwrap();
        db.insert(
            "patients",
            Record::new("legacy").with_metadata(json!({"name": "Legacy"})),
        )
        .unwrap();
        db.close().unwrap();
    }

    assert_eq!(
        BicDb::open_with_config(temp.path(), plain_config())
            .unwrap()
            .get("patients", "legacy")
            .unwrap()
            .unwrap()
            .metadata["name"],
        "Legacy"
    );

    {
        let mut db = BicDb::open_with_encryption(
            temp.path(),
            plain_config().with_fsync(false),
            encrypted_config(),
        )
        .unwrap();
        assert!(db.get("patients", "legacy").unwrap().is_some());
        db.insert(
            "patients",
            Record::new("encrypted").with_metadata(json!({"name": "Encrypted"})),
        )
        .unwrap();
        db.close().unwrap();
    }

    let db = BicDb::open_with_encryption(
        temp.path(),
        plain_config().with_fsync(false),
        encrypted_config(),
    )
    .unwrap();
    assert!(db.get("patients", "legacy").unwrap().is_some());
    assert!(db.get("patients", "encrypted").unwrap().is_some());
    let report = db.verify_integrity().unwrap();
    assert!(report.encrypted_frames > 0);
    assert!(report.unencrypted_frames > 0);
}

#[test]
fn tampered_encrypted_segment_is_detected_by_strict_verification() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_encryption(temp.path(), db_config(), encrypted_config()).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.compact().unwrap();
        db.close().unwrap();
    }

    let segment = temp.path().join("segments").join("patients.seg");
    let mut bytes = fs::read(&segment).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x55;
    fs::write(&segment, bytes).unwrap();

    assert!(matches!(
        BicDb::verify_path(
            temp.path(),
            plain_config().with_fsync(false),
            Some(encrypted_config())
        )
        .unwrap_err(),
        BicDbError::TamperEvidence { .. }
    ));
}

#[test]
fn encrypted_sync_bundle_round_trips_without_plaintext() {
    let left_temp = tempfile::tempdir().unwrap();
    let right_temp = tempfile::tempdir().unwrap();
    let bundle_temp = tempfile::tempdir().unwrap();
    let bundle_path = bundle_temp.path().join("left.syncbundle");
    let bundle_key = EncryptionConfig::with_passphrase("bundle key");

    let mut left =
        BicDb::open_with_encryption(left_temp.path(), db_config(), encrypted_config()).unwrap();
    left.create_collection("patients").unwrap();
    left.set_collection_mesh_sync_enabled("patients", true)
        .unwrap();
    left.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    left.write_encrypted_sync_bundle_since(
        SyncCheckpoint::default(),
        &bundle_path,
        bundle_key.clone(),
    )
    .unwrap();
    let left_node = left.node_id();
    let left_key = left.mesh_verifying_key().unwrap();

    let bytes = fs::read(&bundle_path).unwrap();
    assert!(!contains(&bytes, b"Asha"));
    assert!(matches!(
        bicdb_core::SyncBundle::read_auto(&bundle_path, None).unwrap_err(),
        BicDbError::EncryptionKeyRequired
    ));

    let mut right =
        BicDb::open_with_encryption(right_temp.path(), db_config(), encrypted_config()).unwrap();
    right.create_collection("patients").unwrap();
    right
        .set_collection_mesh_sync_enabled("patients", true)
        .unwrap();
    right.pin_node_key(&left_node, &left_key).unwrap();
    let report = right
        .import_sync_bundle_file_auto(&bundle_path, Some(bundle_key))
        .unwrap();
    assert_eq!(report.imported_events, 1);
    assert_eq!(
        right.get("patients", "p-1").unwrap().unwrap().metadata["name"],
        "Asha"
    );
}

fn all_file_bytes(path: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            bytes.extend(all_file_bytes(&path));
        } else {
            bytes.extend(fs::read(path).unwrap());
        }
    }
    bytes
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
