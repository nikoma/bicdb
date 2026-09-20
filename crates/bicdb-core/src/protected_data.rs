use std::collections::BTreeSet;
use std::path::Path;

use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::Sha256;

use crate::encryption::NONCE_LEN;
use crate::error::{BicDbError, Result};
use crate::record::{BlindIndexPolicy, CollectionPolicy, ColumnSecurity, Record, RedactionPolicy};

type HmacSha256 = Hmac<Sha256>;

const FIELD_KEY_ENV: &str = "BICDB_PROTECTED_FIELD_ENCRYPTION_KEY";
const LEGACY_FIELD_KEY_ENV: &str = "PHI_FIELD_ENCRYPTION_KEY";
const LOOKUP_KEY_ENV: &str = "BICDB_PROTECTED_LOOKUP_HMAC_KEY";
const LEGACY_LOOKUP_KEY_ENV: &str = "PHI_LOOKUP_HMAC_KEY";
// The persisted marker is immutable storage compatibility, not an
// industry-specific public API. Existing encrypted databases must remain
// readable after the terminology change.
const ENVELOPE_MARKER: &str = "__bicdb_phi";
const LEGACY_PATH_ENVELOPE_VERSION: u32 = 1;
const ENVELOPE_VERSION: u32 = 2;
const FIELD_ALGORITHM: &str = "xchacha20poly1305";
const BLIND_INDEX_PREFIX: &str = "__bicdb_blind_index__";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ProtectedDataEnvelope {
    #[serde(rename = "__bicdb_phi")]
    marker: u32,
    algorithm: String,
    key_ref: String,
    key_version: u32,
    nonce: String,
    ciphertext: String,
}

pub(crate) fn blind_index_field(field: &str) -> String {
    format!("{BLIND_INDEX_PREFIX}{field}")
}

pub(crate) fn validate_policy(policy: &CollectionPolicy) -> Result<()> {
    if (policy.protected || policy.columns.values().any(|column| column.encrypted))
        && policy.tenant_field.trim().is_empty()
    {
        return Err(BicDbError::Authorization(
            "protected data policy requires a tenant_field".to_string(),
        ));
    }

    for (field, security) in &policy.columns {
        if field.trim().is_empty() {
            return Err(BicDbError::Authorization(
                "column security metadata requires non-empty field names".to_string(),
            ));
        }
        if security.encrypted && security.key_ref.as_deref().is_some_and(str::is_empty) {
            return Err(BicDbError::Authorization(format!(
                "encrypted field `{field}` has an empty key_ref"
            )));
        }
        if let Some(blind_index) = &security.blind_index {
            validate_blind_index(field, blind_index)?;
        }
    }
    Ok(())
}

pub(crate) fn requires_field_key(policy: &CollectionPolicy) -> bool {
    policy.protected || policy.columns.values().any(|column| column.encrypted)
}

pub(crate) fn requires_lookup_key(policy: &CollectionPolicy) -> bool {
    policy
        .columns
        .values()
        .any(|column| column.encrypted && column.blind_index.is_some())
}

pub(crate) fn encrypted_field(policy: &CollectionPolicy, path: &[String]) -> Option<String> {
    if path.len() == 1 {
        let field = &path[0];
        policy
            .columns
            .get(field)
            .filter(|security| security.encrypted)
            .map(|_| field.clone())
    } else {
        None
    }
}

/// Whether an indexed path is itself security-classified.
///
/// A collection is marked `protected` as soon as any one of its columns carries security
/// metadata. That is the right granularity for record handling, but not for index admission:
/// a business collection routinely holds one contact or government-id column alongside the
/// ordinary tenant, code and scope columns its unique constraints are built on, and a unique
/// index over unclassified columns is not an oracle for the classified one. Only the classified
/// paths need the blind-index rule.
pub(crate) fn is_security_classified(policy: &CollectionPolicy, path: &[String]) -> bool {
    if path.len() != 1 {
        return false;
    }
    policy.columns.get(&path[0]).is_some_and(|security| {
        security.encrypted
            || security.pii_category.is_some()
            || security.key_ref.is_some()
            || security.blind_index.is_some()
            || !security.decrypt_roles.is_empty()
            || security.redaction != RedactionPolicy::Null
    })
}

pub(crate) fn is_approved_blind_index(policy: &CollectionPolicy, path: &[String]) -> bool {
    if path.len() != 1 {
        return false;
    }
    policy.columns.iter().any(|(field, security)| {
        security.blind_index.is_some() && path[0] == blind_index_field(field)
    })
}

pub(crate) fn prepare_record(
    database_id: &str,
    collection: &str,
    policy: &CollectionPolicy,
    record: &mut Record,
) -> Result<()> {
    if requires_field_key(policy) {
        field_key()?;
    }
    if requires_lookup_key(policy) {
        lookup_key()?;
    }

    let tenant_id = record
        .metadata
        .get(&policy.tenant_field)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            BicDbError::Authorization(format!(
                "protected data write requires tenant field `{}`",
                policy.tenant_field
            ))
        })?
        .to_string();

    let Some(metadata) = record.metadata.as_object_mut() else {
        return Err(BicDbError::Authorization(
            "protected data writes require object metadata".to_string(),
        ));
    };
    if policy.protected && record.vector.is_some() && !policy.vector_non_sensitive {
        return Err(BicDbError::Authorization(
            "protected data collection vectors must be declared non-sensitive".to_string(),
        ));
    }

    for (field, security) in &policy.columns {
        if !security.encrypted {
            continue;
        }
        let Some(value) = metadata.get(field).cloned() else {
            continue;
        };
        if envelope_from_value(&value).is_ok() {
            continue;
        }
        if let Some(blind_index) = &security.blind_index {
            metadata.insert(
                blind_index_field(field),
                Value::String(compute_blind_index(
                    blind_index,
                    database_id,
                    &tenant_id,
                    &value,
                )?),
            );
        }
        let aad = aad(
            database_id,
            collection,
            &record.id,
            &tenant_id,
            policy.schema_version,
            key_version(security),
            field,
        );
        let envelope = encrypt_value(security, &value, &aad)?;
        metadata.insert(field.clone(), serde_json::to_value(envelope)?);
    }

    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BackfilledRecord {
    pub encrypted_fields: usize,
    pub blind_indexes_written: usize,
    pub skipped_fields: usize,
}

pub(crate) fn backfill_record(
    database_id: &str,
    legacy_db_path: &Path,
    collection: &str,
    policy: &CollectionPolicy,
    record: &mut Record,
) -> Result<BackfilledRecord> {
    if requires_field_key(policy) {
        field_key()?;
    }
    if requires_lookup_key(policy) {
        lookup_key()?;
    }

    let tenant_id = record
        .metadata
        .get(&policy.tenant_field)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            BicDbError::Authorization(format!(
                "record `{}` is missing tenant field `{}`",
                record.id, policy.tenant_field
            ))
        })?
        .to_string();
    let Some(metadata) = record.metadata.as_object_mut() else {
        return Err(BicDbError::Authorization(format!(
            "record `{}` protected data metadata must be an object",
            record.id
        )));
    };

    let mut report = BackfilledRecord::default();
    for (field, security) in &policy.columns {
        if !security.encrypted {
            report.skipped_fields += 1;
            continue;
        }
        let Some(value) = metadata.get(field).cloned() else {
            report.skipped_fields += 1;
            continue;
        };

        let plaintext = match envelope_from_value(&value) {
            Ok(envelope) => {
                let old_aad = aad(
                    &envelope_database_id(&envelope, database_id, legacy_db_path),
                    collection,
                    &record.id,
                    &tenant_id,
                    policy.schema_version,
                    envelope.key_version,
                    field,
                );
                let plaintext = decrypt_value(&envelope, &old_aad)?;
                if envelope.marker == LEGACY_PATH_ENVELOPE_VERSION {
                    let new_aad = aad(
                        database_id,
                        collection,
                        &record.id,
                        &tenant_id,
                        policy.schema_version,
                        key_version(security),
                        field,
                    );
                    let migrated = encrypt_value(security, &plaintext, &new_aad)?;
                    metadata.insert(field.clone(), serde_json::to_value(migrated)?);
                    report.encrypted_fields += 1;
                }
                plaintext
            }
            Err(_) => {
                let aad = aad(
                    database_id,
                    collection,
                    &record.id,
                    &tenant_id,
                    policy.schema_version,
                    key_version(security),
                    field,
                );
                let envelope = encrypt_value(security, &value, &aad)?;
                metadata.insert(field.clone(), serde_json::to_value(envelope)?);
                report.encrypted_fields += 1;
                value
            }
        };

        if let Some(blind_index) = &security.blind_index {
            let blind_field = blind_index_field(field);
            let token = Value::String(compute_blind_index(
                blind_index,
                database_id,
                &tenant_id,
                &plaintext,
            )?);
            if metadata.get(&blind_field) != Some(&token) {
                metadata.insert(blind_field, token);
                report.blind_indexes_written += 1;
            }
        }
    }

    Ok(report)
}

pub(crate) fn is_envelope(value: &Value) -> bool {
    envelope_from_value(value).is_ok()
}

pub(crate) fn has_plaintext(value: &Value, plaintext: &str) -> bool {
    match value {
        Value::String(value) => value.contains(plaintext),
        Value::Array(values) => values.iter().any(|value| has_plaintext(value, plaintext)),
        Value::Object(object) => object.values().any(|value| has_plaintext(value, plaintext)),
        _ => false,
    }
}

pub(crate) fn rotate_record(
    database_id: &str,
    legacy_db_path: &Path,
    collection: &str,
    policy: &mut CollectionPolicy,
    record: &mut Record,
    old_key_material: &str,
    new_key_material: &str,
    new_key_ref: &str,
) -> Result<usize> {
    let tenant_id = record
        .metadata
        .get(&policy.tenant_field)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            BicDbError::Authorization(format!(
                "record `{}` is missing tenant field `{}`",
                record.id, policy.tenant_field
            ))
        })?
        .to_string();
    let Some(metadata) = record.metadata.as_object_mut() else {
        return Ok(0);
    };
    let mut rotated = 0;
    for (field, security) in &mut policy.columns {
        if !security.encrypted {
            continue;
        }
        let Some(value) = metadata.get(field).cloned() else {
            continue;
        };
        let old_envelope = envelope_from_value(&value)?;
        let old_aad = aad(
            &envelope_database_id(&old_envelope, database_id, legacy_db_path),
            collection,
            &record.id,
            &tenant_id,
            policy.schema_version,
            old_envelope.key_version,
            field,
        );
        let plaintext = decrypt_value_with_key(
            &old_envelope,
            &old_aad,
            material_key(old_key_material, FIELD_KEY_ENV)?,
        )?;
        security.key_ref = Some(new_key_ref.to_string());
        let new_aad = aad(
            database_id,
            collection,
            &record.id,
            &tenant_id,
            policy.schema_version,
            key_version(security),
            field,
        );
        let new_envelope = encrypt_value_with_key(
            security,
            &plaintext,
            &new_aad,
            material_key(new_key_material, FIELD_KEY_ENV)?,
        )?;
        decrypt_value_with_key(
            &new_envelope,
            &new_aad,
            material_key(new_key_material, FIELD_KEY_ENV)?,
        )?;
        metadata.insert(field.clone(), serde_json::to_value(new_envelope)?);
        rotated += 1;
    }
    Ok(rotated)
}

pub(crate) fn project_record(
    database_id: &str,
    legacy_db_path: &Path,
    collection: &str,
    policy: &CollectionPolicy,
    roles: &BTreeSet<String>,
    record: &Record,
    diagnostics: bool,
) -> Result<Record> {
    let mut projected = record.clone();
    let tenant_id = projected
        .metadata
        .get(&policy.tenant_field)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            BicDbError::Authorization(format!(
                "record `{}` is missing tenant field `{}`",
                record.id, policy.tenant_field
            ))
        })?
        .to_string();
    let Some(metadata) = projected.metadata.as_object_mut() else {
        return Ok(projected);
    };

    for (field, security) in &policy.columns {
        if !security.encrypted {
            continue;
        }
        let Some(value) = metadata.get(field).cloned() else {
            continue;
        };
        let envelope = envelope_from_value(&value)?;
        if diagnostics {
            continue;
        }
        if !security.decrypt_roles.is_empty() && roles.is_disjoint(&security.decrypt_roles) {
            metadata.insert(field.clone(), redacted_value(&security.redaction));
            continue;
        }
        let aad = aad(
            &envelope_database_id(&envelope, database_id, legacy_db_path),
            collection,
            &record.id,
            &tenant_id,
            policy.schema_version,
            envelope.key_version,
            field,
        );
        metadata.insert(field.clone(), decrypt_value(&envelope, &aad)?);
    }
    Ok(projected)
}

pub(crate) fn blind_index_value(
    namespace: &str,
    database_id: &str,
    tenant_id: &str,
    value: &Value,
) -> Result<String> {
    compute_blind_index(
        &BlindIndexPolicy {
            namespace: namespace.to_string(),
            version: 1,
        },
        database_id,
        tenant_id,
        value,
    )
}

fn validate_blind_index(field: &str, blind_index: &BlindIndexPolicy) -> Result<()> {
    let namespace = blind_index.namespace.trim();
    if namespace.is_empty() || !namespace.ends_with(":") || !namespace.contains(":v") {
        return Err(BicDbError::Authorization(format!(
            "blind index for `{field}` requires a namespaced version prefix like `patient-email:v1:`"
        )));
    }
    Ok(())
}

fn encrypt_value(
    security: &ColumnSecurity,
    value: &Value,
    aad: &[u8],
) -> Result<ProtectedDataEnvelope> {
    encrypt_value_with_key(security, value, aad, field_key()?)
}

fn encrypt_value_with_key(
    security: &ColumnSecurity,
    value: &Value,
    aad: &[u8],
    key: [u8; 32],
) -> Result<ProtectedDataEnvelope> {
    let mut nonce = [0_u8; NONCE_LEN];
    chacha20poly1305::aead::rand_core::RngCore::fill_bytes(&mut OsRng, &mut nonce);
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = serde_json::to_vec(value)?;
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: &plaintext,
                aad,
            },
        )
        .map_err(|_| {
            BicDbError::EncryptionKeyInvalid("protected data field encryption failed".to_string())
        })?;
    Ok(ProtectedDataEnvelope {
        marker: ENVELOPE_VERSION,
        algorithm: FIELD_ALGORITHM.to_string(),
        key_ref: security
            .key_ref
            .clone()
            .unwrap_or_else(|| format!("env:{FIELD_KEY_ENV}")),
        key_version: key_version(security),
        nonce: hex::encode(nonce),
        ciphertext: hex::encode(ciphertext),
    })
}

fn decrypt_value(envelope: &ProtectedDataEnvelope, aad: &[u8]) -> Result<Value> {
    decrypt_value_with_key(envelope, aad, field_key()?)
}

fn decrypt_value_with_key(
    envelope: &ProtectedDataEnvelope,
    aad: &[u8],
    key: [u8; 32],
) -> Result<Value> {
    if !matches!(
        envelope.marker,
        LEGACY_PATH_ENVELOPE_VERSION | ENVELOPE_VERSION
    ) || envelope.algorithm != FIELD_ALGORITHM
    {
        return Err(BicDbError::DecryptionFailed {
            path: Path::new("<protected_data-field>").to_path_buf(),
            message: "unsupported protected data field envelope".to_string(),
        });
    }
    let nonce = hex::decode(&envelope.nonce).map_err(|_| BicDbError::DecryptionFailed {
        path: Path::new("<protected_data-field>").to_path_buf(),
        message: "invalid protected data field nonce".to_string(),
    })?;
    let ciphertext =
        hex::decode(&envelope.ciphertext).map_err(|_| BicDbError::DecryptionFailed {
            path: Path::new("<protected_data-field>").to_path_buf(),
            message: "invalid protected data field ciphertext".to_string(),
        })?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = cipher
        .decrypt(
            XNonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: &ciphertext,
                aad,
            },
        )
        .map_err(|_| BicDbError::DecryptionFailed {
            path: Path::new("<protected_data-field>").to_path_buf(),
            message: "protected data field authentication failed".to_string(),
        })?;
    serde_json::from_slice(&plaintext).map_err(Into::into)
}

fn compute_blind_index(
    policy: &BlindIndexPolicy,
    database_id: &str,
    tenant_id: &str,
    value: &Value,
) -> Result<String> {
    let key = lookup_key()?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(&key).map_err(|_| {
        BicDbError::EncryptionKeyInvalid("invalid protected data lookup key".to_string())
    })?;
    mac.update(b"bicdb.protected_data.blind-index.v2");
    mac.update(&[0]);
    mac.update(database_id.as_bytes());
    mac.update(&[0]);
    mac.update(tenant_id.as_bytes());
    mac.update(&[0]);
    mac.update(policy.namespace.as_bytes());
    mac.update(&[0]);
    mac.update(&policy.version.to_be_bytes());
    mac.update(&[0]);
    mac.update(canonical_lookup_value(value).as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn canonical_lookup_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.trim().to_ascii_lowercase(),
        _ => value.to_string(),
    }
}

fn envelope_from_value(value: &Value) -> Result<ProtectedDataEnvelope> {
    if value
        .as_object()
        .and_then(|object| object.get(ENVELOPE_MARKER))
        .is_none()
    {
        return Err(BicDbError::DecryptionFailed {
            path: Path::new("<protected_data-field>").to_path_buf(),
            message: "encrypted protected data field is missing its envelope".to_string(),
        });
    }
    let envelope: ProtectedDataEnvelope = serde_json::from_value(value.clone())?;
    if !matches!(
        envelope.marker,
        LEGACY_PATH_ENVELOPE_VERSION | ENVELOPE_VERSION
    ) || envelope.algorithm != FIELD_ALGORITHM
    {
        return Err(BicDbError::DecryptionFailed {
            path: Path::new("<protected_data-field>").to_path_buf(),
            message: "unsupported protected data field envelope".to_string(),
        });
    }
    Ok(envelope)
}

fn envelope_database_id<'a>(
    envelope: &ProtectedDataEnvelope,
    database_id: &'a str,
    legacy_db_path: &'a Path,
) -> std::borrow::Cow<'a, str> {
    if envelope.marker == LEGACY_PATH_ENVELOPE_VERSION {
        legacy_db_path.to_string_lossy()
    } else {
        std::borrow::Cow::Borrowed(database_id)
    }
}

fn redacted_value(redaction: &RedactionPolicy) -> Value {
    match redaction {
        RedactionPolicy::Null => Value::Null,
        RedactionPolicy::Fixed(value) => Value::String(value.clone()),
        RedactionPolicy::Ciphertext => json!({"redacted": "ciphertext"}),
    }
}

fn key_version(security: &ColumnSecurity) -> u32 {
    security
        .key_ref
        .as_deref()
        .and_then(|key_ref| key_ref.rsplit(":v").next())
        .and_then(|version| version.parse::<u32>().ok())
        .unwrap_or(1)
}

fn aad(
    database_id: &str,
    collection: &str,
    record_id: &str,
    tenant_id: &str,
    schema_version: u32,
    key_version: u32,
    field: &str,
) -> Vec<u8> {
    let mut object = Map::new();
    object.insert(
        "database_id".to_string(),
        Value::String(database_id.to_string()),
    );
    object.insert(
        "collection".to_string(),
        Value::String(collection.to_string()),
    );
    object.insert(
        "record_id".to_string(),
        Value::String(record_id.to_string()),
    );
    object.insert(
        "tenant_id".to_string(),
        Value::String(tenant_id.to_string()),
    );
    object.insert("schema_version".to_string(), json!(schema_version));
    object.insert("key_version".to_string(), json!(key_version));
    object.insert("field".to_string(), Value::String(field.to_string()));
    serde_json::to_vec(&Value::Object(object)).expect("protected data AAD is serializable")
}

fn field_key() -> Result<[u8; 32]> {
    env_key_with_legacy(FIELD_KEY_ENV, LEGACY_FIELD_KEY_ENV)
}

fn lookup_key() -> Result<[u8; 32]> {
    env_key_with_legacy(LOOKUP_KEY_ENV, LEGACY_LOOKUP_KEY_ENV)
}

fn env_key_with_legacy(name: &str, legacy_name: &str) -> Result<[u8; 32]> {
    if let Ok(value) = std::env::var(name) {
        return material_key(&value, name);
    }
    let value = std::env::var(legacy_name).map_err(|_| BicDbError::EncryptionKeyRequired)?;
    material_key(&value, legacy_name)
}

fn material_key(value: &str, name: &str) -> Result<[u8; 32]> {
    let value = value.trim();
    if value.is_empty() {
        return Err(BicDbError::EncryptionKeyRequired);
    }
    let value = value.strip_prefix("hex:").unwrap_or(value);
    let decoded = hex::decode(value).map_err(|_| {
        BicDbError::EncryptionKeyInvalid(format!(
            "{name} must contain exactly 32 random bytes encoded as 64 hexadecimal characters"
        ))
    })?;
    decoded.try_into().map_err(|_| {
        BicDbError::EncryptionKeyInvalid(format!(
            "{name} must contain exactly 32 random bytes encoded as 64 hexadecimal characters"
        ))
    })
}
