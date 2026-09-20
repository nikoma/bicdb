//! Signed-frame tests: origin authenticity through relays. The property
//! under test is the role separation — authorized transporter, unauthorized
//! reader (Phase 2), unauthorized author: transporting bytes must never
//! confer authorship, and a rewritten payload (including causality
//! metadata) must fail verification at any pinned receiver.

use bicdb_core::{BicDb, DbConfig, NodeId, Record, SyncCheckpoint};
use ed25519_dalek::SigningKey;
use serde_json::json;
use uuid::Uuid;

fn node(value: u128) -> NodeId {
    NodeId(Uuid::from_u128(value))
}

fn signing_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
        .with_mesh_signing(true)
        .with_unsafe_legacy_mesh_collections(true)
}

fn plain_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
        .with_mesh_signing(false)
        .with_require_signed_imports(false)
        .with_unsafe_legacy_mesh_collections(true)
}

#[test]
fn mesh_signing_and_strict_imports_are_secure_by_default() {
    let config = DbConfig::default();
    assert!(config.mesh_signing);
    assert!(config.require_signed_imports);

    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_node_id(dir.path(), config.with_fsync(false), node(1)).unwrap();
    assert!(db.mesh_verifying_key().is_some());
}

#[test]
fn secure_mesh_import_requires_an_enabled_preprovisioned_matching_collection() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mismatch_dir = tempfile::tempdir().unwrap();
    let mut source =
        BicDb::open_with_node_id(source_dir.path(), signing_config(), node(1)).unwrap();
    let secure_config = DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true);
    let mut target =
        BicDb::open_with_node_id(target_dir.path(), secure_config.clone(), node(2)).unwrap();
    let mut mismatch =
        BicDb::open_with_node_id(mismatch_dir.path(), secure_config, node(3)).unwrap();
    let source_key = source.mesh_verifying_key().unwrap();
    target.pin_node_key(&node(1), &source_key).unwrap();
    mismatch.pin_node_key(&node(1), &source_key).unwrap();

    source.create_collection("patients").unwrap();
    source.insert("patients", Record::new("p-1")).unwrap();
    let bundle = source
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();

    let error = target
        .import_sync_bundle(bundle.clone())
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown") || error.contains("mesh-disabled"));

    target.create_collection("patients").unwrap();
    let error = target
        .import_sync_bundle(bundle.clone())
        .unwrap_err()
        .to_string();
    assert!(error.contains("mesh-disabled"), "unexpected error: {error}");

    target
        .set_collection_mesh_sync_enabled("patients", true)
        .unwrap();
    assert!(
        target
            .import_sync_bundle(bundle.clone())
            .unwrap()
            .imported_events
            > 0
    );
    assert!(target.get("patients", "p-1").unwrap().is_some());

    mismatch.create_timeseries_collection("patients").unwrap();
    mismatch
        .set_collection_mesh_sync_enabled("patients", true)
        .unwrap();
    let error = mismatch.import_sync_bundle(bundle).unwrap_err().to_string();
    assert!(
        error.contains("mode does not match"),
        "unexpected error: {error}"
    );
    assert!(mismatch.get("patients", "p-1").unwrap().is_none());
}

#[test]
fn a_signing_key_persists_across_reopen_and_signs_exports() {
    let dir = tempfile::tempdir().unwrap();
    let key = {
        let mut a = BicDb::open_with_node_id(dir.path(), signing_config(), node(1)).unwrap();
        a.create_collection("patients").unwrap();
        a.insert("patients", Record::new("p-1")).unwrap();
        let bundle = a
            .export_sync_bundle_since(SyncCheckpoint::default())
            .unwrap();
        assert!(bundle
            .events
            .iter()
            .all(|entry| entry.envelope.signature.is_some()));
        a.close().unwrap();
        BicDb::open_with_node_id(dir.path(), signing_config(), node(1))
            .unwrap()
            .mesh_verifying_key()
            .unwrap()
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir.path().join("mesh_signing_key.json"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let reopened = BicDb::open_with_node_id(dir.path(), signing_config(), node(1)).unwrap();
    assert_eq!(reopened.mesh_verifying_key().unwrap(), key);
}

#[test]
fn pinned_receivers_verify_and_tampered_causality_is_refused() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), signing_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), plain_config(), node(2)).unwrap();
    b.pin_node_key(&node(1), &a.mesh_verifying_key().unwrap())
        .unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    let bundle = a
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();

    // Intact bundle verifies and imports.
    let report = b.import_sync_bundle(bundle.clone()).unwrap();
    assert!(report.imported_events > 0);

    // A malicious relay rewrites the causality metadata and reassembles a
    // perfectly checksummed bundle. The origin signature still refuses it.
    let mut tampered_events = bundle.events.clone();
    tampered_events[0].event.payload["write_context"] =
        json!({"origins": {"00000000-0000-0000-0000-000000000009": 999_999}});
    let tampered = bicdb_core::SyncBundle::new(
        bundle.source_node_id.clone(),
        bundle.from_checkpoint,
        bundle.next_checkpoint,
        tampered_events,
    )
    .unwrap();
    let error = b.import_sync_bundle(tampered).unwrap_err().to_string();
    assert!(
        error.contains("origin signature"),
        "unexpected error: {error}"
    );

    // Event type is part of the v2 origin signature. Reassembling a valid
    // bundle checksum cannot turn a signed create into another event kind.
    let mut type_tampered_events = bundle.events.clone();
    type_tampered_events[0].event.event_type = "RecordDeleted".to_string();
    let type_tampered = bicdb_core::SyncBundle::new(
        bundle.source_node_id.clone(),
        bundle.from_checkpoint,
        bundle.next_checkpoint,
        type_tampered_events,
    )
    .unwrap();
    let error = b.import_sync_bundle(type_tampered).unwrap_err().to_string();
    assert!(
        error.contains("origin signature"),
        "unexpected error: {error}"
    );

    // Signature stripping is covered both by the bundle checksum and by the
    // pinned-origin verifier, even when legacy unsigned imports are enabled.
    let mut checksum_tampered = bundle.clone();
    checksum_tampered.events[0].envelope.signature = None;
    assert!(checksum_tampered.verify().is_err());

    let mut stripped_events = bundle.events.clone();
    stripped_events[0].envelope.signature = None;
    let stripped = bicdb_core::SyncBundle::new(
        bundle.source_node_id.clone(),
        bundle.from_checkpoint,
        bundle.next_checkpoint,
        stripped_events,
    )
    .unwrap();
    let error = b.import_sync_bundle(stripped).unwrap_err().to_string();
    assert!(
        error.contains("unsigned event") && error.contains("pinned origin"),
        "unexpected error: {error}"
    );
}

#[test]
fn signatures_survive_relay_and_verify_two_hops_from_the_origin() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut a = BicDb::open_with_node_id(dirs[0].path(), signing_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dirs[1].path(), plain_config(), node(2)).unwrap();
    let mut c = BicDb::open_with_node_id(dirs[2].path(), plain_config(), node(3)).unwrap();
    // C has pinned only A — it has never met B and extends B no authorship
    // trust at all.
    c.pin_node_key(&node(1), &a.mesh_verifying_key().unwrap())
        .unwrap();

    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-1")).unwrap();

    // A → B → C by vector deltas; B is a pure transporter.
    let to_b = a
        .export_sync_bundle_delta(&b.sync_vector().unwrap())
        .unwrap();
    b.import_sync_bundle(to_b).unwrap();
    let relayed = b
        .export_sync_bundle_delta(&c.sync_vector().unwrap())
        .unwrap();
    let a_entries: Vec<_> = relayed
        .events
        .iter()
        .filter(|entry| entry.envelope.node_id == node(1))
        .collect();
    assert!(!a_entries.is_empty());
    assert!(a_entries
        .iter()
        .all(|entry| entry.envelope.signature.is_some()));

    // C verifies A's signatures on import even though the bytes came from B.
    let report = c.import_sync_bundle(relayed).unwrap();
    assert!(report.imported_events > 0);
    assert!(c.get("patients", "p-1").unwrap().is_some());
}

#[test]
fn require_signed_imports_refuses_unsigned_and_unpinned_events() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let dir_strict = tempfile::tempdir().unwrap();
    let mut unsigned = BicDb::open_with_node_id(dir_a.path(), plain_config(), node(1)).unwrap();
    let mut signed = BicDb::open_with_node_id(dir_b.path(), signing_config(), node(2)).unwrap();
    let mut strict = BicDb::open_with_node_id(
        dir_strict.path(),
        plain_config().with_require_signed_imports(true),
        node(3),
    )
    .unwrap();

    unsigned.create_collection("patients").unwrap();
    unsigned.insert("patients", Record::new("p-plain")).unwrap();
    let unsigned_bundle = unsigned
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    let error = strict
        .import_sync_bundle(unsigned_bundle)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unsigned event"),
        "unexpected error: {error}"
    );

    // Signed but unpinned: still refused under strict mode — a signature
    // nobody can check proves nothing.
    signed.create_collection("patients").unwrap();
    signed.insert("patients", Record::new("p-signed")).unwrap();
    let signed_bundle = signed
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    let error = strict
        .import_sync_bundle(signed_bundle.clone())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unpinned origin"),
        "unexpected error: {error}"
    );

    // Pin the key and the same bundle imports.
    strict
        .pin_node_key(&node(2), &signed.mesh_verifying_key().unwrap())
        .unwrap();
    assert!(
        strict
            .import_sync_bundle(signed_bundle)
            .unwrap()
            .imported_events
            > 0
    );
}

#[test]
fn conflicting_key_pins_are_alarms_not_updates() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_node_id(dir.path(), plain_config(), node(1)).unwrap();
    let key_a = hex::encode(
        SigningKey::from_bytes(&[1_u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let key_b = hex::encode(
        SigningKey::from_bytes(&[2_u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    db.pin_node_key(&node(9), &key_a).unwrap();
    db.pin_node_key(&node(9), &key_a).unwrap();
    let error = db.pin_node_key(&node(9), &key_b).unwrap_err().to_string();
    assert!(error.contains("conflicting"), "unexpected error: {error}");
    assert_eq!(db.pinned_node_key(&node(9)), Some(key_a));
}

#[test]
fn forged_origin_without_the_key_cannot_survive_a_pinned_receiver() {
    // An attacker (node 666) crafts events CLAIMING node 1 as origin, but
    // cannot produce node 1's signature. A strict receiver that has pinned
    // node 1 refuses the forgery.
    let dir_attacker = tempfile::tempdir().unwrap();
    let dir_victim = tempfile::tempdir().unwrap();
    let dir_receiver = tempfile::tempdir().unwrap();
    let mut attacker =
        BicDb::open_with_node_id(dir_attacker.path(), plain_config(), node(666)).unwrap();
    let victim = BicDb::open_with_node_id(dir_victim.path(), signing_config(), node(1)).unwrap();
    let mut receiver = BicDb::open_with_node_id(
        dir_receiver.path(),
        plain_config().with_require_signed_imports(true),
        node(3),
    )
    .unwrap();
    receiver
        .pin_node_key(&node(1), &victim.mesh_verifying_key().unwrap())
        .unwrap();

    attacker.create_collection("patients").unwrap();
    attacker
        .insert(
            "patients",
            Record::new("p-1").with_metadata(json!({"phone": "attacker"})),
        )
        .unwrap();
    let mut forged = attacker
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    // Claim the victim as origin (unsigned — the attacker cannot sign).
    let mut events = forged.events.clone();
    for entry in &mut events {
        entry.envelope.node_id = node(1);
    }
    forged = bicdb_core::SyncBundle::new(
        node(1),
        forged.from_checkpoint,
        forged.next_checkpoint,
        events,
    )
    .unwrap();

    let error = receiver.import_sync_bundle(forged).unwrap_err().to_string();
    assert!(
        error.contains("unsigned event"),
        "unexpected error: {error}"
    );
}

#[test]
fn write_context_cannot_claim_origin_positions_absent_from_received_history() {
    let source_dir = tempfile::tempdir().unwrap();
    let receiver_dir = tempfile::tempdir().unwrap();
    let mut source = BicDb::open_with_node_id(source_dir.path(), plain_config(), node(1)).unwrap();
    let mut receiver =
        BicDb::open_with_node_id(receiver_dir.path(), plain_config(), node(2)).unwrap();
    source.create_collection("patients").unwrap();
    source.insert("patients", Record::new("p-1")).unwrap();
    let bundle = source
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    let mut events = bundle.events.clone();
    events[0].event.payload["write_context"] = json!({
        "origins": {"00000000-0000-0000-0000-000000000009": u64::MAX}
    });
    let forged = bicdb_core::SyncBundle::new(
        bundle.source_node_id,
        bundle.from_checkpoint,
        bundle.next_checkpoint,
        events,
    )
    .unwrap();
    let error = receiver.import_sync_bundle(forged).unwrap_err().to_string();
    assert!(
        error.contains("unproven write_context"),
        "unexpected error: {error}"
    );
    assert!(receiver.get("patients", "p-1").is_err());
}
