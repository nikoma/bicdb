//! Mesh convergence torture suite: the Phase 0 acceptance gate for BicDB
//! Mesh. Three completely offline devices collaboratively complete one
//! patient encounter and deterministically converge — then the same
//! machinery survives partitions, relays, duplicate bundles, interrupted
//! transfers, skewed clocks, and a reboot mid-import. No transports here on
//! purpose: if convergence is wrong, radios only deliver the wrong answer
//! faster.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{
    BicDb, DbConfig, Event, NodeId, Record, SyncBundle, SyncCheckpoint, SyncImportReport,
    SyncVector, RECORD_AUDIT_STREAM,
};
use serde_json::json;
use uuid::Uuid;

fn node(value: u128) -> NodeId {
    NodeId(Uuid::from_u128(value))
}

fn sync_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
        .with_mesh_signing(false)
        .with_require_signed_imports(false)
        .with_unsafe_legacy_mesh_collections(true)
}

/// One direction of the mesh wire protocol: `target` announces its vector,
/// `source` answers with exactly the delta, `target` imports it.
fn sync_into(target: &mut BicDb, source: &mut BicDb) -> SyncImportReport {
    let vector = target.sync_vector().unwrap();
    let bundle = source.export_sync_bundle_delta(&vector).unwrap();
    target.import_sync_bundle(bundle).unwrap()
}

fn event_ids(db: &BicDb) -> BTreeSet<Uuid> {
    db.export_events_since(0)
        .into_iter()
        .map(|stored| stored.event.id)
        .collect()
}

fn sorted_records(db: &BicDb, collection: &str) -> Vec<Record> {
    let mut records = db.scan_collection(collection).unwrap();
    records.sort_by(|left, right| left.id.cmp(&right.id));
    records
}

fn assert_converged(dbs: &[&BicDb], collections: &[&str]) {
    let reference_events = event_ids(dbs[0]);
    let reference_vector = dbs[0].sync_vector().unwrap();
    for db in &dbs[1..] {
        assert_eq!(event_ids(db), reference_events, "event logs diverged");
        assert_eq!(
            db.sync_vector().unwrap(),
            reference_vector,
            "sync vectors diverged"
        );
    }
    for collection in collections {
        let reference = sorted_records(dbs[0], collection);
        for db in &dbs[1..] {
            assert_eq!(
                sorted_records(db, collection),
                reference,
                "collection `{collection}` diverged"
            );
        }
    }
}

fn record_updated_event(record_id: &str, metadata: serde_json::Value, timestamp: i64) -> Event {
    Event::new(
        RECORD_AUDIT_STREAM,
        "RecordUpdated",
        json!({
            "collection": "patients",
            "collection_mode": "standard",
            "record_id": record_id,
            "record": Record::new(record_id).with_metadata(metadata),
        }),
    )
    .with_timestamp(timestamp)
}

/// The acceptance test: registration, triage, and doctor devices complete one
/// encounter with no shared server and no pairwise link between all of them —
/// A and B converge through C's relay.
#[test]
fn three_offline_devices_complete_one_patient_encounter_and_converge() {
    let (dir_a, dir_b, dir_c) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let mut registration = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut triage = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();
    let mut doctor = BicDb::open_with_node_id(dir_c.path(), sync_config(), node(3)).unwrap();

    registration.create_collection("patients").unwrap();
    registration
        .insert(
            "patients",
            Record::new("p-1").with_metadata(json!({"name": "Asha", "camp": "rural-7"})),
        )
        .unwrap();

    sync_into(&mut triage, &mut registration);
    assert!(triage.get("patients", "p-1").unwrap().is_some());

    triage.create_collection("observations").unwrap();
    for (id, kind, value) in [
        (
            "obs-bp",
            "blood_pressure",
            json!({"systolic": 145, "diastolic": 92}),
        ),
        ("obs-pulse", "pulse", json!(88)),
        ("obs-spo2", "spo2", json!(97)),
    ] {
        triage
            .insert(
                "observations",
                Record::new(id).with_metadata(json!({
                    "patient": "p-1",
                    "type": kind,
                    "value": value,
                    "taken_by": "triage-nurse",
                })),
            )
            .unwrap();
    }

    // Doctor has only ever met triage; the patient arrives by relay.
    sync_into(&mut doctor, &mut triage);
    assert!(doctor.get("patients", "p-1").unwrap().is_some());
    assert_eq!(doctor.scan_collection("observations").unwrap().len(), 3);

    doctor.create_collection("encounters").unwrap();
    doctor
        .insert(
            "encounters",
            Record::new("enc-1").with_metadata(json!({
                "patient": "p-1",
                "assessment": "stage 1 hypertension",
                "plan": "amlodipine 5mg, review in 2 weeks",
            })),
        )
        .unwrap();

    // Registration has never met triage; observations and the encounter both
    // arrive through the doctor's device.
    sync_into(&mut registration, &mut doctor);
    assert!(registration.get("encounters", "enc-1").unwrap().is_some());
    assert_eq!(
        registration.scan_collection("observations").unwrap().len(),
        3
    );

    sync_into(&mut triage, &mut registration);
    assert_converged(
        &[&registration, &triage, &doctor],
        &["patients", "observations", "encounters"],
    );
}

/// The horrible one: A contributes and disappears, its data travels B→C→D by
/// relay with origin attribution intact, then A returns and catches up from a
/// device it never met — and the reverse delta is empty because the mesh
/// already carried everything A had.
#[test]
fn partitioned_mesh_relays_through_returning_nodes() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut a = BicDb::open_with_node_id(dirs[0].path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dirs[1].path(), sync_config(), node(2)).unwrap();
    let mut c = BicDb::open_with_node_id(dirs[2].path(), sync_config(), node(3)).unwrap();
    let mut d = BicDb::open_with_node_id(dirs[3].path(), sync_config(), node(4)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-100").with_metadata(json!({"name": "Ravi"})),
    )
    .unwrap();
    let a_authored = event_ids(&a);

    sync_into(&mut b, &mut a);
    // A disappears. The rest of the camp keeps working and relaying.
    b.create_collection("notes").unwrap();
    b.insert("notes", Record::new("note-b")).unwrap();
    sync_into(&mut c, &mut b);
    c.insert("notes", Record::new("note-c")).unwrap();
    sync_into(&mut d, &mut c);
    d.insert("notes", Record::new("note-d")).unwrap();

    // Two hops from the source, D still attributes A's events to A.
    let d_view = d.export_sync_bundle_delta(&SyncVector::default()).unwrap();
    let relayed_from_a: BTreeSet<Uuid> = d_view
        .events
        .iter()
        .filter(|entry| entry.envelope.node_id == node(1))
        .map(|entry| entry.envelope.event_id)
        .collect();
    assert_eq!(relayed_from_a, a_authored);

    // A returns and meets only D. It catches up on the whole camp; the
    // reverse direction has nothing to send because the relay chain already
    // delivered everything A authored.
    sync_into(&mut a, &mut d);
    let reverse = a
        .export_sync_bundle_delta(&d.sync_vector().unwrap())
        .unwrap();
    assert_eq!(reverse.event_count, 0);
    let reverse_report = sync_into(&mut d, &mut a);
    assert_eq!(reverse_report.imported_events, 0);

    // A now relays for the nodes that missed the later notes.
    sync_into(&mut b, &mut a);
    sync_into(&mut c, &mut a);
    assert_converged(&[&a, &b, &c, &d], &["patients", "notes"]);
}

#[test]
fn duplicate_bundles_and_multipath_arrivals_are_idempotent() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut a = BicDb::open_with_node_id(dirs[0].path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dirs[1].path(), sync_config(), node(2)).unwrap();
    let mut c = BicDb::open_with_node_id(dirs[2].path(), sync_config(), node(3)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-1")).unwrap();
    a.insert("patients", Record::new("p-2")).unwrap();

    let bundle = a
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    let first = b.import_sync_bundle(bundle.clone()).unwrap();
    assert_eq!(first.imported_events, 2);

    let ids_after_first = event_ids(&b);
    let vector_after_first = b.sync_vector().unwrap();
    // The same bundle arrives six times (flaky link, impatient operator).
    for _ in 0..5 {
        let report = b.import_sync_bundle(bundle.clone()).unwrap();
        assert_eq!(report.imported_events, 0);
        assert_eq!(report.duplicate_events, 2);
    }
    assert_eq!(event_ids(&b), ids_after_first);
    assert_eq!(b.sync_vector().unwrap(), vector_after_first);

    // The same events also arrive over a second path (A→C→B): every one is a
    // duplicate, nothing changes.
    sync_into(&mut c, &mut a);
    let relayed = c.export_sync_bundle_delta(&SyncVector::default()).unwrap();
    let multipath = b.import_sync_bundle(relayed).unwrap();
    assert_eq!(multipath.imported_events, 0);
    assert_eq!(event_ids(&b), ids_after_first);
    assert_converged(&[&a, &b, &c], &["patients"]);
}

#[test]
fn interrupted_and_tampered_transfers_are_refused_without_state_change() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let bundle_dir = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(source_dir.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(target_dir.path(), sync_config(), node(2)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();

    let full_path = bundle_dir.path().join("full.syncbundle");
    a.sync()
        .export_to_path(SyncCheckpoint::default(), &full_path)
        .unwrap();
    let bytes = std::fs::read(&full_path).unwrap();

    // Transfer stops halfway: the file is a JSON prefix, not a bundle.
    let truncated_path = bundle_dir.path().join("truncated.syncbundle");
    std::fs::write(&truncated_path, &bytes[..bytes.len() / 2]).unwrap();
    assert!(b.import_sync_bundle_file(&truncated_path).is_err());
    assert!(event_ids(&b).is_empty());

    // Payload flipped in transit: envelope hashes refuse the bundle whole.
    let mut tampered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    tampered["events"][0]["event"]["payload"]["record"]["metadata"]["name"] = json!("tampered");
    let tampered_path = bundle_dir.path().join("tampered.syncbundle");
    std::fs::write(&tampered_path, serde_json::to_vec(&tampered).unwrap()).unwrap();
    assert!(b.import_sync_bundle_file(&tampered_path).is_err());
    assert!(event_ids(&b).is_empty());

    // The retry with the intact file converges as if nothing had happened.
    b.import_sync_bundle_file(&full_path).unwrap();
    assert_converged(&[&a, &b], &["patients"]);
}

/// A device clock that is 45 minutes slow neither blocks transport nor breaks
/// determinism: both nodes pick the same winner. The winner is chosen by
/// wall-clock-first ordering, so the slow clock loses even though its edit
/// happened later in real time — the documented Phase 0 caveat that motivates
/// field-level versioning for mutable demographics in a later phase.
#[test]
fn skewed_device_clocks_still_converge_deterministically() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    sync_into(&mut b, &mut a);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let base = now + 10_000;
    // A edits at its (correct) clock; B edits 60 real seconds later, but its
    // clock reads 45 minutes behind.
    a.events_mut()
        .append(record_updated_event("p-1", json!({"phone": "222"}), base))
        .unwrap();
    b.events_mut()
        .append(record_updated_event(
            "p-1",
            json!({"phone": "333"}),
            base + 60 - 2_700,
        ))
        .unwrap();

    sync_into(&mut b, &mut a);
    sync_into(&mut a, &mut b);

    let a_patient = a.get("patients", "p-1").unwrap().unwrap();
    let b_patient = b.get("patients", "p-1").unwrap().unwrap();
    assert_eq!(a_patient, b_patient);
    assert_eq!(a_patient.metadata["phone"], "222");
    assert_converged(&[&a, &b], &["patients"]);
}

/// A device reboots mid-import: the durably appended half survives, the
/// recomputed vector reflects exactly what landed, and re-sending the same
/// bundle finishes the job through dedupe.
#[test]
fn reboot_mid_import_recovers_and_resumes_from_the_log() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(source_dir.path(), sync_config(), node(1)).unwrap();

    a.create_collection("patients").unwrap();
    for id in ["p-1", "p-2", "p-3", "p-4", "p-5", "p-6"] {
        a.insert("patients", Record::new(id)).unwrap();
    }
    let full = a
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    assert_eq!(full.event_count, 6);
    let prefix = SyncBundle::new(
        full.source_node_id.clone(),
        full.from_checkpoint,
        SyncCheckpoint::new(3),
        full.events[..3].to_vec(),
    )
    .unwrap();
    let prefix_watermark = prefix.events.last().unwrap().envelope.sequence;

    {
        let mut b = BicDb::open_with_node_id(target_dir.path(), sync_config(), node(2)).unwrap();
        let report = b.import_sync_bundle(prefix).unwrap();
        assert_eq!(report.imported_events, 3);
        // Reboot: dropped without close().
    }

    let mut b = BicDb::open_with_node_id(target_dir.path(), sync_config(), node(2)).unwrap();
    assert_eq!(
        b.sync_vector().unwrap().watermark(&node(1)),
        Some(prefix_watermark)
    );
    assert_eq!(b.scan_collection("patients").unwrap().len(), 3);

    let resumed = b.import_sync_bundle(full).unwrap();
    assert_eq!(resumed.imported_events, 3);
    assert_eq!(resumed.duplicate_events, 3);
    assert_converged(&[&a, &b], &["patients"]);
}

/// Appends a RecordUpdated audit event carrying an explicit wall-clock time
/// and the device's true write context — the shape a device with a broken
/// clock produces: garbage timestamp, honest causality.
fn contexted_update(db: &mut BicDb, record_id: &str, metadata: serde_json::Value, wall_time: i64) {
    let context = db.sync_vector().unwrap();
    let event = Event::new(
        RECORD_AUDIT_STREAM,
        "RecordUpdated",
        json!({
            "collection": "patients",
            "collection_mode": "standard",
            "record_id": record_id,
            "record": Record::new(record_id).with_metadata(metadata),
            "write_context": context,
        }),
    )
    .with_timestamp(wall_time);
    db.events_mut().append(event).unwrap();
}

/// The 1970 phone: its clock is absurd, but its edit was made after
/// receiving the original — causal dominance outranks any wall clock, so the
/// edit survives instead of silently losing to a "newer" timestamp.
#[test]
fn causal_edit_from_a_1970_clock_beats_fresh_wall_clocks() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_c = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut c = BicDb::open_with_node_id(dir_c.path(), sync_config(), node(3)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    sync_into(&mut c, &mut a);

    // C's RTC is dead: it thinks it is 1970. Its edit still causally follows
    // A's create, and the context proves it.
    contexted_update(&mut c, "p-1", json!({"phone": "555"}), 100);

    sync_into(&mut a, &mut c);
    sync_into(&mut c, &mut a);

    assert_eq!(
        a.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
        "555"
    );
    assert_eq!(
        c.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
        "555"
    );
    // A dominated write is not a conflict.
    assert!(a.record_conflict("patients", "p-1").unwrap().is_none());
    assert_converged(&[&a, &c], &["patients"]);
}

/// The manual-clock-jump phone: a device edits its own record after its
/// clock jumped four years backwards. Self-succession is causal, so the
/// newer edit wins everywhere despite the older timestamp.
#[test]
fn own_edit_after_backwards_clock_jump_wins() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"version": "first"})),
    )
    .unwrap();

    let four_years = 4 * 365 * 24 * 3600;
    let jumped_back = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - four_years;
    contexted_update(&mut a, "p-1", json!({"version": "second"}), jumped_back);

    sync_into(&mut b, &mut a);
    sync_into(&mut a, &mut b);

    for db in [&a, &b] {
        assert_eq!(
            db.get("patients", "p-1").unwrap().unwrap().metadata["version"],
            "second"
        );
        assert!(db.record_conflict("patients", "p-1").unwrap().is_none());
    }
}

/// Genuinely concurrent demographic edits: replicas converge on one
/// deterministic projection but surface the concurrency identically instead
/// of presenting the winner as truth — and a later write made after seeing
/// both candidates resolves the conflict on every replica, because the
/// resolution is itself just a replicated, causally dominant event.
#[test]
fn concurrent_edits_surface_identically_and_a_covering_write_resolves() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut a = BicDb::open_with_node_id(dirs[0].path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dirs[1].path(), sync_config(), node(2)).unwrap();
    let mut c = BicDb::open_with_node_id(dirs[2].path(), sync_config(), node(3)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    sync_into(&mut b, &mut a);
    sync_into(&mut c, &mut a);

    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 1_000;
    // Neither edit has seen the other: true concurrency.
    contexted_update(&mut a, "p-1", json!({"phone": "222"}), base + 5);
    contexted_update(&mut b, "p-1", json!({"phone": "333"}), base + 9);

    sync_into(&mut b, &mut a);
    sync_into(&mut a, &mut b);

    let a_conflict = a.record_conflict("patients", "p-1").unwrap().unwrap();
    let b_conflict = b.record_conflict("patients", "p-1").unwrap().unwrap();
    assert_eq!(a_conflict, b_conflict);
    assert_eq!(a_conflict.candidates.len(), 2);
    assert_eq!(
        a_conflict.projected_event_id,
        a_conflict.candidates[0].event_id
    );
    assert_eq!(a.list_record_conflicts("patients").unwrap().len(), 1);
    // Deterministic projection on both replicas (higher timestamp in the
    // concurrent frontier), presented as projection, not truth.
    for db in [&a, &b] {
        assert_eq!(
            db.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
            "333"
        );
    }

    // The doctor's device syncs both candidates, then writes with a context
    // covering the whole frontier: the conflict clears camp-wide.
    sync_into(&mut c, &mut a);
    contexted_update(&mut c, "p-1", json!({"phone": "999"}), base + 30);
    sync_into(&mut a, &mut c);
    sync_into(&mut b, &mut c);
    // C authored the resolving event through the raw test helper, which does
    // not project locally; its next import (even an empty delta) reconciles.
    sync_into(&mut c, &mut b);

    for db in [&a, &b, &c] {
        assert_eq!(
            db.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
            "999"
        );
        assert!(db.record_conflict("patients", "p-1").unwrap().is_none());
        assert!(db.list_record_conflicts("patients").unwrap().is_empty());
    }
    assert_converged(&[&a, &b, &c], &["patients"]);
}

/// Ordinary sequential edits are causal chains, not conflicts: the import
/// report and the conflict API both stay quiet.
#[test]
fn sequential_edits_are_not_conflicts() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "222"})),
    )
    .unwrap();

    let report = sync_into(&mut b, &mut a);
    assert_eq!(report.conflicts_resolved, 0);
    assert_eq!(
        b.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
        "222"
    );
    assert!(b.record_conflict("patients", "p-1").unwrap().is_none());
}

fn clock_observation(observer: &NodeId, peer: &NodeId, offset_ms: i64, error_ms: i64) -> Event {
    Event::new(
        bicdb_core::CLOCK_OBSERVATION_STREAM,
        "ClockObserved",
        json!({
            "observer": observer.to_string(),
            "peer": peer.to_string(),
            "offset_ms": offset_ms,
            "error_ms": error_ms,
        }),
    )
}

/// Peer-controlled timing stays diagnostic and cannot suppress or bias a
/// genuine concurrent-write conflict.
#[test]
fn timing_evidence_cannot_bias_concurrent_edits() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    // A live session measured B's clock 45 minutes (2,700,000 ms) behind A's.
    a.events_mut()
        .append(clock_observation(&node(1), &node(2), -2_700_000, 100))
        .unwrap();
    sync_into(&mut b, &mut a);

    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 1_000;
    // A edits at its (correct) clock; B edits 60 real seconds later, stamped
    // by its slow clock. True concurrency — neither saw the other.
    contexted_update(&mut a, "p-1", json!({"phone": "222"}), base);
    contexted_update(&mut b, "p-1", json!({"phone": "333"}), base + 60 - 2_700);

    let report = sync_into(&mut b, &mut a);
    assert_eq!(report.conflicts_resolved, 1);
    sync_into(&mut a, &mut b);

    for db in [&a, &b] {
        assert_eq!(
            db.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
            "222",
            "peer timing must not override deterministic conflict projection"
        );
        assert!(db.record_conflict("patients", "p-1").unwrap().is_some());
    }
    assert_converged(&[&a, &b], &["patients"]);
}

/// When the corrected difference is inside the uncertainty, the resolver
/// refuses to pretend: deterministic projection plus a surfaced conflict,
/// exactly as if no timing evidence existed.
#[test]
fn timing_within_uncertainty_stays_a_conflict() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    // The only measurement was loose: ±100 seconds.
    a.events_mut()
        .append(clock_observation(&node(1), &node(2), 0, 100_000))
        .unwrap();
    sync_into(&mut b, &mut a);

    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 1_000;
    contexted_update(&mut a, "p-1", json!({"phone": "222"}), base + 5);
    contexted_update(&mut b, "p-1", json!({"phone": "333"}), base + 9);

    let report = sync_into(&mut b, &mut a);
    assert_eq!(report.conflicts_resolved, 1);
    sync_into(&mut a, &mut b);

    for db in [&a, &b] {
        assert_eq!(
            db.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
            "333"
        );
        assert!(db.record_conflict("patients", "p-1").unwrap().is_some());
    }
    assert_converged(&[&a, &b], &["patients"]);
}

/// Reconciliation is incremental: an import only re-resolves records touched
/// by events above the reconciliation high-water mark. An existing untouched
/// conflict neither recounts in later import reports nor loses its surfaced
/// state.
#[test]
fn incremental_reconcile_only_revisits_touched_records() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    sync_into(&mut b, &mut a);

    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 1_000;
    contexted_update(&mut a, "p-1", json!({"phone": "222"}), base + 5);
    contexted_update(&mut b, "p-1", json!({"phone": "333"}), base + 9);
    let conflicted = sync_into(&mut b, &mut a);
    assert_eq!(conflicted.conflicts_resolved, 1);
    assert!(b.record_conflict("patients", "p-1").unwrap().is_some());

    // A later import touching only an unrelated record does not recount the
    // standing conflict — and does not disturb it either.
    a.insert("patients", Record::new("p-2")).unwrap();
    let unrelated = sync_into(&mut b, &mut a);
    assert_eq!(unrelated.conflicts_resolved, 0);
    assert!(b.get("patients", "p-2").unwrap().is_some());
    assert!(b.record_conflict("patients", "p-1").unwrap().is_some());
    assert_eq!(
        b.get("patients", "p-1").unwrap().unwrap().metadata["phone"],
        "333"
    );
}

/// The bandwidth property vectors exist for: a delta contains only what the
/// peer is missing, no matter which path previously delivered the rest.
#[test]
fn vector_deltas_transfer_only_missing_events() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut a = BicDb::open_with_node_id(dirs[0].path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dirs[1].path(), sync_config(), node(2)).unwrap();
    let mut c = BicDb::open_with_node_id(dirs[2].path(), sync_config(), node(3)).unwrap();
    let mut d = BicDb::open_with_node_id(dirs[3].path(), sync_config(), node(4)).unwrap();

    a.create_collection("patients").unwrap();
    for id in ["p-1", "p-2", "p-3", "p-4", "p-5"] {
        a.insert("patients", Record::new(id)).unwrap();
    }
    let first = sync_into(&mut b, &mut a);
    assert_eq!(first.imported_events, 5);

    for id in ["p-6", "p-7", "p-8"] {
        a.insert("patients", Record::new(id)).unwrap();
    }
    let delta = a
        .export_sync_bundle_delta(&b.sync_vector().unwrap())
        .unwrap();
    assert_eq!(delta.event_count, 3);
    assert_eq!(
        delta.source_vector.as_ref().unwrap(),
        &a.sync_vector().unwrap()
    );
    b.import_sync_bundle(delta).unwrap();

    // C catches up straight from A; its delta toward B is then empty even
    // though C and B have never spoken.
    sync_into(&mut c, &mut a);
    let redundant = c
        .export_sync_bundle_delta(&b.sync_vector().unwrap())
        .unwrap();
    assert_eq!(redundant.event_count, 0);

    // A brand-new device receives everything exactly once from any peer.
    let bootstrap = sync_into(&mut d, &mut b);
    assert_eq!(bootstrap.imported_events, 8);
    assert_eq!(bootstrap.duplicate_events, 0);
    assert_converged(&[&a, &b, &c, &d], &["patients"]);
}
