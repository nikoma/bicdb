//! Duplex mesh-session tests over real byte pipes (Unix socket pairs) — the
//! airplane-mode demo minus radios: independent BicDB instances in separate
//! threads, no shared state, converging through framed bytes alone.

#![cfg(unix)]

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use bicdb_core::{BicDb, DbConfig, NodeId, Record};
use bicdb_sync::{
    run_mesh_initiator, run_mesh_responder, run_mesh_responder_shared, MeshSyncReport,
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
        .with_unsafe_legacy_mesh_collections(true)
}

fn open(dir: &tempfile::TempDir, id: u128) -> BicDb {
    BicDb::open_with_node_id(dir.path(), sync_config(), node(id)).unwrap()
}

/// Runs one live session between two databases, each driven from its own
/// thread across a socket pair, and returns both reports.
fn session(left: &mut BicDb, right: &mut BicDb) -> (MeshSyncReport, MeshSyncReport) {
    let left_node = left.node_id();
    let right_node = right.node_id();
    let left_key = left.mesh_verifying_key().unwrap();
    let right_key = right.mesh_verifying_key().unwrap();
    left.pin_node_key(&right_node, &right_key).unwrap();
    right.pin_node_key(&left_node, &left_key).unwrap();
    let (mut left_io, mut right_io) = UnixStream::pair().unwrap();
    thread::scope(|scope| {
        let right_side = scope.spawn(move || run_mesh_responder(right, &mut right_io).unwrap());
        let left_report = run_mesh_initiator(left, &mut left_io).unwrap();
        (left_report, right_side.join().unwrap())
    })
}

fn event_ids(db: &BicDb) -> BTreeSet<Uuid> {
    db.export_events_since(0)
        .into_iter()
        .map(|stored| stored.event.id)
        .collect()
}

/// Data events only: sessions mint clock observations *after* the exchange,
/// so that evidence intentionally trails one session behind the data.
fn data_event_ids(db: &BicDb) -> BTreeSet<Uuid> {
    db.export_events_since(0)
        .into_iter()
        .filter(|stored| stored.event.stream != bicdb_core::CLOCK_OBSERVATION_STREAM)
        .map(|stored| stored.event.id)
        .collect()
}

fn assert_converged(dbs: &[&BicDb]) {
    let reference = data_event_ids(dbs[0]);
    for db in &dbs[1..] {
        assert_eq!(data_event_ids(db), reference);
    }
}

struct ReadSignalingStream {
    inner: UnixStream,
    first_read: Option<mpsc::SyncSender<()>>,
}

impl Read for ReadSignalingStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if let Some(signal) = self.first_read.take() {
            let _ = signal.send(());
        }
        self.inner.read(buffer)
    }
}

impl Write for ReadSignalingStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[test]
fn a_live_session_converges_two_diverged_nodes_in_one_round_trip() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut a = open(&dir_a, 1);
    let mut b = open(&dir_b, 2);

    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    b.create_collection("observations").unwrap();
    b.insert(
        "observations",
        Record::new("obs-1").with_metadata(json!({"patient": "p-1", "type": "pulse"})),
    )
    .unwrap();

    let (a_report, b_report) = session(&mut a, &mut b);
    assert_eq!(a_report.peer_node_id, node(2));
    assert_eq!(b_report.peer_node_id, node(1));
    assert!(a_report.imported_events > 0);
    assert!(b_report.imported_events > 0);
    assert_eq!(a_report.peer_imported_events, b_report.imported_events);

    // Same host, same clock: the session's NTP-style estimate must exist and
    // read approximately zero, and the tight measurement becomes a
    // replicated clock observation.
    let offset = a_report.peer_clock_offset_ms.expect("timing estimate");
    assert!(offset.abs() < 2_000, "loopback offset was {offset}ms");
    assert!(a_report.peer_clock_error_ms.unwrap() < 2_000);
    let observations = a
        .export_events_since(0)
        .into_iter()
        .filter(|stored| stored.event.stream == bicdb_core::CLOCK_OBSERVATION_STREAM)
        .count();
    assert!(
        observations >= 1,
        "session should record a clock observation"
    );
    assert!(a.get("observations", "obs-1").unwrap().is_some());
    assert!(b.get("patients", "p-1").unwrap().is_some());
    assert_converged(&[&a, &b]);

    // The second session ships the freshly minted clock observations; after
    // it, even the evidence streams are identical.
    session(&mut a, &mut b);
    assert_eq!(event_ids(&a), event_ids(&b));

    // The third session is truly idle: the observation rate limiter mints
    // nothing new, so a quiet mesh stays quiet.
    let (idle_a, idle_b) = session(&mut a, &mut b);
    assert_eq!(idle_a.sent_events, 0);
    assert_eq!(idle_a.received_events, 0);
    assert_eq!(idle_b.sent_events, 0);
}

#[test]
fn unprovisioned_peer_is_rejected_before_vectors_or_data_are_exchanged() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut a = open(&dir_a, 1);
    let mut b = open(&dir_b, 2);
    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-1")).unwrap();

    let (mut a_io, mut b_io) = UnixStream::pair().unwrap();
    let (a_result, b_result) = thread::scope(|scope| {
        let responder = scope.spawn(|| run_mesh_responder(&mut b, &mut b_io));
        let initiator = run_mesh_initiator(&mut a, &mut a_io);
        (initiator, responder.join().unwrap())
    });
    for result in [a_result, b_result] {
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("no provisioned key pin"),
            "unexpected error: {error}"
        );
    }
    assert!(b.get("patients", "p-1").ok().flatten().is_none());
}

#[test]
fn shared_mesh_session_releases_database_mutex_while_waiting_for_socket_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Mutex::new(open(&dir, 1)));
    let (server_io, peer_io) = UnixStream::pair().unwrap();
    let (signal, entered_read) = mpsc::sync_channel(1);
    let db_for_thread = Arc::clone(&db);
    let responder = thread::spawn(move || {
        let mut io = ReadSignalingStream {
            inner: server_io,
            first_read: Some(signal),
        };
        run_mesh_responder_shared(&db_for_thread, &mut io)
    });

    entered_read
        .recv_timeout(Duration::from_secs(1))
        .expect("responder should block waiting for the peer hello");
    assert!(
        db.try_lock().is_ok(),
        "database mutex must not be held while mesh waits on socket I/O"
    );

    drop(peer_io);
    assert!(responder.join().unwrap().is_err());
}

#[test]
fn three_nodes_converge_through_pairwise_sessions_without_a_hub() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut a = open(&dirs[0], 1);
    let mut b = open(&dirs[1], 2);
    let mut c = open(&dirs[2], 3);

    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-1")).unwrap();
    session(&mut a, &mut b);

    // Relayed origins must also be provisioned out of band; trusting B as a
    // transporter does not grant B authority to invent events as A.
    c.pin_node_key(&a.node_id(), &a.mesh_verifying_key().unwrap())
        .unwrap();

    b.create_collection("observations").unwrap();
    b.insert(
        "observations",
        Record::new("obs-bp").with_metadata(json!({"patient": "p-1"})),
    )
    .unwrap();
    // C meets only B, yet receives A's patient by relay.
    session(&mut b, &mut c);
    assert!(c.get("patients", "p-1").unwrap().is_some());

    c.create_collection("encounters").unwrap();
    c.insert(
        "encounters",
        Record::new("enc-1").with_metadata(json!({"patient": "p-1"})),
    )
    .unwrap();
    // Every data event A authored already reached C through the relay — the
    // only thing left for A to send is post-session clock evidence.
    assert!(data_event_ids(&a).is_subset(&data_event_ids(&c)));
    // A meets only C and catches up on the whole camp.
    session(&mut a, &mut c);
    assert!(a.get("encounters", "enc-1").unwrap().is_some());
    assert!(a.get("observations", "obs-bp").unwrap().is_some());

    session(&mut b, &mut a);
    assert_converged(&[&a, &b, &c]);
}

#[test]
fn a_session_cut_mid_transfer_loses_nothing_and_retries_clean() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut a = open(&dir_a, 1);
    let mut b = open(&dir_b, 2);

    a.create_collection("patients").unwrap();
    for id in ["p-1", "p-2", "p-3"] {
        a.insert("patients", Record::new(id)).unwrap();
    }
    let before = event_ids(&b);

    // The peer vanishes right after saying hello — mid-session, before any
    // delta arrives.
    {
        let (mut a_io, mut b_io) = UnixStream::pair().unwrap();
        let vanishing_peer = thread::spawn(move || {
            // Read the initiator's hello, then drop the link.
            let mut scratch = [0_u8; 4];
            use std::io::Read;
            b_io.read_exact(&mut scratch).unwrap();
            let mut body = vec![0_u8; u32::from_be_bytes(scratch) as usize];
            b_io.read_exact(&mut body).unwrap();
            // Reply with the same hello bytes re-tagged as node 2's is not
            // possible without the framing internals, so just hang up: the
            // initiator must fail cleanly on a dead pipe.
            drop(b_io);
        });
        let result = run_mesh_initiator(&mut a, &mut a_io);
        vanishing_peer.join().unwrap();
        assert!(result.is_err());
    }
    // Nothing changed on either side.
    assert_eq!(event_ids(&b), before);

    // A later, intact session converges as if nothing had happened.
    let (a_report, b_report) = session(&mut a, &mut b);
    assert_eq!(b_report.imported_events, 3);
    assert_eq!(a_report.peer_imported_events, 3);
    assert_converged(&[&a, &b]);
}

#[test]
fn sessions_use_out_of_band_provisioned_signing_key_pins() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let signing = || {
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true)
            .with_mesh_signing(true)
            .with_unsafe_legacy_mesh_collections(true)
    };
    let mut a = BicDb::open_with_node_id(dir_a.path(), signing(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), signing(), node(2)).unwrap();
    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-1")).unwrap();

    let a_key = a.mesh_verifying_key().unwrap();
    let b_key = b.mesh_verifying_key().unwrap();
    session(&mut a, &mut b);
    assert_eq!(a.pinned_node_key(&node(2)), Some(b_key));
    assert_eq!(b.pinned_node_key(&node(1)), Some(a_key.clone()));
    assert!(b.get("patients", "p-1").unwrap().is_some());

    // A pre-pinned WRONG key for the peer aborts the session — the
    // impersonation alarm.
    let dir_c = tempfile::tempdir().unwrap();
    let mut c = BicDb::open_with_node_id(dir_c.path(), signing(), node(3)).unwrap();
    c.pin_node_key(&node(1), &hex::encode([9_u8; 32])).unwrap();
    thread::scope(|scope| {
        let (mut c_io, mut a_io) = UnixStream::pair().unwrap();
        let responder = scope.spawn(move || {
            // Errors once the initiator hangs up — that is the point.
            let _ = run_mesh_responder(&mut a, &mut a_io);
        });
        let c_result = run_mesh_initiator(&mut c, &mut c_io);
        assert!(c_result.is_err(), "conflicting pin must abort the session");
        // Close the pipe so the responder sees EOF instead of waiting for a
        // delta that will never come.
        drop(c_io);
        responder.join().unwrap();
    });
    // Nothing was imported: the collection does not even exist on C.
    assert!(c.get("patients", "p-1").ok().flatten().is_none());
}

#[test]
fn garbage_frames_are_refused_without_state_change() {
    let dir_a = tempfile::tempdir().unwrap();
    let mut a = open(&dir_a, 1);
    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-1")).unwrap();
    let before = event_ids(&a);

    let (mut a_io, mut peer_io) = UnixStream::pair().unwrap();
    let garbage_peer = thread::spawn(move || {
        use std::io::{Read, Write};
        let mut scratch = [0_u8; 4];
        peer_io.read_exact(&mut scratch).unwrap();
        let mut body = vec![0_u8; u32::from_be_bytes(scratch) as usize];
        peer_io.read_exact(&mut body).unwrap();
        // Claim an absurd frame length: the initiator must refuse to
        // allocate it.
        peer_io.write_all(&u32::MAX.to_be_bytes()).unwrap();
    });
    let result = run_mesh_initiator(&mut a, &mut a_io);
    garbage_peer.join().unwrap();
    assert!(result.is_err());
    assert_eq!(event_ids(&a), before);
}
