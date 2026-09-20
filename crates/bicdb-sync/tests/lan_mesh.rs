//! LAN mesh tests. The deterministic paths (direct dial, unicast-injected
//! beacons, per-peer status) must always pass; the real-multicast test
//! skips gracefully where the environment forbids multicast, since that is
//! an environment property, not a code property.

use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bicdb_core::{BicDb, DbConfig, NodeId, Record};
use bicdb_sync::{sync_with_addr, LanBeacon, LanMesh, LanMeshConfig, LAN_BEACON_MAGIC};
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

fn provision_pair(left: &mut BicDb, right: &mut BicDb) {
    let left_key = left.mesh_verifying_key().unwrap();
    let right_key = right.mesh_verifying_key().unwrap();
    left.pin_node_key(&right.node_id(), &right_key).unwrap();
    right.pin_node_key(&left.node_id(), &left_key).unwrap();
}

fn wait_until(timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    done()
}

#[test]
fn beacons_round_trip_and_reject_foreign_payloads() {
    let beacon = LanBeacon {
        magic: LAN_BEACON_MAGIC.to_string(),
        version: 1,
        node_id: node(7),
        tcp_port: 4711,
        group: Some("camp-rural-7".to_string()),
    };
    let decoded = LanBeacon::decode(&beacon.encode().unwrap()).unwrap();
    assert_eq!(decoded, beacon);
    assert!(LanBeacon::decode(b"{\"magic\":\"something-else\",\"version\":1,\"node_id\":\"00000000-0000-0000-0000-000000000001\",\"tcp_port\":1}").is_err());
    assert!(LanBeacon::decode(b"garbage").is_err());
}

#[test]
fn direct_dial_syncs_against_a_running_node_and_updates_peer_status() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = open(&dir_a, 1);
    a.create_collection("patients").unwrap();
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    let mut b = open(&dir_b, 2);
    b.create_collection("observations").unwrap();
    b.insert(
        "observations",
        Record::new("obs-1").with_metadata(json!({"patient": "p-1"})),
    )
    .unwrap();
    provision_pair(&mut a, &mut b);
    let a = Arc::new(Mutex::new(a));

    // Node A runs the full LAN stack; node B just dials the address.
    let mesh = LanMesh::spawn(
        Arc::clone(&a),
        LanMeshConfig {
            beacon_interval: Duration::from_secs(3600),
            ..LanMeshConfig::default()
        },
    )
    .unwrap();
    let addr = mesh.local_addr();

    let report = sync_with_addr(&mut b, addr).unwrap();
    assert_eq!(report.peer_node_id, node(1));
    assert!(report.imported_events > 0);
    assert!(b.get("patients", "p-1").unwrap().is_some());
    {
        let guard = a.lock().unwrap();
        assert!(guard.get("observations", "obs-1").unwrap().is_some());

        // Both sides learned each other's coverage. The one thing still
        // pending on each side is the clock observation the session itself
        // just minted.
        let status = guard.mesh_peer_status().unwrap();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].node_id, node(2));
        assert_eq!(status[0].pending_events_for_peer, 1);
    }
    assert_eq!(b.mesh_peer_status().unwrap()[0].pending_events_for_peer, 1);

    // The second session ships the observations; the rate limiter mints no
    // new ones, so the mesh reaches a genuinely quiet steady state.
    sync_with_addr(&mut b, addr).unwrap();
    assert_eq!(b.mesh_peer_status().unwrap()[0].pending_events_for_peer, 0);
    assert_eq!(
        a.lock().unwrap().mesh_peer_status().unwrap()[0].pending_events_for_peer,
        0
    );

    // New local writes show up as pending for the known peer until the next
    // session.
    b.insert("observations", Record::new("obs-2")).unwrap();
    assert_eq!(b.mesh_peer_status().unwrap()[0].pending_events_for_peer, 1);
    sync_with_addr(&mut b, addr).unwrap();
    assert_eq!(b.mesh_peer_status().unwrap()[0].pending_events_for_peer, 0);

    mesh.shutdown();
}

#[test]
fn an_injected_unicast_beacon_triggers_automatic_convergence() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = open(&dir_a, 1);
    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-1")).unwrap();

    let mut b = open(&dir_b, 2);
    b.create_collection("notes").unwrap();
    b.insert("notes", Record::new("note-b")).unwrap();
    provision_pair(&mut a, &mut b);
    let a = Arc::new(Mutex::new(a));
    let b = Arc::new(Mutex::new(b));

    // Port zero disables multicast announcements and gives each mesh an
    // OS-assigned discovery listener, so parallel tests cannot cross-talk or
    // collide with another process on the host.
    let mesh_a = LanMesh::spawn(
        Arc::clone(&a),
        LanMeshConfig {
            multicast_addr: "239.255.71.14:0".parse().unwrap(),
            resync_interval: Duration::from_millis(500),
            ..LanMeshConfig::default()
        },
    )
    .unwrap();
    let mesh_b = LanMesh::spawn(
        Arc::clone(&b),
        LanMeshConfig {
            multicast_addr: "239.255.71.14:0".parse().unwrap(),
            resync_interval: Duration::from_millis(500),
            ..LanMeshConfig::default()
        },
    )
    .unwrap();

    // Simulate discovery by handing A a unicast beacon for B — exactly what
    // a multicast datagram would deliver.
    let beacon = LanBeacon {
        magic: LAN_BEACON_MAGIC.to_string(),
        version: 1,
        node_id: mesh_b.node_id().clone(),
        tcp_port: mesh_b.local_addr().port(),
        group: None,
    };
    let injector = UdpSocket::bind("127.0.0.1:0").unwrap();
    injector
        .send_to(
            &beacon.encode().unwrap(),
            ("127.0.0.1", mesh_a.discovery_addr().port()),
        )
        .unwrap();

    let converged = wait_until(Duration::from_secs(10), || {
        let a_sees = a
            .lock()
            .unwrap()
            .get("notes", "note-b")
            .ok()
            .flatten()
            .is_some();
        let b_sees = b
            .lock()
            .unwrap()
            .get("patients", "p-1")
            .ok()
            .flatten()
            .is_some();
        // The duplex data exchange becomes visible just before the worker
        // records its completed-session status. Treat both as the convergence
        // boundary so the assertion below cannot race that final bookkeeping
        // write on a slow or heavily loaded runner.
        let session_recorded = mesh_a
            .peers()
            .first()
            .is_some_and(|peer| peer.sessions_completed >= 1);
        a_sees && b_sees && session_recorded
    });
    assert!(converged, "beacon-driven session did not converge");
    assert_eq!(mesh_a.peers().len(), 1);
    assert!(mesh_a.peers()[0].sessions_completed >= 1);

    mesh_a.shutdown();
    mesh_b.shutdown();
}

/// Real multicast on the local stack. Environments (containers, CI
/// sandboxes) may forbid it — that is not a code failure, so the test skips
/// when no beacon crosses within the window.
#[test]
fn multicast_discovery_converges_where_the_network_allows() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = open(&dir_a, 1);
    a.create_collection("patients").unwrap();
    a.insert("patients", Record::new("p-multicast")).unwrap();
    let mut b = open(&dir_b, 2);
    provision_pair(&mut a, &mut b);
    let a = Arc::new(Mutex::new(a));
    let b = Arc::new(Mutex::new(b));

    let config = LanMeshConfig {
        multicast_addr: "239.255.71.15:47613".parse().unwrap(),
        beacon_interval: Duration::from_millis(300),
        resync_interval: Duration::from_millis(500),
        group: Some("camp-test".to_string()),
        ..LanMeshConfig::default()
    };
    let mesh_a = LanMesh::spawn(Arc::clone(&a), config.clone()).unwrap();
    let mesh_b = LanMesh::spawn(Arc::clone(&b), config).unwrap();

    let discovered = wait_until(Duration::from_secs(6), || !mesh_a.peers().is_empty());
    if !discovered {
        eprintln!("skipping: multicast beacons did not propagate in this environment");
        mesh_a.shutdown();
        mesh_b.shutdown();
        return;
    }

    let converged = wait_until(Duration::from_secs(10), || {
        b.lock()
            .unwrap()
            .get("patients", "p-multicast")
            .ok()
            .flatten()
            .is_some()
    });
    mesh_a.shutdown();
    mesh_b.shutdown();
    assert!(
        converged,
        "peers discovered each other but did not converge"
    );
}
