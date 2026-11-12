//! Actual loopback mTLS necessary-refusal control. This is not a Strong read,
//! failover, ACK-loss or comprehensive RPC replacement-race qualification.
use super::*;

#[test]
fn native_service_actual_tls_original_open_ended_refuses_before_held_service_and_replacement_latches(
) {
    let fixture = EnforcedNetwork::new();
    let destination = node("n1");
    let client = Arc::clone(&fixture.clients[&node("n0")]);
    let range = fixture.ranges()[0].clone();
    // The same authenticated current data leader can issue this actual RPC
    // successfully before replacement. No application write is needed.
    let healthy = client
        .range_write_progress(&destination, range.id, range.epoch)
        .unwrap();
    assert_eq!(healthy.node_id, destination);
    assert_eq!(healthy.last_index, 0);

    let replacement_root = tempfile::tempdir().unwrap();
    let mut replacement = BicDb::open_with_config(
        replacement_root.path().join("replacement-legacy"),
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
    )
    .unwrap();
    replacement.create_collection("items").unwrap();
    {
        // Diagnostic replacement data, not a distributed application ACK.
        let mut tx = replacement.begin_transaction().unwrap();
        tx.insert("items", Record::new("replacement-canary"))
            .unwrap();
        tx.commit().unwrap();
    }
    let before = replacement
        .get("items", "replacement-canary")
        .unwrap()
        .unwrap();
    let before_sequence = replacement.current_commit_seq();
    let before_incarnation = replacement.incarnation_id().to_owned();
    let database = Arc::clone(&fixture.native.databases[&destination]);
    let service = Arc::clone(&fixture.native.services[&destination]);
    let actual_open = service.lock().listener_open_guard();
    let retained_directory = database
        .read()
        .native_service_directory_lifetime()
        .unwrap()
        .upgrade()
        .unwrap();
    let original = std::mem::replace(&mut *database.write(), replacement);
    actual_open.require_live().unwrap();
    assert!(actual_open.require_database(&database.read()).is_err());
    drop(original);
    assert!(Arc::strong_count(&retained_directory) > 0);
    assert!(actual_open
        .require_live()
        .unwrap_err()
        .to_string()
        .contains("actual database open ended"));

    // Neither retaining the SAME outer Arc, the old directory claim, nor the
    // owned metadata store can revive the original actual BicDb lifetime.
    // Hold both latches while the genuine TLS RPC completes. A fallback into
    // service/schema/DB dispatch would wait or report another refusal instead.
    let database_latch = database.write();
    let service_latch = service.lock();
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let worker_destination = destination.clone();
    let worker = std::thread::spawn(move || {
        let result = client.range_write_progress(&worker_destination, range.id, range.epoch);
        let _ = send.send(result);
    });
    let result = receive.recv_timeout(Duration::from_secs(2));
    // Always unlock before joining/asserting so a regression retains a useful
    // failure instead of stranding a server worker behind a test-owned latch.
    drop(service_latch);
    drop(database_latch);
    worker.join().unwrap();
    let error = result
        .expect("authenticated actual-open refusal must precede both held latches")
        .unwrap_err();
    assert!(
        error.to_string().contains("actual database open ended"),
        "expected original-open-ended remote refusal, got {error}"
    );

    let replacement = database.read();
    assert_eq!(replacement.incarnation_id(), before_incarnation);
    assert_eq!(replacement.current_commit_seq(), before_sequence);
    assert_eq!(
        replacement.get("items", "replacement-canary").unwrap(),
        Some(before)
    );
    assert_eq!(
        replacement
            .actual_native_writer_enforcement_version()
            .unwrap(),
        0
    );
    drop(replacement);
    fixture.idle();
    drop(retained_directory);
}
