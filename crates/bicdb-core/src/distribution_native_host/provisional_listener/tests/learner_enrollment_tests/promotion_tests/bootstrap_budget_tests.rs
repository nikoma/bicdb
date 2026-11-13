//! Real TLS bootstrap is held at the actual receiving MStore. No synthetic
//! reply, MQ, term or role is used to prove the original I/O budget expires.
use super::*;

#[test]
fn native_metadata_fresh_bootstrap_original_budget_and_cancel_refuse_before_root() {
    let mut f = EnrollmentFixture::new();
    let token = CancellationToken::uncancelable();
    let mut configuration = config(&f);
    configuration.transport = f.networks[0].clone();
    let actual_store = Arc::clone(&f.launches[0].metadata);
    let mut control = crate::distribution_native_host::NativeMetadataControlHost::new(
        f.launches.remove(0),
        f.listeners.remove(0),
        f.networks[0].clone(),
        configuration.clone(),
    );
    control.advance(&token).unwrap();
    let address0 = actual_store.lock().committed_topology().nodes[&node(0)]
        .address
        .clone();
    let address4 = actual_store.lock().committed_topology().nodes[&node(4)]
        .address
        .clone();
    let client = crate::TcpClusterBootstrapClient::new(
        configuration.cluster_id.clone(),
        node(4),
        f.networks[4].clone(),
    )
    .unwrap();
    client.fetch_snapshot(&node(0), &address0).unwrap();
    // The TEST fault holds the recipient's store during the TLS request. The
    // client must finish its short original budget while the real handler is
    // still unable to produce bootstrap data. This does not assert an OS I/O
    // hard bound for arbitrary filesystem work.
    let held = actual_store.lock();
    let expired = token.child_with_deadline(Instant::now() + Duration::from_millis(100));
    let deadline = expired.deadline();
    let started = Instant::now();
    assert!(client
        .fetch_snapshot_cancellable(&expired, &node(0), &address0)
        .is_err());
    assert!(expired.check().is_err());
    assert_eq!(expired.deadline(), deadline);
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(held);
    let cancelled = token.child();
    cancelled.cancel();
    let elapsed = token.child_with_deadline(Instant::now());
    for (name, refused) in [
        ("cancelled-before-discovery", cancelled),
        ("expired-before-discovery", elapsed),
    ] {
        let root = f.root.path().join(name);
        assert!(crate::NativeMetadataLearnerHost::start_fresh_metadata_only(
            &root,
            configuration.cluster_id.clone(),
            node(4),
            &address4,
            1,
            node(0),
            &address0,
            f.networks[4].clone(),
            8 * 1024 * 1024,
            &refused,
        )
        .is_err());
        assert!(!root.exists());
    }
    // Real handler/control is still usable after the held request drains.
    client.fetch_snapshot(&node(0), &address0).unwrap();
    control.stop(&token).unwrap();
    f.shutdown();
}
