//! Actual socket/mTLS failure boundaries, never constructed reply/error votes.
//! These controls do not by themselves qualify whole HOT recovery liveness.
use super::*;
use crate::distribution_range_recovery::RecoveryEpochFenceRequest;
use crate::distribution_transport::{HotRecoveryPeerFailure, HotRecoveryPhaseRequest};
use std::io;
use std::net::{TcpListener, TcpStream};

pub(crate) struct SocketFault {
    stop: Arc<AtomicBool>,
    accepted: Arc<std::sync::atomic::AtomicUsize>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl SocketFault {
    pub(crate) fn bind(address: &str, close: bool) -> Self {
        let listener = TcpListener::bind(address).unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stopped = stop.clone();
        let count = accepted.clone();
        let worker = std::thread::spawn(move || {
            let mut held: Vec<TcpStream> = Vec::new();
            while !stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        count.fetch_add(1, Ordering::AcqRel);
                        if !close {
                            held.push(stream);
                        } else {
                            #[cfg(unix)]
                            {
                                use std::os::fd::AsRawFd;
                                let linger = libc::linger {
                                    l_onoff: 1,
                                    l_linger: 0,
                                };
                                // This live owned TEST socket alone is reset.
                                // No synthesized IO error or native authority.
                                let result = unsafe {
                                    libc::setsockopt(
                                        stream.as_raw_fd(),
                                        libc::SOL_SOCKET,
                                        libc::SO_LINGER,
                                        &linger as *const _ as *const libc::c_void,
                                        std::mem::size_of_val(&linger) as libc::socklen_t,
                                    )
                                };
                                assert_eq!(
                                    result,
                                    0,
                                    "actual reset socket option: {}",
                                    io::Error::last_os_error()
                                );
                            }
                        }
                        // close=true ends the REAL accepted TCP socket without
                        // pretending a TLS/metadata/recovery response exists.
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(e) => panic!("actual fault endpoint accept failed: {e}"),
                }
            }
        });
        Self {
            stop,
            accepted,
            worker: Some(worker),
        }
    }
}
impl Drop for SocketFault {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            // Only this test-owned nonblocking accept/poll worker; no engine
            // lock, socket read/write, authority or native worker is joined.
            let _ = worker.join();
        }
    }
}
fn scope_and_range(
    f: &Fixture,
    controls: &[NativeMetadataControlHost],
) -> (
    crate::distribution_range_consensus::RecoveryFenceScope,
    crate::RangeDescriptor,
) {
    let range = f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
    let scope = controls[0]
        .test_actual_hot_scope(&range, &budget(3000))
        .unwrap();
    (scope, range)
}
#[test]
fn native_hot_recovery_actual_socket_blackhole_subrequest_timeout_and_parent_cancel() {
    let (f, mut controls, configs) = opened();
    let (_, _, _, transport) = controls[0].test_hot_recovery_bindings().unwrap();
    let (scope, _) = scope_and_range(&f, &controls);
    controls[1].stop(&budget(5000)).unwrap();
    let fault = SocketFault::bind(&configs[1].node_address, false);
    let parent = budget(2500);
    let start = Instant::now();
    let error = transport
        .hot_recovery_phase_observation(
            &node(1),
            HotRecoveryPhaseRequest::Fence(RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 2500,
            }),
            0,
            Duration::from_millis(200),
            &parent,
        )
        .err()
        .expect("actual blackhole cannot create a fence observation");
    parent.check().unwrap();
    let actual = HotRecoveryPeerFailure::from_error(&error)
        .expect("actual socket child timeout must retain typed transport provenance");
    assert!(matches!(
        actual.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    ));
    assert!(actual.may_have_dispatched());
    assert!(fault.accepted.load(Ordering::Acquire) > 0);
    assert!(start.elapsed() < Duration::from_secs(2));
    let canceled_parent = budget(2000);
    let cancel = canceled_parent.clone();
    let canceler = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(30));
        cancel.cancel();
    });
    let error = transport
        .hot_recovery_phase_observation(
            &node(1),
            HotRecoveryPhaseRequest::Fence(RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 2000,
            }),
            0,
            Duration::from_millis(500),
            &canceled_parent,
        )
        .err()
        .expect("original caller cancel must abort");
    canceler.join().unwrap();
    assert!(matches!(error, BicDbError::QueryCanceled));
    assert!(HotRecoveryPeerFailure::from_error(&error).is_none());
    let expired_parent = budget(1);
    std::thread::sleep(Duration::from_millis(3));
    let error = transport
        .hot_recovery_phase_observation(
            &node(1),
            HotRecoveryPhaseRequest::Fence(RecoveryEpochFenceRequest {
                scope,
                budget_ms: 2000,
            }),
            0,
            Duration::from_millis(500),
            &expired_parent,
        )
        .err()
        .expect("expired ORIGINAL caller must abort before dispatch");
    assert!(matches!(error, BicDbError::QueryTimedOut));
    assert!(HotRecoveryPeerFailure::from_error(&error).is_none());
    drop(fault);
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
#[test]
fn native_hot_recovery_actual_socket_eof_reset_and_authenticated_rejection_are_distinct() {
    let (f, mut controls, configs) = opened();
    let (_, _, _, leader) = controls[0].test_hot_recovery_bindings().unwrap();
    let (_, _, _, follower) = controls[1].test_hot_recovery_bindings().unwrap();
    let (scope, _) = scope_and_range(&f, &controls);
    let parent = budget(2500);
    // The genuine follower's certificate/identity is valid. Its request cannot
    // acquire actual leader authority at the genuine receiver. No DTO or fake
    // role is installed to produce this authenticated semantic rejection.
    let error = follower
        .hot_recovery_phase_observation(
            &node(2),
            HotRecoveryPhaseRequest::Fence(RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 2000,
            }),
            0,
            Duration::from_millis(2000),
            &parent,
        )
        .err()
        .expect("actual follower cannot issue a leader-only fence");
    parent.check().unwrap();
    assert!(
        matches!(error, BicDbError::Cluster(_)),
        "unexpected actual rejection: {error}"
    );
    assert!(HotRecoveryPeerFailure::from_error(&error).is_none());
    controls[1].stop(&budget(5000)).unwrap();
    let fault = SocketFault::bind(&configs[1].node_address, true);
    let parent = budget(2000);
    let error = leader
        .hot_recovery_phase_observation(
            &node(1),
            HotRecoveryPhaseRequest::Fence(RecoveryEpochFenceRequest {
                scope,
                budget_ms: 1500,
            }),
            0,
            Duration::from_millis(500),
            &parent,
        )
        .err()
        .expect("closed actual socket cannot produce a fence observation");
    parent.check().unwrap();
    let actual = HotRecoveryPeerFailure::from_error(&error).expect("actual reset/EOF provenance");
    assert!(matches!(
        actual.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::BrokenPipe
    ));
    assert!(fault.accepted.load(Ordering::Acquire) > 0);
    drop(fault);
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}

#[test]
fn native_hot_recovery_wire30_refuses_actual29_peer_without_catalogue_mutation() {
    let (f, mut controls, _) = opened();
    let (_, _, _, transport) = controls[0].test_hot_recovery_bindings().unwrap();
    let range = f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|r| r.leader == node(0))
        .unwrap();
    let path = f.hosts[2]
        .seed
        .database
        .read()
        .data_path()
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let catalog = std::fs::read(&path).unwrap();
    let before = f.hosts[2]
        .seed
        .database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let parent = budget(1500);
    let message = transport
        .test_previous_hot_protocol_refusal(&node(2), &parent)
        .unwrap();
    parent.check().unwrap();
    assert!(
        message.contains("unsupported cluster data protocol 29; expected 30"),
        "wrong actual old-wire refusal: {message}"
    );
    assert_eq!(std::fs::read(path).unwrap(), catalog);
    assert_eq!(
        f.hosts[2]
            .seed
            .database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap(),
        before
    );
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}

#[test]
fn native_hot_recovery_completed_materialization_requires_actual_QQ_leader_and_live_parent() {
    let (f, mut controls, configs) = opened();
    let (range_scope, range) = scope_and_range(&f, &controls);
    let (_, _, _, leader) = controls[0].test_hot_recovery_bindings().unwrap();
    let (_, _, _, follower) = controls[1].test_hot_recovery_bindings().unwrap();
    let (actual_coordinator, actual_guard, _, _) =
        controls[2].test_hot_recovery_bindings().unwrap();
    let bootstrap = crate::TcpClusterBootstrapClient::new(
        configs[0].cluster_id.clone(),
        node(0),
        f.networks[0].clone(),
    )
    .unwrap();
    let topology = bootstrap
        .fetch_snapshot(&node(0), &configs[0].node_address)
        .unwrap()
        .topology;
    let key = (0..10_000)
        .map(|n| format!("completed-denial-{n}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .id
                == range.id
        })
        .unwrap();
    let client = crate::ClusterEventClient::new(&configs[0], topology)
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap();
    let value = serde_json::json!({"actual":"original full ACK", "nested":[null,{},[7,8]]});
    assert!(client.append("items", &key, value).unwrap());
    let command = f.hosts[0]
        ._service
        .lock()
        .probe_range_write_for_peer(&range, &node(0), 1)
        .unwrap()
        .entry
        .unwrap()
        .command;
    let (root, before, full_record) = {
        let db = f.hosts[2].seed.database.read();
        let lineage = db.current_native_range_source_lineage(range.id).unwrap();
        db.require_native_command_materialized(&lineage, &command)
            .unwrap();
        (
            db.data_path().to_owned(),
            db.native_range_prefix_observation(range.id).unwrap(),
            db.get(
                crate::CLUSTER_EVENTS_COLLECTION,
                &crate::cluster_event_record_id("items", &key).unwrap(),
            )
            .unwrap(),
        )
    };
    let path = root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let catalogue = std::fs::read(&path).unwrap();
    let request = crate::distribution_range_recovery::NativeCompletedMaterializeRequest {
        historical: crate::distribution_range_recovery::RecoveryHistoricalCommandRequest {
            scope: range_scope,
            command,
            budget_ms: 1500,
        },
        challenge: Uuid::now_v7(),
    };
    let denied = follower
        .hot_recovery_phase_observation(
            &node(2),
            HotRecoveryPhaseRequest::MaterializeCompleted(request.clone()),
            1,
            Duration::from_millis(1500),
            &budget(2500),
        )
        .err()
        .expect("actual nonleader cannot create a completed decision");
    assert!(
        denied.to_string().contains("metadata recovery-role"),
        "wrong authenticated nonleader refusal: {denied}"
    );
    let denied = leader
        .hot_recovery_phase_observation(
            &node(2),
            HotRecoveryPhaseRequest::MaterializeCompleted(request.clone()),
            1,
            Duration::from_millis(1500),
            &budget(2500),
        )
        .err()
        .expect("raw Applied labels cannot supply real completed quorum");
    assert!(
        denied
            .to_string()
            .contains("distinct durable completion quorum unavailable"),
        "wrong genuine completed-QQ refusal: {denied}"
    );
    let expired = budget(1);
    std::thread::sleep(Duration::from_millis(2));
    let denied = leader
        .hot_recovery_phase_observation(
            &node(2),
            HotRecoveryPhaseRequest::MaterializeCompleted(request),
            1,
            Duration::from_millis(1500),
            &expired,
        )
        .err()
        .expect("expired original caller cannot dispatch");
    assert!(matches!(denied, BicDbError::QueryTimedOut));
    assert_eq!(std::fs::read(path).unwrap(), catalogue);
    let db = f.hosts[2].seed.database.read();
    assert_eq!(
        db.native_range_prefix_observation(range.id).unwrap(),
        before
    );
    assert_eq!(
        db.get(
            crate::CLUSTER_EVENTS_COLLECTION,
            &crate::cluster_event_record_id("items", &key).unwrap()
        )
        .unwrap(),
        full_record
    );
    assert!(!crate::format::native_completed_decision_materialization_active(&root).unwrap());
    actual_guard
        .require_for_coordinator(&db, &range, &actual_coordinator)
        .unwrap();
    drop(db);
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}

#[test]
fn native_hot_recovery_completed_fence_simulated_old_reader_refuses_before_receipt_and_after_rewrite(
) {
    let (f, mut controls, _) = opened();
    let root = f.hosts[2].seed.database.read().data_path().to_owned();
    let range = f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|r| r.leader == node(0))
        .unwrap();
    let before = f.hosts[2]
        .seed
        .database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let catalogue =
        std::fs::read(root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG))
            .unwrap();
    let portable = crate::format::required_reader_features(&root).unwrap();
    // Necessary old-reader refusal only; this test does NOT create a QQ,
    // completed decision, native receipt or new write/read authority.
    crate::format::enable_native_completed_decision_materialization(&root).unwrap();
    let first =
        crate::format::test_simulated_reader_without_completed_materialization(&root).unwrap_err();
    assert!(first
        .to_string()
        .contains(crate::format::NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG));
    crate::format::persist_current(&root, true).unwrap();
    let later =
        crate::format::test_simulated_reader_without_completed_materialization(&root).unwrap_err();
    assert!(later
        .to_string()
        .contains(crate::format::NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG));
    assert_eq!(
        crate::format::required_reader_features(&root).unwrap(),
        portable
    );
    assert_eq!(
        std::fs::read(root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG))
            .unwrap(),
        catalogue
    );
    assert_eq!(
        f.hosts[2]
            .seed
            .database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap(),
        before
    );
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
