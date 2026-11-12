//! Real fifth-node same-service metadata catch-up; no data-ready/operator claim.
use super::*;
use crate::distribution_consensus::NativeMetadataInitializationCut;

mod cold_tests;

fn own_mq(
    f: &EnrollmentFixture,
    parent: &CancellationToken,
) -> (
    crate::distribution_consensus::HostMetadataReadObservation,
    Uuid,
) {
    let business = Uuid::now_v7();
    let observed = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), parent)
        .unwrap();
    (observed, business)
}
fn protected_id(factory: &NativeUnassignedLearnerFactory) -> (Uuid, bool) {
    let actual = factory
        .database
        .read()
        .native_metadata_initialization_record(
            &factory.spec.cluster_id,
            factory.spec.bootstrap_id,
            &factory.spec.node_id,
            factory.spec.member_incarnation,
        )
        .unwrap()
        .unwrap();
    (actual.store_id(), actual.registered())
}

#[test]
fn native_enrolled_learner_same_service_metadata_tls_catchup_bound_ack_and_strict_reopen() {
    let f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("actual-metadata-transition");
    let token = CancellationToken::uncancelable();
    let enrolled = f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    let service = host.test_service();
    let range_store = service.lock().native_initial_listener_store_open_id();
    let uid = factory
        .database
        .read()
        .collections()
        .into_iter()
        .find(|m| m.name == "items")
        .unwrap()
        .local_namespace_id;
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(1, 16)
        .unwrap();
    let error = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap_err();
    assert!(
        error.to_string().contains("provisional learner listener"),
        "{error}"
    );
    let (current, business) = own_mq(&f, &token);
    let id = host
        .start_owned_metadata(&current, business, &token)
        .unwrap();
    assert_eq!(protected_id(&factory), (id, true));
    assert!(host.test_old_listener_drained());
    assert!(Arc::ptr_eq(&service, &host.test_service()));
    assert_eq!(
        service.lock().native_initial_listener_store_open_id(),
        range_store
    );
    let actual = host.test_owned_metadata().unwrap();
    assert_eq!(actual.lock().status().current_term, 0);
    assert_eq!(actual.lock().status().commit_index, 0);
    assert!(actual.lock().start_election().is_err());
    let mut promoted = f.launches[0].metadata.lock().committed_topology().clone();
    promoted.generation += 1;
    promoted.nodes.get_mut(&node(4)).unwrap().metadata_role = MetadataMemberRole::Voter;
    assert!(f.launches[0]
        .metadata
        .lock()
        .propose_topology(promoted)
        .unwrap_err()
        .to_string()
        .contains("caught-up metadata acknowledgement"));
    let (retry, retry_business) = own_mq(&f, &token);
    assert_eq!(
        host.start_owned_metadata(&retry, retry_business, &token)
            .unwrap(),
        id
    );
    assert!(Arc::ptr_eq(&actual, &host.test_owned_metadata().unwrap()));
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(1, 16)
        .unwrap();
    let ack = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(ack.success && ack.native_provenance.is_some());
    assert_eq!(
        ack.native_receiver.as_ref().unwrap().metadata_store_id(),
        id
    );
    assert_eq!(
        ack.native_receiver.as_ref().unwrap().native_birth(),
        enrolled.birth()
    );
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &ack)
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();
    let before = actual.lock().status();
    assert!(before.commit_index >= enrolled.committed_index() && before.current_term > 0);
    assert_eq!(
        actual.lock().committed_topology().nodes[&node(4)].metadata_role,
        MetadataMemberRole::Learner
    );
    assert!(actual.lock().start_election().is_err());
    drop(actual);
    host.stop_owned_metadata(&token).unwrap();
    host.close_stopped_owned_metadata(&token).unwrap();
    let (reopen, reopen_business) = own_mq(&f, &token);
    assert_eq!(
        host.start_owned_metadata(&reopen, reopen_business, &token)
            .unwrap(),
        id
    );
    let actual = host.test_owned_metadata().unwrap();
    assert_eq!(actual.lock().status().current_term, before.current_term);
    assert_eq!(actual.lock().status().commit_index, before.commit_index);
    assert_eq!(protected_id(&factory), (id, true));
    assert_eq!(
        factory
            .database
            .read()
            .collections()
            .into_iter()
            .find(|m| m.name == "items")
            .unwrap()
            .local_namespace_id,
        uid
    );
    assert!(factory
        .database
        .read()
        .scan_collection("items")
        .unwrap()
        .is_empty());
    assert!(!factory.database.read().commit_admission_ready());
    let range = f.launches[0]
        .metadata
        .lock()
        .committed_topology()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(
        factory
            .database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        None
    );
    {
        let _service = service.lock();
        let _db = factory.database.write();
        let error = f.launches[0]
            .transport
            .application_event(
                &node(4),
                crate::ClusterEventRequest {
                    range_id: range.id,
                    range_epoch: range.epoch,
                    operation: crate::ClusterEventOperation::Scan {
                        namespace: "items".into(),
                        after: None,
                        limit: 1,
                    },
                },
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("metadata endpoints only"),
            "{error}"
        );
    }
    // A genuine candidate asks over TLS; the caught-up member remains a learner.
    let request = f.launches[1].metadata.lock().start_election().unwrap();
    let vote = f.launches[1]
        .transport
        .request_metadata_vote(&node(4), request)
        .unwrap();
    assert!(!vote.vote_granted);
    drop(actual);
    host.stop_owned_metadata(&token).unwrap();
    host.shutdown().unwrap();
    f.shutdown();
}

#[test]
fn native_enrolled_learner_metadata_constructor_failure_retains_prepared_quarantine_identity() {
    for chosen_cut in [
        NativeMetadataInitializationCut::AfterProtectedPrepare,
        NativeMetadataInitializationCut::AfterStateFile,
    ] {
        let f = EnrollmentFixture::new();
        let (factory, mut host) = f.factory("actual-prepared-transition");
        let token = CancellationToken::uncancelable();
        f.launches[0]
            .enroll_registered_native_learner(&node(4), &token)
            .unwrap();
        let (current, business) = own_mq(&f, &token);
        let error = host
            .test_start_owned_metadata_with(
                &current,
                business,
                &token,
                |cut| {
                    if cut == chosen_cut {
                        Err(
                            std::io::Error::other("actual learner protected initialization cut")
                                .into(),
                        )
                    } else {
                        Ok(())
                    }
                },
                |_| Ok(()),
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("actual learner protected initialization cut"),
            "{error}"
        );
        let (id, registered) = protected_id(&factory);
        assert!(!registered);
        assert_eq!(
            host.test_transition_custody(),
            (true, true, false, false, false)
        );
        let path = factory
            .database
            .read()
            .data_path()
            .join("native-learner-metadata")
            .join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        assert_eq!(host.test_retained_metadata_identity(), Some(id));
        let bytes = std::fs::read(&path).ok();
        if chosen_cut == NativeMetadataInitializationCut::AfterStateFile {
            let state: serde_json::Value = serde_json::from_slice(bytes.as_ref().unwrap()).unwrap();
            assert_eq!(state["state"]["native_store_id"], serde_json::json!(id));
        } else {
            assert!(
                bytes.is_none(),
                "no state file exists before its first publication"
            );
        }
        let (retry, retry_business) = own_mq(&f, &token);
        let error = host
            .start_owned_metadata(&retry, retry_business, &token)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Prepared learner metadata initialization is quarantined"),
            "{error}"
        );
        assert_eq!(protected_id(&factory), (id, false));
        assert_eq!(std::fs::read(&path).ok(), bytes);
        assert_eq!(host.test_retained_metadata_identity(), Some(id));
        assert_eq!(
            host.test_transition_custody(),
            (true, true, false, false, false)
        );
        host.shutdown().unwrap();
        f.shutdown();
    }
}
#[test]
fn native_enrolled_learner_metadata_registered_cancel_and_bind_retry_keep_same_owned_uuid() {
    let f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("actual-bind-transition");
    let token = CancellationToken::uncancelable();
    f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    let canceled = token.child();
    let (current, business) = own_mq(&f, &canceled);
    let error = host
        .test_start_owned_metadata_with(
            &current,
            business,
            &canceled,
            |_| Ok(()),
            |phase| {
                if phase == "registered" {
                    canceled.cancel();
                }
                Ok(())
            },
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
    let (id, registered) = protected_id(&factory);
    assert!(registered);
    assert_eq!(
        host.test_transition_custody(),
        (true, false, true, false, false)
    );
    let held = host.test_owned_metadata().unwrap();
    let (bind_current, bind_business) = own_mq(&f, &token);
    let address = bind_current
        .committed_topology(bind_business)
        .unwrap()
        .nodes[&node(4)]
        .address
        .clone();
    let mut occupied = None;
    let bind_error = host
        .test_start_owned_metadata_with(
            &bind_current,
            bind_business,
            &token,
            |_| Ok(()),
            |phase| {
                if phase == "before-bind" {
                    occupied = Some(std::net::TcpListener::bind(&address).unwrap());
                }
                Ok(())
            },
        )
        .unwrap_err();
    assert!(
        bind_error
            .to_string()
            .to_ascii_lowercase()
            .contains("address already in use"),
        "{bind_error}"
    );
    assert_eq!(
        host.test_transition_custody(),
        (true, false, true, false, false)
    );
    assert!(Arc::ptr_eq(&held, &host.test_owned_metadata().unwrap()));
    assert_eq!(protected_id(&factory), (id, true));
    drop(occupied);
    let canceled_bind = token.child();
    let (retry, retry_business) = own_mq(&f, &canceled_bind);
    let error = host
        .test_start_owned_metadata_with(
            &retry,
            retry_business,
            &canceled_bind,
            |_| Ok(()),
            |phase| {
                if phase == "after-bind" {
                    canceled_bind.cancel();
                }
                Ok(())
            },
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
    assert_eq!(
        host.test_transition_custody(),
        (true, false, true, true, true)
    );
    assert!(Arc::ptr_eq(&held, &host.test_owned_metadata().unwrap()));
    let (fresh, fresh_business) = own_mq(&f, &token);
    assert_eq!(
        host.start_owned_metadata(&fresh, fresh_business, &token)
            .unwrap(),
        id
    );
    assert!(Arc::ptr_eq(&held, &host.test_owned_metadata().unwrap()));
    assert_eq!(protected_id(&factory), (id, true));
    drop(held);
    host.stop_owned_metadata(&token).unwrap();
    host.shutdown().unwrap();
    f.shutdown();
}
