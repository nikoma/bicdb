//! Genuine promotion while a canceled after-bind owner remains retained. No
//! READY from global role, numeric position or a serialized metadata reply.
use super::*;
fn promotion_config(f: &EnrollmentFixture) -> crate::DistributionConfig {
    let topology = f.launches[0].metadata.lock().committed_topology().clone();
    let local = &topology.nodes[&node(0)];
    crate::DistributionConfig {
        enabled: true,
        cluster_id: topology.cluster_id.clone(),
        range_image_bootstrap_id: topology.range_image_bootstrap_id,
        node_id: local.id.clone(),
        node_address: local.address.clone(),
        node_incarnation: local.incarnation,
        node_capacity_bytes: local.capacity_bytes,
        node_tls_certificate_sha256: local.tls_certificate_sha256.clone(),
        replication_factor: 3,
        initial_ranges: 4,
        ..Default::default()
    }
}
fn promotion_entry(f: &EnrollmentFixture, token: &CancellationToken) -> crate::MetadataLogEntry {
    let (current, business) = f.current();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_for_peer(&node(4), 16)
        .unwrap();
    let reply = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(reply.success);
    let proof = {
        let mut store = f.launches[0].metadata.lock();
        store.record_append_response(&node(4), &reply).unwrap();
        store
            .observe_native_learner_promotion_readiness(&current, business, &node(4), &reply, token)
            .unwrap()
    };
    let (post, business) = f.current();
    let configuration = promotion_config(f);
    f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&proof, &post, business, &configuration, 50, token)
        .unwrap()
        .unwrap()
}
fn commit_only_old_voters(f: &EnrollmentFixture, index: u64, token: &CancellationToken) {
    // Three of the four real old voters also form the new five-voter majority.
    // Intentionally withhold ONLY delivery of this CONFIG to the new member.
    for _ in 0..2 {
        for i in 1..4 {
            let request = f.launches[0]
                .metadata
                .lock()
                .append_request_for_peer(&node(i), 16)
                .unwrap();
            let reply = f.launches[0]
                .transport
                .append_metadata_cancellable(&node(i), request, token)
                .unwrap();
            assert!(reply.success);
            f.launches[0]
                .metadata
                .lock()
                .record_append_response(&node(i), &reply)
                .unwrap();
        }
    }
    assert!(f.launches[0].metadata.lock().status().commit_index >= index);
    assert_eq!(
        f.launches[0].metadata.lock().committed_topology().nodes[&node(4)].metadata_role,
        MetadataMemberRole::Voter
    );
}
fn bound_catchup(f: &EnrollmentFixture, id: Uuid) {
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(1, 16)
        .unwrap();
    let reply = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(reply.success);
    assert_eq!(
        reply.native_receiver.as_ref().unwrap().metadata_store_id(),
        id
    );
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &reply)
        .unwrap();
}
#[test]
fn native_registered_metadata_cancel_promotion_catchup_and_all_owner_cold_voter_reopen() {
    let f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("registered-catchup-original");
    let token = CancellationToken::uncancelable();
    f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    let (own, business) = own_mq(&f, &token);
    let id = host.start_owned_metadata(&own, business, &token).unwrap();
    bound_catchup(&f, id);
    let uid = namespaces(&factory);
    let birth = factory.actual_birth().clone();
    let root = factory.database.read().data_path().to_path_buf();
    let original = std::fs::read(root.join(META).join(ORIGINAL)).unwrap();
    let held = host.test_owned_metadata().unwrap();
    let entry = promotion_entry(&f, &token);
    host.stop_owned_metadata(&token).unwrap();
    let canceled = token.child();
    let (before_promotion, business) = own_mq(&f, &canceled);
    let error = host
        .test_start_owned_metadata_with(
            &before_promotion,
            business,
            &canceled,
            |_| Ok(()),
            |phase| {
                if phase == "after-bind" {
                    commit_only_old_voters(&f, entry.index, &token);
                    canceled.cancel();
                }
                Ok(())
            },
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
    assert_eq!(protected_id(&factory), (id, true));
    assert!(Arc::ptr_eq(&held, &host.test_owned_metadata().unwrap()));
    assert_eq!(
        held.lock().committed_topology().nodes[&node(4)].metadata_role,
        MetadataMemberRole::Learner,
        "promotion was genuinely committed elsewhere without delivery to target"
    );
    let (fresh, business) = own_mq(&f, &token);
    assert_eq!(
        fresh.committed_topology(business).unwrap().nodes[&node(4)].metadata_role,
        MetadataMemberRole::Voter
    );
    // Strong READY and every original blank/Prepared factory gate remain closed.
    assert!(host
        .validate_owned_metadata_member(&fresh, business, &token)
        .is_err());
    assert!(factory
        .enrolled_metadata_context(&fresh, business, &f.networks[4])
        .is_err());
    assert!(!host
        .validate_registered_metadata_catchup(&fresh, business, &token)
        .unwrap());
    // The independent original MQ cancellation cannot be dropped by supplying
    // an uncanceled parent to the lower producer/consumer.
    let mq_parent = token.child();
    let (to_cancel, cancel_business) = own_mq(&f, &mq_parent);
    let proof = held
        .lock()
        .observe_registered_native_metadata_catchup(&to_cancel, cancel_business, &token)
        .unwrap();
    mq_parent.cancel();
    assert!(proof.require_current(&held.lock()).is_err());
    drop(proof);
    let (retry, business) = own_mq(&f, &token);
    let before = held.lock().status();
    assert_eq!(
        host.start_owned_metadata(&retry, business, &token).unwrap(),
        id
    );
    assert!(Arc::ptr_eq(&held, &host.test_owned_metadata().unwrap()));
    assert_eq!(namespaces(&factory), uid);
    assert_eq!(factory.actual_birth(), &birth);
    assert_eq!(
        std::fs::read(root.join(META).join(ORIGINAL)).unwrap(),
        original
    );
    let resumed = held.lock().status();
    assert!(
        resumed.current_term >= before.current_term
            && resumed.commit_index >= before.commit_index
            && resumed.last_log_index >= before.last_log_index
    );
    if resumed.current_term == before.current_term {
        assert_eq!(resumed.voted_for, before.voted_for);
    }
    // Only real Append/probe delivery may install promotion; rebind did not
    // construct a snapshot from HostMQ getters or numerically advance the FSM.
    bound_catchup(&f, id);
    let (installed, business) = own_mq(&f, &token);
    host.validate_owned_metadata_member(&installed, business, &token)
        .unwrap();
    assert!(held.lock().status().voters.contains(&node(4)));
    let final_status = held.lock().status();
    host.stop_owned_metadata(&token).unwrap();
    host.shutdown().unwrap();
    drop(held);
    drop(factory);
    // ALL target DB/MStore/provider/service/listener owners have ended. Copy
    // only stopped native bytes; no factory/schema/genesis initializer runs.
    let expected_durable = durable_state(&root);
    let closed = std::fs::read(
        root.join(META)
            .join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
    )
    .unwrap();
    for wrong_uuid in [false, true] {
        let copy = f.root.path().join(if wrong_uuid {
            "registered-bad-uuid"
        } else {
            "registered-state-missing"
        });
        copy_closed(&root, &copy);
        if wrong_uuid {
            MetadataConsensusStore::test_corrupt_cold_learner_state(&copy.join(META), true)
                .unwrap();
        } else {
            std::fs::remove_file(
                copy.join(META)
                    .join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
            )
            .unwrap();
        }
        let before = std::fs::read(
            copy.join(META)
                .join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
        )
        .ok();
        let mut bad = open_cold(&f, &copy);
        assert!(start_cold(&f, &mut bad).is_err());
        assert_eq!(protected_id(bad.test_factory()), (id, true));
        assert_eq!(bad.test_custody().2, false);
        assert_eq!(
            std::fs::read(
                copy.join(META)
                    .join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
            )
            .ok(),
            before
        );
        drop(bad);
    }
    let copy = f.root.path().join("registered-voter-cold");
    copy_closed(&root, &copy);
    let mut cold = open_cold(&f, &copy);
    assert_eq!(
        std::fs::read(
            copy.join(META)
                .join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
        )
        .unwrap(),
        closed
    );
    assert_eq!(namespaces(cold.test_factory()), uid);
    assert_eq!(cold.test_factory().actual_birth(), &birth);
    assert_eq!(start_cold(&f, &mut cold).unwrap(), id);
    let actual = cold.test_metadata().unwrap();
    let reopened = actual.lock().status();
    assert_eq!(reopened.current_term, final_status.current_term);
    assert_eq!(reopened.voted_for, final_status.voted_for);
    assert_eq!(reopened.commit_index, final_status.commit_index);
    assert_eq!(reopened.last_log_index, final_status.last_log_index);
    assert_eq!(
        durable_state(&copy),
        expected_durable,
        "Registered promoted reopen must preserve FULL durable state, not just counters"
    );
    bound_catchup(&f, id);
    let (current, business) = own_mq(&f, &token);
    cold.validate_owned_metadata_member(&current, business, &token)
        .unwrap();
    assert_eq!(
        actual
            .lock()
            .registered_native_metadata_member_store_id()
            .unwrap(),
        id
    );
    assert_eq!(namespaces(cold.test_factory()), uid);
    assert_eq!(
        std::fs::read(copy.join(META).join(ORIGINAL)).unwrap(),
        original
    );
    let range = current
        .committed_topology(business)
        .unwrap()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    assert!(f.launches[0]
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
                }
            }
        )
        .is_err());
    drop(actual);
    cold.stop(&token).unwrap();
    drop(cold);
    f.shutdown();
}
