//! A gateway that already returned an externally ACKed full value retains its
//! actual old route while a real same-voter metadata transfer is committed.
//! Joint placement transfer remains outside the independent read delivery.
use super::*;

#[test]
fn native_event_endpoint_same_stale_gateway_preserves_full_acked_value_after_real_transfer() {
    let mut fixture = EnforcedNetwork::new_endpoint_mode(true, true);
    let (old_range, key) = fixture.event_range_key("retained-gateway");
    let original_topology = fixture.native.topology.read().clone();
    let gateway = fixture.event_client(original_topology.clone(), Duration::from_secs(20));
    let expected = json!({
        "external_id": "ack-before-transfer-1",
        "payload": "0123456789-abcdef".repeat(1024),
        "nested": { "none": null, "empty": {}, "list": [true, 7, "original", [1, 2, 3]] }
    });
    let serialized = serde_json::to_vec(&expected).unwrap();
    assert!(serialized.len() > 16 * 1024 && serialized.len() < crate::MAX_CLUSTER_EVENT_BYTES);
    // This SAME public gateway really sends Append through the TLS receiver,
    // receives an application ACK, and successfully reads before any transfer.
    assert!(gateway.append("tenant", &key, expected.clone()).unwrap());
    assert_eq!(
        gateway.get_strong("tenant", &key).unwrap(),
        Some(expected.clone())
    );
    fixture.idle();

    // Genuine fsynced metadata proposal and quorum commit. A fixture decision
    // selects the new leader; no MQ, native history, Ready or serving fact is
    // synthesized, and this is not automatic election or membership placement.
    fixture.change_committed_leader(old_range.id);
    let range = fixture
        .native
        .topology
        .read()
        .range_by_id(old_range.id)
        .unwrap()
        .clone();
    assert_eq!(range.epoch, old_range.epoch + 1);
    assert_eq!(range.leader, node("n2"));
    assert_eq!(range.replicas, old_range.replicas);
    fixture.refresh_actual_writer_policies();
    for id in [node("n0"), node("n1")] {
        fixture.dispatchers[&id]
            .publish(&fixture.coordinators[&id], &fixture.hosts[&id])
            .unwrap();
    }
    fixture.dispatchers[&node("n2")].clear();

    // Assert the ACTUAL authenticated response to the old route. A request
    // trace alone would not distinguish a redirect from an unrelated failure.
    let stale_request = fixture.event_request(&key, &old_range);
    let stale_reply = fixture.wire_get("n0", &stale_request);
    assert_eq!(
        stale_reply.business_request_id,
        stale_request.business_request_id
    );
    assert_eq!(stale_reply.challenge, stale_request.challenge);
    match stale_reply.result {
        NativeEventGetResult::Redirect {
            route,
            metadata_context_id,
            metadata_term,
            metadata_read_index,
        } => {
            assert_eq!(route.leader, range.leader);
            assert_eq!(route.range_id, range.id);
            assert_eq!(route.range_epoch, range.epoch);
            assert!(route.generation > original_topology.generation);
            assert!(!metadata_context_id.is_nil() && metadata_term > 0 && metadata_read_index > 0);
        }
        other => {
            panic!("old committed route returned value/absence or an unstructured error: {other:?}")
        }
    }
    let current_request = fixture.event_request(&key, &range);
    let unready_reply = fixture.wire_get("n2", &current_request);
    match unready_reply.result {
        NativeEventGetResult::Unavailable {
            code,
            retry_after_ms,
            ..
        } => {
            assert!(matches!(
                code,
                NativeEventGetUnavailableCode::MissingWriter
                    | NativeEventGetUnavailableCode::MissingDispatcher
            ));
            assert!(retry_after_ms > 0 && retry_after_ms <= 1000);
        }
        other => panic!("unready new leader returned successful value/absence: {other:?}"),
    }
    fixture.idle();
    let offsets = fixture
        .dispatchers
        .iter()
        .map(|(id, d)| (id.clone(), d.test_observed_requests().len()))
        .collect::<BTreeMap<_, _>>();
    let read_key = key.clone();
    let started = Instant::now();
    // Retain the actual gateway object created BEFORE transfer. No refresh,
    // route replacement, new client or caller-injected new MQ occurs here.
    let read = std::thread::spawn(move || gateway.get_strong("tenant", &read_key));
    let until = Instant::now() + Duration::from_secs(3);
    while fixture.dispatchers[&node("n2")]
        .test_observed_requests()
        .len()
        == offsets[&node("n2")]
    {
        assert!(
            Instant::now() < until,
            "retained stale gateway never reached actual unready new leader"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    recover_and_publish_endpoint_leader(&fixture, &range);
    assert_eq!(
        read.join().unwrap().unwrap(),
        Some(expected.clone()),
        "full externally ACKed value must survive; absence is forbidden"
    );
    assert!(started.elapsed() < Duration::from_secs(20));
    let attempts = fixture
        .dispatchers
        .iter()
        .flat_map(|(id, d)| d.test_observed_requests().into_iter().skip(offsets[id]))
        .collect::<Vec<_>>();
    assert!(attempts.len() >= 3);
    assert_eq!(
        attempts.iter().map(|t| t.0).collect::<BTreeSet<_>>().len(),
        1
    );
    assert_eq!(
        attempts.iter().map(|t| t.1).collect::<BTreeSet<_>>().len(),
        attempts.len()
    );
    assert!(fixture.dispatchers[&node("n0")]
        .test_observed_requests()
        .iter()
        .skip(offsets[&node("n0")])
        .any(|t| t.2 == old_range.epoch));
    assert!(
        fixture.dispatchers[&node("n2")]
            .test_observed_requests()
            .len()
            >= offsets[&node("n2")] + 2
    );
    let id = crate::cluster_event_record_id("tenant", &key).unwrap();
    assert_eq!(
        fixture.native.databases[&node("n2")]
            .read()
            .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap()
            .unwrap()
            .metadata,
        expected
    );
    fixture.idle();
}
