//! Actual native serving publication versus a DATA predecessor selector. The
//! fixture declares metadata configuration and uses real MQ/closed RF1/native
//! WAL producers; this is not an operator/election/physical qualification.
use super::*;
#[test]
fn allocation_predecessor_never_normalizes_a_genuine_later_native_serving_epoch() {
    let mut fixture = Fixture::new();
    fixture.seed(fixture.range().id, 1);
    let before = fixture.range().clone();
    {
        let db = fixture.db.read();
        let actual = db.current_native_range_source_lineage(before.id).unwrap();
        db.require_native_allocation_predecessor(&actual, &before)
            .unwrap();
    }
    fixture.topology.ranges.values_mut().next().unwrap().epoch =
        before.epoch.checked_add(2).unwrap();
    let nonce = Uuid::now_v7();
    let role = fixture.role(nonce);
    let frozen = fixture.freeze(&role);
    let mut store = fixture.store();
    let local = fixture
        .attest(
            role,
            &mut store,
            Arc::clone(&frozen),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let prepared = store
        .prepare_native_recovery_ready(&fixture.db.read(), local)
        .unwrap();
    let closed = actual_local_closed_ready_quorum(
        &fixture,
        &store,
        &prepared,
        nonce,
        &CancellationToken::uncancelable(),
    );
    let fresh = fixture.role(nonce);
    let receipt = fixture
        .db
        .read()
        .persist_native_recovered_serving_configuration(&prepared, &store, &closed, &fresh)
        .unwrap();
    receipt.require_current(&fixture.db.read()).unwrap();
    let (original, result, after) = {
        let db = fixture.db.read();
        let actual = db.current_native_range_source_lineage(before.id).unwrap();
        let snapshot = serde_json::json!({"base":actual.base_range(),"native_write":actual.native_write_range().unwrap(),
            "index":actual.range_index(),"history":actual.logical_history_sha256(),"receipt":receipt.checksum_sha256()});
        let result = db.require_native_allocation_predecessor(&actual, &before);
        let again = db.current_native_range_source_lineage(before.id).unwrap();
        let after = serde_json::json!({"base":again.base_range(),"native_write":again.native_write_range().unwrap(),
            "index":again.range_index(),"history":again.logical_history_sha256(),"receipt":receipt.checksum_sha256()});
        (snapshot, result, after)
    };
    eprintln!(
        "ACTUAL_ALLOCATION_PREDECESSOR_REFUSAL {}",
        serde_json::json!({"original":original,
        "before_selector":before,"candidate_epoch_DATA_only":before.epoch+1,
        "actual_error":result.as_ref().err().map(ToString::to_string),"after":after,
        "actual_native_WAL_and_closed_RF1_not_operator_qualification":true})
    );
    assert!(matches!(&result,Err(BicDbError::Cluster(message))
        if message=="native range image: allocation actual native predecessor requires distinct transition proof"));
    assert_eq!(
        original, after,
        "read-only predecessor refusal changed native history"
    );
    assert_eq!(original["base"]["epoch"], before.epoch);
    assert_eq!(original["native_write"]["epoch"], before.epoch + 2);
}
