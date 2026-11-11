//! Genuine recovered serving fixture; the new reads themselves are LOCAL and
//! issue no metadata/data RPC, recovery nonce, freeze or MVCC read barrier.
use super::*;
use crate::distribution_native_read::{NativeRangeReadRequest, NativeRangeReadScope};
#[test]
fn native_read_observation_recovered_serving_requires_actual_activated_open() {
    let (fixture, scope) = NetworkFixture::all_r1_prepared();
    let topology = fixture.current.read().clone();
    let role = fixture.ready_role(&scope);
    let request = NativeRangeReadRequest {
        protocol_version: crate::distribution_native_read::NATIVE_RANGE_READ_VERSION,
        scope: NativeRangeReadScope {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id: role.bootstrap_id(),
            range: role.current_range().clone(),
            members: role.member_credentials().clone(),
            schema_sha256: role.expected_schema_sha256().unwrap().into(),
            restore_activation: role.restore_activation().cloned(),
        },
        challenge: Uuid::now_v7(),
        budget_ms: 5000,
    };
    assert!(fixture.native.services[&node("n1")]
        .lock()
        .observe_current_native_range(&request, &CancellationToken::uncancelable())
        .is_err());
    for name in ["n0", "n1", "n2"] {
        fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
    }
    assert!(fixture.native.services[&node("n1")]
        .lock()
        .observe_current_native_range(&request, &CancellationToken::uncancelable())
        .is_err());
    for name in ["n0", "n1", "n2"] {
        fixture.runtimes[&node(name)]
            .activate_native_recovery(
                &fixture.actor(),
                &fixture.completion_request(&scope),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.wait_idle();
    }
    let service = fixture.native.services[&node("n1")].lock();
    let before = std::fs::read(
        fixture.native.databases[&node("n1")]
            .read()
            .data_path()
            .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
    )
    .unwrap();
    let observed = service
        .observe_current_native_range(&request, &CancellationToken::uncancelable())
        .unwrap();
    assert_eq!(observed.facts().range_index, 1);
    assert!(observed
        .facts()
        .native_serving_record_commitment_sha256
        .is_some());
    observed.require_current(&service).unwrap();
    let path = fixture.native.databases[&node("n1")]
        .read()
        .data_path()
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    assert_eq!(std::fs::read(path).unwrap(), before);
    drop(service);
    let gate = fixture.native.databases[&node("n1")]
        .read()
        .range_mutation_gate();
    let frozen = gate.try_freeze(scope.range_id, Uuid::now_v7()).unwrap();
    assert!(observed.require_lifetime().is_err());
    drop(frozen);
    assert!(observed
        .require_current(&fixture.native.services[&node("n1")].lock())
        .is_err());
    // Dropping a new freeze cannot restore the old current-open serving arm.
    assert!(fixture.native.services[&node("n1")]
        .lock()
        .observe_current_native_range(&request, &CancellationToken::uncancelable())
        .is_err());
}
