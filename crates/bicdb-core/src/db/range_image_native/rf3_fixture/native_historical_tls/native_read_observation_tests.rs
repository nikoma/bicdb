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

#[test]
fn native_read_observation_uses_current_r2_not_activation_anchor_and_refuses_cq_only_peer() {
    use super::activation_tests::{completion, data_zero_scope};
    use crate::distribution_range_recovery::RecoveryMemberIdentity;
    let mut fixture = NetworkFixture::new_ready_network_with_roles(true, true, false, "n0", "n1");
    fixture.native.root.disable_cleanup(true);
    eprintln!(
        "CURRENT_NATIVE_READ_RPLUSONE_ROOT {}",
        fixture.native.root.path().display()
    );
    let scope = data_zero_scope(&fixture);
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let parent = CancellationToken::uncancelable();
    for apply in [false, true] {
        for destination in [node("n0"), node("n1"), node("n2")] {
            fixture.clients[&node("n0")]
                .recovery_historical_command_observation(
                    &destination,
                    &fixture.request(&scope),
                    apply,
                    &parent,
                )
                .unwrap();
            fixture.wait_idle();
        }
    }
    for destination in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .prepare_native_ready_observation(&destination, &fixture.ready_request(&scope), &parent)
            .unwrap();
        fixture.wait_idle();
    }
    for destination in [node("n0"), node("n2")] {
        fixture.clients[&node("n0")]
            .complete_native_recovery_observation(&destination, &completion(&scope), &parent)
            .unwrap();
        fixture.wait_idle();
    }
    for destination in [node("n0"), node("n2")] {
        fixture.runtimes[&destination]
            .activate_native_recovery(&actor, &completion(&scope), &parent)
            .unwrap();
        fixture.wait_idle();
    }
    fixture.native.transport.set_unavailable(&node("n1"), true);
    fixture.native.transport.enable_native_delivery();
    fixture
        .native
        .transport
        .drop_native_apply_once(node("n2"), 2);
    *fixture.native.topology.write() = fixture.current.read().clone();
    fixture
        .native
        .coordinator
        .recover_local_leadership(8)
        .unwrap();
    fixture.native.databases[&node("n0")]
        .read()
        .install_commit_admission(fixture.native.coordinator.clone());
    fixture
        .native
        .append("current-native-read-real-ack-r2")
        .unwrap();
    let current = fixture.current.read();
    let range = current.range_by_id(scope.range_id).unwrap().clone();
    let spec = &fixture.native.specs[&node("n0")];
    let members = range
        .replicas
        .iter()
        .filter(|r| r.role == crate::RangeReplicaRole::Voter)
        .map(|r| {
            let member = &spec.initial_members[&r.node_id];
            (
                r.node_id.clone(),
                RecoveryMemberIdentity {
                    incarnation: member.incarnation,
                    active_certificate_sha256: member.tls_certificate_sha256.clone(),
                    pending_certificate_sha256: member.pending_tls_certificate_sha256.clone(),
                },
            )
        })
        .collect();
    let request = NativeRangeReadRequest {
        protocol_version: crate::distribution_native_read::NATIVE_RANGE_READ_VERSION,
        scope: NativeRangeReadScope {
            cluster_id: current.cluster_id.clone(),
            bootstrap_id: spec.bootstrap_id,
            range,
            members,
            schema_sha256: current.nodes[&node("n0")].labels
                [crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL]
                .clone(),
            restore_activation: spec.restore_activation.clone(),
        },
        challenge: Uuid::now_v7(),
        budget_ms: 5000,
    };
    drop(current);
    let source = fixture.native.services[&node("n0")].lock();
    let observed = source
        .observe_current_native_range(&request, &parent)
        .unwrap();
    assert_eq!(observed.facts().range_index, 2);
    let source_lineage = fixture.native.databases[&node("n0")]
        .read()
        .current_native_range_source_lineage(scope.range_id)
        .unwrap();
    assert_eq!(
        observed.facts().logical_history_sha256,
        source_lineage.logical_history_sha256()
    );
    assert_eq!(
        observed.local_visibility().get(),
        source_lineage.actual_local_visibility_sequence()
    );
    observed.require_current(&source).unwrap();
    drop(source);
    let anchor = fixture.clients[&node("n0")]
        .observe_native_completion_observation(&node("n0"), &completion(&scope), &parent)
        .unwrap();
    fixture.wait_idle();
    assert_eq!(anchor.reply().range_index, 1);
    assert_ne!(
        anchor.reply().logical_history_sha256,
        observed.facts().logical_history_sha256
    );
    assert!(
        observed.local_visibility().get() > anchor.reply().local_visibility_sequence_at_completion
    );
    let peer = fixture.native.services[&node("n2")].lock();
    assert_eq!(peer.range_write_progress(scope.range_id).last_index, 2);
    assert_eq!(
        fixture.native.databases[&node("n2")]
            .read()
            .current_native_range_source_lineage(scope.range_id)
            .unwrap()
            .range_index(),
        1
    );
    assert!(
        peer.observe_current_native_range(&request, &parent)
            .is_err(),
        "CQ-only R2 plus ActivatedAnchor R1 must not become a current R1 read vote"
    );
    drop(peer);
    // Revalidation is not a lease renewal. This actual observation expires
    // with its original parent even with unchanged physical/catalogue state.
    let deadline = Instant::now() + Duration::from_millis(500);
    let short = parent.child_with_deadline(deadline);
    let source = fixture.native.services[&node("n0")].lock();
    let expiring = source
        .observe_current_native_range(&request, &short)
        .unwrap();
    expiring.require_current(&source).unwrap();
    drop(source);
    std::thread::sleep(
        deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
    );
    assert!(expiring.require_lifetime().is_err());
    assert!(expiring
        .require_current(&fixture.native.services[&node("n0")].lock())
        .is_err());
    assert!(fixture.native.databases[&node("n0")]
        .read()
        .get("items", "current-native-read-real-ack-r2")
        .unwrap()
        .is_some());
}
