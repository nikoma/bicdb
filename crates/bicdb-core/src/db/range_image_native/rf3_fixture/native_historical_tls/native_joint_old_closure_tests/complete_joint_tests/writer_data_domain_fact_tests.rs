//! Lower actual CLOSED-fact and protected OWN-policy prerequisites only.
//! No host lifecycle, writer audit, Serving/APP publication or physical claim.
use super::*;

#[test]
fn native_writer_data_domain_actual_decided_closed_points_preserve_original_policy() {
    assert_eq!(
        run_complete_joint_with_continuation(
            0,
            JointFullContinuation::WriterDataDomainFacts(false)
        ),
        JointTestOutcome::WriterDataDomainFactsOnly
    );
}
#[test]
fn native_writer_data_domain_actual_foreign_mstore_refuses_without_native_change() {
    assert_eq!(
        run_complete_joint_with_continuation(0, JointFullContinuation::WriterDataDomainFacts(true)),
        JointTestOutcome::WriterDataDomainFactsOnly
    );
}
pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    old: &crate::RangeDescriptor,
    decision: NativeJointDurableDecision,
    original: BTreeMap<ClusterNodeId, closed_policy_baseline_tests::OriginalPolicy>,
    acknowledged: &crate::Record,
    parent: &CancellationToken,
    foreign: bool,
) {
    let data = decision.data().clone();
    let topology = fixture.native.topology.read().clone();
    let before = [node("n0"), node("n1"), node("n2"), node("n3")]
        .into_iter()
        .map(|local| {
            (
                local.clone(),
                post_joint_no_data_tests::snapshot(fixture, data.activation_range(), &local),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let owners = decision.into_new_serving_owners().unwrap();
    assert_eq!(
        owners.keys().cloned().collect::<Vec<_>>(),
        data.new_members().keys().cloned().collect::<Vec<_>>()
    );
    for (local, (actual_service, lease)) in owners {
        let native = fixture.native.databases[&local]
            .read()
            .native_recovery_admission(
                &topology.cluster_id,
                topology.range_image_bootstrap_id.unwrap(),
                &local,
                topology.nodes[&local].incarnation,
            )
            .unwrap();
        {
            let service = actual_service.try_lock().unwrap();
            let db = fixture.native.databases[&local].try_read().unwrap();
            let fact = service
                .with_writer_data_domain_store(&db, |store| {
                    db.observe_closed_writer_joint_fact(
                        &native,
                        &data,
                        store,
                        lease.image(),
                        lease.try_frozen()?,
                    )
                })
                .unwrap();
            assert_eq!(fact.point().range_index, data.range_index());
            assert_eq!(
                fact.point().logical_history_sha256,
                data.logical_history_sha256()
            );
            assert_eq!(fact.point().birth_id, native.birth_id());
            assert_eq!(
                fact.point().old_close_sha256.is_some(),
                data.old_members().contains_key(&local)
            );
            assert_eq!(
                db.get("items", &acknowledged.id).unwrap().as_deref(),
                Some(acknowledged)
            );
            service
                .with_writer_data_domain_store(&db, |store| {
                    fact.require_current(&db, store, lease.image(), lease.try_frozen()?)
                })
                .unwrap();
        }
        let business = Uuid::now_v7();
        let own = fixture.clients[&local]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                parent,
            )
            .unwrap();
        let owned = fixture.metadata[&local]
            .try_lock()
            .unwrap()
            .observe_owned_native_metadata_member(&own, business, parent)
            .unwrap();
        {
            let db = fixture.native.databases[&local].try_read().unwrap();
            let _journal = db.try_native_image_journal().unwrap();
            // The real native-WAL callback must recheck the protected original
            // owner without recursively acquiring this same journal.
            owned.require_database_point(&db).unwrap();
        }
        let policy = crate::db::NativeWriterDataDomainPolicy::from_actual(
            &actual_service,
            &fixture.metadata[&local],
            own,
            business,
            parent,
        )
        .unwrap();
        policy.require_owned_metadata_current().unwrap();
        if foreign && local == node("n1") {
            let business = Uuid::now_v7();
            let original_request =
                parent.child_with_deadline(std::time::Instant::now() + Duration::from_secs(5));
            let own = fixture.clients[&local]
                .acquire_host_metadata_observation(
                    &node("n1"),
                    business,
                    Duration::from_secs(5),
                    &original_request,
                )
                .unwrap();
            let error = crate::db::NativeWriterDataDomainPolicy::from_actual(
                &actual_service,
                &fixture.metadata[&node("n2")],
                own,
                business,
                &original_request,
            )
            .err()
            .expect("real foreign protected MStore must refuse");
            original_request.check().unwrap();
            assert!(
                matches!(error, crate::BicDbError::Cluster(ref message) if message=="metadata RPC destination differs from actual native birth/incarnation"),
                "{error}"
            );
        }
    }
    closed_policy_baseline_tests::require_original_unchanged(fixture, old, &original);
    for (local, snapshot) in before {
        assert_eq!(post_joint_no_data_tests::snapshot(fixture, data.activation_range(), &local), snapshot,
            "lower CLOSED facts/OWN policy must not write DATA audit, install policy, alter rows/UID/history or publish serving");
    }
}
