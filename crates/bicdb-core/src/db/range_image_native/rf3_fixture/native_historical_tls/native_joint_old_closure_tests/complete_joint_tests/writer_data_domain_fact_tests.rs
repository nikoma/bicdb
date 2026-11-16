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
    for (member_ordinal, (local, (actual_service, lease))) in owners.into_iter().enumerate() {
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
        // SAME original OWN/business/deadline. The production capture owns
        // the actual attached MStore/slot and bounds its member acquisition;
        // instantaneous test-thread mutex availability is not authority.
        let capture_entered = std::time::Instant::now();
        let captured =
            crate::distribution_data::ClusterDataNodeService::capture_actual_joint_metadata_point(
                &actual_service,
                &own,
                business,
                parent,
            );
        let capture_returned = std::time::Instant::now();
        if !matches!(&captured, Ok(Some(_))) {
            // Failure-only observation AFTER the original producer returned.
            // This availability is diagnostic-time, not the original holder.
            let immediate_available_after_return = fixture.metadata[&local].try_lock().is_some();
            eprintln!(
                "RF3_OWNED_METADATA_POINT_CAPTURE {}",
                serde_json::json!({
                    "member_ordinal":member_ordinal,"node":local,"original_OWN_context":own.context_id(),
                    "capture_elapsed_us":capture_returned.duration_since(capture_entered).as_micros(),
                    "OWN_remaining_us_at_capture_entry":own.deadline().saturating_duration_since(capture_entered).as_micros(),
                    "OWN_remaining_us_at_capture_return":own.deadline().saturating_duration_since(capture_returned).as_micros(),
                    "diagnostic_mstore_immediate_available_after_return":immediate_available_after_return,
                    "missing_actual_point":matches!(&captured, Ok(None)),
                    "original_error":captured.as_ref().err().map(ToString::to_string),
                    "new_OWN_or_deadline_issued":false,
                })
            );
        }
        let owned = captured
            .unwrap()
            .expect("born-enforced RF3 fixture requires actual native MStore point");
        {
            let db = fixture.native.databases[&local].try_read().unwrap();
            let _journal = db.try_native_image_journal().unwrap();
            // The real native-WAL callback must recheck the protected original
            // owner without recursively acquiring this same journal.
            // require_native_point invokes the SAME member's
            // require_database_point and additionally checks the retained
            // actual database/slot/version. It does not reacquire this journal.
            owned.require_native_point(&db).unwrap();
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
