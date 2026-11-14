//! Real faults AFTER full fresh collection and owned pin drainage. No
//! physical corruption or caller cancellation is substituted during setup.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointActivationAdmission, NativeJointPreparationPhaseQuorum,
    NativeJointServingPublicationRound,
};

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetCollectionForeignArchive
            | SourceCase::TargetCollectionLocalArchive
            | SourceCase::TargetCollectionProducerCanceled
            | SourceCase::TargetCollectionStoreRevoked
            | SourceCase::TargetCollectionMetadataChanged
            | SourceCase::TargetCollectionControlledWriter
    )
}
macro_rules! case {
    ($name:ident,$case:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::$case));
        }
    };
}
case!(
    native_registered_fifth_collection_actual_foreign_archive_after_collection_refuses_publication,
    TargetCollectionForeignArchive
);
case!(
    native_registered_fifth_collection_actual_local_archive_after_collection_refuses_publication,
    TargetCollectionLocalArchive
);
case!(
    native_registered_fifth_collection_actual_producer_session_after_collection_refuses_publication,
    TargetCollectionProducerCanceled
);
case!(
    native_registered_fifth_collection_actual_store_aba_after_collection_refuses_publication,
    TargetCollectionStoreRevoked
);
case!(
    native_registered_fifth_collection_actual_mstore_after_collection_refuses_publication,
    TargetCollectionMetadataChanged
);
case!(native_registered_fifth_collection_actual_controlled_writer_after_collection_refuses_publication, TargetCollectionControlledWriter);

struct Outcome {
    publication: Result<()>,
    before: serde_json::Value,
    after: Result<serde_json::Value>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    phase: &Arc<NativeJointPreparationPhaseQuorum>,
    admission: &NativeJointActivationAdmission,
    publication: &mut NativeJointServingPublicationRound,
    producer: &CancellationToken,
    aggregate: &CancellationToken,
    registered: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>,
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    process: &CancellationToken,
    output: &std::path::Path,
    acknowledged: &[serde_json::Value],
) {
    let mut events = Vec::new();
    let result = run(
        case,
        phase,
        admission,
        publication,
        producer,
        aggregate,
        registered,
        services,
        metadata,
        process,
        output,
        &mut events,
    );
    // Exact API result/error and all trigger events precede assertions. A
    // prerequisite Err cannot become a successful intended fault observation.
    let value = serde_json::json!({"case":format!("{case:?}"),
        "full_fresh_collection_and_activation_returned":true,
        "control_error":result.as_ref().err().map(ToString::to_string),
        "publisher_error":result.as_ref().ok().and_then(|r|r.publication.as_ref().err()).map(ToString::to_string),
        "publisher_returned_success":result.as_ref().ok().map(|r|r.publication.is_ok()),
        "before_publication":result.as_ref().ok().map(|r|&r.before),
        "after_publication":result.as_ref().ok().and_then(|r|r.after.as_ref().ok()),
        "after_publication_diagnostic_error":result.as_ref().ok().and_then(|r|r.after.as_ref().err()).map(ToString::to_string),
        "events":events,"original_source_ACKs":acknowledged,
        "scope":"same-process actual protected publication refusal; not physical qualification"});
    eprintln!("NATIVE_COLLECTION_BINDING_CONTROL {value}");
    evidence(
        &output.join("collection-binding-before-assertions.json"),
        &value,
    )
    .unwrap();
    let outcome = result.unwrap();
    let error = outcome
        .publication
        .err()
        .expect("actual final publisher must refuse");
    match case {
        SourceCase::TargetCollectionForeignArchive => {
            assert!(matches!(error, BicDbError::Cluster(ref d)
            if d == "native range image: NEW joint preparation immutable archive missing"))
        }
        SourceCase::TargetCollectionLocalArchive => {
            assert!(matches!(error, BicDbError::Cluster(ref d)
            if d == "native range image: publication scope lost actual preparation pointer/archive"))
        }
        SourceCase::TargetCollectionProducerCanceled => {
            assert!(matches!(error, BicDbError::QueryCanceled))
        }
        SourceCase::TargetCollectionStoreRevoked => {
            assert!(matches!(error, BicDbError::Cluster(ref d)
            if d == "actual service range Store slot ended or changed"))
        }
        SourceCase::TargetCollectionMetadataChanged => {
            assert!(matches!(error, BicDbError::Cluster(ref d)
            if d == "metadata consensus: actual metadata authority version changed/dirty; retained native outcome is not a current grant"))
        }
        SourceCase::TargetCollectionControlledWriter => {
            assert!(matches!(error, BicDbError::Cluster(ref d)
            if d == "range mutation gate: controlled WAL/native callback is still active"))
        }
        _ => unreachable!(),
    }
    let after_snapshot = outcome.after.unwrap();
    let before = outcome.before["members"].as_object().unwrap();
    let after = after_snapshot["members"].as_object().unwrap();
    for snapshot in [&outcome.before, &after_snapshot] {
        assert_eq!(snapshot["phase_cancel_requested"], false);
        assert!(snapshot["phase_remaining_us"]
            .as_u64()
            .is_some_and(|n| n > 0));
        assert_eq!(snapshot["aggregate_caller_live"], true);
    }
    assert_eq!(before.len(), after.len());
    for (id, original) in before {
        let current = &after[id];
        assert!(original["storage"]["bridge_current"].is_null());
        assert!(current["storage"]["bridge_current"].is_null());
        assert_eq!(
            original["storage"]["native_next_xid"],
            current["storage"]["native_next_xid"]
        );
        assert_eq!(
            original["storage"]["native_next_lsn"],
            current["storage"]["native_next_lsn"]
        );
        assert_eq!(
            original["storage"]["preparation"],
            current["storage"]["preparation"]
        );
        assert_eq!(original["frozen_owner"], current["frozen_owner"]);
        assert!(!current["gate"]["frozen"].is_null());
        assert_eq!(original["gate"]["frozen"], current["gate"]["frozen"]);
        assert_eq!(current["gate"]["frozen_owner_dropped"], false);
        assert_eq!(current["gate"]["poisoned"], false);
        assert!(current["gate"]["capture_pin"].is_null());
        for member in [original, current] {
            assert_eq!(member["request"]["cancel_requested"], false);
            assert!(member["request"]["remaining_us"]
                .as_u64()
                .is_some_and(|n| n > 0));
            assert_eq!(
                member["producer_session_error"].is_null(),
                !matches!(case, SourceCase::TargetCollectionProducerCanceled)
            );
        }
    }
    aggregate.check().unwrap();
    process.check().unwrap();
}

#[allow(clippy::too_many_arguments)]
fn run(
    case: SourceCase,
    phase: &Arc<NativeJointPreparationPhaseQuorum>,
    admission: &NativeJointActivationAdmission,
    publication: &mut NativeJointServingPublicationRound,
    producer: &CancellationToken,
    aggregate: &CancellationToken,
    registered: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>,
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    process: &CancellationToken,
    output: &std::path::Path,
    events: &mut Vec<serde_json::Value>,
) -> Result<Outcome> {
    let local = admission.local_preparation().local_node_id().clone();
    let i = (0..services.len())
        .find(|i| node(*i) == local)
        .ok_or_else(|| invalid("actual publication member absent"))?;
    if local != node(4)
        || !admission.data().new_members().contains_key(&local)
        || !Arc::ptr_eq(registered.service(), &services[i])
        || registered.target().native().local_node_id() != &local
        || admission.local_preparation().native_database_incarnation()
            != registered.target().native().database_incarnation()
    {
        return Err(invalid(
            "post-collection fault requires SAME actual Registered target owner/open",
        ));
    }
    let original_a = registered.retained_freeze()?.try_as_frozen()?.owner();
    aggregate.check()?;
    producer.check()?;
    process.check()?;
    admission.require_live()?;
    let before_fault = phase.test_prepublication_snapshot()?;
    if before_fault["members"][local.as_str()]["frozen_owner"] != serde_json::to_value(original_a)?
    {
        return Err(invalid(
            "post-collection fault changed original Registered A",
        ));
    }
    events.push(
        serde_json::json!({"stage":"actual_registered_target_origin_bound",
        "member":local,"original_a":original_a,
        "native_database_incarnation":registered.target().native().database_incarnation(),
        "actual_registered_service_same_arc":true}),
    );
    events.push(serde_json::json!({"stage":"full_collection_and_drain_before_fault", "actual":before_fault}));
    let controlled = if matches!(case, SourceCase::TargetCollectionControlledWriter) {
        events.push(
            serde_json::json!({"stage":"genuine_controlled_admission_intent", "member":local}),
        );
        let held = phase.test_hold_post_collection_controlled(&local);
        events.push(serde_json::json!({"stage":"genuine_controlled_admission_returned", "success":held.is_ok(),
            "error":held.as_ref().err().map(ToString::to_string)}));
        let held = held?;
        events.push(
            serde_json::json!({"stage":"genuine_controlled_admission_acquired", "member":local}),
        );
        Some(held)
    } else {
        None
    };
    match case {
        SourceCase::TargetCollectionForeignArchive | SourceCase::TargetCollectionLocalArchive => {
            let selected = if matches!(case, SourceCase::TargetCollectionForeignArchive) {
                admission
                    .data()
                    .new_members()
                    .keys()
                    .find(|n| **n != local)
                    .ok_or_else(|| invalid("actual foreign prepared member absent"))?
                    .clone()
            } else {
                local.clone()
            };
            events.push(serde_json::json!({"stage":"actual_native_archive_delete_intent", "member":selected}));
            let damage = phase.test_remove_post_collection_archive(&selected, process);
            events.push(serde_json::json!({"stage":"actual_native_archive_delete_returned", "member":selected,
                "success":damage.is_ok(),"error":damage.as_ref().err().map(ToString::to_string),
                "actual_receipt":damage.as_ref().ok(),"no_returned_receipt_is_not_proof_of_no_effect":true}));
            let damage = damage?;
            if damage["original_current"] != damage["removed_archive"]
                || damage["actual_archive_absent"] != true
                || damage["actual_controlled_admission_validated_before_and_after"] != true
            {
                return Err(invalid("post-collection actual archive fault not proven"));
            }
            events.push(serde_json::json!({"stage":"actual_native_archive_deleted", "member":selected, "damage":damage}));
        }
        SourceCase::TargetCollectionProducerCanceled => {
            events.push(serde_json::json!({"stage":"actual_sealed_producer_cancel_intent"}));
            producer.cancel();
            events.push(serde_json::json!({"stage":"actual_sealed_producer_session_canceled", "producer_canceled":producer.is_cancel_requested(),
                "aggregate_live":aggregate.check().is_ok(),"phase_and_own":phase.test_prepublication_snapshot()?}));
        }
        SourceCase::TargetCollectionStoreRevoked => {
            let original = services[i]
                .lock()
                .native_replica_listener_store_open_guard();
            events.push(serde_json::json!({"stage":"actual_store_replacement_intent", "original_id":original.id()}));
            let replacement = crate::RangeWriteStore::open(
                output.join("post-collection-store"),
                admission.data().cluster_id().clone(),
                local.clone(),
                true,
            )?;
            let old = services[i]
                .lock()
                .test_replace_native_range_write_store(replacement);
            let removed = services[i]
                .lock()
                .test_replace_native_range_write_store(old);
            drop(removed);
            events.push(serde_json::json!({"stage":"actual_store_replaced_and_restored", "original_id":original.id(),
                "current_id":services[i].lock().native_replica_listener_store_open_guard().id(),
                "original_slot_error":original.require_live().err().map(|e|e.to_string())}));
        }
        SourceCase::TargetCollectionMetadataChanged => {
            let mut store = metadata[i].lock();
            let before = store.test_actual_publication_version_evidence()?;
            let term = store
                .status()
                .current_term
                .checked_add(1)
                .ok_or_else(|| invalid("actual term overflow"))?;
            events.push(serde_json::json!({"stage":"actual_local_mstore_term_intent", "before":before,"requested_term":term}));
            let changed = store.observe_remote_term(term);
            let after = store.test_actual_publication_version_evidence();
            events.push(serde_json::json!({"stage":"actual_local_mstore_term_returned", "requested_term":term,
                "changed":changed.as_ref().ok(),"mutation_error":changed.as_ref().err().map(ToString::to_string),
                "actual_after":after.as_ref().ok(),"readback_error":after.as_ref().err().map(ToString::to_string),
                "no_returned_receipt_is_not_proof_of_no_effect":true}));
            let changed = changed?;
            let after = after?;
            events.push(serde_json::json!({"stage":"actual_local_mstore_term_advanced", "before":before,"after":after,"requested_term":term,"changed":changed}));
            if !changed {
                return Err(invalid("actual MStore did not advance"));
            }
            if after["durable_state_term"] != term
                || after["status"]["current_term"] != term
                || after["version"]["actual_open"] != before["version"]["actual_open"]
                || !after["version"]["generation"]
                    .as_u64()
                    .zip(before["version"]["generation"].as_u64())
                    .is_some_and(|(a, b)| a > b)
                || after["version"]["ready"] != true
                || after["version"]["failed"] != false
                || after["durable_state_checksum_valid"] != true
            {
                return Err(invalid(
                    "actual MStore durable term/version advance not proven",
                ));
            }
        }
        SourceCase::TargetCollectionControlledWriter => {}
        _ => unreachable!(),
    }
    // Baseline AFTER intentional corruption WAL. No snapshot/absence is used
    // as authority; actual global pager counters diagnose publisher effects.
    let before = phase.test_prepublication_snapshot()?;
    if matches!(case, SourceCase::TargetCollectionControlledWriter)
        && before["members"][local.as_str()]["gate"]["controlled"] != 1
    {
        return Err(invalid("genuine controlled mutation was not retained"));
    }
    aggregate.check()?;
    process.check()?;
    let publication_result = publication.publish_actual_local_member(admission, process);
    events.push(serde_json::json!({"stage":"actual_publisher_returned", "success":publication_result.is_ok(),
        "error":publication_result.as_ref().err().map(ToString::to_string)}));
    // Preserve the primary publisher outcome if independent diagnostics fail.
    let after = phase.test_prepublication_snapshot();
    drop(controlled);
    Ok(Outcome {
        publication: publication_result,
        before,
        after,
    })
}
