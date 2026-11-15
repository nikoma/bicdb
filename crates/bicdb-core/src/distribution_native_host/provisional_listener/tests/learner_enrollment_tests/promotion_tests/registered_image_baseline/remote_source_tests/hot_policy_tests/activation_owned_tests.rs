//! Actual SAME-owner activation writes and fresh admitted retries. No changed
//! timeout, synthetic health, new native transaction or DTO-derived permission.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointOpenAdmission, NativeJointOpenPhaseQuorum, NativeJointPublishedServingQuorum,
};
pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetActivationRenameRetry
            | SourceCase::TargetActivationDirSyncRetry
            | SourceCase::TargetActivationForeignOwner
            | SourceCase::TargetActivationProducerCanceled
            | SourceCase::TargetActivationLaterSameBytes
            | SourceCase::TargetActivationLawfulOtherRange
    )
}
macro_rules! case {
    ($name:ident,$variant:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::$variant));
        }
    };
}
case!(
    native_registered_fifth_activation_actual_rename_unknown_same_owner_fresh_retry,
    TargetActivationRenameRetry
);
case!(
    native_registered_fifth_activation_actual_directory_sync_unknown_same_owner_fresh_retry,
    TargetActivationDirSyncRetry
);
case!(
    native_registered_fifth_activation_actual_foreign_owner_and_gate_refused,
    TargetActivationForeignOwner
);
case!(
    native_registered_fifth_activation_actual_original_producer_cancel_refuses_retry,
    TargetActivationProducerCanceled
);
case!(
    native_registered_fifth_activation_actual_later_same_bytes_io_cannot_reuse_verified_owner,
    TargetActivationLaterSameBytes
);
fn fresh(
    published: &Arc<NativeJointPublishedServingQuorum>,
    clients: &[Arc<TcpClusterRelocationTransport>],
) -> Result<Arc<NativeJointOpenPhaseQuorum>> {
    let mut phase = published.begin_fresh_open_phase(Duration::from_secs(4))?;
    for member in published.data().new_members().keys() {
        let i = (0..5).find(|i| node(*i) == *member).unwrap();
        phase.acquire_actual_member(&clients[i], &node(0))?;
    }
    phase.finish().map(Arc::new)
}
type Activated = (
    Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner,
);
#[allow(clippy::too_many_arguments)]
pub(super) fn activate(
    case: SourceCase,
    published: &Arc<NativeJointPublishedServingQuorum>,
    admission: &NativeJointOpenAdmission,
    clients: &[Arc<TcpClusterRelocationTransport>],
    database: &Arc<parking_lot::RwLock<BicDb>>,
    producer: &CancellationToken,
    process: &CancellationToken,
    output: &std::path::Path,
) -> Option<Activated> {
    let root = database.read().data_path().to_path_buf();
    let mut events = Vec::new();
    let result = (|| -> Result<Option<Activated>> {
        let site = if matches!(case, SourceCase::TargetActivationDirSyncRetry) {
            "native_joint_activation.catalogue.dir_sync"
        } else {
            "native_joint_activation.catalogue.rename"
        };
        let fault = bicdb_page::test_faults::arm_io_failure(site, &root, 1, false);
        events
            .push(serde_json::json!({"stage":"actual_activation_write_intent","fault_site":site}));
        let initial = admission.persist_local_catalogue_activation(process);
        let fired = fault.fired();
        drop(fault);
        events.push(serde_json::json!({"stage":"actual_activation_write_return","returned_ok":initial.is_ok(),"exact_error":initial.as_ref().err().map(ToString::to_string),"fault_fired":fired}));
        let before = published.test_owned_activation_state(&node(4))?;
        events
            .push(serde_json::json!({"stage":"retained_original_failed_write","snapshot":before}));
        if matches!(case, SourceCase::TargetActivationForeignOwner) {
            let foreign = published.test_owned_activation_foreign(&node(4));
            events.push(serde_json::json!({"stage":"actual_foreign_owner_and_gate_return","result":foreign.as_ref().ok(),"error":foreign.as_ref().err().map(ToString::to_string)}));
            evidence(
                &output.join("owned-activation-before-assertions.json"),
                &events,
            )?;
            let foreign = foreign?;
            assert_eq!(fired, 1);
            assert!(initial.is_err());
            assert_eq!(foreign["actual_foreign_owner_returned_ok"], false);
            assert_eq!(foreign["actual_foreign_gate_returned_ok"], false);
            assert_eq!(foreign["owner_error"],"cluster error: activation retry lost same owner/Store/open/native/whole-head binding");
            assert_eq!(foreign["uncertain"], true);
            assert_eq!(foreign["IO_attempts"], 1);
            return Ok(None);
        }
        if matches!(case, SourceCase::TargetActivationProducerCanceled) {
            events.push(serde_json::json!({"stage":"cancel_actual_original_producer_intent"}));
            producer.cancel();
        }
        // A distinct fresh phase obtains every actual member's OWN/MStore.
        // The failed admission/request is neither extended nor re-issued.
        let retry = fresh(published, clients);
        events.push(serde_json::json!({"stage":"fresh_actual_collection_return","returned_ok":retry.is_ok(),"exact_error":retry.as_ref().err().map(ToString::to_string)}));
        let after = published.test_owned_activation_state(&node(4));
        events.push(serde_json::json!({"stage":"after_fresh_retry","snapshot":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string)}));
        evidence(
            &output.join("owned-activation-before-assertions.json"),
            &events,
        )?;
        assert_eq!(fired, 1);
        assert!(initial.is_err());
        assert_eq!(before["catalogue"]["verified_at_snapshot_entry"], false);
        assert_eq!(before["catalogue"]["owns_uncertainty"], true);
        assert_eq!(
            before["catalogue"]["actual_io_returns"][0]["actual_return"],
            "returned_error"
        );
        assert_eq!(before["catalogue"]["uncertain_at_snapshot_entry"], true);
        let after = after?;
        assert_same_native(&before, &after);
        if matches!(case, SourceCase::TargetActivationProducerCanceled) {
            assert!(matches!(retry, Err(BicDbError::QueryCanceled)));
            assert_eq!(
                after["catalogue"]["actual_io_returns"],
                before["catalogue"]["actual_io_returns"]
            );
            assert_eq!(after["catalogue"]["uncertain_at_snapshot_entry"], true);
            return Ok(None);
        }
        let phase = retry?;
        assert_eq!(after["catalogue"]["verified_at_snapshot_entry"], true);
        assert_eq!(after["catalogue"]["uncertain_at_snapshot_entry"], false);
        assert_eq!(
            after["catalogue"]["actual_io_returns"][0],
            before["catalogue"]["actual_io_returns"][0]
        );
        assert_eq!(
            after["catalogue"]["actual_io_returns"][1]["actual_return"],
            "returned_ok"
        );
        assert_eq!(
            after["catalogue"]["native_bridge_sha256"],
            before["catalogue"]["native_bridge_sha256"]
        );
        assert_eq!(
            after["native_preparation_sha256"],
            before["native_preparation_sha256"]
        );
        assert_eq!(after["frozen_owner"], before["frozen_owner"]);
        let fresh_admission = phase.acquire_local_open(&clients[4], &node(0))?;
        let receipt = fresh_admission.persist_local_catalogue_activation(process);
        events.push(serde_json::json!({"stage":"fresh_recovered_activation_receipt_return","returned_ok":receipt.is_ok(),"exact_error":receipt.as_ref().err().map(ToString::to_string)}));
        let receipt = Arc::new(receipt?);
        let release = fresh_admission.retain_registered_release_owner(receipt.clone())?;
        Ok(Some((receipt, release)))
    })();
    // Preserve every actual return before a prerequisite assertion/panic.
    let final_value = serde_json::json!({"case":format!("{case:?}"),"events":events,"control_error":result.as_ref().err().map(ToString::to_string),"scope":"actual same-process catalogue recovery; not physical qualification"});
    eprintln!("NATIVE_OWNED_ACTIVATION_CONTROL {final_value}");
    evidence(
        &output.join("owned-activation-before-assertions.json"),
        &final_value,
    )
    .unwrap();
    result.unwrap()
}

case!(
    native_registered_fifth_activation_actual_unrelated_range_head_refuses_old_comparison,
    TargetActivationLawfulOtherRange
);
/// The stock original SDK, rather than direct metadata/file mutation, produces
/// the later head. A setup refusal is preserved and fails the intended control.
#[allow(clippy::too_many_arguments)]
pub(super) fn lawful_other_range(
    published: &Arc<NativeJointPublishedServingQuorum>,
    local: &ClusterNodeId,
    clients: &[Arc<TcpClusterRelocationTransport>],
    client: &crate::ClusterEventClient,
    topology: &ClusterTopology,
    other: &crate::RangeDescriptor,
    output: &std::path::Path,
) {
    let mut events = Vec::new();
    let result = (|| -> Result<()> {
        let before = published.test_owned_activation_state(local)?;
        let key = (0..100_000)
            .map(|i| format!("activation-real-unrelated-range-{i}"))
            .find(|key| {
                topology
                    .range_for_key(
                        crate::CLUSTER_EVENTS_COLLECTION,
                        &crate::cluster_event_record_id("items", key).unwrap(),
                    )
                    .is_ok_and(|r| r.id == other.id)
            })
            .expect("fixture second-range key");
        let expected = serde_json::json!({"source":"original-stock-sdk-during-first-activation","nested":{"range":other.id,"value":[null,true,17]}});
        events.push(serde_json::json!({"stage":"actual_other_range_append_intent","local":local,"range":other.id,"key":key,"expected":expected,"before":before}));
        let append = client.append("items", &key, expected.clone());
        events.push(serde_json::json!({"stage":"actual_other_range_append_return","result":append.as_ref().ok(),"exact_error":append.as_ref().err().map(ToString::to_string),"error_is_not_rollback":true}));
        let read = client
            .get_strong("items", &key)
            .map(|r| serde_json::to_value(r).unwrap());
        events.push(serde_json::json!({"stage":"actual_other_range_full_strong_return","result":read.as_ref().ok(),"exact_error":read.as_ref().err().map(ToString::to_string)}));
        let append = append?;
        let read = read?;
        let phase = fresh(published, clients)?;
        let i = (0..5).find(|i| node(*i) == *local).unwrap();
        let admission = phase.acquire_local_open(&clients[i], &node(0))?;
        let comparison = admission.test_stale_activation_owner_comparison();
        let after = published.test_owned_activation_state(local);
        events.push(serde_json::json!({"stage":"fresh_admitted_stale_owner_comparison","comparison":comparison.as_ref().ok(),"error":comparison.as_ref().err().map(ToString::to_string),"after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string)}));
        evidence(
            &output.join("owned-activation-lawful-head-before-assertions.json"),
            &events,
        )?;
        assert!(append);
        assert_eq!(read, expected);
        let comparison = comparison?;
        let after = after?;
        assert_eq!(comparison["global_healthy"], true);
        assert_eq!(comparison["selected_native_bridge_current"], true);
        assert_eq!(comparison["fresh_actual_admission_live"], true);
        assert_eq!(comparison["same_memory_head"], false);
        assert_eq!(comparison["comparison_returned_ok"], false);
        assert_eq!(
            comparison["comparison_error"],
            "cluster error: activation original owner superseded by later catalogue IO"
        );
        assert_eq!(
            before["catalogue"]["actual_io_returns"],
            after["catalogue"]["actual_io_returns"]
        );
        assert_eq!(
            before["catalogue"]["native_bridge_sha256"],
            after["catalogue"]["native_bridge_sha256"]
        );
        assert_eq!(before["frozen_owner"], after["frozen_owner"]);
        Ok(())
    })();
    let record = serde_json::json!({"events":events,"control_error":result.as_ref().err().map(ToString::to_string),"scope":"genuine stock-SDK other-range head and permissionless comparison; no stall-isolation or physical qualification"});
    eprintln!("NATIVE_ACTIVATION_LAWFUL_HEAD_CONTROL {record}");
    evidence(
        &output.join("owned-activation-lawful-head-before-assertions.json"),
        &record,
    )
    .unwrap();
    result.unwrap();
}

fn assert_same_native(before: &serde_json::Value, after: &serde_json::Value) {
    for key in [
        "native_next_xid",
        "native_next_lsn",
        "bridge_current",
        "preparation",
    ] {
        assert!(
            before["storage"].get(key).is_some(),
            "missing actual native snapshot key {key}"
        );
        assert_eq!(
            before["storage"][key], after["storage"][key],
            "catalogue-only retry must retain actual {key}"
        );
    }
}

/// Run on a genuine OLD survivor with its real installed writer; Registered
/// target before FIRST cannot lawfully use the generic refusal-persist sink.
pub(super) fn later_same_bytes(
    published: &Arc<NativeJointPublishedServingQuorum>,
    local: &ClusterNodeId,
    clients: &[Arc<TcpClusterRelocationTransport>],
    database: &Arc<parking_lot::RwLock<BicDb>>,
    output: &std::path::Path,
) {
    let root = database.read().data_path().to_path_buf();
    let mut events = Vec::new();
    let result = (|| -> Result<()> {
        let before = published.test_owned_activation_state(local)?;
        let fault = bicdb_page::test_faults::arm_io_failure(
            "test_activation_later_same_bytes.dir_sync",
            &root,
            1,
            false,
        );
        events.push(serde_json::json!({"stage":"later_same_bytes_actual_refusal_persistence_intent","before":before}));
        let later = published.test_owned_activation_later_io(local);
        let fired = fault.fired();
        drop(fault);
        events.push(serde_json::json!({"stage":"later_same_bytes_actual_return","result":later.as_ref().ok(),"error":later.as_ref().err().map(ToString::to_string),"fault_fired":fired}));
        let later = later?;
        let retry = fresh(published, clients);
        let after = published.test_owned_activation_state(local);
        events.push(serde_json::json!({"stage":"old_verified_owner_retry_return","retry_error":retry.as_ref().err().map(ToString::to_string),"after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string)}));
        evidence(
            &output.join("owned-activation-before-assertions.json"),
            &events,
        )?;
        assert_eq!(fired, 1);
        assert_eq!(later["real_catalogue_returned_ok"], false);
        assert_eq!(later["same_memory_head"], true);
        assert_eq!(later["global_uncertain"], true);
        assert_eq!(later["old_verified_activation"], true);
        assert_eq!(later["pending_activation_owner"], false);
        assert!(
            later["generation_after"].as_u64().unwrap()
                > later["generation_before"].as_u64().unwrap()
        );
        assert!(
            matches!(retry,Err(BicDbError::Cluster(ref d)) if d=="activation uncertainty is not owned by original pending publication")
        );
        let after = after?;
        assert_same_native(&before, &after);
        assert_eq!(after["catalogue"]["uncertain_at_snapshot_entry"], true);
        assert_eq!(
            before["catalogue"]["actual_io_returns"],
            after["catalogue"]["actual_io_returns"]
        );
        Ok(())
    })();
    let record = serde_json::json!({"events":events,"control_error":result.as_ref().err().map(ToString::to_string),"scope":"actual survivor unchanged-head refusal IO; not a Registered concurrent-R2 claim"});
    eprintln!("NATIVE_ACTIVATION_SAME_BYTES_CONTROL {record}");
    evidence(
        &output.join("owned-activation-before-assertions.json"),
        &record,
    )
    .unwrap();
    result.unwrap();
}
