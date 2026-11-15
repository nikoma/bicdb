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
            | SourceCase::TargetActivationLargeHeadDirSyncRetry
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
    Option<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner>,
);
#[allow(clippy::too_many_arguments)]
pub(super) fn activate(
    case: SourceCase,
    local: &ClusterNodeId,
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
        let site = if matches!(
            case,
            SourceCase::TargetActivationDirSyncRetry
                | SourceCase::TargetActivationLargeHeadDirSyncRetry
        ) {
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
        let before = published.test_owned_activation_state(local)?;
        events
            .push(serde_json::json!({"stage":"retained_original_failed_write","snapshot":before}));
        if matches!(case, SourceCase::TargetActivationForeignOwner) {
            let foreign = published.test_owned_activation_foreign(local);
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
        let after = published.test_owned_activation_state(local);
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
        if matches!(case, SourceCase::TargetActivationLargeHeadDirSyncRetry) {
            let catalogue = &before["catalogue"];
            let total = catalogue["original_serialized_bytes"].as_u64().unwrap()
                + catalogue["candidate_serialized_bytes"].as_u64().unwrap()
                + catalogue["candidate_envelope_bytes"].as_u64().unwrap();
            assert!(
                total > 8 * 1024 * 1024,
                "genuine owned intent must exceed old triple cap"
            );
            assert_eq!(catalogue["existing_head_bound_bytes"], 64 * 1024 * 1024);
        }

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
        let index = (0..5).find(|i| node(*i) == *local).unwrap();
        let fresh_admission = phase.acquire_local_open(&clients[index], &node(0))?;
        let receipt = fresh_admission.persist_local_catalogue_activation(process);
        events.push(serde_json::json!({"stage":"fresh_recovered_activation_receipt_return","returned_ok":receipt.is_ok(),"exact_error":receipt.as_ref().err().map(ToString::to_string)}));
        let receipt = Arc::new(receipt?);
        let release = if *local == node(4) {
            Some(fresh_admission.retain_registered_release_owner(receipt.clone())?)
        } else {
            None
        };
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

case!(
    native_registered_fifth_activation_actual_large_unrelated_catalogue_unknown_refsync,
    TargetActivationLargeHeadDirSyncRetry
);

pub(super) struct LargeHeadRecords {
    records: Vec<crate::Record>,
}
/// The existing bootstrap already created the ordinary `items` collection.
/// This uses its PUBLIC Core transaction/native commit hook, not oversized
/// event-API values, raw catalogues, a no-admission transaction or fake grants.
pub(super) fn prepare_large_head(
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    topology: &ClusterTopology,
    other: &crate::RangeDescriptor,
    output: &std::path::Path,
) -> LargeHeadRecords {
    let mut events = Vec::new();
    let result = (|| -> Result<LargeHeadRecords> {
        let leader = (0..4)
            .find(|i| node(*i) == other.leader)
            .expect("real R2 leader");
        let database = services[leader].lock().database();
        let mut records = Vec::new();
        for ordinal in 0..2 {
            let id = (0..100_000)
                .map(|i| format!("actual-native-capacity-{ordinal}-{i}"))
                .find(|id| {
                    topology
                        .range_for_key("items", id)
                        .is_ok_and(|r| r.id == other.id)
                })
                .expect("ordinary Core R2 key");
            let record=crate::Record::new(id).with_metadata(serde_json::json!({"body":"x".repeat(1536*1024),"ordinal":ordinal,"contract":"public Core/native entry, not event API"}));
            let record_bytes = serde_json::to_vec(&record)?;
            events.push(serde_json::json!({"stage":"actual_public_core_insert_intent","collection":"items","record_id":record.id,"record_bytes":record_bytes.len(),"record_sha256":hex::encode(Sha256::digest(&record_bytes)),"real_range":other.id,"leader":other.leader,"event_API_64KiB_limit_unchanged":true}));
            evidence(
                &output.join("large-native-entry-preparation-before-assertions.json"),
                &events,
            )?;
            let committed = (|| -> Result<()> {
                let db = database.write();
                if !db.commit_admission_ready() {
                    return Err(invalid(
                        "large-entry actual native commit hook not installed",
                    ));
                }
                let begun = db.begin_transaction();
                events.push(serde_json::json!({"stage":"actual_public_core_begin_return","returned_ok":begun.is_ok(),"exact_error":begun.as_ref().err().map(ToString::to_string)}));
                let mut tx = begun?;
                let inserted = tx.insert("items", record.clone());
                events.push(serde_json::json!({"stage":"actual_public_core_insert_return","returned_ok":inserted.is_ok(),"exact_error":inserted.as_ref().err().map(ToString::to_string)}));
                inserted?;
                events.push(serde_json::json!({"stage":"actual_public_core_commit_intent","record_id":record.id}));
                evidence(
                    &output.join("large-native-entry-preparation-before-assertions.json"),
                    &events,
                )?;
                let committed = tx.commit();
                events.push(serde_json::json!({"stage":"actual_public_core_commit_return","returned_ok":committed.is_ok(),"exact_error":committed.as_ref().err().map(ToString::to_string),"record_id":record.id,"error_is_not_rollback":true}));
                committed
            })();
            events.push(serde_json::json!({"stage":"public_core_write_scope_return","returned_ok":committed.is_ok(),"exact_error":committed.as_ref().err().map(ToString::to_string)}));
            let saved = evidence(
                &output.join("large-native-entry-preparation-before-assertions.json"),
                &events,
            );
            match committed {
                Err(primary) => {
                    let _ = saved;
                    return Err(primary);
                }
                Ok(()) => saved?,
            }

            records.push(record);
        }
        let expected = LargeHeadRecords { records };
        let observed = inspect_large_head(services, topology, other, &expected)?;
        events.push(serde_json::json!({"stage":"actual_R2_native_materialization_after_public_core_ACKs","members":observed}));
        evidence(
            &output.join("large-native-entry-preparation-before-assertions.json"),
            &events,
        )?;
        assert_large_members(&observed, &expected);
        Ok(expected)
    })();
    let record = serde_json::json!({"events":events,"control_error":result.as_ref().err().map(ToString::to_string),"scope":"actual public Core/native hook ACKs before R1 owned intent; no oversized event request or catalogue synthesis"});
    eprintln!("NATIVE_LARGE_ACTIVATION_PREPARATION {record}");
    let saved = evidence(
        &output.join("large-native-entry-preparation-before-assertions.json"),
        &record,
    );
    if result.is_err() {
        let _ = saved;
        return result.unwrap();
    }
    saved.unwrap();
    result.unwrap()
}
fn inspect_large_head(
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    _topology: &ClusterTopology,
    other: &crate::RangeDescriptor,
    expected: &LargeHeadRecords,
) -> Result<serde_json::Value> {
    let mut members = serde_json::Map::new();
    for member in other
        .replicas
        .iter()
        .filter(|r| r.role == crate::RangeReplicaRole::Voter)
    {
        let index = (0..4)
            .find(|i| node(*i) == member.node_id)
            .expect("real R2 voter");
        let (database, catalogue) = {
            let service = services[index].lock();
            (
                service.database(),
                service.test_large_activation_catalogue_entries(other.id)?,
            )
        };
        let database = database.read();
        let native = database.current_native_range_source_lineage(other.id)?;
        let mut rows = Vec::new();
        for expected in &expected.records {
            let actual = database.get("items", &expected.id)?;
            let bytes = actual
                .as_ref()
                .map(|r| serde_json::to_vec(r.as_ref()))
                .transpose()?;
            rows.push(serde_json::json!({"record_id":expected.id,"full_record_equal":actual.as_ref().is_some_and(|r|r.as_ref()==expected),"actual_record_bytes":bytes.as_ref().map(Vec::len),"actual_record_sha256":bytes.as_ref().map(|b|hex::encode(Sha256::digest(b)))}));
        }
        members.insert(member.node_id.as_str().into(),serde_json::json!({"catalogue":catalogue,"actual_native_range_index":native.range_index(),"actual_native_history_sha256":native.logical_history_sha256(),"rows":rows}));
    }
    Ok(members.into())
}
fn assert_large_members(observed: &serde_json::Value, expected: &LargeHeadRecords) {
    for member in observed.as_object().expect("actual R2 members").values() {
        assert!(
            member["catalogue"]["catalogue_model_bytes"]
                .as_u64()
                .unwrap()
                > 3 * 1024 * 1024
        );
        for row in member["rows"].as_array().unwrap() {
            assert_eq!(row["full_record_equal"], true);
        }
        for record in &expected.records {
            let entry = member["catalogue"]["entries"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| {
                    entry["record_ids"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|id| id.as_str() == Some(record.id.as_str()))
                })
                .expect("actual retained ordinary native command");
            assert_eq!(entry["state"], "applied");
            let bytes = entry["complete_entry_bytes"].as_u64().unwrap();
            assert!(bytes > 1536 * 1024 && bytes <= 2 * 1024 * 1024);
        }
    }
}
pub(super) fn verify_large_head(
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    topology: &ClusterTopology,
    other: &crate::RangeDescriptor,
    expected: &LargeHeadRecords,
    output: &std::path::Path,
) {
    let result = inspect_large_head(services, topology, other, expected);
    let report = serde_json::json!({"stage":"after_actual_R1_large_head_activation_and_fresh_refsync","members":result.as_ref().ok(),"exact_error":result.as_ref().err().map(ToString::to_string),"no_new_R2_write_or_replayed_native_decision":true});
    eprintln!("NATIVE_LARGE_ACTIVATION_PRESERVATION {report}");
    let saved = evidence(
        &output.join("large-native-entry-post-refsync-before-assertions.json"),
        &report,
    );
    if result.is_err() {
        let _ = saved;
        let _ = result.unwrap();
        unreachable!();
    }
    saved.unwrap();
    assert_large_members(&result.unwrap(), expected);
}
