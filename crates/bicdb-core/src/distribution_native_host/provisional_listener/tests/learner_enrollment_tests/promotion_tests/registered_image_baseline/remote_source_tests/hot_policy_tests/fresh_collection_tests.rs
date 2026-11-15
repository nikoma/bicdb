//! Genuine retained native facts are recollected, never made live by time edits.
use super::*;
use crate::distribution_range_handoff::NativeJointPreparedServingQuorum;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetPreparedFreshExpired
            | SourceCase::TargetPreparedFreshCallerCanceled
            | SourceCase::TargetPreparedFreshForeignService
            | SourceCase::TargetPreparedFreshMetadataChanged
            | SourceCase::TargetPreparedFreshMissingArchive
    )
}
macro_rules! actual_case {
    ($name:ident,$case:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::$case));
        }
    };
}
actual_case!(
    native_registered_fifth_prepared_fresh_actual_expired_round_then_new_collection,
    TargetPreparedFreshExpired
);
actual_case!(
    native_registered_fifth_prepared_fresh_actual_original_session_cancel_refuses,
    TargetPreparedFreshCallerCanceled
);
actual_case!(
    native_registered_fifth_prepared_fresh_actual_foreign_service_refuses_count,
    TargetPreparedFreshForeignService
);
actual_case!(
    native_registered_fifth_prepared_fresh_actual_local_metadata_version_change_refuses,
    TargetPreparedFreshMetadataChanged
);
actual_case!(
    native_registered_fifth_prepared_fresh_actual_missing_archive_refuses_count,
    TargetPreparedFreshMissingArchive
);

fn snapshot(custody: &[Arc<NativeJointPreparationCustody>]) -> Result<Vec<serde_json::Value>> {
    custody.iter().map(|c| c.test_native_evidence()).collect()
}
enum Collected {
    Quorum(NativeJointPreparedServingQuorum),
    Refused,
}
#[allow(clippy::too_many_arguments)]
pub(super) fn collect(
    case: SourceCase,
    custody: &[Arc<NativeJointPreparationCustody>],
    original: Option<NativeJointPreparedServingRound>,
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    session: &CancellationToken,
    output: &std::path::Path,
    acknowledged: &[serde_json::Value],
) -> Option<NativeJointPreparedServingQuorum> {
    let before = snapshot(custody);
    let mut events = Vec::new();
    let result = run(
        case,
        custody,
        original,
        clients,
        services,
        metadata,
        session,
        &mut events,
    );
    // Preserve the original Result before any forensic read or formatting.
    let after = snapshot(custody);
    let saved = evidence(
        &output.join("fresh-preparation-collection-before-assertions.json"),
        &serde_json::json!({
        "case":format!("{case:?}"),"returned_success":result.is_ok(),
        "error":result.as_ref().err().map(ToString::to_string),"error_debug":result.as_ref().err().map(|e|format!("{e:?}")),
        "original_session_cancel_requested":session.is_cancel_requested(),
        "before":before.as_ref().ok(),"before_error":before.as_ref().err().map(ToString::to_string),
        "after":after.as_ref().ok(),"after_error":after.as_ref().err().map(ToString::to_string),
        "all_phase_events":events,"original_source_ACKs":acknowledged,
        "scope":"same-process fresh metadata collection over genuine retained native preparations; not cold/physical qualification",
        "old_original_deadline_not_extended":true,"new_native_preparation_or_pin_requested":false}),
    );
    if let Err(error) = saved {
        if let Err(primary) = &result {
            eprintln!("FRESH_PREPARATION_DIAGNOSTIC_FAILED {error}; primary={primary:?}");
        } else {
            panic!("fresh preparation evidence failed: {error}");
        }
    }
    let collected = result.unwrap();
    let before = before.unwrap();
    let after = after.unwrap();
    assert_eq!(before.len(), after.len());
    for (index, (before, after)) in before.iter().zip(&after).enumerate() {
        assert_eq!(
            before["actual_protected"]["record"], after["actual_protected"]["record"],
            "fresh metadata collection changed actual native preparation/xid/body"
        );
        if index == 0 && matches!(case, SourceCase::TargetPreparedFreshMissingArchive) {
            assert!(after["actual_protected"]["archive"].is_null());
        } else {
            assert_eq!(
                before["actual_protected"]["archive"],
                after["actual_protected"]["archive"]
            );
        }
    }
    match collected {
        Collected::Quorum(q) => Some(q),
        Collected::Refused => None,
    }
}
#[allow(clippy::too_many_arguments)]
fn run(
    case: SourceCase,
    custody: &[Arc<NativeJointPreparationCustody>],
    mut original: Option<NativeJointPreparedServingRound>,
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    session: &CancellationToken,
    events: &mut Vec<serde_json::Value>,
) -> Result<Collected> {
    session.check()?;
    if matches!(case, SourceCase::TargetPreparedFreshExpired) {
        let round = original
            .as_mut()
            .ok_or_else(|| invalid("actual old preparation round absent"))?;
        let deadline = round
            .test_collection_deadline()
            .ok_or_else(|| invalid("old round has no actual deadline"))?;
        while Instant::now() <= deadline {
            session.check()?;
            std::thread::sleep(Duration::from_millis(2));
        }
        let before = round.test_collection_memory();
        let last = custody
            .last()
            .ok_or_else(|| invalid("original preparation custody absent"))?;
        let denied = round.record_candidate(last.test_original_candidate());
        events.push(serde_json::json!({"operation":"original expired strict round",
            "before":before,"after":round.test_collection_memory(),"returned_success":denied.is_ok(),
            "error":denied.as_ref().err().map(ToString::to_string),"session_live_after":session.check().is_ok()}));
        if !matches!(denied, Err(crate::BicDbError::QueryTimedOut)) {
            return Err(invalid(
                "original expired round did not retain strict timeout refusal",
            ));
        }
        session.check()?;
    }
    drop(original);
    if matches!(case, SourceCase::TargetPreparedFreshCallerCanceled) {
        session.cancel();
    }
    // A new phase is bounded and linked to the SAME original Session; no old
    // token is renewed and no additional native preparation is invoked.
    let phase = budget(4000).child_linked_to(session);
    let mut round: Option<NativeJointPreparedServingRound> = None;
    for (index, retained) in custody.iter().enumerate() {
        let client = clients
            .iter()
            .find(|c| c.local_metadata_read_participant().node_id() == retained.local_node_id())
            .ok_or_else(|| invalid("fresh preparation actual recipient absent"))?;
        let candidate = retained.fresh_candidate(client, &node(0), &phase);
        events.push(serde_json::json!({"operation":"real fresh recipient OWN and MStore", "node":retained.local_node_id(),
            "returned_success":candidate.is_ok(),"error":candidate.as_ref().err().map(ToString::to_string),
            "original_custody":retained.test_evidence(),"fresh_request":candidate.as_ref().ok().map(|c|c.test_request_memory())}));
        if matches!(case, SourceCase::TargetPreparedFreshCallerCanceled) {
            if !matches!(candidate, Err(crate::BicDbError::QueryCanceled)) {
                return Err(invalid(
                    "canceled original Session did not refuse fresh preparation",
                ));
            }
            return Ok(Collected::Refused);
        }
        let mut candidate = candidate?;
        let request_probe = candidate.test_request_memory_probe();
        let negative = index == 0
            && matches!(
                case,
                SourceCase::TargetPreparedFreshForeignService
                    | SourceCase::TargetPreparedFreshMetadataChanged
                    | SourceCase::TargetPreparedFreshMissingArchive
            );
        let before_request = candidate.test_request_memory();
        if negative {
            match case {
                SourceCase::TargetPreparedFreshForeignService => {
                    let selected = services
                        .iter()
                        .find(|s| s.lock().node_id() != retained.local_node_id())
                        .ok_or_else(|| invalid("genuine foreign service absent"))?
                        .clone();
                    candidate = candidate.test_replace_service_association(selected);
                }
                SourceCase::TargetPreparedFreshMetadataChanged => {
                    let selected = (0..metadata.len())
                        .find(|i| node(*i) == *retained.local_node_id())
                        .ok_or_else(|| invalid("actual local MStore not in fixture"))?;
                    let mut store = metadata[selected].lock();
                    let before = store.test_actual_publication_version_evidence()?;
                    let term = store
                        .status()
                        .current_term
                        .checked_add(1)
                        .ok_or_else(|| invalid("actual term overflow"))?;
                    events.push(
                        serde_json::json!({"operation":"actual local metadata term/version intent",
                        "before":before,"requested_term":term}),
                    );
                    let changed = store.observe_remote_term(term);
                    let after = store.test_actual_publication_version_evidence();
                    events.push(
                        serde_json::json!({"operation":"actual local metadata term/version advance",
                        "before":before,"after":after.as_ref().ok(),"changed":changed.as_ref().ok(),"requested_term":term,
                        "mutation_error":changed.as_ref().err().map(ToString::to_string),
                        "readback_error":after.as_ref().err().map(ToString::to_string)}),
                    );
                    let changed = changed?;
                    let after = after?;
                    if !changed || after["durable_state_term"] != term {
                        return Err(invalid("actual MStore mutation not established"));
                    }
                }
                SourceCase::TargetPreparedFreshMissingArchive => {
                    events.push(
                        serde_json::json!({"operation":"actual preparation archive deletion intent",
                        "actual_custody":retained.test_evidence()}),
                    );
                    let damage = retained.test_remove_actual_archive(&phase);
                    events.push(serde_json::json!({"operation":"actual native preparation archive removal",
                        "damage":damage.as_ref().ok(),"error":damage.as_ref().err().map(ToString::to_string)}));
                    damage?;
                }
                _ => unreachable!(),
            }
        }
        let after_request = candidate.test_request_memory();
        let count = if let Some(round) = &mut round {
            round.record_candidate(candidate)
        } else {
            match NativeJointPreparedServingRound::begin_candidate(candidate, session) {
                Ok(first) => {
                    round = Some(first);
                    Ok(())
                }
                Err(error) => Err(error),
            }
        };
        let after_count_request = request_probe();
        events.push(serde_json::json!({"operation":"full current prepared count", "node":retained.local_node_id(),
            "before_fresh_request":before_request,"at_count_fresh_request":after_request,
                "after_count_fresh_request":after_count_request,
            "returned_success":count.is_ok(),"error":count.as_ref().err().map(ToString::to_string),
            "round":round.as_ref().map(|r|r.test_collection_memory())}));
        if negative {
            phase.check()?;
            let error = count
                .err()
                .ok_or_else(|| invalid("invalid fresh preparation counted"))?;
            let expected = match case {
                SourceCase::TargetPreparedFreshForeignService => {
                    "fresh preparation substituted original actual service association"
                }
                SourceCase::TargetPreparedFreshMetadataChanged => {
                    "actual metadata authority version changed/dirty"
                }
                SourceCase::TargetPreparedFreshMissingArchive => {
                    "NEW joint preparation immutable archive missing"
                }
                _ => unreachable!(),
            };
            if !error.to_string().contains(expected) {
                return Err(error);
            }
            if before_request["remaining_us"].as_u64().unwrap_or(0) == 0
                || after_request["remaining_us"].as_u64().unwrap_or(0) == 0
                || after_count_request["remaining_us"].as_u64().unwrap_or(0) == 0
                || before_request["cancel_requested"] != false
                || after_request["cancel_requested"] != false
                || after_count_request["cancel_requested"] != false
            {
                return Err(invalid(
                    "negative fresh preparation did not preserve live original request",
                ));
            }
            return Ok(Collected::Refused);
        }
        count?;
    }
    let round = round.ok_or_else(|| invalid("no actual preparations retained"))?;
    events.push(serde_json::json!({"operation":"complete fresh count before finish","round":round.test_collection_memory()}));
    Ok(Collected::Quorum(round.finish()?))
}
