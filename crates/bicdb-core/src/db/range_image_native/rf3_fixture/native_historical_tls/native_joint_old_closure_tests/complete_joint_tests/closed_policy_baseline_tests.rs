//! Characterizes the missing operational DATA-policy transition. A passing
//! diagnostic means unsupported, never successful relocation or serving.
//! Required prerequisites are not should-panic/ignored: an earlier image-policy
//! refusal remains a failing baseline until its actual producer is delivered.
use super::*;

pub(super) struct OriginalPolicy {
    guard: Arc<crate::db::InstalledNativeWriterHostGuard>,
    state: Arc<crate::db::NativeWriterHostState>,
    binding: serde_json::Value,
}

fn actual_writer(
    fixture: &EnforcedNetwork,
    id: &ClusterNodeId,
) -> Arc<crate::db::NativeWriterHostState> {
    let db = fixture.native.databases[id].read();
    let hook = db.commit_admission.read();
    hook.native_writer().unwrap().clone()
}

#[test]
fn native_joint_policy_original_writer_reaches_actual_decision_without_reinstall_baseline() {
    assert_eq!(
        run_complete_joint_with_continuation(0, JointFullContinuation::ClosedPolicyBaseline),
        JointTestOutcome::ClosedPolicyTransitionStillUnsupported,
        "this characterization must not report a completed handoff"
    );
}

pub(super) fn capture(
    fixture: &EnforcedNetwork,
    old: &crate::RangeDescriptor,
) -> BTreeMap<ClusterNodeId, OriginalPolicy> {
    fixture
        .hosts
        .iter()
        .map(|(id, guard)| {
            let state = actual_writer(fixture, id);
            state.require_range(old).unwrap();
            (
                id.clone(),
                OriginalPolicy {
                    guard: guard.clone(),
                    state,
                    binding: guard.binding_diagnostic(),
                },
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    old: &crate::RangeDescriptor,
    decision: &NativeJointDurableDecision,
    closed: &crate::distribution_range_handoff::NativeJointOldClosedQuorum,
    original: BTreeMap<ClusterNodeId, OriginalPolicy>,
    acknowledged: &crate::Record,
    parent: &CancellationToken,
) {
    assert_eq!(original.len(), 4);
    closed.require_current().unwrap();
    let topology = fixture.native.topology.read().clone();
    let new = topology.range_by_id(old.id).unwrap();
    assert_ne!(new, old);
    assert_eq!(new, decision.data().activation_range());
    let before: BTreeMap<_, _> = original
        .keys()
        .map(|id| {
            (
                id.clone(),
                post_joint_no_data_tests::snapshot(fixture, new, id),
            )
        })
        .collect();
    let mut observations = Vec::new();
    for (id, original) in &original {
        parent.check().unwrap();
        assert!(Arc::ptr_eq(&fixture.hosts[id], &original.guard));
        assert_eq!(original.binding, fixture.hosts[id].binding_diagnostic());
        let state = actual_writer(fixture, id);
        assert!(Arc::ptr_eq(&state, &original.state));
        state.require_range(old).unwrap();
        let expected = state.require_range(new).unwrap_err().to_string();
        assert!(expected.contains("range configuration changed"));
        {
            let service = fixture.native.services[id].lock();
            let db = fixture.native.databases[id].read();
            service
                .require_native_writer_host_for_range(&db, old)
                .unwrap();
        }
        // Lower actual protected current+archive reads work without installing
        // a NEW writer. This historical fact grants no serving permission.
        let protected_sha = {
            let db = fixture.native.databases[id].read();
            let native = db
                .native_recovery_admission(
                    &topology.cluster_id,
                    topology.range_image_bootstrap_id.unwrap(),
                    id,
                    topology.nodes[id].incarnation,
                )
                .unwrap();
            let actual = db
                .read_native_joint_decision(&native, old.id, decision.data().checksum_sha256())
                .unwrap();
            actual.require_current(&db).unwrap();
            assert_eq!(actual.decision_sha256(), decision.data().checksum_sha256());
            assert_eq!(actual.local_node_id(), id);
            assert_eq!(
                db.get("items", &acknowledged.id).unwrap().as_deref(),
                Some(acknowledged)
            );
            actual.checksum_sha256().to_owned()
        };
        let business = Uuid::now_v7();
        let mq = fixture.clients[id]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                parent,
            )
            .unwrap();
        assert_eq!(mq.committed_topology(business).unwrap(), &topology);
        let error = fixture.native.services[id]
            .lock()
            .observe_committed_native_joint_decision(mq, business, old.id)
            .err()
            .expect("unchanged OLD writer cannot authorize the NEW descriptor");
        assert_eq!(error.to_string(), expected);
        observations.push(serde_json::json!({
            "node":id,"actual_protected_receipt_sha256":protected_sha,
            "committed_observer_error":error.to_string(),
            "original_writer_binding":original.binding,
        }));
    }
    closed.require_current().unwrap();
    for (id, original) in &original {
        assert_eq!(original.binding, fixture.hosts[id].binding_diagnostic());
        assert_eq!(
            post_joint_no_data_tests::snapshot(fixture, new, id),
            before[id],
            "refused observation must preserve native R/H/B/UID, slots, promises and raw catalogue"
        );
    }
    let report = serde_json::json!({
        "classification":"unsupported operational DATA-policy transition; not relocation success",
        "old_range":old,"new_range":new,
        "actual_decision_sha256":decision.data().checksum_sha256(),
        "acknowledged":acknowledged,"observations":observations,"unchanged_native":before,
        "ordinary_writer_reinstalled":false,"new_serving_grant":false,
    });
    post_joint_no_data_tests::report(
        fixture,
        "actual-closed-policy-transition-baseline.json",
        &report,
    );
    eprintln!("ACTUAL_CLOSED_POLICY_BASELINE {}", report);
}

pub(super) fn report_before_image(
    fixture: &EnforcedNetwork,
    original_range: &crate::RangeDescriptor,
    closed: &crate::distribution_range_handoff::NativeJointOldClosedQuorum,
    original: &BTreeMap<ClusterNodeId, OriginalPolicy>,
    acknowledged: &crate::Record,
) {
    closed.require_current().unwrap();
    let mut observations = Vec::new();
    for (id, original) in original {
        assert!(Arc::ptr_eq(&fixture.hosts[id], &original.guard));
        assert!(Arc::ptr_eq(&actual_writer(fixture, id), &original.state));
        assert_eq!(fixture.hosts[id].binding_diagnostic(), original.binding);
        original.state.require_range(original_range).unwrap();
        let actual = {
            let db = fixture.native.databases[id].read();
            // A not-yet-installed learner legitimately has NO trusted anchor.
            // Preserve its actual refusal; never invent a trusted R0 floor or
            // let an observation helper prevent the real image operation.
            let native = match db.current_native_range_source_lineage(original_range.id) {
                Ok(point) => serde_json::json!({
                    "r":point.range_index(),"h0":point.logical_genesis_sha256(),
                    "h":point.logical_history_sha256(),
                    "local_data_b":point.actual_local_visibility_sequence(),
                    "history_generation":point.history_generation(),
                }),
                Err(error) => serde_json::json!({"actual_source_refusal":error.to_string()}),
            };
            let raw: serde_json::Value = serde_json::from_slice(
                &std::fs::read(
                    db.data_path()
                        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
                )
                .unwrap(),
            )
            .unwrap();
            let key = original_range.id.get().to_string();
            let record = db.get("items", &acknowledged.id).unwrap();
            if closed.plan().old_members().contains_key(id) {
                assert_eq!(record.as_deref(), Some(acknowledged));
            }
            serde_json::json!({
                "native":native,"uid":db.local_namespace_identity("items").unwrap(),
                "acknowledged_record_on_this_node":record.as_deref(),
                "slots":db.native_abort_slot_inventory().unwrap(),
                "raw_range_log":raw["catalog"]["ranges"][&key],
                "handoff_promise":raw["catalog"]["handoff_promises"][&key],
                "joint":raw["catalog"]["native_joint_handoffs"][&key],
            })
        };
        observations.push(serde_json::json!({
            "node":id,"original_writer_binding":original.binding,
            "actual_native":actual,
        }));
    }
    let report = serde_json::json!({
        "classification":"actual prerequisite cut before target image; no adoption or joint decision yet",
        "initial_range":original_range,"pending_old_range":closed.plan().old_range(),
        "closed_r":closed.range_index(),"closed_h0":closed.logical_genesis_sha256(),
        "closed_h":closed.logical_history_sha256(),"acknowledged":acknowledged,
        "original_policies":observations,"ordinary_writer_reinstalled":false,
    });
    post_joint_no_data_tests::report(fixture, "actual-original-writer-before-image.json", &report);
    eprintln!("ACTUAL_ORIGINAL_WRITER_BEFORE_IMAGE {}", report);
}
