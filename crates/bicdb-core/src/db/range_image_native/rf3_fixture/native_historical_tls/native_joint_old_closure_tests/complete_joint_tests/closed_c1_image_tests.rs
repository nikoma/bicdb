//! Actual native OLD QQ and image rows with every original C1 policy retained.
//! Image-only acceptance is deliberately distinct from full joint/serving.
use super::*;
use std::cell::Cell;
use std::rc::Rc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Control {
    Copy,
    WriterReplaced,
    CallerCanceledBeforeWal,
    StepRoleCanceledBeforeWal,
    OldOwnersEnded,
}
fn case(control: Control) {
    assert_eq!(
        run_complete_joint_with_continuation(0, JointFullContinuation::ClosedC1Image(control)),
        JointTestOutcome::ClosedC1ImageContractChecked
    );
}
#[test]
fn native_joint_closed_c1_image_actual_rows_copy_without_policy_reinstall() {
    case(Control::Copy);
}
#[test]
fn native_joint_closed_c1_image_live_original_writer_replacement_refused() {
    case(Control::WriterReplaced);
}
#[test]
fn native_joint_closed_c1_image_original_caller_cancel_before_actual_row_wal_refused() {
    case(Control::CallerCanceledBeforeWal);
}
#[test]
fn native_joint_closed_c1_image_step_role_cancel_before_actual_row_wal_refused() {
    case(Control::StepRoleCanceledBeforeWal);
}

fn step_role_with_actual_parent(
    fixture: &EnforcedNetwork,
    plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    recipient: &ClusterNodeId,
    parent: &CancellationToken,
) -> crate::distribution_range_recovery::HandoffRoleAdmission {
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let promise = plan.promise();
    let purpose = crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
        relocation_id: promise.relocation_id,
        plan_id: promise.plan_id.clone(),
        session_id: promise.session_id,
        nonce: promise.nonce,
    };
    let witness = fixture.providers[recipient]
        .witness_recovery_role(
            &actor,
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            purpose.clone(),
            parent,
        )
        .unwrap();
    crate::distribution_range_recovery::RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
        recipient,
        business,
        plan.old_range().id,
        plan.old_range().epoch,
        promise.nonce,
        challenge,
        &purpose,
    )
    .unwrap()
    .into_handoff()
    .unwrap()
}
#[test]
fn native_joint_closed_c1_image_actual_old_owners_ended_cannot_reissue_quorum() {
    case(Control::OldOwnersEnded);
}

pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    old: &crate::RangeDescriptor,
    plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    mut capture: RangeImageCapture,
    closed: Arc<crate::distribution_range_handoff::NativeJointOldClosedQuorum>,
    original: BTreeMap<ClusterNodeId, closed_policy_baseline_tests::OriginalPolicy>,
    acknowledged: &crate::Record,
    control: Control,
) {
    closed_policy_baseline_tests::require_original_unchanged(fixture, old, &original);
    closed.require_current().unwrap();
    let target = node("n3");
    let parent = CancellationToken::uncancelable();
    let db = &fixture.native.databases[&target];
    let namespaces = db
        .read()
        .capture_range_image_bindings()
        .unwrap()
        .namespace_ids();
    let before_b = db.read().current_commit_seq();
    assert!(db.read().get("items", &acknowledged.id).unwrap().is_none());
    db.read()
        .test_closed_image_requires_empty_target_slots(old.id)
        .unwrap();
    // Genuine source origin slots exist and bind an actual ACK. The learner's
    // necessary empty-slot check must refuse that actual nonempty DB, not DTOs.
    let source = fixture.native.databases[&old.leader].read();
    assert!(!source.native_abort_slot_inventory().unwrap().is_empty());
    assert!(source
        .test_closed_image_requires_empty_target_slots(old.id)
        .unwrap_err()
        .to_string()
        .contains("nonempty protected slot"));
    drop(source);

    let actor = fixture.clients[&old.leader].local_metadata_read_participant();
    let initial = role(fixture, plan, &target);
    let scope = RecoveryFenceScope::from_role(&initial).unwrap();
    fixture.runtimes[&target]
        .fence_epoch(
            &actor,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    let handoff = role(fixture, plan, &target).into_handoff().unwrap();
    if control == Control::OldOwnersEnded {
        drop(closed);
    } else {
        let _keep = &closed;
    }
    // The original owner stays in this scope except in the explicit real
    // owner-drop negative; capture does not retain those observation owners.
    let source = capture.authorize_target_install().unwrap();
    let attempt = Uuid::now_v7();
    let image_role = NativeImageInstallRole::CommittedHandoff(&handoff);
    let lease = fixture.runtimes[&target]
        .transfer_image_install(
            &scope,
            &source,
            &image_role,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &parent,
        )
        .unwrap();
    let begun = fixture.native.services[&target]
        .lock()
        .begin_native_range_image_install(
            &source,
            &image_role,
            lease,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &parent,
        );
    if control == Control::OldOwnersEnded {
        handoff.role().require_live_request().unwrap();
        parent.check().unwrap();
        let error = begun
            .err()
            .expect("dead actual OLD owners cannot mint a new image permission");
        assert!(error.to_string().contains("OLD owner ended"), "{error}");
        assert_eq!(db.read().current_commit_seq(), before_b);
        assert!(db.read().get("items", &acknowledged.id).unwrap().is_none());
        return;
    }
    let mut install = begun.unwrap();
    drop(source);
    loop {
        let cursor = capture.history_cursor().clone();
        let step = capture.export_history_step(&cursor, 256, &parent).unwrap();
        let fresh = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        let stage = install.history_stage_id();
        let progress = fixture.native.services[&target]
            .lock()
            .stage_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&fresh),
                &source,
                stage,
                &step,
                &parent,
            )
            .unwrap();
        if progress.cursor.complete() {
            break;
        }
    }
    loop {
        let fresh = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        if fixture.native.services[&target]
            .lock()
            .audit_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&fresh),
                &source,
                256,
                &parent,
            )
            .unwrap()
            .audit_complete
        {
            break;
        }
    }
    install.test_closed_original_request_live().unwrap();
    if control == Control::WriterReplaced {
        let original_state = {
            let database = db.read();
            let hook = database.commit_admission.read();
            hook.native_writer().unwrap().clone()
        };
        let business = Uuid::now_v7();
        let mq = fixture.clients[&target]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                &parent,
            )
            .unwrap();
        let policy = {
            let service = fixture.native.services[&target].lock();
            let database = db.read();
            service
                .prepare_native_host_writer_enrollment(&database, mq, business)
                .unwrap()
        };
        let _replacement = db
            .read()
            .install_enrolled_native_coordinator(fixture.coordinators[&target].clone(), policy)
            .unwrap();
        let changed_state = {
            let database = db.read();
            let hook = database.commit_admission.read();
            hook.native_writer().unwrap().clone()
        };
        assert!(!Arc::ptr_eq(&original_state, &changed_state));
        install.test_closed_original_request_live().unwrap();
        let fresh = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        let error = fixture.native.services[&target]
            .lock()
            .clear_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&fresh),
                &source,
                &parent,
            )
            .unwrap_err();
        parent.check().unwrap();
        install.test_closed_original_request_live().unwrap();
        let detail = error.to_string();
        assert!(
            detail.contains("native writer")
                && (detail.contains("hook") || detail.contains("policy")),
            "{detail}"
        );
        assert_eq!(db.read().current_commit_seq(), before_b);
        assert!(db.read().get("items", &acknowledged.id).unwrap().is_none());
        assert_eq!(
            db.read()
                .capture_range_image_bindings()
                .unwrap()
                .namespace_ids(),
            namespaces
        );
        return;
    }
    loop {
        let fresh = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        if fixture.native.services[&target]
            .lock()
            .clear_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&fresh),
                &source,
                &parent,
            )
            .unwrap()
        {
            break;
        }
    }
    let fired = Rc::new(Cell::new(false));
    let step_parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let _hook = if matches!(
        control,
        Control::CallerCanceledBeforeWal | Control::StepRoleCanceledBeforeWal
    ) {
        let canceled = if control == Control::CallerCanceledBeforeWal {
            parent.clone()
        } else {
            step_parent.clone()
        };
        let reached = fired.clone();
        Some(
            crate::db::range_image_native::history_stage::test_before_closed_image_transaction(
                Box::new(move || {
                    canceled.check().unwrap();
                    reached.set(true);
                    canceled.cancel();
                }),
            ),
        )
    } else {
        None
    };
    let options = RangeSnapshotOptions {
        max_records_per_batch: 16,
        max_bytes_per_batch: 1024 * 1024,
        max_record_bytes: 1024 * 1024,
    };
    loop {
        let cursor = capture.cursor().clone();
        let step = capture.export_step(&cursor, &options, &parent).unwrap();
        let fresh = if control == Control::StepRoleCanceledBeforeWal {
            step_role_with_actual_parent(fixture, plan, &target, &step_parent)
        } else {
            role(fixture, plan, &target).into_handoff().unwrap()
        };
        if control == Control::StepRoleCanceledBeforeWal {
            fresh.role().require_live_request().unwrap();
            install.test_closed_original_request_live().unwrap();
            parent.check().unwrap();
        }
        let source = capture.authorize_target_install().unwrap();
        let result = fixture.native.services[&target]
            .lock()
            .copy_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&fresh),
                &source,
                attempt,
                &step,
                &parent,
            );
        if matches!(
            control,
            Control::CallerCanceledBeforeWal | Control::StepRoleCanceledBeforeWal
        ) && fired.get()
        {
            let error = result.unwrap_err();
            if control == Control::CallerCanceledBeforeWal {
                assert!(matches!(error, crate::BicDbError::QueryCanceled), "{error}");
            } else {
                parent.check().unwrap();
                install.test_closed_original_request_live().unwrap();
                assert!(matches!(
                    step_parent.check(),
                    Err(crate::BicDbError::QueryCanceled)
                ));
                let exact_role_error = fresh.role().require_live_request().unwrap_err();
                assert_eq!(error.to_string(), exact_role_error.to_string());
            }
            assert_eq!(db.read().current_commit_seq(), before_b);
            assert!(db.read().get("items", &acknowledged.id).unwrap().is_none());
            assert_eq!(
                db.read()
                    .capture_range_image_bindings()
                    .unwrap()
                    .namespace_ids(),
                namespaces
            );
            return;
        }
        result.unwrap();
        if step.complete {
            break;
        }
    }
    assert_eq!(control, Control::Copy);
    install.test_closed_original_request_live().unwrap();
    assert_eq!(
        db.read().get("items", &acknowledged.id).unwrap().as_deref(),
        Some(acknowledged)
    );
    assert!(
        db.read().current_commit_seq() > before_b,
        "actual controlled image row transaction committed"
    );
    assert_eq!(
        db.read()
            .capture_range_image_bindings()
            .unwrap()
            .namespace_ids(),
        namespaces
    );
    assert!(db.read().range_mutation_gate().try_enter(old.id).is_err());
    closed_policy_baseline_tests::require_original_unchanged(fixture, old, &original);
    let current = plan.old_range();
    assert!(db
        .read()
        .require_installed_native_writer_database_for_range(current)
        .unwrap_err()
        .to_string()
        .contains("range configuration changed"));
    eprintln!("ACTUAL_CLOSED_C1_IMAGE_ROWS copiedOriginalACK=true originalPoliciesUnchanged=true targetFrozen=true noServingOrNewACK=true");
}
