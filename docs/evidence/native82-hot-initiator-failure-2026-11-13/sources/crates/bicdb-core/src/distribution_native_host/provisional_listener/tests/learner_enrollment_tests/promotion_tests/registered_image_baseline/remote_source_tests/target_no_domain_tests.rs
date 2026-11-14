//! Genuine original fifth receiver, actual authenticated source and runtime.
use super::*;

#[test]
fn native_registered_fifth_remote_step_actual_no_domain_preserves_all_state() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetNoDomain));
}
#[test]
fn native_registered_fifth_remote_step_no_domain_original_hook_aba_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNoDomainHookRevoked),
    );
}
#[test]
fn native_registered_fifth_remote_step_no_domain_actual_mstore_term_change_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNoDomainMemberChanged),
    );
}
#[test]
fn native_registered_fifth_remote_step_no_domain_actual_schema_generation_revokes() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNoDomainSchemaRevoked),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &NativeMetadataControlHost,
    runtime: &Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    service: &Arc<Mutex<crate::ClusterDataNodeService>>,
    metadata: &Arc<Mutex<crate::MetadataConsensusStore>>,
    client: &TcpClusterRelocationTransport,
    actual: &crate::distribution_range_image::NativeRemoteImageStep<'_>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
) {
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let schema_generation_before = factory
        .database
        .read()
        .schema_compatibility_authority()
        .generation();
    let metadata_term_before = metadata.lock().status().current_term;
    let parent = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &parent)
        .into_handoff()
        .unwrap();
    let actual_scope = RecoveryFenceScope::from_role(role.role()).unwrap();
    let frozen = runtime.source_capture_freeze(&actual_scope).unwrap();
    let business = Uuid::now_v7();
    let own = client
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
        .unwrap();
    let mut reached = false;
    let mut exact_error = None;
    let result = control.with_registered_remote_image_step(
        actual,
        role,
        own,
        business,
        &frozen,
        &parent,
        |step| {
            let no_domain = step.observe_selected_no_domain()?;
            let database = service
                .try_lock()
                .expect("actual selected target service")
                .database();
            {
                let actual = service.try_lock().expect("actual selected target service");
                let db = database.try_read().expect("actual selected target DB");
                no_domain.with_native_exclusion(&db, |scope| {
                    actual.with_writer_data_domain_store(&db, |store| {
                        no_domain.require_database_store(&db, store, scope)
                    })
                })?;
            }
            reached = true;
            if matches!(case, SourceCase::TargetNoDomainHookRevoked) {
                let original = no_domain.test_keep_original_registration()?;
                database.read().clear_commit_admission();
                let db = database.try_read().unwrap();
                let error = no_domain
                    .with_native_exclusion(&db, |_scope| Ok(()))
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("original registered receiver hook changed"),
                    "{error}"
                );
                std::hint::black_box(&original);
                exact_error = Some(error.to_string());
                return Err(error);
            }
            if matches!(case, SourceCase::TargetNoDomainMemberChanged) {
                let mut actual = metadata.try_lock().expect("original actual MStore");
                let next = actual.status().current_term.checked_add(1).unwrap();
                assert!(actual.observe_remote_term(next)?);
                drop(actual);
                parent.check()?;
                let error = no_domain.require_metadata_current().unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("actual owned metadata member state changed"),
                    "{error}"
                );
                exact_error = Some(error.to_string());
                return Err(error);
            }
            if matches!(case, SourceCase::TargetNoDomainSchemaRevoked) {
                let actual = service.try_lock().unwrap();
                let db = database.try_read().unwrap();
                let error = no_domain
                    .with_native_exclusion(&db, |scope| {
                        actual.with_writer_data_domain_store(&db, |store| {
                            no_domain.require_database_store(&db, store, scope)?;
                            db.test_invalidate_registered_no_domain_schema(scope)?;
                            no_domain.require_database_store(&db, store, scope)
                        })
                    })
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("database/schema/reader identity changed"),
                    "{error}"
                );
                exact_error = Some(error.to_string());
                return Err(error);
            }
            no_domain.require_metadata_current()?;
            parent.check()
        },
    );
    assert!(
        reached,
        "genuine selected NoDomain point was never established: {:?}",
        result.as_ref().err()
    );
    if matches!(case, SourceCase::TargetNoDomain) {
        result.unwrap();
    } else {
        assert!(result.is_err());
        assert!(exact_error.is_some());
        parent.check().unwrap();
    }
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    let schema_generation_after = factory
        .database
        .read()
        .schema_compatibility_authority()
        .generation();
    let metadata_term_after = metadata.lock().status().current_term;
    if matches!(case, SourceCase::TargetNoDomainSchemaRevoked) {
        assert!(schema_generation_after > schema_generation_before);
    } else {
        assert_eq!(schema_generation_after, schema_generation_before);
    }
    if !matches!(case, SourceCase::TargetNoDomainHookRevoked) {
        assert_eq!(
            after, before,
            "NoDomain point/revocation must preserve target native/raw/row/UID state"
        );
    } else {
        assert_eq!(
            after, before,
            "empty-hook ABA changes original registration, no native/user bytes"
        );
    }
    evidence(&output.join(format!("no-domain-{}-{}.json", plan.old_range().id.get(),
        match case {SourceCase::TargetNoDomain=>"point",SourceCase::TargetNoDomainHookRevoked=>"hook",SourceCase::TargetNoDomainMemberChanged=>"member",_=>"schema"})),
        &serde_json::json!({"actual_original_fifth":true,"reached_full_no_domain":reached,
            "result":if exact_error.is_none(){"point_observed"}else{"revoked_before_installing"},
            "exact_error":exact_error,"before":before,"after":after,
            "schema_generation_before":schema_generation_before,"schema_generation_after":schema_generation_after,
            "metadata_term_before":metadata_term_before,"metadata_term_after":metadata_term_after,
            "no_installing_created":true,"no_rows_installed":true,"no_R0_or_serving_grant":true,
            "schema_case_is_generation_invalidation_not_DDL":matches!(case,SourceCase::TargetNoDomainSchemaRevoked)})).unwrap();
}
