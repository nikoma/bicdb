//! Genuine original fifth receiver, actual authenticated source and runtime.
use super::*;

// Both are specific refusal boundaries of the SAME actual protected member.
// Generic Cluster/timeout/unavailable errors cannot satisfy this control.
fn actual_metadata_revocation_boundary(error: &BicDbError) -> Option<&'static str> {
    match error {
        BicDbError::Cluster(detail)
            if detail == "metadata consensus: actual metadata authority version changed/dirty; retained native outcome is not a current grant" => Some("original_metadata_version"),
        BicDbError::Cluster(detail)
            if detail == "metadata consensus: actual owned metadata member state changed during validation" => Some("original_owned_member"),
        _ => None,
    }
}

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
    let original_own_deadline = own.deadline();
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
                let (term_before_mutation, next, version_before) = {
                    let actual = metadata.try_lock().expect("original actual MStore");
                    let term = actual.status().current_term;
                    (term, term.checked_add(1).unwrap(), actual.test_actual_publication_version_evidence()?)
                };
                evidence(&output.join("no-domain-member-term-mutation-intent.json"),
                    &serde_json::json!({"phase":"before_actual_term_mutator","requested_term":next,
                        "metadata_term_before":metadata_term_before,"version_before":version_before,
                        "original_native_before":before,"actual_mutator":"MetadataConsensusStore.observe_remote_term"}))?;
                let mut actual = metadata.try_lock().expect("original actual MStore");
                assert_eq!(actual.status().current_term, term_before_mutation);
                let mutation = actual.observe_remote_term(next);
                let actual_term_after_mutation = actual.status().current_term;
                let version_after = actual.test_actual_publication_version_evidence();
                drop(actual);
                let validation = no_domain.require_metadata_current();
                let state_after = state(&factory.database, pending, &node(4), ranges);
                let saved = evidence(&output.join("no-domain-member-refusal-before-assertions.json"),
                    &serde_json::json!({"phase":"after_actual_term_mutator_and_original_point_check",
                        "actual_mutation_result":mutation.as_ref().ok(),"actual_mutation_error":mutation.as_ref().err().map(|e|format!("{e:?}")),
                        "requested_term":next,"actual_term_after_mutation":actual_term_after_mutation,
                        "version_before":version_before,"version_after":version_after.as_ref().ok(),
                        "version_observation_error":version_after.as_ref().err().map(ToString::to_string),
                        "actual_validation_succeeded":validation.is_ok(),"actual_validation_error":validation.as_ref().err().map(|e|format!("{e:?}")),
                        "refusal_boundary":validation.as_ref().err().and_then(actual_metadata_revocation_boundary),
                        "original_native_before":before,"original_native_after":state_after.as_ref().ok(),
                        "native_after_observation_error":state_after.as_ref().err().map(ToString::to_string),
                        "original_parent_live":parent.check().is_ok(),"actual_step_lifetime_still_live":step.require_live().is_ok(),
                        "original_own_remaining_us":original_own_deadline.saturating_duration_since(Instant::now()).as_micros(),
                        "local_term_only_not_new_leader":true,"no_role_or_parent_replacement":true}));
                if let Err(diagnostic) = &saved {
                    eprintln!("REGISTERED_NO_DOMAIN_MEMBER_EVIDENCE_ERROR actual_mutation={mutation:?} actual_validation={validation:?} diagnostic={diagnostic}");
                }
                assert!(mutation?);
                assert_eq!(actual_term_after_mutation, next);
                parent.check()?;
                let error = validation.expect_err("actual original NoDomain member survived its real term mutation");
                exact_error = Some(error.to_string());
                assert!(
                    actual_metadata_revocation_boundary(&error).is_some(),
                    "{error}"
                );
                saved.expect("actual NoDomain refusal outcome evidence was not retained");
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
