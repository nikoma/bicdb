//! Preflight ordering only. Actual factory A admission must reject actual B
//! before B's deliberately unverified schema can mask the identity mismatch.
use super::*;

fn mismatched_target(
    fixture: &Fixture,
    attempt: Uuid,
) -> (
    Arc<RwLock<BicDb>>,
    NativeRecoveryAdmission,
    FrozenRangeMutationPermit,
    BicDb,
    NativeRecoveryAdmission,
    u64,
) {
    let target = fixture.target();
    let admission = target
        .read()
        .native_recovery_admission(
            &fixture.topology.cluster_id,
            fixture.target_spec.bootstrap_id,
            &fixture.target_spec.node_id,
            fixture.target_spec.member_incarnation,
        )
        .unwrap();
    let frozen = target
        .read()
        .range_mutation_gate()
        .try_freeze(fixture.range().id, attempt)
        .unwrap();
    let (mut replacement, _) = BicDb::create_new_range_image_database(
        &fixture.root.path().join("different-actual-target"),
        config(),
        fixture.target_spec.clone(),
    )
    .unwrap();
    replacement.create_collection("different_schema").unwrap();
    let own_admission = replacement
        .native_recovery_admission(
            &fixture.topology.cluster_id,
            fixture.target_spec.bootstrap_id,
            &fixture.target_spec.node_id,
            fixture.target_spec.member_incarnation,
        )
        .unwrap();
    own_admission.require_current(&replacement).unwrap();
    assert_ne!(admission.birth_id(), own_admission.birth_id());
    // Both are real completed factories in the SAME logical domain. B is not
    // damaged or missing a birth; it simply is another actual database/open.
    // Its schema has not been scanned, so bindings-first returns another error.
    let bindings_error = replacement
        .capture_range_image_bindings()
        .err()
        .expect("B schema is deliberately unverified");
    assert!(
        bindings_error
            .to_string()
            .contains("schema has no current verified digest"),
        "{bindings_error}"
    );
    let before_sequence = replacement.current_commit_seq();
    let original = std::mem::replace(&mut *target.write(), replacement);
    (
        target,
        admission,
        frozen,
        original,
        own_admission,
        before_sequence,
    )
}

fn catalogue_bytes(fixture: &Fixture) -> Option<Vec<u8>> {
    let path = fixture
        .root
        .path()
        .join("target")
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("actual target catalogue read failed: {error}"),
    }
}

fn require_original_admission_error(error: BicDbError) {
    assert!(
        error
            .to_string()
            .contains("recovery admission belongs to another DB/open or native birth"),
        "wrong-target rejection must precede schema/namespace inspection: {error}"
    );
}

#[test]
fn native_image_reborrow_begin_checks_actual_admission_before_replacement_bindings() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let attempt = Uuid::now_v7();
    let (target, admission, frozen, original, own, sequence) = mismatched_target(&fixture, attempt);
    let role = fixture.role();
    let source = fixture
        .capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    let before_catalogue = catalogue_bytes(&fixture);
    let error = NativeRangeImageInstaller::begin(
        target.clone(),
        &mut store,
        admission,
        &source,
        &NativeImageInstallRole::CommittedHandoff(&role),
        frozen,
        ImageInstallPurpose::RelocationLearner,
        attempt,
        None,
        &CancellationToken::uncancelable(),
    )
    .err()
    .expect("A admission cannot inspect/install B");
    require_original_admission_error(error);
    let replacement = target.read();
    own.require_current(&replacement).unwrap();
    assert_eq!(replacement.current_commit_seq(), sequence);
    assert!(replacement
        .schema_compatibility_authority()
        .verified_sha256()
        .is_none());
    assert_eq!(
        replacement
            .collections()
            .iter()
            .map(|m| m.name.as_str())
            .collect::<Vec<_>>(),
        vec!["different_schema"]
    );
    assert_eq!(catalogue_bytes(&fixture), before_catalogue);
    drop(replacement);
    drop(original);
}

#[test]
fn native_image_reborrow_recover_checks_actual_admission_before_replacement_bindings() {
    let mut fixture = Fixture::new();
    loop {
        let capture = fixture.capture.as_mut().unwrap();
        let cursor = capture.cursor().clone();
        if capture
            .export_step(&cursor, &options(), &CancellationToken::uncancelable())
            .unwrap()
            .complete
        {
            break;
        }
    }
    fixture.export_history();
    let source = fixture.capture.take().unwrap().complete().unwrap();
    let mut store = fixture.store();
    let attempt = Uuid::now_v7();
    let (target, admission, frozen, original, own, sequence) = mismatched_target(&fixture, attempt);
    let role = fixture.role();
    let before_catalogue = catalogue_bytes(&fixture);
    let error = NativeRangeImageInstaller::recover_completed(
        target.clone(),
        &mut store,
        admission,
        source,
        &NativeImageInstallRole::CommittedHandoff(&role),
        frozen,
        ImageInstallPurpose::RelocationLearner,
        attempt,
        &CancellationToken::uncancelable(),
    )
    .err()
    .expect("A recovery admission cannot inspect B");
    require_original_admission_error(error);
    let replacement = target.read();
    own.require_current(&replacement).unwrap();
    assert_eq!(replacement.current_commit_seq(), sequence);
    assert!(replacement
        .schema_compatibility_authority()
        .verified_sha256()
        .is_none());
    assert_eq!(
        replacement
            .collections()
            .iter()
            .map(|m| m.name.as_str())
            .collect::<Vec<_>>(),
        vec!["different_schema"]
    );
    assert_eq!(catalogue_bytes(&fixture), before_catalogue);
    drop(replacement);
    drop(original);
}
