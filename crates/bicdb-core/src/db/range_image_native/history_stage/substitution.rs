//! A current-quorum substitution is narrower than materialized native history.
//! It preserves a protected accepted cut and the actual old stage's progress;
//! it cannot mint a target floor, complete a stage or authorize data clearing.
use super::*;
use crate::distribution_range_consensus::NativeCurrentImageProposalPointer;

/// Persisted audit/bindings only. Deserializing this never constructs coverage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct SubstitutionRecord {
    previous_stage_id: Uuid,
    previous_generation: Uuid,
    previous_header_sha256: String,
    previous_proposal: NativeCurrentImageProposalPointer,
    current_proposal: NativeCurrentImageProposalPointer,
    source: HistorySourceScope,
    pub(super) points: BTreeMap<u64, String>,
}
impl SubstitutionRecord {
    pub(super) fn archive_key(&self, range: RangeId) -> String {
        format!(
            "history-stage:superseded:{}:{}",
            range.get(),
            self.previous_stage_id
        )
    }
    pub(super) fn validate(&self, header: &StageHeader) -> Result<()> {
        self.previous_proposal.validate()?;
        self.current_proposal.validate()?;
        if self.previous_stage_id.is_nil()
            || self.previous_generation.is_nil()
            || self.previous_generation == header.generation
            || self.previous_header_sha256.len() != 64
            || self.source != header.source
            || self.points.is_empty()
            || self.points.len() > 3
            || self.points.iter().any(|(index, hash)| {
                *index > header.source.range_index
                    || hash.len() != 64
                    || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            })
        {
            return Err(invalid("invalid protected history substitution binding"));
        }
        Ok(())
    }
    pub(super) fn require_archive(&self, database: &BicDb, header: &StageHeader) -> Result<()> {
        self.validate(header)?;
        let archived: StageHeader = database
            .read_native_range_image_record(&format!(
                "history-stage:superseded:{}:{}",
                header.source.range_id.get(),
                self.previous_stage_id
            ))?
            .ok_or_else(|| invalid("substituted prior history stage archive is missing"))?;
        archived.validate()?;
        if archived.stage_id != self.previous_stage_id
            || archived.generation != self.previous_generation
            || archived.checksum_sha256 != self.previous_header_sha256
            || archived.target_birth_id != header.target_birth_id
            || archived.target_birth_sha256 != header.target_birth_sha256
            || archived.source.logical_genesis_sha256 != header.source.logical_genesis_sha256
            || self.points.get(&archived.source.range_index)
                != Some(&archived.source.logical_history_sha256)
        {
            return Err(invalid(
                "substituted prior stage archive/accepted point differs",
            ));
        }
        Ok(())
    }
}

/// No serde/Clone/public fields or caller-constructed hash factory. Only the
/// actual target proposal chain AND a live full-source quorum verifier mint it.
pub(super) struct CurrentQuorumSubstitutionCoverage {
    database_incarnation: String,
    attempt_id: Uuid,
    intent_sha256: String,
    record: SubstitutionRecord,
}
impl CurrentQuorumSubstitutionCoverage {
    pub(super) fn record(&self) -> &SubstitutionRecord {
        &self.record
    }

    pub(super) fn prove(
        database: &BicDb,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        previous: &StageHeader,
    ) -> Result<Self> {
        context.admission.require_current(database)?;
        context.fence.require_live_role(context.role)?;
        previous.validate()?;
        source.cut().require_current_quorum_history_coverage()?;
        let source_scope = HistorySourceScope::actual(source)?;
        let current = context.fence.protected_proposal().ok_or_else(|| {
            invalid("history substitution requires actual protected target proposal")
        })?;
        let quorum_proposal = source
            .cut()
            .resume_proposal()
            .ok_or_else(|| invalid("history substitution lacks quorum-bound proposal"))?;
        if !matches!(context.role, NativeImageInstallRole::CurrentVoterRepair(_))
            || source.cut().target_id() != context.admission.local_node_id()
            || source.cut().target_incarnation() != context.admission.local_member_incarnation()
            || source.cut().session_id() != context.role.role().recovery_nonce()
            || previous.target_birth_id != context.admission.birth_id()
            || previous.target_birth_sha256 != context.admission.birth_sha256()
            || previous.source.range_id != source_scope.range_id
            || previous.source.logical_genesis_sha256 != source_scope.logical_genesis_sha256
            || previous.source.range_index > source_scope.range_index
        {
            return Err(invalid(
                "history substitution target/open/current role or logical domain differs",
            ));
        }
        let current_receipt =
            database.read_native_current_image_proposal(context.admission, current)?;
        let accepted = current_receipt.historical_source();
        if accepted.source_id != *source.cut().source_id()
            || accepted.source_member_incarnation != source.cut().source_incarnation()
            || accepted.source_range_index != source_scope.range_index
            || accepted.source_native_receipt_sha256 != source_scope.native_receipt_sha256
            || accepted.logical_genesis_sha256 != source_scope.logical_genesis_sha256
            || accepted.logical_history_sha256 != source_scope.logical_history_sha256
        {
            return Err(invalid(
                "current protected proposal does not accept actual substitution source",
            ));
        }
        // Only an actual protected ancestor can explain the previous stage.
        // Known target identities/UIDs/schema are revalidated at EVERY load.
        let mut receipt = current_receipt;
        let mut covered_proposal = None;
        let mut saw_quorum_proposal = false;
        for _ in 0..128 {
            source.cut().require_current_quorum_history_coverage()?;
            let pointer = receipt.pointer();
            saw_quorum_proposal |= &pointer == quorum_proposal;
            let accepted = receipt.historical_source();
            if accepted.scope.range_id == previous.source.range_id
                && accepted.scope.recovery_nonce == previous.source.session_id
                && accepted.source_range_index == previous.source.range_index
                && accepted.source_native_receipt_sha256 == previous.source.native_receipt_sha256
                && accepted.logical_genesis_sha256 == previous.source.logical_genesis_sha256
                && accepted.logical_history_sha256 == previous.source.logical_history_sha256
            {
                covered_proposal = Some(pointer.clone());
            }
            let Some(parent) = receipt.parent().cloned() else {
                break;
            };
            receipt = database.read_native_current_image_proposal(context.admission, &parent)?;
        }
        let previous_proposal = covered_proposal
            .filter(|_| saw_quorum_proposal)
            .ok_or_else(|| {
                invalid("actual protected proposal ancestry does not cover prior stage/quorum")
            })?;
        let mut points = BTreeMap::from([(
            previous.source.range_index,
            previous.source.logical_history_sha256.clone(),
        )]);
        // A partially audited/received stage still has immutable commitments.
        // Preserve and check both actual endpoints, rather than pretending the
        // old attempt was audited, dropping its cursor or inventing a floor.
        for (count, expected) in [
            (previous.verified_entries, previous.current_head.as_ref()),
            (previous.audit_next_index, previous.audit_head.as_ref()),
        ] {
            match (count.checked_sub(1), expected) {
                (Some(index), Some(expected)) => {
                    let entry = database.read_native_history_entry(
                        previous.source.range_id,
                        previous.generation,
                        index,
                    )?;
                    if &entry.commitment_sha256 != expected
                        || points
                            .insert(index, expected.clone())
                            .is_some_and(|old| old != *expected)
                    {
                        return Err(invalid(
                            "prior stored history cursor/audit commitment changed",
                        ));
                    }
                }
                (None, None) => {}
                _ => return Err(invalid("prior history cursor/audit has no actual endpoint")),
            }
        }
        // Actual source reads under its exclusive capture pin, with the
        // ORIGINAL mixed-quorum cancellation/deadline, prove membership.
        source.require_current_quorum_history_points(&points)?;
        Ok(Self {
            database_incarnation: database.incarnation.clone(),
            attempt_id: context.fence.attempt_id(),
            intent_sha256: context.fence.intent_sha256().to_string(),
            record: SubstitutionRecord {
                previous_stage_id: previous.stage_id,
                previous_generation: previous.generation,
                previous_header_sha256: previous.checksum_sha256.clone(),
                previous_proposal,
                current_proposal: current.clone(),
                source: source_scope,
                points,
            },
        })
    }
    pub(super) fn require_current(
        &self,
        database: &BicDb,
        context: &NativeHistoryTargetContext<'_>,
        source: &TentativeNativeRangeImageSourceAuthorization<'_>,
        previous: &StageHeader,
    ) -> Result<()> {
        if self.database_incarnation != database.incarnation
            || self.attempt_id != context.fence.attempt_id()
            || self.intent_sha256 != context.fence.intent_sha256()
            || self.record != Self::prove(database, context, source, previous)?.record
        {
            return Err(invalid("stale current-quorum history substitution proof"));
        }
        Ok(())
    }
}

pub(super) fn archive_key(header: &StageHeader) -> String {
    format!(
        "history-stage:superseded:{}:{}",
        header.source.range_id.get(),
        header.stage_id
    )
}

#[cfg(test)]
pub(super) fn assert_actual_negative_controls(
    database: &BicDb,
    context: &NativeHistoryTargetContext<'_>,
    source: &TentativeNativeRangeImageSourceAuthorization<'_>,
    previous: &StageHeader,
    coverage: &CurrentQuorumSubstitutionCoverage,
) {
    // These controls run only after the real full source verifier + actual
    // protected ancestry minted coverage. Mutating private binding copies
    // grants nothing and leaves every persisted target record untouched.
    let original = database
        .native_history_stage_state_commitment(previous.source.range_id)
        .unwrap();
    let make = || CurrentQuorumSubstitutionCoverage {
        database_incarnation: coverage.database_incarnation.clone(),
        attempt_id: coverage.attempt_id,
        intent_sha256: coverage.intent_sha256.clone(),
        record: coverage.record.clone(),
    };
    let mut wrong = make();
    wrong.attempt_id = Uuid::now_v7();
    assert!(
        wrong
            .require_current(database, context, source, previous)
            .is_err(),
        "different attempt is not a substitution proof"
    );
    let mut wrong = make();
    wrong.database_incarnation.push_str("-different-open");
    assert!(
        wrong
            .require_current(database, context, source, previous)
            .is_err(),
        "different target open is not a substitution proof"
    );
    let mut wrong = make();
    let point = wrong.record.points.values_mut().next().unwrap();
    *point = if point.bytes().all(|byte| byte == b'0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    assert!(
        wrong
            .require_current(database, context, source, previous)
            .is_err(),
        "wrong old prefix commitment must refuse"
    );
    assert!(
        source
            .require_current_quorum_history_points(&wrong.record.points)
            .is_err(),
        "actual source history must reject wrong old prefix"
    );
    let wrong_role = NativeImageInstallRole::CurrentVoterRepair(
        source.cut().test_history_coverage_source_role(),
    );
    let wrong_context = NativeHistoryTargetContext {
        store: context.store,
        fence: context.fence,
        admission: context.admission,
        safety: context.safety,
        catalog: context.catalog,
        role: &wrong_role,
        frozen: context.frozen,
        substitution: None,
    };
    assert!(
        CurrentQuorumSubstitutionCoverage::prove(database, &wrong_context, source, previous)
            .is_err(),
        "genuine source-local role is not the target-local role"
    );
    assert_eq!(
        database
            .native_history_stage_state_commitment(previous.source.range_id)
            .unwrap(),
        original,
        "negative substitution checks must not change the original stage"
    );
}
