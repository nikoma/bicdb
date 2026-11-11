//! Full actual source history for a separate mixed Installing quorum. No
//! ordinary recovery authority or certificate is synthesized by this verifier.
use super::image_compatibility::ActualSourceIdentity;
use super::*;
use crate::db::{LocalNativeRecoveryReady, NativeLogicalHistoryOutcomeV1};
use crate::distribution_range_consensus::RangeWriteStore;
use std::sync::Arc;
fn add(values: &mut BTreeMap<u64, String>, index: u64, checksum: &str) -> Result<()> {
    if values
        .insert(index, checksum.to_string())
        .is_some_and(|old| old != checksum)
    {
        return Err(invalid("conflicting interrupted-image source commitments"));
    }
    Ok(())
}
#[derive(Debug)]
pub(crate) struct CurrentImageResumeHistoryRound {
    authority: Arc<FreshCurrentImageResumeAuthority>,
    source: ActualSourceIdentity,
    expected_points: BTreeMap<u64, String>,
    expected_commands: BTreeMap<u64, String>,
    next_index: Option<u64>,
    previous_sha256: Option<String>,
    invalid: bool,
}
impl CurrentImageResumeHistoryRound {
    pub(crate) fn begin(
        authority: Arc<FreshCurrentImageResumeAuthority>,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<Self> {
        authority.require_live()?;
        source.require_current(db, store)?;
        authority.scope().require_role(source.role())?;
        let accepted = &authority.installing().source_acceptance;
        let native = source.native_admission();
        if source.role().local_node_id() != &accepted.source_id
            || native.birth_id() != accepted.source_birth_id
            || native.birth_sha256() != accepted.source_birth_sha256
            || source.range_index() != accepted.source_range_index
            || source.logical_genesis_sha256() != accepted.logical_genesis_sha256
            || source.logical_history_sha256() != accepted.logical_history_sha256
            || source.native_receipt_sha256() != accepted.source_native_receipt_sha256
        {
            return Err(invalid(
                "resumed source must match actual exact historically accepted cut/birth",
            ));
        }
        let mut expected_points = BTreeMap::new();
        let mut expected_commands = BTreeMap::new();
        for vote in authority.votes().values() {
            match vote {
                CurrentImageResumeVote::Ordinary(observation) => {
                    let history = observation.image_history();
                    history.require_shape(authority.scope(), observation.progress())?;
                    if history.compacted_through
                        > history.native_point.as_ref().map_or(0, |p| p.range_index)
                    {
                        return Err(invalid(
                            "compacted ordinary resume voter lacks actual native binding",
                        ));
                    }
                    if observation.progress().last_index > source.range_index() {
                        return Err(invalid("resumed source omits actual ordered tail"));
                    }
                    if let Some(point) = &history.native_point {
                        check_point(source, &mut expected_points, point)?;
                    }
                    for (index, checksum) in &history.irreversible_command_sha256 {
                        check_command(source, &mut expected_commands, *index, checksum)?;
                    }
                    if let Some(pending) = &history.pending_native {
                        check_command(
                            source,
                            &mut expected_commands,
                            pending.index,
                            &pending.command_checksum_sha256,
                        )?;
                    }
                    if observation
                        .native()
                        .pending_command_index
                        .is_some_and(|r| r > source.range_index())
                        || observation
                            .native()
                            .materialized_pending_index
                            .is_some_and(|r| r > source.range_index())
                    {
                        return Err(invalid("resumed source omits ordinary native pending"));
                    }
                }
                CurrentImageResumeVote::Installing(observation) => {
                    let evidence = observation.evidence();
                    if evidence.progress.last_index > source.range_index()
                        || evidence
                            .preimage
                            .maximum_index()
                            .is_some_and(|r| r > source.range_index())
                    {
                        return Err(invalid(
                            "resumed source is below protected/current irreversible bounds",
                        ));
                    }
                    let mut known = 0;
                    for point in evidence
                        .preimage
                        .native_point
                        .iter()
                        .chain(evidence.current_native_point.iter())
                    {
                        check_point(source, &mut expected_points, point)?;
                        known = known.max(point.range_index);
                    }
                    if evidence.progress.compacted_through > known {
                        return Err(invalid(
                            "compacted Installing voter lacks preserved native binding",
                        ));
                    }
                    for (index, checksum) in evidence.irreversible_union()? {
                        check_command(source, &mut expected_commands, index, &checksum)?;
                    }
                }
            }
        }
        add(
            &mut expected_points,
            accepted.source_range_index,
            &accepted.logical_history_sha256,
        )?;
        Ok(Self {
            source: ActualSourceIdentity::from_actual(source),
            authority,
            expected_points,
            expected_commands,
            next_index: Some(0),
            previous_sha256: None,
            invalid: false,
        })
    }
    /// At most 256 actual entries per call. Original role/parent deadlines are
    /// retained; callers cannot renew a round by passing a fresh timeout.
    pub(crate) fn advance(
        &mut self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &RangeWriteStore,
        parent: &CancellationToken,
    ) -> Result<bool> {
        if self.invalid {
            return Err(invalid(
                "source history round was invalidated; start a new actual round",
            ));
        }
        // An error can follow consumption of some expected obligations. Never
        // let a retry skip the very conflicting obligation that caused it.
        self.invalid = true;
        let result = self.advance_inner(source, db, store, parent);
        if result.is_ok() {
            self.invalid = false;
        }
        result
    }
    fn advance_inner(
        &mut self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &RangeWriteStore,
        parent: &CancellationToken,
    ) -> Result<bool> {
        self.authority.require_live()?;
        self.source.require_actual(source, db, store)?;
        let first = self
            .next_index
            .ok_or_else(|| invalid("source history already completely verified"))?;
        let token = parent.child_with_deadline(self.authority.deadline());
        let step = source.export_actual_history_step(db, store, first, &token)?;
        if step.first_index != first
            || step.source_range_index != self.source.range_index
            || step.source_native_receipt_sha256 != self.source.native_receipt
            || step.logical_genesis_sha256 != self.source.genesis
            || step.logical_history_sha256 != self.source.history
            || step.entries.is_empty()
        {
            return Err(invalid(
                "actual source history step changed its verified native identity",
            ));
        }
        let mut expected_index = first;
        for entry in &step.entries {
            token.check()?;
            entry.validate_shape()?;
            if entry.range_id != self.authority.current_range().id
                || entry.index != expected_index
                || entry.index > self.source.range_index
                || (entry.index == 0
                    && (entry.outcome != NativeLogicalHistoryOutcomeV1::Genesis
                        || entry.commitment_sha256 != self.source.genesis))
                || (entry.index > 0
                    && self.previous_sha256.as_deref() != Some(entry.previous_sha256.as_str()))
            {
                return Err(invalid("actual source history has a discontinuity"));
            }
            if self
                .expected_points
                .remove(&entry.index)
                .is_some_and(|sha| sha != entry.commitment_sha256)
            {
                return Err(invalid(
                    "actual source history conflicts with a fenced native point",
                ));
            }
            if let Some(expected) = self.expected_commands.remove(&entry.index) {
                if !matches!(&entry.outcome, NativeLogicalHistoryOutcomeV1::Applied { command_checksum_sha256 } if command_checksum_sha256 == &expected)
                {
                    return Err(invalid(
                        "actual source history conflicts with a fenced irreversible command",
                    ));
                }
            }
            self.previous_sha256 = Some(entry.commitment_sha256.clone());
            expected_index = entry.index.checked_add(1).unwrap_or(entry.index);
        }
        let expected_next = if step.entries.last().unwrap().index == self.source.range_index {
            None
        } else {
            Some(expected_index)
        };
        if step.next_index != expected_next {
            return Err(invalid(
                "actual source history cursor does not cover its returned entries",
            ));
        }
        self.next_index = expected_next;
        self.authority.require_live()?;
        self.source.require_actual(source, db, store)?;
        Ok(self.next_index.is_none())
    }
    pub(crate) fn finish(
        self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<VerifiedCurrentImageResumeHistory> {
        self.authority.require_live()?;
        self.source.require_actual(source, db, store)?;
        if self.invalid
            || self.next_index.is_some()
            || !self.expected_points.is_empty()
            || !self.expected_commands.is_empty()
            || self.previous_sha256.as_deref() != Some(self.source.history.as_str())
        {
            return Err(invalid(
                "source history has not verified the complete fenced quorum obligations",
            ));
        }
        Ok(VerifiedCurrentImageResumeHistory {
            authority: self.authority,
            source: self.source,
        })
    }
}

#[derive(Debug)]
pub(crate) struct VerifiedCurrentImageResumeHistory {
    authority: Arc<FreshCurrentImageResumeAuthority>,
    source: ActualSourceIdentity,
}
impl VerifiedCurrentImageResumeHistory {
    pub(crate) fn require_current(
        &self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<()> {
        self.authority.require_live()?;
        self.source.require_actual(source, db, store)
    }
    pub(crate) fn authority(&self) -> &FreshCurrentImageResumeAuthority {
        &self.authority
    }
}
fn check_point(
    source: &LocalNativeRecoveryReady,
    points: &mut BTreeMap<u64, String>,
    point: &RecoveryNativeHistoryPointV1,
) -> Result<()> {
    if point.range_index > source.range_index()
        || point.logical_genesis_sha256 != source.logical_genesis_sha256()
    {
        return Err(invalid(
            "resumed source has incompatible native history domain",
        ));
    }
    add(points, point.range_index, &point.logical_history_sha256)
}
fn check_command(
    source: &LocalNativeRecoveryReady,
    commands: &mut BTreeMap<u64, String>,
    index: u64,
    checksum: &str,
) -> Result<()> {
    if index > source.range_index() {
        return Err(invalid("resumed source omits irreversible command"));
    }
    add(commands, index, checksum)
}
