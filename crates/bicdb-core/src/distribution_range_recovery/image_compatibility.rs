//! Actual full source-history comparison against EVERY irreversible observation
//! of the fenced current quorum. Numeric maxima alone never certify a source.
use super::{invalid, FreshRangeRecoveryAuthority};
use crate::db::{LocalNativeRecoveryReady, NativeLogicalHistoryOutcomeV1};
use crate::distribution::ClusterNodeId;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::{BicDb, CancellationToken, Result};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub(super) struct ActualSourceIdentity {
    pub(super) node: ClusterNodeId,
    pub(super) role_context: Uuid,
    pub(super) range_index: u64,
    pub(super) native_receipt: String,
    pub(super) genesis: String,
    pub(super) history: String,
}
impl ActualSourceIdentity {
    pub(super) fn from_actual(source: &LocalNativeRecoveryReady) -> Self {
        Self {
            node: source.role().local_node_id().clone(),
            role_context: source.role().context_id(),
            range_index: source.range_index(),
            native_receipt: source.native_receipt_sha256().to_string(),
            genesis: source.logical_genesis_sha256().to_string(),
            history: source.logical_history_sha256().to_string(),
        }
    }
    pub(super) fn require_actual(
        &self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<()> {
        source.require_current(db, store)?;
        if &self.node != source.role().local_node_id()
            || self.role_context != source.role().context_id()
            || self.range_index != source.range_index()
            || self.native_receipt != source.native_receipt_sha256()
            || self.genesis != source.logical_genesis_sha256()
            || self.history != source.logical_history_sha256()
        {
            return Err(invalid(
                "actual recovery image source changed during history verification",
            ));
        }
        Ok(())
    }
}

fn require_same_commitment(
    values: &mut BTreeMap<u64, String>,
    index: u64,
    commitment: &str,
) -> Result<()> {
    if values
        .insert(index, commitment.to_string())
        .is_some_and(|old| old != commitment)
    {
        return Err(invalid(
            "fenced quorum contains conflicting irreversible history commitments",
        ));
    }
    Ok(())
}

/// Bounded-memory streaming verifier. Source steps come directly from the
/// actual owned native producer; there is no method accepting a wire frame.
#[derive(Debug)]
pub(crate) struct RecoverySourceHistoryRound {
    authority: Arc<FreshRangeRecoveryAuthority>,
    source: ActualSourceIdentity,
    target: ClusterNodeId,
    expected_points: BTreeMap<u64, String>,
    expected_commands: BTreeMap<u64, String>,
    next_index: Option<u64>,
    previous_sha256: Option<String>,
    invalid: bool,
}
impl RecoverySourceHistoryRound {
    pub(crate) fn begin(
        authority: Arc<FreshRangeRecoveryAuthority>,
        source: &LocalNativeRecoveryReady,
        target: ClusterNodeId,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<Self> {
        authority.require_live()?;
        source.require_current(db, store)?;
        authority.scope().require_role(source.role())?;
        if !matches!(
            authority.role().purpose(),
            MetadataRecoveryRolePurpose::NormalRecovery
        ) || source.role().local_node_id() == &target
            || !authority.member_credentials().contains_key(&target)
        {
            return Err(invalid(
                "current-voter image repair needs an actual distinct current target voter",
            ));
        }
        // This is only a LOWER bound. Actual native history/command comparison
        // below supplies the proof; max(R) itself never establishes a cut.
        if source.range_index() < authority.highest_observed_index() {
            return Err(invalid(
                "source does not cover the complete observed fenced tail",
            ));
        }
        let mut expected_points = BTreeMap::new();
        let mut expected_commands = BTreeMap::new();
        for observation in authority.observations().values() {
            let obligations = observation.image_history();
            obligations.require_shape(authority.scope(), observation.progress())?;
            if obligations.compacted_through
                > obligations
                    .native_point
                    .as_ref()
                    .map_or(0, |point| point.range_index)
            {
                return Err(invalid("compacted fenced history has no actual native binding; repair source remains unknown"));
            }
            if let Some(point) = &obligations.native_point {
                if point.logical_genesis_sha256 != source.logical_genesis_sha256()
                    || point.range_index > source.range_index()
                {
                    return Err(invalid(
                        "source cannot cover a fenced native history domain/point",
                    ));
                }
                require_same_commitment(
                    &mut expected_points,
                    point.range_index,
                    &point.logical_history_sha256,
                )?;
            }
            for (index, checksum) in &obligations.irreversible_command_sha256 {
                if *index > source.range_index() {
                    return Err(invalid("source omits a fenced irreversible command"));
                }
                require_same_commitment(&mut expected_commands, *index, checksum)?;
            }
            if let Some(pending) = &obligations.pending_native {
                if pending.index > source.range_index() {
                    return Err(invalid(
                        "source omits an actual fenced native pending command",
                    ));
                }
                require_same_commitment(
                    &mut expected_commands,
                    pending.index,
                    &pending.command_checksum_sha256,
                )?;
            }
        }
        Ok(Self {
            authority,
            source: ActualSourceIdentity::from_actual(source),
            target,
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
    ) -> Result<VerifiedRecoverySourceHistory> {
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
        Ok(VerifiedRecoverySourceHistory {
            authority: self.authority,
            source: self.source,
            target: self.target,
        })
    }
}

/// Cross-voter physical image compatibility only. No serde/Clone/numeric
/// constructor, and no leader, serving-config, Apply or StrongGet admission.
#[derive(Debug)]
pub(crate) struct VerifiedRecoverySourceHistory {
    authority: Arc<FreshRangeRecoveryAuthority>,
    source: ActualSourceIdentity,
    target: ClusterNodeId,
}
impl VerifiedRecoverySourceHistory {
    pub(crate) fn require_current(
        &self,
        source: &LocalNativeRecoveryReady,
        db: &BicDb,
        store: &RangeWriteStore,
    ) -> Result<()> {
        self.authority.require_live()?;
        self.source.require_actual(source, db, store)
    }
    pub(crate) fn authority(&self) -> &FreshRangeRecoveryAuthority {
        &self.authority
    }
    pub(crate) fn target(&self) -> &ClusterNodeId {
        &self.target
    }
}
