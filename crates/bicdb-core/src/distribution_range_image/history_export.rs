//! Source-owned full logical-spine export. A wire history step is not an
//! installation proof; the target must stage/verify it against its own floor.
use super::*;
use crate::db::NativeLogicalHistoryStepV1;

const MAX_HISTORY_ENTRIES: usize = 256;
const MAX_HISTORY_WIRE_BYTES: usize = 272 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeImageHistoryCursorV1 {
    next_index: u64,
    step: u64,
    complete: bool,
}
impl RangeImageHistoryCursorV1 {
    pub(crate) fn next_index(&self) -> u64 {
        self.next_index
    }
    pub(crate) fn step(&self) -> u64 {
        self.step
    }
    pub(crate) fn complete(&self) -> bool {
        self.complete
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeImageHistoryStepV1 {
    pub format_version: u32,
    pub session_id: Uuid,
    pub nonce: Uuid,
    pub previous_cursor: RangeImageHistoryCursorV1,
    pub next_cursor: RangeImageHistoryCursorV1,
    pub history: NativeLogicalHistoryStepV1,
    pub previous_chain_sha256: String,
    pub chain_sha256: String,
}

pub(super) fn initial_history_chain() -> String {
    hex::encode(Sha256::digest(b"bicdb.range-image.history-wire.v1\0"))
}

impl RangeImageCapture {
    pub(crate) fn history_cursor(&self) -> &RangeImageHistoryCursorV1 {
        &self.history_cursor
    }

    /// Exactly one bounded actual-native step, beginning at entry zero. A
    /// successful row stream without this complete spine can never be sealed.
    pub(crate) fn export_history_step(
        &mut self,
        expected: &RangeImageHistoryCursorV1,
        max_entries: usize,
        cancellation: &CancellationToken,
    ) -> Result<RangeImageHistoryStepV1> {
        cancellation.check()?;
        if max_entries == 0 || max_entries > MAX_HISTORY_ENTRIES {
            return Err(invalid("history export entry bound must be 1..=256"));
        }
        let database = read_database(&self.db)?;
        self.validate(&database)?;
        if let Some(previous) = &self.previous_history_step {
            if &previous.previous_cursor == expected {
                return Ok(previous.clone());
            }
        }
        if expected != &self.history_cursor || expected.complete {
            return Err(invalid(
                "history cursor is stale, forged or already complete",
            ));
        }
        let total_entries = self
            .range_index
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid("history entry count overflow"))?;
        let history = database.export_native_logical_history_step_with_source_pin(
            &self.lineage,
            &self.frozen,
            &self.source_read_pin,
            expected.next_index,
            max_entries,
            cancellation,
        )?;
        if history.format_version != 1
            || history.range_id != self.cut.range().id
            || history.source_range_index != self.range_index.get()
            || history.source_native_receipt_sha256 != self.lineage.native_receipt_sha256()
            || history.logical_genesis_sha256 != self.lineage.logical_genesis_sha256()
            || history.logical_history_sha256 != self.lineage.logical_history_sha256()
            || history.first_index != expected.next_index
            || history.entries.is_empty()
            || history.entries.len() > max_entries
        {
            return Err(invalid(
                "actual history export source/range/head/cursor contract changed",
            ));
        }
        let mut next_index = expected.next_index;
        let mut previous_head = self.history_last_sha256.clone();
        for entry in &history.entries {
            cancellation.check()?;
            entry.validate_shape()?;
            if entry.range_id != self.cut.range().id || entry.index != next_index {
                return Err(invalid(
                    "actual history export has a missing/reordered entry",
                ));
            }
            if next_index == 0 {
                if previous_head.is_some()
                    || entry.commitment_sha256 != history.logical_genesis_sha256
                {
                    return Err(invalid(
                        "actual history export did not begin at the bound native genesis",
                    ));
                }
            } else if previous_head.as_deref() != Some(entry.previous_sha256.as_str()) {
                return Err(invalid(
                    "actual history export link does not extend the exported prefix",
                ));
            }
            previous_head = Some(entry.commitment_sha256.clone());
            next_index = next_index
                .checked_add(1)
                .ok_or_else(|| invalid("history index overflow"))?;
        }
        let complete = next_index == total_entries;
        if next_index > total_entries
            || history.next_index != if complete { None } else { Some(next_index) }
            || (complete && previous_head.as_deref() != Some(self.lineage.logical_history_sha256()))
        {
            return Err(invalid(
                "actual history export completion/head does not cover exactly zero through R",
            ));
        }
        let next = RangeImageHistoryCursorV1 {
            next_index,
            step: expected
                .step
                .checked_add(1)
                .ok_or_else(|| invalid("history step overflow"))?,
            complete,
        };
        let mut step = RangeImageHistoryStepV1 {
            format_version: 1,
            session_id: self.cut.session_id(),
            nonce: self.cut.nonce(),
            previous_cursor: expected.clone(),
            next_cursor: next.clone(),
            history,
            previous_chain_sha256: self.history_wire_chain_sha256.clone(),
            chain_sha256: String::new(),
        };
        step.chain_sha256 =
            hex::encode(Sha256::digest(bounded_json(&step, MAX_HISTORY_WIRE_BYTES)?));
        self.validate(&database)?;
        cancellation.check()?;
        self.history_verified_entries = next_index;
        self.history_last_sha256 = previous_head;
        self.history_cursor = next;
        self.history_wire_chain_sha256
            .clone_from(&step.chain_sha256);
        self.previous_history_step = Some(step.clone());
        Ok(step)
    }
}
