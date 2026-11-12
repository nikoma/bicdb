//! Interim native recovery contract: a COMPLETE serialized retained entry,
//! not a user value, must fit the bounded historical recovery path. Legacy
//! command/value limits are unchanged. This does not qualify large values.
use super::*;

pub(crate) const MAX_NATIVE_RECOVERY_ENTRY_BYTES: usize = 2 * 1024 * 1024;
/// Room for the complete entry plus bounded scope/status/history-obligation
/// envelopes. Native TCP admission refuses smaller configured frames; this
/// does not change the globally supported legacy transport configuration.
pub(crate) const MIN_NATIVE_RECOVERY_FRAME_BYTES: usize = 4 * 1024 * 1024;

struct Budget {
    remaining: usize,
    written: usize,
    limit: usize,
}
impl std::io::Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.remaining = self.remaining.checked_sub(bytes.len()).ok_or_else(|| {
            std::io::Error::other(format!(
                "native serialized recovery payload exceeds {} byte limit",
                self.limit,
            ))
        })?;
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn validate_native_json_size(value: &impl Serialize, limit: usize) -> Result<usize> {
    let mut budget = Budget {
        remaining: limit,
        written: 0,
        limit,
    };
    serde_json::to_writer(&mut budget, value)?;
    Ok(budget.written)
}
pub(super) fn validate_native_recovery_command_shape(command: &impl Serialize) -> Result<()> {
    #[derive(Serialize)]
    struct Entry<'a, T: Serialize> {
        command: &'a T,
        state: RangeWriteState,
    }
    // QuorumCommitted is the longest serialized state in protocol v2. Include
    // the actual entry wrapper instead of reserving guessed command overhead.
    validate_native_json_size(
        &Entry {
            command,
            state: RangeWriteState::QuorumCommitted,
        },
        MAX_NATIVE_RECOVERY_ENTRY_BYTES,
    )?;
    Ok(())
}
pub(crate) fn validate_native_recovery_command_size(command: &RangeWriteCommand) -> Result<()> {
    validate_native_recovery_command_shape(command)
}
pub(crate) fn validate_native_recovery_entry_size(entry: &RangeWriteLogEntry) -> Result<()> {
    validate_native_json_size(entry, MAX_NATIVE_RECOVERY_ENTRY_BYTES)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use serde_json::json;

    fn intent(bytes: usize) -> CommitAdmissionIntent {
        CommitAdmissionIntent {
            transaction_id: 7,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".into(),
                record_id: "boundary".into(),
                record: Some(
                    Record::new("boundary").with_metadata(json!({"body":"x".repeat(bytes)})),
                ),
            }],
        }
    }
    #[test]
    fn native_complete_entry_limit_matches_exact_boundary_and_preserves_legacy_constructor() {
        let (_root, _db, topology, _spec) = native_origin_fixture();
        let range = topology.range_for_key("items", "boundary").unwrap();
        let command = RangeWriteCommand::new(
            topology.cluster_id.clone(),
            range,
            1,
            range.leader.clone(),
            &intent(0),
        )
        .unwrap();
        let overhead = serde_json::to_vec(&RangeWriteLogEntry {
            command: command.clone(),
            state: RangeWriteState::QuorumCommitted,
        })
        .unwrap()
        .len();
        let fit = MAX_NATIVE_RECOVERY_ENTRY_BYTES - overhead;
        let bounded = RangeWriteCommand::new_native_bounded(
            topology.cluster_id.clone(),
            range,
            1,
            range.leader.clone(),
            &intent(fit),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_vec(&RangeWriteLogEntry {
                command: bounded,
                state: RangeWriteState::QuorumCommitted
            })
            .unwrap()
            .len(),
            MAX_NATIVE_RECOVERY_ENTRY_BYTES
        );
        let oversized = intent(fit + 1);
        assert!(RangeWriteCommand::new_native_bounded(
            topology.cluster_id.clone(),
            range,
            1,
            range.leader.clone(),
            &oversized
        )
        .is_err());
        let legacy = RangeWriteCommand::new(
            topology.cluster_id.clone(),
            range,
            1,
            range.leader.clone(),
            &oversized,
        )
        .unwrap();
        assert!(validate_native_recovery_command_size(&legacy).is_err());
        // Constructor compatibility only: not a qualification of large-value recovery.
        legacy.validate().unwrap();
    }
}
