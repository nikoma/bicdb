//! Bounded observations of irreversible history at an actual fenced voter.
//! These DTOs are observations, never repair, Apply, or serving authority.
use super::invalid;
use crate::distribution::{ClusterNodeId, RangeId};
use crate::distribution_range_consensus::{RangeWriteProgress, RecoveryFenceScope};
use crate::Result;
use std::collections::BTreeMap;

/// A closed ready point has at most the retained resolved suffix plus the
/// independently retained historical certification map. A fence must also
/// represent an ordinary unresolved command following that resolved suffix.
/// These are supported COMPLETE reply bounds, not a claim that every possible
/// raw-log backlog has this size. Larger histories require another bounded
/// protocol; none of their checksums may be silently omitted here.
pub(super) const MAX_NATIVE_READY_IRREVERSIBLE_COMMANDS: usize = 2 * 1024;
pub(crate) const MAX_FENCED_IRREVERSIBLE_COMMANDS: usize =
    MAX_NATIVE_READY_IRREVERSIBLE_COMMANDS + 1;

pub(crate) fn fenced_obligation_limit_error() -> crate::BicDbError {
    invalid(&format!(
        "fenced history exceeds the complete {MAX_FENCED_IRREVERSIBLE_COMMANDS}-command obligation reply bound; paginated history is required and no partial evidence is admitted"
    ))
}
fn digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryNativeHistoryPointV1 {
    pub range_index: u64,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    /// Local identity only; different replicas normally have different native
    /// receipts. Portable history comparison MUST use logical commitments.
    pub native_receipt_sha256: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryNativePendingHistoryV1 {
    pub index: u64,
    pub command_checksum_sha256: String,
    pub materialized: bool,
}

/// Required in a fence reply; no serde defaults may turn omitted irreversible
/// evidence into an empty/zero state. Only an authenticated opaque fence reply
/// may supply these observations to the cross-voter compatibility verifier.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryImageHistoryObligationsV1 {
    pub format_version: u32,
    pub node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub last_index: u64,
    pub compacted_through: u64,
    pub native_point: Option<RecoveryNativeHistoryPointV1>,
    pub pending_native: Option<RecoveryNativePendingHistoryV1>,
    /// Includes old CQ/Applied entries AND durable current-session certify-only
    /// markers. A later compaction cannot silently remove their obligations.
    pub irreversible_command_sha256: BTreeMap<u64, String>,
    /// Version 2 explicitly includes permanent sparse committed decisions.
    /// None is legacy v1 only; a v2 omission/null is invalid, including when empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sparse_committed_command_sha256: Option<BTreeMap<u64, String>>,
}
impl RecoveryImageHistoryObligationsV1 {
    pub(crate) fn require_shape(
        &self,
        scope: &RecoveryFenceScope,
        progress: &RangeWriteProgress,
    ) -> Result<()> {
        if self.irreversible_command_sha256.len() > MAX_FENCED_IRREVERSIBLE_COMMANDS {
            return Err(fenced_obligation_limit_error());
        }
        if !matches!(self.format_version, 1 | 2)
            || self.node_id != progress.node_id
            || self.range_id != scope.range_id
            || self.range_id != progress.range_id
            || self.last_index != progress.last_index
            || self.compacted_through != progress.compacted_through
            || self.compacted_through > progress.resolved_through
            || progress.resolved_through > self.last_index
            || self.irreversible_command_sha256.iter().any(|(index, sha)| {
                *index == 0
                    || !digest(sha)
                    || (*index > self.last_index
                        && self
                            .sparse_committed_command_sha256
                            .as_ref()
                            .and_then(|map| map.get(index))
                            != Some(sha))
            })
        {
            return Err(invalid("malformed fenced image-history obligations"));
        }
        match (&self.sparse_committed_command_sha256, self.format_version) {
            (None, 1) => {}
            (Some(sparse), 2) if sparse.len() <= 1024 => {
                for (index, sha) in sparse {
                    if *index == 0
                        || !digest(sha)
                        || self.irreversible_command_sha256.get(index) != Some(sha)
                    {
                        return Err(invalid(
                            "sparse history is omitted/conflicting in complete obligations",
                        ));
                    }
                }
            }
            _ => {
                return Err(invalid(
                    "sparse history field requires its explicit version 2 fence",
                ))
            }
        }
        if let Some(point) = &self.native_point {
            if !digest(&point.logical_genesis_sha256)
                || !digest(&point.logical_history_sha256)
                || !digest(&point.native_receipt_sha256)
                || (point.range_index == 0
                    && point.logical_genesis_sha256 != point.logical_history_sha256)
            {
                return Err(invalid("malformed irreversible native history point"));
            }
        }
        if let Some(pending) = &self.pending_native {
            if pending.index == 0
                || !digest(&pending.command_checksum_sha256)
                || self
                    .native_point
                    .as_ref()
                    .and_then(|point| point.range_index.checked_add(1))
                    != Some(pending.index)
            {
                return Err(invalid(
                    "native pending history lacks its actual predecessor",
                ));
            }
        }
        // An unknown point is retained as UNKNOWN. In particular, a compacted
        // catalogue with no point is not automatically an empty native range.
        Ok(())
    }
}

/// Synthetic DTOs ONLY for structural/wire-size tests. These do not construct
/// a native observation, authenticated vote, quorum or recovery permission.
#[cfg(test)]
pub(crate) fn test_only_structural_fence_dtos(
    count: usize,
) -> (
    RecoveryFenceScope,
    RangeWriteProgress,
    RecoveryImageHistoryObligationsV1,
) {
    use crate::distribution_consensus::MetadataRecoveryRolePurpose;
    use crate::ClusterId;
    use uuid::Uuid;
    let node = ClusterNodeId::new("shape-only-node").unwrap();
    let range_id = RangeId::new(1).unwrap();
    let scope = RecoveryFenceScope {
        protocol_version: 1,
        cluster_id: ClusterId::new("shape-only-cluster").unwrap(),
        bootstrap_id: Uuid::new_v4(),
        range_id,
        range_epoch: 1,
        initiator_node_id: node.clone(),
        initiator_incarnation: 1,
        recovery_nonce: Uuid::new_v4(),
        business_request_id: Uuid::new_v4(),
        metadata_context_id: Uuid::new_v4(),
        metadata_term: 1,
        metadata_read_index: 1,
        configuration_sha256: "a".repeat(64),
        range_configuration_sha256: "b".repeat(64),
        purpose: MetadataRecoveryRolePurpose::NormalRecovery,
    };
    let progress = RangeWriteProgress {
        node_id: node.clone(),
        range_id,
        current_epoch: 1,
        last_index: u64::MAX,
        resolved_through: u64::MAX - 1,
        compacted_through: u64::MAX - 4096,
    };
    let first = u64::MAX - count as u64 + 1;
    let image = RecoveryImageHistoryObligationsV1 {
        format_version: 1,
        node_id: node,
        range_id,
        last_index: progress.last_index,
        compacted_through: progress.compacted_through,
        native_point: Some(RecoveryNativeHistoryPointV1 {
            range_index: progress.resolved_through,
            logical_genesis_sha256: "c".repeat(64),
            logical_history_sha256: "d".repeat(64),
            native_receipt_sha256: "e".repeat(64),
        }),
        pending_native: Some(RecoveryNativePendingHistoryV1 {
            index: u64::MAX,
            command_checksum_sha256: "f".repeat(64),
            materialized: false,
        }),
        sparse_committed_command_sha256: None,
        irreversible_command_sha256: (first..=u64::MAX)
            .map(|index| (index, "f".repeat(64)))
            .collect(),
    };
    (scope, progress, image)
}

#[cfg(test)]
mod obligation_bound_tests {
    use super::*;

    #[test]
    fn sparse_obligations_require_explicit_v2_and_never_claim_a_raw_applied_floor() {
        // Structural wire shape only: no native producer, authenticated peer,
        // selected decision or ready/read authority is created here.
        let (scope, mut progress, mut history) = test_only_structural_fence_dtos(1);
        progress.last_index = 0;
        progress.resolved_through = 0;
        progress.compacted_through = 0;
        history.last_index = 0;
        history.compacted_through = 0;
        history.native_point = None;
        history.pending_native = None;
        history.irreversible_command_sha256 = BTreeMap::from([(7, "a".repeat(64))]);
        history.sparse_committed_command_sha256 = Some(history.irreversible_command_sha256.clone());
        history.format_version = 2;
        history.require_shape(&scope, &progress).unwrap();
        let mut missing = serde_json::to_value(&history).unwrap();
        missing
            .as_object_mut()
            .unwrap()
            .remove("sparse_committed_command_sha256");
        let decoded: RecoveryImageHistoryObligationsV1 = serde_json::from_value(missing).unwrap();
        assert!(decoded.require_shape(&scope, &progress).is_err());
        history.format_version = 1;
        assert!(history.require_shape(&scope, &progress).is_err());
        history.format_version = 2;
        history.sparse_committed_command_sha256 = Some(BTreeMap::new());
        assert!(history.require_shape(&scope, &progress).is_err());
        assert_eq!((progress.last_index, progress.resolved_through), (0, 0));
    }

    #[test]
    fn complete_fenced_obligation_bound_preserves_every_entry_and_refuses_overflow() {
        let (scope, progress, image) =
            test_only_structural_fence_dtos(MAX_FENCED_IRREVERSIBLE_COMMANDS);
        image.require_shape(&scope, &progress).unwrap();
        assert_eq!(image.irreversible_command_sha256.len(), 2049);
        let encoded = serde_json::to_vec(&image).unwrap();
        let decoded: RecoveryImageHistoryObligationsV1 = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, image, "the complete map survives serialization");
        let (scope, progress, oversized) =
            test_only_structural_fence_dtos(MAX_FENCED_IRREVERSIBLE_COMMANDS + 1);
        let error = oversized.require_shape(&scope, &progress).unwrap_err();
        assert!(error
            .to_string()
            .contains("complete 2049-command obligation reply bound"));
        assert_eq!(oversized.irreversible_command_sha256.len(), 2050);
    }
}
