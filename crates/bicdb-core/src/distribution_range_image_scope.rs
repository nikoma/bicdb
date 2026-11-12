//! Serializable purpose labels, never metadata, quorum or image authority.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RangeImageSourcePurposeV1 {
    CommittedHandoff {
        relocation_id: crate::RelocationId,
        plan_id: String,
    },
    CurrentVoterRepair,
}
