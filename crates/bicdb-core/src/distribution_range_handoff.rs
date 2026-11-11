//! Durable data-plane promises for a relocation handoff.
//!
//! A decoded request is not authority. The RPC/service boundary must bind it to
//! fresh committed metadata, authenticated members and the exclusive range gate.
//! These promises never expire: time passing cannot make an old writer safe.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::distribution::{ClusterId, ClusterNodeId, RangeDescriptor, RangeId, RelocationId};
use crate::error::{BicDbError, Result};

pub(crate) const HANDOFF_RANGE_LOG_FORMAT: u32 = 4;
pub(crate) const MAX_HANDOFF_PROMISES: usize = 65_536;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeHandoffPromise {
    pub session_id: Uuid,
    pub relocation_id: RelocationId,
    pub plan_id: String,
    pub range_id: RangeId,
    pub old_epoch: u64,
    /// The final post-cleanup epoch. The intermediate promoted epoch must not
    /// admit writes before the final image/quorum is established.
    pub activation_epoch: u64,
    pub old_config_sha256: String,
    pub new_config_sha256: String,
    pub nonce: Uuid,
    pub resolved_through: u64,
}

impl RangeHandoffPromise {
    pub(crate) fn validate(&self) -> Result<()> {
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit());
        if self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.plan_id.is_empty()
            || self.plan_id.len() > 1024
            || self.old_epoch == 0
            || self.old_epoch.checked_add(2) != Some(self.activation_epoch)
            || !digest(&self.old_config_sha256)
            || !digest(&self.new_config_sha256)
        {
            return Err(BicDbError::Cluster(
                "invalid permanent range handoff promise".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn same_request(&self, other: &Self) -> bool {
        // The local resolved cut is observed by the store, never accepted from
        // the request. Every other binding must match on an idempotent retry.
        let mut expected = other.clone();
        expected.resolved_through = self.resolved_through;
        self == &expected
    }
}

/// Non-serializable proof issued only after fresh durable old-data-quorum
/// promises and source recovery have established the exact range-command cut.
/// A source DB commit sequence is deliberately not a field of this proof.
#[derive(Debug)]
pub(crate) struct CertifiedRangeImageCut {
    promise: RangeHandoffPromise,
    cluster_id: ClusterId,
    range: RangeDescriptor,
    range_index: u64,
    source_id: ClusterNodeId,
    source_incarnation: u64,
    target_id: ClusterNodeId,
    target_incarnation: u64,
}

impl CertifiedRangeImageCut {
    pub(crate) fn session_id(&self) -> Uuid {
        self.promise.session_id
    }
    pub(crate) fn relocation_id(&self) -> RelocationId {
        self.promise.relocation_id
    }
    pub(crate) fn plan_id(&self) -> &str {
        &self.promise.plan_id
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn source_id(&self) -> &ClusterNodeId {
        &self.source_id
    }
    pub(crate) fn source_incarnation(&self) -> u64 {
        self.source_incarnation
    }
    pub(crate) fn target_id(&self) -> &ClusterNodeId {
        &self.target_id
    }
    pub(crate) fn target_incarnation(&self) -> u64 {
        self.target_incarnation
    }
    pub(crate) fn old_config_sha256(&self) -> &str {
        &self.promise.old_config_sha256
    }
    pub(crate) fn new_config_sha256(&self) -> &str {
        &self.promise.new_config_sha256
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.promise.nonce
    }
    pub(crate) fn activation_epoch(&self) -> u64 {
        self.promise.activation_epoch
    }
}
