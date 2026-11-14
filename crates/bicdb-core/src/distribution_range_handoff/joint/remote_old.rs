//! Historical OLD responses from actual node-local producers. Wire fields are
//! DATA: only the pinned transport can mint an authenticated observation and
//! only a donor's real immutable spine can certify the resulting remote cut.
use super::*;
use crate::distribution::NativeStoreBirthV1;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_data::NativeJointIrrevocableFacts;
use crate::CancellationToken;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeRemoteOldOperationV1 {
    Close,
    /// Distinct real guarded capture; never a closed OLD vote.
    Constrain,
    Observe,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteOldRequestV1 {
    pub operation: NativeRemoteOldOperationV1,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub relocation_id: RelocationId,
    pub plan_id: String,
    pub session_id: Uuid,
    pub nonce: Uuid,
    pub scope_sha256: String,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeRemoteOldRequestV1 {
    pub(crate) fn from_plan(
        plan: &NativeJointHandoffPlan,
        operation: NativeRemoteOldOperationV1,
        business: Uuid,
        budget_ms: u64,
    ) -> Result<Self> {
        let p = plan.promise();
        let result = Self {
            operation,
            range_id: plan.old_range().id,
            range_epoch: plan.old_range().epoch,
            relocation_id: p.relocation_id,
            plan_id: p.plan_id.clone(),
            session_id: p.session_id,
            nonce: p.nonce,
            scope_sha256: plan.checksum_sha256()?,
            business,
            challenge: Uuid::now_v7(),
            budget_ms,
        };
        result.require_shape()?;
        Ok(result)
    }
    pub(crate) fn require_shape(&self) -> Result<()> {
        if self.range_id.get() == 0
            || self.relocation_id.get() == 0
            || self.range_epoch == 0
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.business.is_nil()
            || self.challenge.is_nil()
            || self.plan_id.is_empty()
            || self.plan_id.len() > 1024
            || self.budget_ms == 0
            || self.budget_ms > 5000
            || self.scope_sha256.len() != 64
            || !self.scope_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("invalid bounded remote OLD request DATA"));
        }
        Ok(())
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
    pub(crate) fn purpose(&self) -> MetadataRecoveryRolePurpose {
        MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: self.relocation_id,
            plan_id: self.plan_id.clone(),
            session_id: self.session_id,
            nonce: self.nonce,
        }
    }
    pub(crate) fn require_plan(&self, plan: &NativeJointHandoffPlan) -> Result<()> {
        self.require_shape()?;
        let p = plan.promise();
        if plan.old_range().id != self.range_id
            || plan.old_range().epoch != self.range_epoch
            || p.relocation_id != self.relocation_id
            || p.plan_id != self.plan_id
            || p.session_id != self.session_id
            || p.nonce != self.nonce
            || plan.checksum_sha256()? != self.scope_sha256
        {
            return Err(invalid(
                "remote OLD request differs from actual Pending plan",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteOldReplyV1 {
    pub request: NativeRemoteOldRequestV1,
    pub node_id: ClusterNodeId,
    pub birth: NativeStoreBirthV1,
    pub database_open: String,
    pub store_open: Uuid,
    pub metadata_store_id: Uuid,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub metadata_context_id: Uuid,
    pub closed: Option<NativeJointMaterializedFacts>,
    pub closed_native_record: Option<crate::db::NativeJointOldCloseRecordData>,
    pub constraints: NativeJointIrrevocableFacts,
    pub checksum_sha256: String,
}
impl NativeRemoteOldReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        if let Some(record) = &self.closed_native_record {
            record.require_byte_bound()?;
        }
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("native-remote-old-frame-v1", value),
                2 * 1024 * 1024,
            )?,
        )))
    }
}
