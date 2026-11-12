//! Actual physical identity for request/refusal binding, never authority.
use super::*;
impl BicDb {
    /// Actual local identity for a listener refusal fence. This is not a
    /// topology declaration, quorum proof, or metadata-voter authorization.
    pub(crate) fn native_metadata_listener_identity(
        &self,
    ) -> Result<
        Option<(
            ClusterId,
            ClusterNodeId,
            crate::distribution::NativeStoreBirthV1,
        )>,
    > {
        let Some(birth) = self.read_native_birth()? else {
            return Ok(None);
        };
        let native = self.native_recovery_admission(
            &birth.spec.cluster_id,
            birth.spec.bootstrap_id,
            &birth.spec.node_id,
            birth.spec.member_incarnation,
        )?;
        Ok(Some((
            native.cluster_id().clone(),
            native.local_node_id().clone(),
            crate::distribution::NativeStoreBirthV1::from_actual_native(&native)?,
        )))
    }
}
