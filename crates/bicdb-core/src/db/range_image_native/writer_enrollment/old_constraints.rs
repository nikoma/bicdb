//! Necessary actual predecessor policy checks for an unresolved OLD peer.
//! This reads the original installed writer record; it creates no source
//! lineage, ready replica, closure vote, role or mutation authority.
use super::*;
impl NativeWriterHostState {
    pub(crate) fn require_old_constraint_predecessor(
        &self,
        database: &BicDb,
        plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    ) -> Result<()> {
        let predecessor = plan.old_native_predecessor_data()?;
        self.require_database(database, &predecessor)?;
        self.require_old_constraint_predecessor_bindings(database, plan)
    }
    /// Actual identity/schema/restore/member constraints only. The distinct
    /// step must also consume its retained pre-freeze predecessor scope.
    pub(crate) fn require_old_constraint_predecessor_bindings(
        &self,
        database: &BicDb,
        plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    ) -> Result<()> {
        let actual = &self.record.facts;
        let native = database.native_recovery_admission(
            &actual.cluster_id,
            actual.bootstrap_id,
            &actual.local_node_id,
            actual.local_incarnation,
        )?;
        plan.require_actual_native(&native)?;
        if actual.schema_sha256 != plan.constraint_schema_sha256()
            || actual.restore_activation.as_ref() != plan.constraint_restore_activation()
            || native.restore_activation() != plan.constraint_restore_activation()
            || database
                .schema_compatibility_authority()
                .verified_sha256()
                .as_deref()
                != Some(plan.constraint_schema_sha256())
        {
            return Err(invalid(
                "OLD constraint actual predecessor schema/restore changed",
            ));
        }
        for (id, expected) in plan.old_members() {
            let member = actual
                .members
                .get(id)
                .ok_or_else(|| invalid("OLD constraint original writer lacks old member"))?;
            if member.incarnation != expected.incarnation
                || member.active_pin != expected.tls_certificate_sha256
                || member.pending_pin != expected.pending_tls_certificate_sha256
                || &member.birth != plan.enrolled_birth(id)?
            {
                return Err(invalid(
                    "OLD constraint actual predecessor old member/pin/birth differs",
                ));
            }
        }
        Ok(())
    }
}
