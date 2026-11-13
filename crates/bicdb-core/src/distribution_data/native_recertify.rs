//! Actual bounded recertification, with no DB/service/mutation owner exported.
use super::*;
use crate::distribution_native_apply::{NativeLeaderInstructionAdmission, NativeRecertifyReply};

pub(crate) struct LocalNativeRecertifyEvidence {
    reply: NativeRecertifyReply,
}
impl LocalNativeRecertifyEvidence {
    pub(crate) fn into_reply(self) -> NativeRecertifyReply {
        self.reply
    }
}
impl ClusterDataNodeService {
    fn require_native_recertify_current(
        &self,
        admission: &NativeLeaderInstructionAdmission,
        parent: &CancellationToken,
    ) -> Result<(Uuid, String, u64)> {
        parent.check()?;
        admission.require_live()?;
        let role = admission.role();
        if role.local_node_id() != &self.node_id
            || role.cluster_id() != &self.cluster_id
            || !matches!(
                role.purpose(),
                crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery
            )
            || !self.range_writes.requires_native_abort_tracking()?
        {
            return Err(data_error(
                "native recertification requires actual current protected native host",
            ));
        }
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&db)?;
        self.require_native_writer_host_for_range(&db, role.current_range())?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        if native.writer_enforcement_version() != 1 {
            return Err(data_error(
                "native recertification requires from-birth enforced writer",
            ));
        }
        let source = db.current_native_range_source_lineage(role.current_range().id)?;
        admission.intent().validate_native_configuration(&source)?;
        self.range_writes
            .normal_native_write_allowed(admission.command())?;
        admission.require_live()?;
        parent.check()?;
        Ok((
            self.native_initial_listener_store_open_id(),
            db.incarnation_id().to_owned(),
            role.local_member_incarnation(),
        ))
    }
    pub(crate) fn recertify_native_ordinary(
        &mut self,
        admission: &NativeLeaderInstructionAdmission,
        parent: &CancellationToken,
    ) -> Result<LocalNativeRecertifyEvidence> {
        let original = self.require_native_recertify_current(admission, parent)?;
        let gate = self.range_mutations.clone();
        let permit = gate.try_enter(admission.command().range_id)?;
        let authority = RangeMutationAuthority::Mutation(&permit);
        let range = admission.role().current_range();
        let command = admission.command();
        // Each real step retains native writer-start/everCommit/promise logic.
        // No raw CQ/index or decoded certificate replaces durable consensus.
        let _prepared = self.prepare_range_write_with_mutation(
            range,
            admission.peer_node_id(),
            command.clone(),
            &authority,
        )?;
        if self.require_native_recertify_current(admission, parent)? != original {
            return Err(data_error(
                "native recertification actual owner changed after Prepare",
            ));
        }
        let _committed = self.commit_range_write_decision_with_mutation(
            range,
            admission.peer_node_id(),
            command,
            &authority,
        )?;
        if self.require_native_recertify_current(admission, parent)? != original {
            return Err(data_error(
                "native recertification actual owner changed after Commit",
            ));
        }
        #[cfg(test)]
        eprintln!("NATIVE_HOT_REPLICA_REAL_RECERTIFY node={} command={} index={} actual_prepare={:?} actual_commit={:?} role_context={} challenge={}", self.node_id,command.command_id,command.index,_prepared.state,_committed.state,admission.role().context_id(),admission.challenge());
        let ack = self.certify_range_write_with_mutation(
            range,
            admission.peer_node_id(),
            command,
            &authority,
        )?;
        if self.require_native_recertify_current(admission, parent)? != original {
            return Err(data_error(
                "native recertification actual owner changed after Certify",
            ));
        }
        Ok(LocalNativeRecertifyEvidence {
            reply: NativeRecertifyReply {
                challenge: admission.challenge(),
                command_sha256: command.checksum_sha256.clone(),
                origin_scope_sha256: admission.intent().scope_sha256().to_owned(),
                store_open_id: original.0,
                database_open_incarnation: original.1,
                member_incarnation: original.2,
                ack,
            },
        })
    }
}
