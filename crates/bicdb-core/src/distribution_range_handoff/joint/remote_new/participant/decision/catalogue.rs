//! SAME original native known outcome plus independently fresh catalogue
//! request. Native callbacks are historical here; they are never made live.
use super::*;
impl NativeRemoteNewParticipant {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn catalogue_decision(
        &self,
        request: &NativeRemoteNewRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteCatalogueDecisionV1> {
        self.require_request(request)?;
        let (facts, observe) = match &request.operation {
            NativeRemoteNewOperationV1::PublishCatalogue { facts } => (facts, false),
            NativeRemoteNewOperationV1::ObserveCatalogue { facts } => (facts, true),
            _ => return Err(invalid("remote catalogue needs explicit operation")),
        };
        self.require_lifetime_memory()?;
        let plan = NativeJointHandoffPlan::from_actual_role_and_own(role, own, business)?;
        request.require_plan(&plan)?;
        role.require_own_host_metadata(own, business)?;
        metadata.require_request(own, business)?;
        metadata.require_external_current()?;
        if role.local_node_id() != &self.snapshot.node_id
            || self
                .handoff_outcome()?
                .is_none_or(|h| h.fact_set_sha256 != facts.checksum_sha256)
        {
            return Err(invalid(
                "remote catalogue changed original recipient/pin handoff",
            ));
        }
        let mut held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote catalogue original decision active"))?;
        #[cfg(test)]
        let fail_rename = std::mem::take(&mut held.fail_catalogue_rename);
        #[cfg(test)]
        let fail_dirsync = std::mem::take(&mut held.fail_catalogue_dirsync);
        #[cfg(test)]
        let fault = if fail_rename || fail_dirsync {
            match &self.original {
                OriginalParticipant::Registered { target, .. } => {
                    Some(bicdb_page::test_faults::arm_io_failure(
                        if fail_rename {
                            "remote_joint_decision.catalogue.rename"
                        } else {
                            "remote_joint_decision.catalogue.dir_sync"
                        },
                        target.database_root(),
                        1,
                        false,
                    ))
                }
                _ => {
                    return Err(invalid(
                        "catalogue IO control requires actual original Registered target",
                    ))
                }
            }
        } else {
            None
        };
        #[cfg(test)]
        let cancel_before_io = std::mem::take(&mut held.cancel_catalogue_before_io);
        let DecisionCustody {
            expected,
            completed,
            native,
            catalogue,
            #[cfg(test)]
            catalogue_dirsync_fault_count,
            #[cfg(test)]
            catalogue_rename_fault_count,
            ..
        } = &mut *held;
        let data = expected
            .as_ref()
            .ok_or_else(|| invalid("remote catalogue original D absent"))?;
        let completed=completed.as_ref().ok_or_else(||invalid("remote catalogue actual native successful helper return absent; Unknown cannot advance"))?;
        let native = native
            .as_ref()
            .ok_or_else(|| invalid("remote catalogue original native attempt absent"))?;
        if completed.data() != data
            || role.metadata_term() < completed.metadata_anchor().0
            || role.metadata_read_index() < completed.metadata_anchor().1
        {
            return Err(invalid(
                "remote catalogue changed original native body/metadata lower bound",
            ));
        }
        let admission = NativeRemoteDecisionAdmission {
            participant: self,
            data,
            plan: &plan,
            context: DecisionRequest::Catalogue {
                role,
                own,
                business,
                metadata,
                caller: parent,
                completed,
                native,
                #[cfg(test)]
                cancel_before_io,
            },
        };
        admission.require_current()?;
        let result=match &self.original {
            OriginalParticipant::Registered {target,service,..}=>crate::distribution_data::ClusterDataNodeService::confirm_remote_registered_decision_catalogue(
                service,target,self.original_registered_freeze()?.try_as_frozen()?,&admission,catalogue,observe,parent),
            OriginalParticipant::Old {writer,..}=>self.original_old()?.confirm_remote_decision_catalogue(writer,&admission,catalogue,observe,parent),
        };
        #[cfg(test)]
        {
            let fired = fault.as_ref().map(|f| f.fired()).unwrap_or(0);
            if fail_rename {
                *catalogue_rename_fault_count += fired;
            } else {
                *catalogue_dirsync_fault_count += fired;
            }
        }
        let current_sha = result?;
        let reply = NativeRemoteCatalogueDecisionV1 {
            native: NativeRemoteDecisionReceiptV1 {
                binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
                phase_id: self.snapshot.phase_id,
                fact_set_sha256: facts.checksum_sha256.clone(),
                node_id: self.snapshot.node_id.clone(),
                decision_sha256: completed.decision_sha256().into(),
                native_record_sha256: completed.checksum_sha256().into(),
                native_page_xid: native.actual_committed_xid()?,
                remote_provenance_sha256: native.actual_provenance_sha256()?.into(),
            },
            range_index: data.range_index(),
            h0: data.logical_genesis_sha256().into(),
            h: data.logical_history_sha256().into(),
            old_configuration_sha256: data.promise().old_config_sha256.clone(),
            new_configuration_sha256: data.promise().new_config_sha256.clone(),
            source_snapshot_db_sequence: data.source_snapshot_db_sequence()?,
            target_local_snapshot_db_sequence: data.target_local_snapshot_db_sequence()?,
            original_participant_sha256: self.snapshot.checksum_sha256.clone(),
            publication_catalogue_sha256: catalogue.as_ref().unwrap().candidate_sha256().into(),
            current_catalogue_sha256: current_sha,
        };
        admission.require_current()?;
        metadata.require_external_current()?;
        reply.require_request(request, facts)?;
        Ok(reply)
    }
}
