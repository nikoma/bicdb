//! Recipient-owned remote preparation. Retained native outcome and completed
//! physical owner survive every outer request check; no wire DATA is a minter.
use super::*;
use crate::distribution_range_handoff::{
    CommittedNativeJointDecisionObservation, NativeJointPreparationCustody,
};

#[derive(Default)]
pub(crate) struct NativeRemoteServicePreparation {
    native: Option<crate::db::NativeRemoteJointPreparationAttempt>,
    completed: Option<Arc<NativeJointPreparationCustody>>,
    #[cfg(test)]
    mode: u8,
}
impl NativeRemoteServicePreparation {
    pub(crate) fn completed(&self) -> Option<&Arc<NativeJointPreparationCustody>> {
        self.completed.as_ref()
    }
    pub(crate) fn require_resolved_observation(
        &self,
        observation: &crate::distribution_range_handoff::NativeRemotePreparedObservation,
    ) -> Result<()> {
        self.native
            .as_ref()
            .ok_or_else(|| data_error("preparation resolution original native attempt absent"))?
            .require_original_known_native_outcome()?;
        self.completed
            .as_ref()
            .ok_or_else(|| data_error("preparation resolution actual completed owner absent"))?
            .require_observation_owner(observation)
    }
    #[cfg(test)]
    pub(crate) fn test_arm(&mut self, mode: u8) -> Result<()> {
        if !matches!(mode, 1 | 2) || self.mode != 0 || self.native.is_some() {
            return Err(data_error(
                "remote preparation test requires untouched actual native attempt",
            ));
        }
        self.mode = mode;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_native_outcome(&self) -> serde_json::Value {
        serde_json::json!({"actual_native_attempt":self.native.as_ref().map(|n|n.test_outcome()),
            "completed_original_owner_retained":self.completed.is_some()})
    }
}
enum PreparationCall<'a, 'b> {
    Write(&'a crate::distribution_native_host::NativeRemotePreparationAdmission<'b>),
    Read(&'a crate::distribution_native_host::NativeRemotePreparationReadAdmission<'b>),
}
impl PreparationCall<'_, '_> {
    fn require_current(&self, committed: &CommittedNativeJointDecisionObservation) -> Result<()> {
        match self {
            Self::Write(a) => a.require_current(committed),
            Self::Read(a) => a.require_current(committed),
        }
    }
    fn require_memory_current(&self) -> Result<()> {
        match self {
            Self::Write(a) => a.require_memory_current(),
            Self::Read(a) => a.require_memory_current(),
        }
    }
    fn process(&self) -> &Arc<crate::distribution_native_host::NativeRemotePreparationProcess> {
        match self {
            Self::Write(a) => a.process(),
            Self::Read(a) => a.process(),
        }
    }
    fn request(&self) -> CancellationToken {
        match self {
            Self::Write(a) => a.bound_effect_request(),
            Self::Read(a) => a.bound_read_request(),
        }
    }
}
impl ClusterDataNodeService {
    /// Actual serialized host callers retain `retained` AND their original
    /// lease before entry. A returned Err never discards either native outcome
    /// or an already completed preparation. Later requests cannot re-prepare
    /// after completion; they must freshly observe that exact custody.
    pub(crate) fn prepare_remote_joint_serving(
        original: &Arc<Mutex<Self>>,
        domain: crate::db::NativeJointWriterDomain,
        committed: CommittedNativeJointDecisionObservation,
        lease: NativeJointServingLease,
        admission: &crate::distribution_native_host::NativeRemotePreparationAdmission<'_>,
        retained: &mut NativeRemoteServicePreparation,
    ) -> Result<()> {
        Self::retain_remote_joint_preparation(
            original,
            domain,
            committed,
            lease,
            PreparationCall::Write(admission),
            retained,
        )
    }
    pub(crate) fn read_known_remote_joint_preparation(
        original: &Arc<Mutex<Self>>,
        domain: crate::db::NativeJointWriterDomain,
        committed: CommittedNativeJointDecisionObservation,
        lease: NativeJointServingLease,
        admission: &crate::distribution_native_host::NativeRemotePreparationReadAdmission<'_>,
        retained: &mut NativeRemoteServicePreparation,
    ) -> Result<()> {
        Self::retain_remote_joint_preparation(
            original,
            domain,
            committed,
            lease,
            PreparationCall::Read(admission),
            retained,
        )
    }
    fn retain_remote_joint_preparation(
        original: &Arc<Mutex<Self>>,
        domain: crate::db::NativeJointWriterDomain,
        committed: CommittedNativeJointDecisionObservation,
        lease: NativeJointServingLease,
        admission: PreparationCall<'_, '_>,
        retained: &mut NativeRemoteServicePreparation,
    ) -> Result<()> {
        admission.require_current(&committed)?;
        let process = admission.process().clone();
        let request = admission.request();
        request.check()?;
        if retained.completed.is_some() {
            return Err(data_error("remote preparation already retained; use distinct fresh protected observation, never native replay"));
        }
        let service = original
            .try_lock()
            .ok_or_else(|| data_error("remote preparation original recipient service busy"))?;
        process.require_service(&service)?;
        let preparation_origin =
            bridge::preparation_origin::PreparingOrigin::begin_remote(process.clone())?;
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        committed.require_current(&database)?;
        process.require_original_native(&database, &committed)?;
        domain.require_current(
            &database,
            &service.range_writes,
            committed.data().activation_range(),
        )?;
        if committed.native_receipt().local_node_id() != &service.node_id {
            return Err(data_error(
                "remote preparation substituted actual recipient native decision",
            ));
        }
        match (&domain,&lease) {
            (crate::db::NativeJointWriterDomain::Registered(actual),NativeJointServingLease::Registered {owner,..})
                if actual.upgrade().is_some_and(|a|Arc::ptr_eq(&a,owner))=>{},
            (crate::db::NativeJointWriterDomain::Closed(_),NativeJointServingLease::Survivor(_))=>{
                if lease.try_frozen()?.owner()!=committed.data().promise().session_id {
                    return Err(data_error("remote survivor preparation replaced actual permanent OLD lease"));
                }
            }
            _=>return Err(data_error("remote FIRST preparation requires exact original Registered or CLOSED survivor owner")),
        }
        let catalogue = service.range_writes.observe_native_joint_bridge_catalogue(
            &database,
            &committed,
            lease.image(),
            lease.try_frozen()?,
        )?;
        if retained.native.is_none() {
            if matches!(&admission, PreparationCall::Read(_)) {
                return Err(data_error(
                    "known preparation read cannot create missing original native attempt",
                ));
            }
            retained.native = Some(
                crate::db::NativeRemoteJointPreparationAttempt::retain_original(
                    &database,
                    &service.range_writes,
                    &committed,
                    lease.try_frozen()?,
                )?,
            );
        }
        #[cfg(test)]
        if retained.mode != 0 {
            let mode = std::mem::take(&mut retained.mode);
            retained.native.as_mut().unwrap().test_arm(mode)?;
        }
        request.check()?;
        process.require_service(&service)?;
        let prepared = match &admission {
            PreparationCall::Write(actual) => database.prepare_remote_native_joint_serving(
                &committed,
                &service.range_writes,
                &catalogue,
                lease.image(),
                lease.try_frozen()?,
                &request,
                retained.native.as_mut().unwrap(),
                actual,
            )?,
            PreparationCall::Read(actual) => database.read_known_remote_joint_preparation(
                &committed,
                &service.range_writes,
                &catalogue,
                lease.image(),
                lease.try_frozen()?,
                &request,
                retained.native.as_mut().unwrap(),
                actual,
            )?,
        };
        process.require_prepared(&prepared)?;
        let pin = database
            .range_mutation_gate()
            .try_source_capture_read_pin(committed.data().old_range().id, lease.try_frozen()?)?;
        let writer = domain.phase_owner();
        drop(database);
        let origin = preparation_origin.seal(&prepared, &committed);
        let evidence = bridge::ActualServiceNativeJointPreparedEvidence {
            origin,
            committed,
            prepared,
            catalogue,
            writer,
            writer_domain: domain,
            lease,
            pin: parking_lot::Mutex::new(Some(pin)),
        };
        // Retain actual native owner and its ONE pin before any fallible
        // outer physical/metadata/cancellation check, including wire loss.
        let custody = NativeJointPreparationCustody::retain(evidence, original.clone());
        retained.completed = Some(custody);
        retained
            .completed
            .as_ref()
            .unwrap()
            .require_initial_return_with_service(&service)?;
        process.require_service(&service)?;
        admission.require_memory_current()?;
        request.check()?;
        Ok(())
    }
}
