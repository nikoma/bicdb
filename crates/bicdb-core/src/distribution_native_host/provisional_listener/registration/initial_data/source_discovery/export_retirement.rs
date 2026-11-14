//! Original target -> original S2 donor -> actual Registered target. No Role
//! is borrowed by the donor and no remote fact becomes a local NEW quorum.
use super::*;
use crate::distribution_range_image::{
    NativeObserveRegisteredNewRequestV1, NativeSourceExportOperationV1, NativeSourceExportReplyV1,
    NativeSourceExportRequestV1,
};

impl NativeInitialDataAdmission {
    #[cfg(test)]
    pub(crate) fn test_take_source_export_reply_loss(&self) -> bool {
        self.image_sources
            .upgrade()
            .is_some_and(|registry| registry.test_take_export_reply_loss())
    }
    pub(crate) fn retire_remote_source_export(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeSourceExportRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeSourceExportReplyV1> {
        let token = request.cancellation(parent)?;
        // Translation is comparison DATA only. Describe cannot create a
        // missing S2 attempt; the original exact MF/session is checked below.
        let b = &request.binding;
        let discovery = NativeSourceDiscoveryRequestV1 {
            version: 1,
            cluster_id: b.cluster_id.clone(),
            bootstrap_id: b.bootstrap_id,
            range_id: b.range_id,
            range_epoch: b.range_epoch,
            relocation_id: b.relocation_id,
            plan_id: b.plan_id.clone(),
            handoff_session: b.handoff_session,
            nonce: b.nonce,
            scope_sha256: b.scope_sha256.clone(),
            source_id: b.source_id.clone(),
            source_birth: b.source_birth.clone(),
            target_id: b.target_id.clone(),
            target_birth: b.target_birth.clone(),
            target_attempt: b.target_attempt,
            operation: NativeSourceDiscoveryOperationV1::Describe,
            business: request.business,
            challenge: request.challenge,
            budget_ms: request.budget_ms,
        };
        let first = self.acquire_source_discovery(peer, &discovery, &token)?;
        let registry = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("original export registry ended"))?;
        registry.require_original_process()?;
        let attempt = registry.source_discovery_attempt(&discovery)?;
        let _claim = attempt.claim()?;
        let source = attempt
            .source()?
            .ok_or_else(|| invalid("retirement cannot create a missing S2 export"))?;
        let manifest = source.export_retirement_manifest()?;
        b.require_manifest(&manifest)?;
        b.require_plan(first.old.plan())?;
        first.require_current()?;
        if let Some(retired) = source.retain_export_request(request)? {
            // Fresh recipient admission still bounds a historical lost-reply
            // reconciliation; canceled exporter tokens are not renewed.
            first.require_current()?;
            token.check()?;
            let mut reply = NativeSourceExportReplyV1 {
                request: request.clone(),
                retired,
                source_metadata_term: first.own.term(),
                source_metadata_index: first.own.read_index(),
                source_metadata_context: first.own.context_id(),
                checksum_sha256: String::new(),
            };
            reply.checksum_sha256 = reply.checksum()?;
            return Ok(reply);
        }
        if request.operation == NativeSourceExportOperationV1::Describe {
            return Err(invalid(
                "export retirement has not completed; export remains retained",
            ));
        }
        let old = source.actual_remote_old_certificate()?;
        let transport = self
            .old_transport
            .upgrade()
            .ok_or_else(|| invalid("original export donor transport ended"))?;
        let remaining = token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let observe = NativeObserveRegisteredNewRequestV1 {
            version: 1,
            binding: b.clone(),
            business: Uuid::now_v7(),
            challenge: Uuid::now_v7(),
            budget_ms: remaining.as_millis().min(4000).max(1) as u64,
        };
        // No MStore, service, DB, source session or registry map latch spans
        // this callback. ALL genuine network work precedes final donor OWN.
        let observation = transport.observe_remote_registered_export_new(
            &old.plan(),
            &manifest,
            &observe,
            &first.own,
            first.business,
            &token,
        )?;
        drop(first);
        let current = self.acquire_source_discovery(peer, &discovery, &token)?;
        current.require_current()?;
        let retired = source.retire_export_from_remote_new(
            request,
            observation,
            &current.own,
            current.business,
            &token,
        )?;
        // An error after the transition preserves terminal custody for the
        // next bounded real request; it cannot recreate the source pin.
        current.require_current()?;
        registry.require_original_process()?;
        token.check()?;
        let mut reply = NativeSourceExportReplyV1 {
            request: request.clone(),
            retired,
            source_metadata_term: current.own.term(),
            source_metadata_index: current.own.read_index(),
            source_metadata_context: current.own.context_id(),
            checksum_sha256: String::new(),
        };
        reply.checksum_sha256 = reply.checksum()?;
        Ok(reply)
    }
}
#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_source_export_before_detach(
        &self,
        callback: Box<dyn FnOnce(Instant, &CancellationToken) + Send>,
    ) -> Result<()> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual export donor absent"))?
            .image_sources
            .as_ref()
            .ok_or_else(|| invalid("original export registry absent"))?
            .test_arm_export_detach_cut(callback)
    }
    pub(crate) fn test_drop_next_source_export_reply(&self) -> Result<()> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual export donor absent"))?
            .image_sources
            .as_ref()
            .ok_or_else(|| invalid("original export registry absent"))?
            .test_drop_next_export_reply();
        Ok(())
    }
}
